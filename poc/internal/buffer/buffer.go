// Package buffer holds a run's ingested records in memory, one Buffer per
// (side, bucket) plus one per side for empty keys (§8.1).
//
// A Buffer is a list of byte chunks holding records already in spill-file
// encoding:
//
//	hash u64 (little endian) | uvarint key len | uvarint value len | key | value
//
// A record never spans two chunks. Chunks start at 4 KiB and double up to
// 1 MiB, so a run with thousands of small buckets doesn't reserve a large
// chunk for each one before its records arrive. Each chunk reserves its
// capacity from the mem.Pool. A spill writes a buffer's chunks to a file
// unchanged.
package buffer

import (
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	"github.com/zeebo/xxh3"

	"poc/internal/mem"
	"poc/internal/spill"
)

// Side is the side of a run a record belongs to.
type Side uint8

const (
	Left Side = iota
	Right
)

func (s Side) String() string {
	if s == Left {
		return "left"
	}
	return "right"
}

// Seed is the XXH3 seed records are partitioned with. Runs don't survive a
// restart, so it may change between versions.
const Seed uint64 = 0x5265636f6e53640a

// Hash returns the hash a non-empty key is partitioned by.
func Hash(key []byte) uint64 {
	return xxh3.HashSeed(key, Seed)
}

const (
	minChunk = 4 << 10
	maxChunk = 1 << 20
)

// ErrFreed is returned by Append on a Buffer that was freed, because its
// run was deleted.
var ErrFreed = errors.New("buffer freed")

// EncodedLen returns the size of a record's encoding.
func EncodedLen(key, value []byte) int {
	return 8 + uvarintLen(len(key)) + uvarintLen(len(value)) + len(key) + len(value)
}

func uvarintLen(n int) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

// AppendRecord appends the encoding of a record to dst.
func AppendRecord(dst []byte, hash uint64, key, value []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, hash)
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	dst = append(dst, key...)
	return append(dst, value...)
}

// MaxHeader is the most bytes a record's hash and lengths take.
const MaxHeader = 8 + 2*binary.MaxVarintLen64

// RecordLen returns the length of the record encoded at the start of b, or
// false if b doesn't hold its hash and lengths. With MaxHeader bytes or
// more, false means they are corrupt.
func RecordLen(b []byte) (int, bool) {
	if len(b) < 8 {
		return 0, false
	}
	kl, n := binary.Uvarint(b[8:])
	if n <= 0 {
		return 0, false
	}
	vl, m := binary.Uvarint(b[8+n:])
	if m <= 0 {
		return 0, false
	}
	return 8 + n + m + int(kl) + int(vl), true
}

// Decode decodes the record at b[off:] and returns the offset of the next
// one. The key and value alias b, and are capped so that appending to them
// copies. b must hold complete records, as chunks do.
func Decode(b []byte, off int) (hash uint64, key, value []byte, next int) {
	hash = binary.LittleEndian.Uint64(b[off:])
	off += 8
	kl, n := binary.Uvarint(b[off:])
	off += n
	vl, n := binary.Uvarint(b[off:])
	off += n
	k := off + int(kl)
	v := k + int(vl)
	return hash, b[off:k:k], b[k:v:v], v
}

// Buffer is the records of one (side, bucket). It is safe for concurrent
// use.
//
// Its records are in its spill files, in the order of the manifest, then in
// its chunks: together, in ingest order. A spill writes every chunk to one
// new file. A pinned buffer, which a join is reading, isn't spilled.
type Buffer struct {
	mu       sync.Mutex
	chunks   [][]byte
	next     int   // capacity of the next chunk
	reserved int64 // capacity of chunks, reserved from the pool
	records  int64 // in chunks and files
	bytes    int64 // encoded bytes, in chunks and files
	freed    bool

	memRecords int64 // in chunks
	files      []spill.File
	seq        int           // of the next spill file
	pins       int           // of joins reading the buffer
	spilling   chan struct{} // closed once the spill in progress ends
}

// maxChunkOf returns the largest chunk a buffer reserves from pool, short of
// a record that needs more: 1 MiB, or a sixteenth of a small budget, so that
// a chunk always fits in the room spilling makes.
func maxChunkOf(pool *mem.Pool) int {
	return int(max(minChunk, min(maxChunk, pool.Limit()/16)))
}

// Append encodes a record into the buffer, reserving a new chunk from pool
// when the last one is full. It fails with the error of pool.Reserve when
// the pool can't cover the chunk, and with ErrFreed once the buffer is
// freed.
func (b *Buffer) Append(pool *mem.Pool, hash uint64, key, value []byte) error {
	n := EncodedLen(key, value)
	b.mu.Lock()
	defer b.mu.Unlock()
	var spare int64 // a chunk reserved from the pool but not yet added
	for {
		switch {
		case b.freed:
			if spare > 0 {
				pool.Release(spare)
			}
			return ErrFreed
		case len(b.chunks) > 0 && cap(b.chunks[len(b.chunks)-1])-len(b.chunks[len(b.chunks)-1]) >= n:
			last := &b.chunks[len(b.chunks)-1]
			*last = AppendRecord(*last, hash, key, value)
			b.records++
			b.memRecords++
			b.bytes += int64(n)
			if spare > 0 {
				pool.Release(spare)
			}
			return nil
		case spare > 0:
			b.chunks = append(b.chunks, make([]byte, 0, spare))
			b.reserved += spare
			b.next = min(max(b.next, minChunk)*2, maxChunkOf(pool))
			spare = 0
		default:
			size := int64(max(min(max(b.next, minChunk), maxChunkOf(pool)), n))
			// The pool isn't reserved under the buffer's lock, so a
			// reservation never holds up the buffer's other writers, and it
			// may spill this buffer. Another writer may add a chunk
			// meanwhile, which the loop then fills first.
			b.mu.Unlock()
			err := pool.Reserve(size)
			b.mu.Lock()
			if err != nil {
				return err
			}
			spare = size
		}
	}
}

// AddFile adds a spill file to the manifest of a buffer that has no
// chunks, after its other files. It is how a join builds the buffers of a
// sub-partitioned bucket.
func (b *Buffer) AddFile(f spill.File) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.chunks) > 0 {
		panic("buffer: AddFile to a buffer with chunks")
	}
	b.files = append(b.files, f)
	b.records += f.Records
	b.bytes += f.Bytes
	b.seq++
}

// Chunks returns the buffer's in-memory chunks. They must not be modified,
// and stay valid after the buffer is freed or spilled.
func (b *Buffer) Chunks() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.chunks...)
}

// Records returns the number of records in the buffer, in memory and in
// files.
func (b *Buffer) Records() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.records
}

// Bytes returns the size of the buffer's encoded records, in memory and in
// files.
func (b *Buffer) Bytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes
}

// Files returns the buffer's spill files.
func (b *Buffer) Files() []spill.File {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.files)
}

// lockIdle locks b.mu once no spill of the buffer is in progress.
func (b *Buffer) lockIdle() {
	for {
		b.mu.Lock()
		done := b.spilling
		if done == nil {
			return
		}
		b.mu.Unlock()
		<-done
	}
}

// SpillSize returns the memory a spill would release now: 0 while the
// buffer is pinned, freed or being spilled.
func (b *Buffer) SpillSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pins > 0 || b.freed || b.spilling != nil {
		return 0
	}
	return b.reserved
}

// Spill writes the buffer's chunks with write, as the file numbered seq,
// and releases their memory to pool. While write runs the buffer takes
// new records into new chunks. If write fails, the chunks stay in memory.
// A pinned, freed or empty buffer isn't spilled.
func (b *Buffer) Spill(pool *mem.Pool, write func(seq int, records int64, chunks [][]byte) (spill.File, error)) error {
	b.lockIdle()
	if b.pins > 0 || b.freed || len(b.chunks) == 0 {
		b.mu.Unlock()
		return nil
	}
	chunks, records, seq := b.chunks, b.memRecords, b.seq
	b.chunks, b.memRecords, b.next = nil, 0, 0
	done := make(chan struct{})
	b.spilling = done
	b.mu.Unlock()

	f, err := write(seq, records, chunks)

	b.mu.Lock()
	b.spilling = nil
	close(done)
	if err != nil {
		b.chunks = append(chunks, b.chunks...)
		b.memRecords += records
		b.mu.Unlock()
		return err
	}
	var released int64
	for _, c := range chunks {
		released += int64(cap(c))
	}
	b.files = append(b.files, f)
	b.seq++
	b.reserved -= released
	b.mu.Unlock()
	pool.Release(released)
	return nil
}

// Pin keeps the buffer in memory until Unpin, and returns its records:
// its files, then its chunks, in ingest order. It waits for a spill in
// progress to end.
func (b *Buffer) Pin() (files []spill.File, chunks [][]byte) {
	b.lockIdle()
	defer b.mu.Unlock()
	b.pins++
	return slices.Clone(b.files), slices.Clone(b.chunks)
}

// Unpin undoes a Pin.
func (b *Buffer) Unpin() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pins--
}

// Free drops the buffer's chunks and releases their reservation. Later
// Appends fail with ErrFreed. It waits for a spill in progress to end.
func (b *Buffer) Free(pool *mem.Pool) {
	b.lockIdle()
	defer b.mu.Unlock()
	if b.freed {
		return
	}
	pool.Release(b.reserved)
	b.chunks, b.reserved, b.freed = nil, 0, true
}

// Partitions is a run's buffers: N buckets per side for non-empty keys,
// and one empty-key partition per side.
type Partitions struct {
	buckets [2][]Buffer
	empty   [2]Buffer
}

// NewPartitions returns empty Partitions with n buckets per side.
func NewPartitions(n uint32) *Partitions {
	p := &Partitions{}
	for s := range p.buckets {
		p.buckets[s] = make([]Buffer, n)
	}
	return p
}

// Buckets returns the number of buckets per side.
func (p *Partitions) Buckets() uint32 { return uint32(len(p.buckets[Left])) }

// Bucket returns the buffer of side's bucket i.
func (p *Partitions) Bucket(side Side, i uint32) *Buffer { return &p.buckets[side][i] }

// Empty returns side's empty-key partition.
func (p *Partitions) Empty(side Side) *Buffer { return &p.empty[side] }

// Append adds a record to side: an empty key to the empty-key partition,
// unhashed, and any other key to bucket Hash(key) mod N.
func (p *Partitions) Append(pool *mem.Pool, side Side, key, value []byte) error {
	if len(key) == 0 {
		return p.empty[side].Append(pool, 0, key, value)
	}
	h := Hash(key)
	return p.buckets[side][h%uint64(len(p.buckets[side]))].Append(pool, h, key, value)
}

// Free frees every buffer.
func (p *Partitions) Free(pool *mem.Pool) {
	for s := range p.buckets {
		for i := range p.buckets[s] {
			p.buckets[s][i].Free(pool)
		}
		p.empty[s].Free(pool)
	}
}
