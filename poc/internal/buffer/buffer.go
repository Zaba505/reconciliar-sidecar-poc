// Package buffer holds a run's records: in memory, in blocks, until the
// pool needs the room, and then in the sorted run files they were spilled
// to (§8.1, §8.3).
//
// Records arrive in any order, on any number of Ingest streams, and are
// copied once into fixed-size blocks: the records grow from the front of a
// block, a 16-byte entry {hash, offset, ticket} per record from its back.
// The goroutine that fills a block sorts its entries, by (hash, key, side,
// ticket), and never the records. Nothing is partitioned, indexed or
// looked up by key.
//
// A spill takes every block of the run, merges their sorted entries, and
// writes the records in that order to ONE new run file: so a run is a few
// large sorted files, one per spill, whatever the data's size or keys.
//
// The ticket of a record is that of the Append section that wrote it: the
// records a stream appended before another section began have a smaller
// ticket. Every record in a run file was appended before every record in
// a later one, so records of one key and side keep their ingest order in a
// merge that orders files by their place in the run, and blocks after
// them.
package buffer

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/record"
	"poc/internal/spill"
)

// ErrFreed is returned by Append once the buffer is freed, because its run
// was deleted.
var ErrFreed = errors.New("buffer freed")

// ErrSealed is returned by Append once the buffer is sealed.
var ErrSealed = errors.New("buffer sealed")

// errTickets is returned by Append once a run has taken 2^32-1 sections.
var errTickets = errors.New("the run took too many ingest messages")

// BlockSize returns the size of the blocks of a pool of limit bytes: 1 MiB,
// or a thirty-second of a small budget, so that spilling always makes room
// for a block.
func BlockSize(limit int64) int {
	return int(max(4<<10, min(1<<20, limit/32))) &^ 15
}

// Buffer is a run's records. It is safe for concurrent use.
type Buffer struct {
	pool      *mem.Pool
	blockSize int

	// mu is held for reading by an Append section while it fills a block,
	// and for writing to take the blocks, so that a spill sees no record
	// half written, and every record appended before it is in its file or
	// an earlier one.
	mu     sync.RWMutex
	lmu    sync.Mutex // guards open and full
	open   []*block   // partly filled, each taken by one section at a time
	full   []*block   // sorted
	ticket atomic.Uint32

	// spillMu is held while a spill writes, so that Pin and Free wait for
	// its file.
	spillMu sync.Mutex

	files  []*spill.File // guarded by mu
	meta   int64         // reserved for files, guarded by mu
	pins   int           // of Joins reading the blocks, guarded by mu
	sealed bool          // guarded by mu
	freed  bool          // guarded by mu
}

// New returns an empty Buffer whose blocks reserve their memory from pool.
func New(pool *mem.Pool) *Buffer {
	b := &Buffer{pool: pool, blockSize: BlockSize(pool.Limit())}
	if int64(b.blockSize)+blockOverhead < spill.FileBytes {
		panic("buffer: a block holds less than a File")
	}
	return b
}

// Append copies n records of side into blocks: rec(i) returns the key and
// value of the i-th, which aren't kept. It reserves new blocks from the
// pool, which may spill this buffer or another, and fails with the error
// of the reservation. It returns how many records it copied: those it
// copied before it failed are kept.
func (b *Buffer) Append(side record.Side, n int, rec func(i int) (key, value []byte)) (int, error) {
	var spare *block // reserved, not in a list
	defer func() {
		if spare != nil {
			b.drop(spare)
		}
	}()
	for i := 0; i < n; {
		b.mu.RLock()
		switch {
		case b.freed:
			b.mu.RUnlock()
			return i, ErrFreed
		case b.sealed:
			b.mu.RUnlock()
			return i, ErrSealed
		}
		blk := spare
		spare = nil
		if blk == nil {
			blk = b.popOpen()
		}
		if blk != nil {
			// The ticket orders this section's records after those of every
			// section that ended before it began.
			ticket := b.ticket.Add(1)
			if ticket == 0 {
				b.ticket.Store(^uint32(0))
				b.pushOpen(blk)
				b.mu.RUnlock()
				return i, errTickets
			}
			for i < n {
				k, v := rec(i)
				if !blk.append(side, k, v, ticket) {
					break
				}
				i++
			}
			switch {
			case i == n:
				b.pushOpen(blk)
			case blk.ne > 0:
				blk.sort()
				b.lmu.Lock()
				b.full = append(b.full, blk)
				b.lmu.Unlock()
			default:
				// An empty block too small for the record: only a block of
				// the record's size holds it.
				spare = blk
			}
		}
		b.mu.RUnlock()
		if i == n {
			return n, nil
		}

		k, v := rec(i)
		need := record.Len(len(k), len(v))
		if spare != nil {
			if fits(need, len(spare.buf)) {
				continue
			}
			b.drop(spare)
			spare = nil
		}
		// Reserving may spill, which takes mu for writing: never reserve
		// inside a section.
		var err error
		if spare, err = b.newBlock(max(b.blockSize, (need+entrySize+15)&^15)); err != nil {
			return i, err
		}
	}
	return n, nil
}

func (b *Buffer) popOpen() *block {
	b.lmu.Lock()
	defer b.lmu.Unlock()
	if n := len(b.open); n > 0 {
		blk := b.open[n-1]
		b.open = b.open[:n-1]
		return blk
	}
	return nil
}

func (b *Buffer) pushOpen(blk *block) {
	b.lmu.Lock()
	b.open = append(b.open, blk)
	b.lmu.Unlock()
}

// newBlock returns an empty block of size bytes, reserved: an idle one of
// the pool's, or a new one.
func (b *Buffer) newBlock(size int) (*block, error) {
	if size == b.blockSize {
		if buf, reserved := b.pool.TakeIdle(size); buf != nil {
			if reserved != int64(size)+blockOverhead {
				// Not a block of this package's: keep the reservation right.
				b.pool.Release(reserved)
				if err := b.pool.Reserve(int64(size) + blockOverhead); err != nil {
					return nil, err
				}
			}
			return &block{buf: buf}, nil
		}
	}
	if err := b.pool.Reserve(int64(size) + blockOverhead); err != nil {
		return nil, err
	}
	return &block{buf: make([]byte, size)}, nil
}

// drop gives back a block nothing refers to any more: to the pool's idle
// buffers if it is of the usual size, else to the garbage collector.
func (b *Buffer) drop(blk *block) {
	if len(blk.buf) == b.blockSize {
		b.pool.PutIdle(blk.buf, blk.reserved())
	} else {
		b.pool.Release(blk.reserved())
	}
	blk.buf = nil
}

// SpillSize returns the memory a spill would release now: the blocks',
// unless a Join reads them or the buffer is freed.
func (b *Buffer) SpillSize() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.pins > 0 || b.freed {
		return 0
	}
	b.lmu.Lock()
	defer b.lmu.Unlock()
	var n int64
	for _, blk := range b.open {
		n += blk.reserved()
	}
	for _, blk := range b.full {
		n += blk.reserved()
	}
	return n
}

// WriteFunc writes the records of m, bytes bytes of them, as a new run
// file, and returns it.
type WriteFunc func(bytes int64, m *merge.Merger) (*spill.File, error)

// Spill merges every block into one run file, which write writes, and
// gives back the blocks. While write runs, Appends go on into new blocks,
// as far as the pool lets them. If write fails, the records stay in
// memory. A pinned, freed or empty buffer isn't
// spilled.
//
// The new file's memory is taken from the blocks', since Spill runs inside
// the pool's Reserve, where it can't reserve.
func (b *Buffer) Spill(write WriteFunc) error {
	b.spillMu.Lock()
	defer b.spillMu.Unlock()

	b.mu.Lock()
	if b.pins > 0 || b.freed {
		b.mu.Unlock()
		return nil
	}
	b.lmu.Lock()
	blocks := append(b.full, b.open...)
	b.full, b.open = nil, nil
	b.lmu.Unlock()
	b.mu.Unlock()
	if len(blocks) == 0 {
		return nil
	}

	var bytes int64
	srcs := make([]merge.Source, len(blocks))
	for i, blk := range blocks {
		blk.sort()
		bytes += int64(blk.n)
		srcs[i] = &source{buf: blk.buf, ents: blk.ents()}
	}
	m := merge.New(srcs)
	m.Init()
	f, err := write(bytes, m)
	if err != nil {
		b.mu.Lock()
		b.lmu.Lock()
		b.full = append(blocks, b.full...)
		b.lmu.Unlock()
		b.mu.Unlock()
		return err
	}

	b.mu.Lock()
	b.files = append(b.files, f)
	b.meta += spill.FileBytes
	b.mu.Unlock()
	first := blocks[0]
	b.pool.Release(first.reserved() - spill.FileBytes)
	first.buf = nil
	for _, blk := range blocks[1:] {
		b.drop(blk)
	}
	return nil
}

// Seal ends the buffer's Appends, once a spill in progress ends. The
// blocks still in memory are sorted, and stay there; it reports whether the buffer has spilled, in which case
// the caller spills them too, so that a Join reads only files.
func (b *Buffer) Seal() (spilled bool, err error) {
	b.spillMu.Lock()
	defer b.spillMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.freed {
		return false, ErrFreed
	}
	b.sealed = true
	b.lmu.Lock()
	defer b.lmu.Unlock()
	for _, blk := range b.open {
		if blk.ne > 0 {
			blk.sort()
			b.full = append(b.full, blk)
		} else {
			b.drop(blk)
		}
	}
	b.open = nil
	return len(b.files) > 0, nil
}

// Unseal undoes a Seal whose run failed to seal, so that Appends go on.
func (b *Buffer) Unseal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed = false
}

// Files returns the buffer's run files, in the order they were written.
func (b *Buffer) Files() []*spill.File {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return slices.Clone(b.files)
}

// Replace replaces files lo to hi-1 with f, which holds their records,
// once a merge wrote it, and releases the memory the files took.
func (b *Buffer) Replace(lo, hi int, f *spill.File) {
	b.spillMu.Lock()
	defer b.spillMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files = slices.Replace(b.files, lo, hi, f)
	freed := int64(hi-lo-1) * spill.FileBytes
	b.meta -= freed
	b.pool.Release(freed)
}

// Snapshot is a pinned buffer's records: its run files, and its blocks,
// whose records were all appended after the files'.
type Snapshot struct {
	Files  []*spill.File
	blocks []*block
}

// Pin keeps the buffer's blocks in memory until Unpin, and returns them
// with the files. It waits for a spill in progress to end. Only a sealed
// buffer is pinned, so its blocks are all sorted.
func (b *Buffer) Pin() *Snapshot {
	b.spillMu.Lock()
	defer b.spillMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pins++
	b.lmu.Lock()
	defer b.lmu.Unlock()
	if len(b.open) > 0 {
		panic("buffer: Pin of a buffer that isn't sealed")
	}
	return &Snapshot{Files: slices.Clone(b.files), blocks: slices.Clone(b.full)}
}

// Unpin undoes a Pin.
func (b *Buffer) Unpin() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pins--
}

// Free drops the buffer's blocks and releases the memory it holds. Later
// Appends fail with ErrFreed. It waits for a spill in progress to end. The
// files are the caller's to remove.
func (b *Buffer) Free() {
	b.spillMu.Lock()
	defer b.spillMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.freed {
		return
	}
	b.freed = true
	b.lmu.Lock()
	for _, blk := range b.open {
		b.drop(blk)
	}
	for _, blk := range b.full {
		b.drop(blk)
	}
	b.open, b.full = nil, nil
	b.lmu.Unlock()
	b.pool.Release(b.meta)
	b.meta, b.files = 0, nil
}

// Blocks returns the number of blocks in the snapshot.
func (s *Snapshot) Blocks() int { return len(s.blocks) }

// AddFenceBytes adds the bytes of the records of each fence in the
// snapshot's blocks to sizes.
func (s *Snapshot) AddFenceBytes(sizes *[record.Fences]int64) {
	for _, blk := range s.blocks {
		for _, e := range blk.ents() {
			n, _ := record.Size(blk.buf[e.off:])
			sizes[record.Fence(e.hash)] += int64(n)
		}
	}
}

// Source returns a merge.Source of the records of fences lo to hi-1 in
// block i, whose ords are base plus their tickets. base must be above the
// ords of every file's records.
func (s *Snapshot) Source(i, lo, hi int, base uint64) merge.Source {
	blk := s.blocks[i]
	return &source{buf: blk.buf, ents: blk.fenceRange(lo, hi), base: base}
}
