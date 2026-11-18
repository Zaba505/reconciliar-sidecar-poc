package buffer

import (
	"bytes"
	"cmp"
	"slices"
	"sort"
	"unsafe"

	"poc/internal/merge"
	"poc/internal/record"
)

// entry indexes one record of a block. Sorting a block sorts its entries,
// never its records.
type entry struct {
	hash   uint64
	off    uint32 // of the record in the block
	ticket uint32 // of the Append section that wrote it
}

const entrySize = int(unsafe.Sizeof(entry{}))

// blockOverhead is what a block takes besides its bytes: its struct, its
// slot in the buffer's lists, and its source in a spill's or a join's
// merge.
const blockOverhead = 256

// block is one allocation: records from the front, entries from the back.
type block struct {
	buf    []byte
	n      int // bytes of records
	ne     int // entries
	sorted bool
}

// reserved is what the block takes from the pool.
func (b *block) reserved() int64 { return int64(len(b.buf)) + blockOverhead }

// ents returns the block's entries.
func (b *block) ents() []entry {
	if b.ne == 0 {
		return nil
	}
	return unsafe.Slice((*entry)(unsafe.Pointer(&b.buf[len(b.buf)-b.ne*entrySize])), b.ne)
}

// fits reports whether a record of n bytes fits in an empty block of size
// bytes.
func fits(n, size int) bool { return n+entrySize <= size }

// append adds a record and its entry, or returns false if they don't fit.
func (b *block) append(side record.Side, key, value []byte, ticket uint32) bool {
	n := record.Len(len(key), len(value))
	top := len(b.buf) - b.ne*entrySize
	if top-b.n < n+entrySize {
		return false
	}
	h := record.Hash(key)
	record.Put(b.buf[b.n:b.n+n], h, side, key, value)
	*(*entry)(unsafe.Pointer(&b.buf[top-entrySize])) = entry{hash: h, off: uint32(b.n), ticket: ticket}
	b.n += n
	b.ne++
	return true
}

// sort sorts the block's entries by (hash, key, side, ticket, offset): the
// order of a run, with the records of one key and side in ingest order.
func (b *block) sort() {
	if b.sorted {
		return
	}
	buf := b.buf
	slices.SortFunc(b.ents(), func(x, y entry) int {
		if x.hash != y.hash {
			return cmp.Compare(x.hash, y.hash)
		}
		xs, xk := record.SideKey(buf[x.off:])
		ys, yk := record.SideKey(buf[y.off:])
		if c := bytes.Compare(xk, yk); c != 0 {
			return c
		}
		return cmp.Or(cmp.Compare(xs, ys), cmp.Compare(x.ticket, y.ticket), cmp.Compare(x.off, y.off))
	})
	b.sorted = true
}

// fenceRange returns the entries of fences lo to hi-1 of a sorted block.
func (b *block) fenceRange(lo, hi int) []entry {
	ents := b.ents()
	i := sort.Search(len(ents), func(k int) bool { return record.Fence(ents[k].hash) >= lo })
	j := i + sort.Search(len(ents)-i, func(k int) bool { return record.Fence(ents[i+k].hash) >= hi })
	return ents[i:j]
}

// source is a range of a sorted block's entries, as a merge.Source.
type source struct {
	buf  []byte
	ents []entry
	i    int
	base uint64 // added to the tickets to order them among other sources
}

var _ merge.Source = (*source)(nil)

func (s *source) Peek() (uint64, bool) {
	if s.i >= len(s.ents) {
		return 0, false
	}
	return s.ents[s.i].hash, true
}

func (s *source) Head() []byte {
	if s.i >= len(s.ents) {
		return nil
	}
	off := int(s.ents[s.i].off)
	n, _ := record.Size(s.buf[off:])
	return s.buf[off : off+n : off+n]
}

func (s *source) ReadValue(dst []byte) error {
	_, _, _, v := record.Decode(s.Head())
	copy(dst, v)
	return nil
}

func (s *source) Next() error            { s.i++; return nil }
func (s *source) Ord() uint64            { return s.base | uint64(s.ents[s.i].ticket) }
func (s *source) Pos() int64             { return int64(s.i) }
func (s *source) Rewind(pos int64) error { s.i = int(pos); return nil }
