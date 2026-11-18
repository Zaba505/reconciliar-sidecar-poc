package buffer

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/record"
	"poc/internal/spill"
)

type rec struct {
	side       record.Side
	key, value []byte
}

func appendAll(t testing.TB, b *Buffer, side record.Side, recs ...rec) error {
	t.Helper()
	n, err := b.Append(side, len(recs), func(i int) ([]byte, []byte) { return recs[i].key, recs[i].value })
	if err == nil && n != len(recs) {
		t.Fatalf("Append copied %d of %d records without an error", n, len(recs))
	}
	return err
}

// memFile is a run file kept in memory.
type memFile struct {
	recs [][]byte
}

// spiller writes spills to memory, and fails while err is set.
type spiller struct {
	mu    sync.Mutex
	files map[*spill.File]*memFile
	err   error
}

func (s *spiller) write(size int64, m *merge.Merger) (*spill.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	f, mf := &spill.File{Bytes: size}, &memFile{}
	var n int64
	for src := m.Top(); src != nil; src = m.Top() {
		mf.recs = append(mf.recs, bytes.Clone(src.Head()))
		n += int64(len(src.Head()))
		if err := m.Pop(); err != nil {
			return nil, err
		}
	}
	if n != size {
		return nil, fmt.Errorf("the merge has %d bytes, the spill said %d", n, size)
	}
	f.Records = int64(len(mf.recs))
	if s.files == nil {
		s.files = map[*spill.File]*memFile{}
	}
	s.files[f] = mf
	return f, nil
}

// contents returns what b holds, files first, then blocks, each in its
// order, as (side, key, value) records.
func (s *spiller) contents(b *Buffer) [][]rec {
	snap := b.Pin()
	defer b.Unpin()
	var out [][]rec
	for _, f := range snap.Files {
		var rs []rec
		for _, r := range s.files[f].recs {
			_, side, k, v := record.Decode(r)
			rs = append(rs, rec{side, k, v})
		}
		out = append(out, rs)
	}
	for i := range snap.Blocks() {
		var rs []rec
		src := snap.Source(i, 0, record.Fences, 0)
		for _, ok := src.Peek(); ok; _, ok = src.Peek() {
			_, side, k, v := record.Decode(src.Head())
			rs = append(rs, rec{side, bytes.Clone(k), bytes.Clone(v)})
			_ = src.Next()
		}
		out = append(out, rs)
	}
	return out
}

func sortedRecs(rs []rec) bool {
	return slices.IsSortedFunc(rs, func(a, b rec) int {
		return record.Compare(record.Append(nil, record.Hash(a.key), a.side, a.key, nil), record.Append(nil, record.Hash(b.key), b.side, b.key, nil))
	})
}

func TestBlockSize(t *testing.T) {
	for _, tt := range []struct{ limit, want int64 }{{1 << 30, 1 << 20}, {32 << 20, 1 << 20}, {1 << 20, 32 << 10}, {64 << 10, 4 << 10}, {1, 4 << 10}} {
		if got := BlockSize(tt.limit); int64(got) != tt.want {
			t.Errorf("BlockSize(%d) = %d, want %d", tt.limit, got, tt.want)
		}
	}
}

func TestBuffer_spills_every_block_to_one_sorted_file(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	var want []rec
	for i := range 2000 {
		r := rec{record.Side(i % 2), fmt.Appendf(nil, "key-%d", i%300), fmt.Appendf(nil, "value-%d", i)}
		if i%97 == 0 {
			r.key = nil
		}
		if err := appendAll(t, b, r.side, r); err != nil {
			t.Fatal(err)
		}
		want = append(want, r)
	}
	inMemory := pool.Used()
	if inMemory == 0 || b.SpillSize() != inMemory {
		t.Fatalf("SpillSize %d, pool %d: want the blocks' reservation", b.SpillSize(), inMemory)
	}
	s := &spiller{}
	if err := b.Spill(s.write); err != nil {
		t.Fatal(err)
	}
	if b.SpillSize() != 0 || pool.Used()-pool.Idle() != spill.FileBytes {
		t.Errorf("after the spill: SpillSize %d, %d in use; want only the file's %d", b.SpillSize(), pool.Used()-pool.Idle(), spill.FileBytes)
	}
	if _, err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	got := s.contents(b)
	if len(got) != 1 || len(got[0]) != len(want) || !sortedRecs(got[0]) {
		t.Fatalf("%d files and blocks, want one sorted file of %d records", len(got), len(want))
	}
	// Records of one key and side keep their order.
	groups := map[string][]string{}
	for _, r := range want {
		k := fmt.Sprint(r.side, string(r.key))
		groups[k] = append(groups[k], string(r.value))
	}
	for _, r := range got[0] {
		k := fmt.Sprint(r.side, string(r.key))
		if groups[k][0] != string(r.value) {
			t.Fatalf("key %q of %v: %q came before %q", r.key, r.side, r.value, groups[k][0])
		}
		groups[k] = groups[k][1:]
	}
	b.Free()
	if pool.Used() != pool.Idle() {
		t.Errorf("after Free: %d in use", pool.Used()-pool.Idle())
	}
}

func TestBuffer_a_failed_spill_keeps_the_records_in_memory(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}, rec{key: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	failing := errors.New("disk on fire")
	s := &spiller{err: failing}
	if err := b.Spill(s.write); !errors.Is(err, failing) {
		t.Fatalf("Spill: %v", err)
	}
	if err := appendAll(t, b, record.Right, rec{key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	var n int
	for _, blk := range s.contents(b) {
		n += len(blk)
	}
	if n != 3 || len(b.Files()) != 0 {
		t.Errorf("%d records, %d files after the failed spill; want 3 records in memory", n, len(b.Files()))
	}
}

func TestBuffer_a_record_larger_than_a_block_gets_its_own(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	big := rec{key: []byte("big"), value: make([]byte, 3*b.blockSize)}
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}, big, rec{key: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if pool.Used() < int64(3*b.blockSize) {
		t.Errorf("pool has %d reserved for a record of %d", pool.Used(), 3*b.blockSize)
	}
	s := &spiller{}
	if err := b.Spill(s.write); err != nil {
		t.Fatal(err)
	}
	// Only blocks of the usual size go idle.
	if pool.Idle() > int64(b.blockSize)+blockOverhead {
		t.Errorf("%d bytes idle after spilling one usual block and one large one", pool.Idle())
	}
}

func TestBuffer_fails_when_the_pool_is_exhausted(t *testing.T) {
	pool := mem.NewPool(4 << 10)
	b := New(pool)
	n, err := b.Append(record.Left, 2, func(i int) ([]byte, []byte) { return []byte("k"), nil })
	if !errors.Is(err, mem.ErrExhausted) || n != 0 {
		t.Errorf("Append under a budget smaller than a block: %d, %v; want 0 and ErrExhausted", n, err)
	}
}

func TestBuffer_Append_after_Seal_or_Free_fails(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	if _, err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}); !errors.Is(err, ErrSealed) {
		t.Errorf("Append after Seal: %v", err)
	}
	b.Unseal()
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}); err != nil {
		t.Errorf("Append after Unseal: %v", err)
	}
	b.Free()
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}); !errors.Is(err, ErrFreed) {
		t.Errorf("Append after Free: %v", err)
	}
	if _, err := b.Seal(); !errors.Is(err, ErrFreed) {
		t.Errorf("Seal after Free: %v", err)
	}
}

func TestBuffer_a_pinned_buffer_isnt_spilled(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	if err := appendAll(t, b, record.Left, rec{key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	snap := b.Pin()
	if b.SpillSize() != 0 {
		t.Error("a pinned buffer has a SpillSize")
	}
	s := &spiller{}
	if err := b.Spill(s.write); err != nil || len(s.files) != 0 {
		t.Errorf("Spill of a pinned buffer: %v, %d files", err, len(s.files))
	}
	if snap.Blocks() != 1 {
		t.Errorf("%d blocks pinned, want 1", snap.Blocks())
	}
	b.Unpin()
	if err := b.Spill(s.write); err != nil || len(s.files) != 1 {
		t.Errorf("Spill after Unpin: %v, %d files", err, len(s.files))
	}
}

func TestBuffer_reuses_idle_blocks(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	a, b := New(pool), New(pool)
	block := int64(a.blockSize) + blockOverhead
	for i := 0; a.SpillSize() < 3*block; i++ {
		if err := appendAll(t, a, record.Left, rec{key: fmt.Appendf(nil, "%d", i), value: make([]byte, 100)}); err != nil {
			t.Fatal(err)
		}
	}
	blocks := a.SpillSize() / block
	s := &spiller{}
	if err := a.Spill(s.write); err != nil {
		t.Fatal(err)
	}
	// The first block pays for the file; the others go idle.
	if want := (blocks - 1) * block; pool.Idle() != want {
		t.Fatalf("%d bytes idle after spilling %d blocks, want %d", pool.Idle(), blocks, want)
	}
	before, idle := pool.Used(), pool.Idle()
	if err := appendAll(t, b, record.Left, rec{key: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if pool.Used() != before || pool.Idle() != idle-block {
		t.Errorf("another buffer's Append took %d more and left %d idle; want an idle block taken", pool.Used()-before, pool.Idle())
	}
}

// TestBuffer_property_spills_keep_the_order_of_ingest appends records on
// several goroutines while the buffer spills, and checks that a merge of
// every file and block, ordered by file then ticket, gives each key and
// side's records in the order each goroutine appended them.
func TestBuffer_property_spills_keep_the_order_of_ingest(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		pool := mem.NewPool(rapid.Int64Range(64<<10, 256<<10).Draw(rt, "budget"))
		b := New(pool)
		s := &spiller{}
		pool.SetSource(func() []mem.Spillable { return []mem.Spillable{spillable{b, s}} })
		writers := rapid.IntRange(1, 4).Draw(rt, "writers")
		per := rapid.IntRange(0, 400).Draw(rt, "records per writer")
		keys := rapid.IntRange(1, 30).Draw(rt, "keys")
		var wg sync.WaitGroup
		for w := range writers {
			wg.Go(func() {
				for i := 0; i < per; {
					n := min(per-i, 1+(i*7+w)%13)
					batch := make([]rec, n)
					for j := range batch {
						batch[j] = rec{record.Side((i + j) % 2), fmt.Appendf(nil, "k%d", (i+j)%keys), fmt.Appendf(nil, "%d/%06d", w, i+j)}
					}
					side := batch[0].side
					for j := range batch {
						batch[j].side = side
					}
					if err := appendAll(t, b, side, batch...); err != nil {
						rt.Error(err)
						return
					}
					i += n
				}
			})
		}
		wg.Wait()
		if _, err := b.Seal(); err != nil {
			rt.Fatal(err)
		}
		snap := b.Pin()
		defer b.Unpin()
		var srcs []merge.Source
		for i, f := range snap.Files {
			srcs = append(srcs, &sliceSrc{recs: s.files[f].recs, ord: uint64(i)})
		}
		for i := range snap.Blocks() {
			srcs = append(srcs, snap.Source(i, 0, record.Fences, uint64(len(snap.Files))<<32))
		}
		m := merge.New(srcs)
		m.Init()
		last := map[string]string{} // side, key and writer: the last value
		var prev []byte
		total := 0
		for src := m.Top(); src != nil; src = m.Top() {
			head := bytes.Clone(src.Head())
			if prev != nil && record.Compare(prev, head) > 0 {
				rt.Fatal("the merge isn't sorted")
			}
			prev = head
			_, side, k, v := record.Decode(head)
			id := fmt.Sprint(side, string(k), string(v[:1]))
			if string(v) <= last[id] {
				rt.Fatalf("%s: %q after %q", id, v, last[id])
			}
			last[id] = string(v)
			total++
			_ = m.Pop()
		}
		if total != writers*per {
			rt.Fatalf("merged %d records, appended %d", total, writers*per)
		}
		if pool.Used() > pool.Limit() {
			rt.Fatalf("pool %d above its limit", pool.Used())
		}
	})
}

// spillable spills a buffer through a spiller.
type spillable struct {
	b *Buffer
	s *spiller
}

func (s spillable) SpillSize() int64 { return s.b.SpillSize() }
func (s spillable) Spill() error     { return s.b.Spill(s.s.write) }

// sliceSrc is a merge.Source of whole records in memory.
type sliceSrc struct {
	recs [][]byte
	ord  uint64
	i    int
}

func (s *sliceSrc) Peek() (uint64, bool) {
	if s.i >= len(s.recs) {
		return 0, false
	}
	h, _ := record.ParseHeader(s.recs[s.i])
	return h.Hash, true
}

func (s *sliceSrc) Head() []byte {
	if s.i >= len(s.recs) {
		return nil
	}
	return s.recs[s.i]
}

func (s *sliceSrc) Ord() uint64            { return s.ord }
func (s *sliceSrc) Next() error            { s.i++; return nil }
func (s *sliceSrc) Pos() int64             { return int64(s.i) }
func (s *sliceSrc) Rewind(pos int64) error { s.i = int(pos); return nil }
func (s *sliceSrc) ReadValue(dst []byte) error {
	_, _, _, v := record.Decode(s.Head())
	copy(dst, v)
	return nil
}

func TestSnapshot_fence_bytes_and_sources_split_by_fence(t *testing.T) {
	pool := mem.NewPool(1 << 20)
	b := New(pool)
	var recs []rec
	for i := range 500 {
		recs = append(recs, rec{record.Left, fmt.Appendf(nil, "%d", i), []byte("v")})
	}
	if err := appendAll(t, b, record.Left, recs...); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	snap := b.Pin()
	defer b.Unpin()
	var sizes [record.Fences]int64
	snap.AddFenceBytes(&sizes)
	var want [record.Fences]int64
	for _, r := range recs {
		want[record.Fence(record.Hash(r.key))] += int64(record.Len(len(r.key), len(r.value)))
	}
	if sizes != want {
		t.Error("AddFenceBytes differs from the records' sizes by fence")
	}
	n := 0
	for lo := 0; lo < record.Fences; lo += 64 {
		for i := range snap.Blocks() {
			src := snap.Source(i, lo, lo+64, 0)
			for h, ok := src.Peek(); ok; h, ok = src.Peek() {
				if f := record.Fence(h); f < lo || f >= lo+64 {
					t.Fatalf("a record of fence %d in the source of fences %d to %d", f, lo, lo+64)
				}
				n++
				_ = src.Next()
			}
		}
	}
	if n != len(recs) {
		t.Errorf("the sources of four fence ranges hold %d records, want %d", n, len(recs))
	}
}
