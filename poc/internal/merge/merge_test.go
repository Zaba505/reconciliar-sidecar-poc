package merge

import (
	"cmp"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"poc/internal/record"
)

// slice is a sorted Source of whole records.
type slice struct {
	recs [][]byte
	ord  uint64
	i    int
}

func (s *slice) Peek() (uint64, bool) {
	if s.i >= len(s.recs) {
		return 0, false
	}
	h, _ := record.ParseHeader(s.recs[s.i])
	return h.Hash, true
}

func (s *slice) Head() []byte {
	if s.i >= len(s.recs) {
		return nil
	}
	return s.recs[s.i]
}

func (s *slice) Ord() uint64            { return s.ord }
func (s *slice) Next() error            { s.i++; return nil }
func (s *slice) Pos() int64             { return int64(s.i) }
func (s *slice) Rewind(pos int64) error { s.i = int(pos); return nil }
func (s *slice) ReadValue(dst []byte) error {
	_, _, _, v := record.Decode(s.Head())
	copy(dst, v)
	return nil
}

type tagged struct {
	rec []byte
	ord uint64
}

func TestMerger_property_merges_in_hash_key_side_ord_order(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		k := rapid.IntRange(0, 9).Draw(t, "sources")
		var srcs []Source
		var all []tagged
		for i := range k {
			n := rapid.IntRange(0, 40).Draw(t, "records")
			var recs [][]byte
			for range n {
				key := rapid.SliceOfN(rapid.ByteRange('a', 'c'), 0, 2).Draw(t, "key")
				// Few hashes, so that keys share them.
				hash := uint64(rapid.IntRange(0, 3).Draw(t, "hash")) << 60
				if len(key) == 0 {
					hash = 0
				}
				side := record.Side(rapid.IntRange(0, 1).Draw(t, "side"))
				recs = append(recs, record.Append(nil, hash, side, key, nil))
			}
			slices.SortStableFunc(recs, record.Compare)
			srcs = append(srcs, &slice{recs: recs, ord: uint64(i)})
			for _, r := range recs {
				all = append(all, tagged{r, uint64(i)})
			}
		}
		slices.SortStableFunc(all, func(a, b tagged) int {
			return cmp.Or(record.Compare(a.rec, b.rec), cmp.Compare(a.ord, b.ord))
		})
		m := New(srcs)
		m.Init()
		var got []tagged
		for s := m.Top(); s != nil; s = m.Top() {
			got = append(got, tagged{s.Head(), s.Ord()})
			if err := m.Pop(); err != nil {
				t.Fatal(err)
			}
		}
		if m.Peek() != nil {
			t.Fatal("Peek at the end isn't nil")
		}
		if len(got) != len(all) {
			t.Fatalf("merged %d records, want %d", len(got), len(all))
		}
		for i := range got {
			if record.Compare(got[i].rec, all[i].rec) != 0 || got[i].ord != all[i].ord {
				t.Fatalf("record %d is %x from %d, want %x from %d", i, got[i].rec, got[i].ord, all[i].rec, all[i].ord)
			}
		}
	})
}

func TestMerger_of_no_sources_is_empty(t *testing.T) {
	m := New(nil)
	m.Init()
	if m.Top() != nil || m.Peek() != nil {
		t.Error("a merge of nothing has a record")
	}
}

func TestMerger_rewinds_after_Init(t *testing.T) {
	recs := [][]byte{
		record.Append(nil, 1, record.Left, []byte("a"), nil),
		record.Append(nil, 2, record.Left, []byte("b"), nil),
	}
	other := [][]byte{record.Append(nil, 3, record.Left, []byte("c"), nil)}
	m := New([]Source{&slice{recs: recs}, &slice{recs: other, ord: 1}})
	m.Init()
	saved := []int64{m.Sources()[0].Pos(), m.Sources()[1].Pos()}
	for m.Top() != nil {
		_ = m.Pop()
	}
	for i, s := range m.Sources() {
		_ = s.Rewind(saved[i])
	}
	m.Init()
	if h, _ := record.ParseHeader(m.Peek()); h.Hash != 1 {
		t.Errorf("after the rewind the head has hash %d, want 1", h.Hash)
	}
}
