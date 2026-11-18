// Package merge merges sorted sequences of records into one, in (hash,
// key, side, ord) order, with a loser tree (§8.4).
//
// A source is a sorted block in memory or a segment of a run file. Taking
// a record replays the matches from its source's leaf to the root: one
// comparison per level, of two hashes held in an array, and of keys only
// when the hashes are equal.
package merge

import (
	"bytes"

	"poc/internal/record"
)

// Source is one sorted input of a merge.
type Source interface {
	// Peek returns the hash of the record at the head, or false at the end.
	// It doesn't touch the record, so a merge of blocks reads only their
	// sort entries until two hashes are equal.
	Peek() (hash uint64, ok bool)
	// Head returns the record at the head, or nil at the end: at least its
	// header and key, and its value too unless the value is too large for
	// the source to hold, which ReadValue then reads. It is valid until
	// Next or Rewind.
	Head() []byte
	// Ord orders the head among the records of the same hash, key and side
	// in other sources: the later a record was ingested, the larger.
	Ord() uint64
	// Next moves to the next record.
	Next() error
	// Pos returns the position of the head, for Rewind.
	Pos() int64
	// Rewind moves the head back to a position Pos returned.
	Rewind(pos int64) error
	// ReadValue reads the value of the head into dst, which is exactly as
	// long as the value.
	ReadValue(dst []byte) error
}

// Merger merges sources. node[0] is the source whose head is first, node[p]
// the loser of the match played at p, and the leaf of source i is k+i.
type Merger struct {
	srcs []Source
	hash []uint64
	done []bool
	node []int32
	tmp  []int32
}

// New returns a Merger of srcs. Init must be called before it is used,
// and again whenever a source is moved other than by Pop.
func New(srcs []Source) *Merger {
	k := len(srcs)
	return &Merger{srcs: srcs, hash: make([]uint64, k), done: make([]bool, k), node: make([]int32, max(k, 1)), tmp: make([]int32, 2*k)}
}

// Bytes is the memory a Merger of k sources takes besides the sources:
// its arrays, and room for each source's saved position.
func Bytes(k int) int64 { return int64(k)*48 + 128 }

// Sources returns the merger's sources.
func (m *Merger) Sources() []Source { return m.srcs }

func (m *Merger) set(i int32) {
	h, ok := m.srcs[i].Peek()
	if !ok {
		h = ^uint64(0)
	}
	m.hash[i], m.done[i] = h, !ok
}

// less reports whether the head of source a comes before that of b.
func (m *Merger) less(a, b int32) bool {
	ha, hb := m.hash[a], m.hash[b]
	if ha != hb {
		return ha < hb
	}
	if m.done[a] {
		return false
	}
	if m.done[b] {
		return true
	}
	sa, ka := record.SideKey(m.srcs[a].Head())
	sb, kb := record.SideKey(m.srcs[b].Head())
	if c := bytes.Compare(ka, kb); c != 0 {
		return c < 0
	}
	if sa != sb {
		return sa < sb
	}
	return m.srcs[a].Ord() < m.srcs[b].Ord()
}

// Init builds the tree from the sources' heads.
func (m *Merger) Init() {
	k := len(m.srcs)
	for i := range k {
		m.set(int32(i))
	}
	if k == 0 {
		return
	}
	w := m.tmp // the winner of each node
	for i := range k {
		w[k+i] = int32(i)
	}
	for p := k - 1; p >= 1; p-- {
		a, b := w[2*p], w[2*p+1]
		if m.less(b, a) {
			a, b = b, a
		}
		w[p], m.node[p] = a, b
	}
	if k == 1 {
		m.node[0] = 0
	} else {
		m.node[0] = w[1]
	}
}

// Top returns the source whose head is the first record of the merge, or
// nil at its end.
func (m *Merger) Top() Source {
	if len(m.srcs) == 0 || m.done[m.node[0]] {
		return nil
	}
	return m.srcs[m.node[0]]
}

// Peek returns the first record of the merge, as Source.Head does, or nil
// at its end.
func (m *Merger) Peek() []byte {
	if s := m.Top(); s != nil {
		return s.Head()
	}
	return nil
}

// Pop moves past the first record.
func (m *Merger) Pop() error {
	i := m.node[0]
	if err := m.srcs[i].Next(); err != nil {
		return err
	}
	m.set(i)
	k := int32(len(m.srcs))
	for p := (k + i) / 2; p >= 1; p /= 2 {
		if m.less(m.node[p], i) {
			m.node[p], i = i, m.node[p]
		}
	}
	m.node[0] = i
	return nil
}
