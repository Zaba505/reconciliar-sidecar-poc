package oracle

import (
	"testing"

	reconv1 "poc/gen/recon/v1"
)

func r(side Side, key, value string) Record {
	return Record{Side: side, Key: []byte(key), Value: []byte(value)}
}

func left(key, value string) *reconv1.JoinResult {
	return &reconv1.JoinResult{Result: &reconv1.JoinResult_LeftOnly{LeftOnly: &reconv1.Record{Key: []byte(key), Value: []byte(value)}}}
}

func right(key, value string) *reconv1.JoinResult {
	return &reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{RightOnly: &reconv1.Record{Key: []byte(key), Value: []byte(value)}}}
}

func both(key string, l, rt []string) *reconv1.JoinResult {
	m := &reconv1.Match{Key: []byte(key)}
	for _, v := range l {
		m.Left = append(m.Left, []byte(v))
	}
	for _, v := range rt {
		m.Right = append(m.Right, []byte(v))
	}
	return &reconv1.JoinResult{Result: &reconv1.JoinResult_Both{Both: m}}
}

func TestJoin(t *testing.T) {
	got := Join([]Record{
		r(Left, "a", "1"), r(Right, "a", "2"), r(Left, "a", "3"),
		r(Left, "b", "4"), r(Left, "b", "4"),
		r(Right, "c", "5"),
		r(Left, "", "6"), r(Right, "", "7"),
	})
	want := []*reconv1.JoinResult{
		both("a", []string{"3", "1"}, []string{"2"}), // value order doesn't matter
		left("b", "4"), left("b", "4"),
		right("c", "5"),
		left("", "6"), right("", "7"),
	}
	if d := Diff(want, got); d != "" {
		t.Error(d)
	}
}

func TestDiff_counts_multiplicity(t *testing.T) {
	if Diff([]*reconv1.JoinResult{left("b", "4"), left("b", "4")}, []*reconv1.JoinResult{left("b", "4")}) == "" {
		t.Error("a missing duplicate went unnoticed")
	}
	if Diff([]*reconv1.JoinResult{left("k", "v")}, []*reconv1.JoinResult{right("k", "v")}) == "" {
		t.Error("a wrong state went unnoticed")
	}
	if Diff([]*reconv1.JoinResult{both("k", []string{"1"}, []string{"2"})}, []*reconv1.JoinResult{both("k", []string{"2"}, []string{"1"})}) == "" {
		t.Error("swapped sides went unnoticed")
	}
}

func TestFilter(t *testing.T) {
	all := []*reconv1.JoinResult{left("a", "1"), right("b", "2"), both("c", []string{"3"}, []string{"4"})}
	got := Filter(all, reconv1.JoinState_JOIN_STATE_BOTH, reconv1.JoinState_JOIN_STATE_RIGHT_ONLY)
	if d := Diff(all[1:], got); d != "" {
		t.Error(d)
	}
	if len(Filter(all)) != 3 {
		t.Error("an empty filter must keep every result")
	}
}
