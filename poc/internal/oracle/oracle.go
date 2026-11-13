// Package oracle is a naive full outer join that tests compare the engine
// against (§17). It holds everything in memory and shares no code with
// the engine.
package oracle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"

	reconv1 "poc/gen/recon/v1"
)

// Side is the side of a run a record belongs to.
type Side int

const (
	Left Side = iota
	Right
)

// Record is one ingested record.
type Record struct {
	Side       Side
	Key, Value []byte
}

// Join returns the full outer join of records: a both Match per key with
// records on both sides, holding their values in input order, and a
// one-sided result for every other record, including every empty-key one.
func Join(records []Record) []*reconv1.JoinResult {
	type group struct {
		key         []byte
		left, right [][]byte
	}
	groups := map[string]*group{}
	var order []string
	var out []*reconv1.JoinResult
	for _, r := range records {
		if len(r.Key) == 0 {
			out = append(out, oneSided(r.Side, r.Key, r.Value))
			continue
		}
		g, ok := groups[string(r.Key)]
		if !ok {
			g = &group{key: r.Key}
			groups[string(r.Key)] = g
			order = append(order, string(r.Key))
		}
		if r.Side == Left {
			g.left = append(g.left, r.Value)
		} else {
			g.right = append(g.right, r.Value)
		}
	}
	for _, k := range order {
		g := groups[k]
		switch {
		case len(g.left) > 0 && len(g.right) > 0:
			out = append(out, &reconv1.JoinResult{Result: &reconv1.JoinResult_Both{Both: &reconv1.Match{Key: g.key, Left: g.left, Right: g.right}}})
		default:
			for _, v := range g.left {
				out = append(out, oneSided(Left, g.key, v))
			}
			for _, v := range g.right {
				out = append(out, oneSided(Right, g.key, v))
			}
		}
	}
	return out
}

func oneSided(side Side, key, value []byte) *reconv1.JoinResult {
	rec := &reconv1.Record{Key: key, Value: value}
	if side == Left {
		return &reconv1.JoinResult{Result: &reconv1.JoinResult_LeftOnly{LeftOnly: rec}}
	}
	return &reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{RightOnly: rec}}
}

// State returns r's Venn state.
func State(r *reconv1.JoinResult) reconv1.JoinState {
	switch r.GetResult().(type) {
	case *reconv1.JoinResult_LeftOnly:
		return reconv1.JoinState_JOIN_STATE_LEFT_ONLY
	case *reconv1.JoinResult_RightOnly:
		return reconv1.JoinState_JOIN_STATE_RIGHT_ONLY
	case *reconv1.JoinResult_Both:
		return reconv1.JoinState_JOIN_STATE_BOTH
	}
	return reconv1.JoinState_JOIN_STATE_UNSPECIFIED
}

// Canonical encodes r so that two results are equal exactly when their
// encodings are: its state, key and values, with the values of a Match
// sorted, since their order follows ingest order (§8.3).
func Canonical(r *reconv1.JoinResult) []byte {
	b := []byte{byte(State(r))}
	field := func(v []byte) {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	values := func(vs [][]byte) {
		vs = slices.Clone(vs)
		slices.SortFunc(vs, bytes.Compare)
		b = binary.AppendUvarint(b, uint64(len(vs)))
		for _, v := range vs {
			field(v)
		}
	}
	switch res := r.GetResult().(type) {
	case *reconv1.JoinResult_LeftOnly:
		field(res.LeftOnly.GetKey())
		field(res.LeftOnly.GetValue())
	case *reconv1.JoinResult_RightOnly:
		field(res.RightOnly.GetKey())
		field(res.RightOnly.GetValue())
	case *reconv1.JoinResult_Both:
		field(res.Both.GetKey())
		values(res.Both.GetLeft())
		values(res.Both.GetRight())
	}
	return b
}

// Filter returns the results in states, or all of them if states is
// empty.
func Filter(results []*reconv1.JoinResult, states ...reconv1.JoinState) []*reconv1.JoinResult {
	if len(states) == 0 {
		return results
	}
	var out []*reconv1.JoinResult
	for _, r := range results {
		if slices.Contains(states, State(r)) {
			out = append(out, r)
		}
	}
	return out
}

// Diff compares want and got as multisets of canonical results, and
// describes how they differ, or returns "" if they are equal.
func Diff(want, got []*reconv1.JoinResult) string {
	counts := map[string]int{}
	for _, r := range want {
		counts[string(Canonical(r))]++
	}
	for _, r := range got {
		counts[string(Canonical(r))]--
	}
	var missing, extra int
	var examples []string
	for _, k := range slices.Sorted(maps.Keys(counts)) {
		n := counts[k]
		switch {
		case n > 0:
			missing += n
		case n < 0:
			extra -= n
		default:
			continue
		}
		if len(examples) < 5 {
			examples = append(examples, fmt.Sprintf("  %+d × %q", -n, k))
		}
	}
	if missing == 0 && extra == 0 {
		return ""
	}
	return fmt.Sprintf("%d of %d wanted results missing, %d of %d got results unwanted; for example (+ unwanted, - missing):\n%s",
		missing, len(want), extra, len(got), strings.Join(examples, "\n"))
}
