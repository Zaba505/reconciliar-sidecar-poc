// Package gen generates synthetic runs for the e2e tests, and checks Join
// output against them without holding the dataset (§8.3).
//
// Data comes from a Config: key k gets Counts(k) left and right records,
// 0 to 3 each, with values derived from (side, k, j). A Verifier checks each
// result against its key's expectation as it streams by, and a Fingerprint
// summarizes output so that configurations can be compared without storing
// it.
package gen

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"strconv"

	"github.com/zeebo/xxh3"

	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

// Config describes a synthetic run.
type Config struct {
	Seed      uint64
	Keys      int // keys 0 to Keys-1
	ValueSize int // bytes per value, at least valueHeader
}

// valueHeader is the side, key and index every value starts with.
const valueHeader = 1 + 8 + 1

// block is how many keys Records emits one side of at a time, so that
// Ingest batches hold many records.
const block = 1024

// Counts returns how many left and right records key k has.
func (c Config) Counts(k int) (left, right int) {
	h := xxh3.HashSeed(binary.LittleEndian.AppendUint64(nil, uint64(k)), c.Seed)
	return int(h & 3), int(h >> 2 & 3)
}

// Key returns key k's bytes.
func (c Config) Key(k int) []byte {
	return fmt.Appendf(nil, "key-%012d", k)
}

func (c Config) parseKey(key []byte) (int, bool) {
	s, ok := bytes.CutPrefix(key, []byte("key-"))
	if !ok || len(s) != 12 {
		return 0, false
	}
	k, err := strconv.Atoi(string(s))
	if err != nil || k < 0 || k >= c.Keys {
		return 0, false
	}
	return k, true
}

// Value returns the value of key k's j-th record on side.
func (c Config) Value(side harness.Side, k, j int) []byte {
	v := make([]byte, max(c.ValueSize, valueHeader))
	v[0] = byte(side)
	binary.LittleEndian.PutUint64(v[1:], uint64(k))
	v[9] = byte(j)
	fill := xxh3.HashSeed(v[:valueHeader], c.Seed)
	for i := valueHeader; i < len(v); i++ {
		v[i] = byte(fill >> (8 * (i % 8)))
	}
	return v
}

// Records returns the records of the keys k with k mod parts == part, so
// that parts streams can ingest the run between them. Each block of keys
// emits its left records, then its right ones.
func (c Config) Records(part, parts int) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for start := 0; start < c.Keys; start += block {
			for _, side := range []harness.Side{harness.Left, harness.Right} {
				for k := start; k < min(start+block, c.Keys); k++ {
					if k%parts != part {
						continue
					}
					n, m := c.Counts(k)
					if side == harness.Right {
						n = m
					}
					for j := range n {
						if !yield(harness.Record{Side: side, Key: c.Key(k), Value: c.Value(side, k, j)}) {
							return
						}
					}
				}
			}
		}
	}
}

// Totals returns the run's records per side.
func (c Config) Totals() (left, right uint64) {
	for k := range c.Keys {
		l, r := c.Counts(k)
		left += uint64(l)
		right += uint64(r)
	}
	return left, right
}

// Verifier checks a Join's results against a Config. It keeps 4 bits per
// key: one for its Match, and one for each one-sided record.
type Verifier struct {
	c      Config
	seen   []uint64
	counts [2]uint64
}

// NewVerifier returns a Verifier of c's run.
func NewVerifier(c Config) *Verifier {
	return &Verifier{c: c, seen: make([]uint64, (c.Keys+15)/16)}
}

// mark sets bit b of key k, failing if it was set already.
func (v *Verifier) mark(k, b int) error {
	bit := uint64(1) << (k%16*4 + b)
	if v.seen[k/16]&bit != 0 {
		return fmt.Errorf("key %d: result %d seen twice", k, b)
	}
	v.seen[k/16] |= bit
	return nil
}

// Add checks one result.
func (v *Verifier) Add(r *reconv1.JoinResult) error {
	var (
		side  harness.Side
		rec   *reconv1.Record
		match *reconv1.Match
	)
	switch res := r.GetResult().(type) {
	case *reconv1.JoinResult_LeftOnly:
		side, rec = harness.Left, res.LeftOnly
	case *reconv1.JoinResult_RightOnly:
		side, rec = harness.Right, res.RightOnly
	case *reconv1.JoinResult_Both:
		match = res.Both
	default:
		return errors.New("a result with no state")
	}
	key := rec.GetKey()
	if match != nil {
		key = match.GetKey()
	}
	k, ok := v.c.parseKey(key)
	if !ok {
		return fmt.Errorf("unexpected key %q", key)
	}
	l, rt := v.c.Counts(k)

	if match != nil {
		if l == 0 || rt == 0 {
			return fmt.Errorf("key %d: a Match, but it has %d left and %d right records", k, l, rt)
		}
		if err := v.values(k, harness.Left, match.GetLeft(), l); err != nil {
			return err
		}
		if err := v.values(k, harness.Right, match.GetRight(), rt); err != nil {
			return err
		}
		v.counts[0] += uint64(l)
		v.counts[1] += uint64(rt)
		return v.mark(k, 0)
	}

	n, other := l, rt
	if side == harness.Right {
		n, other = rt, l
	}
	if other > 0 {
		return fmt.Errorf("key %d: a %s-only result, but it has records on both sides", k, sideName(side))
	}
	j, err := v.value(k, side, rec.GetValue(), n)
	if err != nil {
		return err
	}
	v.counts[side-1]++
	return v.mark(k, 1+j)
}

// values checks that vs are key k's n values on side, in any order.
func (v *Verifier) values(k int, side harness.Side, vs [][]byte, n int) error {
	if len(vs) != n {
		return fmt.Errorf("key %d: %d %s values, want %d", k, len(vs), sideName(side), n)
	}
	var seen [4]bool
	for _, val := range vs {
		j, err := v.value(k, side, val, n)
		if err != nil {
			return err
		}
		if seen[j] {
			return fmt.Errorf("key %d: %s value %d appears twice", k, sideName(side), j)
		}
		seen[j] = true
	}
	return nil
}

// value checks that val is one of key k's n values on side and returns its
// index.
func (v *Verifier) value(k int, side harness.Side, val []byte, n int) (int, error) {
	if len(val) < valueHeader {
		return 0, fmt.Errorf("key %d: a %d-byte value", k, len(val))
	}
	j := int(val[9])
	if j >= n || !bytes.Equal(val, v.c.Value(side, k, j)) {
		return 0, fmt.Errorf("key %d: a %s value that isn't one of its %d", k, sideName(side), n)
	}
	return j, nil
}

func sideName(s harness.Side) string {
	if s == harness.Left {
		return "left"
	}
	return "right"
}

// Finish checks that every key's results were all seen, and that they
// hold each side's total of records.
func (v *Verifier) Finish() error {
	var missing int
	first := -1
	for k := range v.c.Keys {
		l, r := v.c.Counts(k)
		var want uint64
		switch {
		case l > 0 && r > 0:
			want = 1
		default:
			want = (uint64(1)<<max(l, r) - 1) << 1
		}
		if got := v.seen[k/16] >> (k % 16 * 4) & 0xf; got != want {
			missing++
			if first < 0 {
				first = k
			}
		}
	}
	if missing > 0 {
		return fmt.Errorf("%d keys are missing results, the first key %d", missing, first)
	}
	left, right := v.c.Totals()
	if v.counts != [2]uint64{left, right} {
		return fmt.Errorf("counted %d left and %d right records, want %d and %d", v.counts[0], v.counts[1], left, right)
	}
	return nil
}

// Fingerprint is an order-independent summary of a Join's results: the sum
// and XOR of the XXH3 of each canonical result, and their count.
type Fingerprint struct {
	Sum, Xor, Count uint64
}

// Add adds one result.
func (f *Fingerprint) Add(r *reconv1.JoinResult) {
	h := xxh3.Hash(oracle.Canonical(r))
	f.Sum += h
	f.Xor ^= h
	f.Count++
}
