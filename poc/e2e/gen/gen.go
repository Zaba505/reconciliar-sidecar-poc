// Package gen generates synthetic runs for the e2e tests, and checks Join
// output against them without holding the dataset (§8.3).
//
// Data comes from a Config: key k gets Counts(k) left and right records,
// 0 to 3 each, with values derived from (side, k, j). A value is either a
// legacy one, a header and a repeated pattern, or a record of a Schema
// marshaled as protobuf, Avro or JSON exactly as generated code would
// marshal it. A Verifier checks each result against its key's expectation
// as it streams by, and a Fingerprint summarizes output so that
// configurations can be compared without storing it.
package gen

import (
	"errors"
	"fmt"
	"iter"
	"math"

	"poc/e2e/harness"
)

// Config describes a synthetic run.
//
// A Config with no schemas makes legacy values: ValueSize bytes of a
// header and a repeated pattern, which compress far better than real
// data. A Config with a Schema on both sides makes realistic values: marshaled records,
// a different schema and encoding on each side if need be. A Config has
// schemas on both sides or on neither (see Validate).
type Config struct {
	Seed      uint64
	Keys      int // keys 0 to Keys-1
	ValueSize int // bytes per legacy value, at least 10

	Left, Right Values    // how each side's values are made
	Mix         Mix       // how many records each side of a key has
	KeyFormat   KeyFormat // how key indexes become key bytes
}

// Validate reports whether c can be generated: Keys is in range for its
// KeyFormat, both sides or neither have a schema, the schemas are valid,
// and no record's identity is 0, which protobuf would leave out.
func (c Config) Validate() error {
	switch {
	case c.Keys < 0 || c.Keys > maxKeys:
		return fmt.Errorf("%d keys is not in [0, 2^60]", c.Keys)
	case c.KeyFormat == DecimalKeys && c.Keys > maxDecimalKey:
		return fmt.Errorf("%d keys don't fit %d decimal digits", c.Keys, decimalDigits)
	case c.KeyFormat != DecimalKeys && c.KeyFormat != RFKeys:
		return fmt.Errorf("unknown key format %v", c.KeyFormat)
	case c.Mix != Stress && c.Mix != Typical:
		return fmt.Errorf("unknown mix %v", c.Mix)
	case (c.Left.Schema == nil) != (c.Right.Schema == nil):
		return errors.New("a schema on one side only: give both sides a schema, or neither")
	case c.Left.Schema == nil:
		return nil
	}
	for _, v := range []Values{c.Left, c.Right} {
		if v.Encoding < Protobuf || v.Encoding > JSON {
			return fmt.Errorf("unknown encoding %v", v.Encoding)
		}
		if err := v.Schema.Validate(); err != nil {
			return err
		}
	}
	if side, k, _ := c.ParseIdentity(0); (side == harness.Left || side == harness.Right) && k < c.Keys {
		return fmt.Errorf("seed %d gives a record the identity 0, which protobuf leaves out: pick another seed", c.Seed)
	}
	return nil
}

// block is how many keys Records emits one side of at a time, so that
// Ingest batches hold many records.
const block = 1024

// slabSize is the size of the buffers Records carves keys and values from.
const slabSize = 1 << 20

// slab hands out byte slices that are never reused, because Ingest keeps
// the records it is given until it sends their batch. Carving them from
// big buffers saves an allocation per record.
type slab struct{ b []byte }

// key carves key k.
func (s *slab) key(c Config, k int) []byte {
	if cap(s.b)-len(s.b) < 32 {
		s.b = make([]byte, 0, slabSize)
	}
	return s.take(c.AppendKey(s.b[len(s.b):], k))
}

// value carves the value of key k's j-th record on side. A value that
// doesn't fit the rest of the buffer gets its own allocation, from append,
// and the next one starts a new buffer.
func (s *slab) value(g *ValueGen, side harness.Side, k, j int) []byte {
	if cap(s.b)-len(s.b) < 64 {
		s.b = make([]byte, 0, slabSize)
	}
	return s.take(g.Append(s.b[len(s.b):], side, k, j))
}

// take accounts for v, appended at the end of the buffer. Appending only
// moves to a new array when the end is full, so v is in the buffer if
// and only if it fits.
func (s *slab) take(v []byte) []byte {
	if room := cap(s.b) - len(s.b); len(v) <= room {
		s.b = s.b[:len(s.b)+len(v)]
	} else {
		s.b = nil
	}
	return v[:len(v):len(v)]
}

// Records returns the records of the keys k with k mod parts == part, so
// that parts streams can ingest the run between them. Each block of keys
// emits its left records, then its right ones. Different parts can be
// iterated at once. The yielded keys and values are never reused.
func (c Config) Records(part, parts int) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		g := c.ValueGen()
		var s slab
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
						key := s.key(c, k)
						if !yield(harness.Record{Side: side, Key: key, Value: s.value(g, side, k, j)}) {
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

// KeysFor samples to size a run: sizeSamples values of each side give
// their mean size, spread sampleStride keys apart, and the counts of up to
// countSamples keys are added up one by one before KeysFor extrapolates.
const (
	sizeSamples  = 16 << 10
	sampleStride = 50_000_017 // keeps sampled keys below 10^12
	countSamples = 4 << 20
)

// KeysFor returns how many keys make a run of about keyValueBytes bytes of
// keys and values, as Records yields them, up to as many as c.KeyFormat
// has; c.Keys is ignored. It takes each side's mean value size from 16Ki
// values of keys spread far apart, then adds up keys' records by c.Mix,
// key 0 first, until they reach keyValueBytes. Past 4Mi keys it
// extrapolates from their mean. A legacy run's value size is fixed, so
// below 4Mi keys it returns exactly the fewest keys that reach
// keyValueBytes. It panics if c is not valid.
func (c Config) KeysFor(keyValueBytes int64) int {
	c.Keys = 0 // so that Validate passes whatever c.Keys was
	g := c.ValueGen()
	key := float64(len(c.Key(0)))
	var record [2]float64 // mean key and value bytes of a record, per side
	var buf []byte
	for i, side := range []harness.Side{harness.Left, harness.Right} {
		var total int
		for n := range sizeSamples {
			buf = g.Append(buf[:0], side, n*sampleStride, n%3)
			total += len(buf)
		}
		record[i] = key + float64(total)/sizeSamples
	}
	target := float64(keyValueBytes)
	var sum float64
	for k := range countSamples {
		if sum >= target {
			return k
		}
		l, r := c.Counts(k)
		sum += float64(l)*record[0] + float64(r)*record[1]
	}
	if sum >= target {
		return countSamples
	}
	limit := float64(maxKeys)
	if c.KeyFormat == DecimalKeys {
		limit = maxDecimalKey
	}
	n := countSamples + math.Ceil((target-sum)/(sum/countSamples))
	return int(min(n, limit))
}
