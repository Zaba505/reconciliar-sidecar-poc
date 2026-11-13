package stats

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"

	"poc/e2e/gen"
	"poc/e2e/harness"
	"poc/internal/buffer"
)

// records yields rs.
func records(rs []harness.Record) iter.Seq[harness.Record] { return slices.Values(rs) }

// random returns n bytes that don't compress.
func random(r *rand.ChaCha8, n int) []byte {
	b := make([]byte, n)
	r.Read(b)
	return b
}

func TestSample_reports_the_sizes_of_a_hand_made_sample(t *testing.T) {
	// Values of 1 to 100 bytes, in a shuffled order, under 4-byte keys.
	var rs []harness.Record
	for _, n := range rand.New(rand.NewPCG(1, 2)).Perm(100) {
		rs = append(rs, harness.Record{Side: harness.Left, Key: fmt.Appendf(nil, "k%03d", n), Value: bytes.Repeat([]byte{'v'}, n+1)})
	}
	got := Sample(records(rs), 0)
	// Each record is 8 bytes of hash, a byte for each length, the key and
	// the value.
	want := Report{
		Records:    100,
		KeyBytes:   400,
		ValueBytes: 5050,
		Value:      Sizes{Mean: 50.5, P50: 51, P90: 91, P99: 100, Max: 100},
		SpillBytes: 100*(8+1+1+4) + 5050,
	}
	got.S2Ratio, got.ZstdRatio = 0, 0
	if got != want {
		t.Errorf("Sample = %+v\nwant     %+v", got, want)
	}
}

func TestSample_of_nothing_is_zero(t *testing.T) {
	if got := Sample(records(nil), 1<<20); got != (Report{}) {
		t.Errorf("Sample of no records = %+v, want zero", got)
	}
}

func TestSample_ratios(t *testing.T) {
	r := rand.NewChaCha8([32]byte{3})
	tests := []struct {
		name   string
		value  func() []byte
		lo, hi float64 // the range both ratios must fall in
	}{
		{"incompressible values", func() []byte { return random(r, 1000) }, 0.98, 1.02},
		{"zero values", func() []byte { return make([]byte, 1000) }, 20, math.Inf(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rs []harness.Record
			for i := range 1000 {
				rs = append(rs, harness.Record{Side: harness.Side(1 + i%2), Key: random(r, 16), Value: tt.value()})
			}
			got := Sample(records(rs), 0)
			if !(got.S2Ratio >= tt.lo && got.S2Ratio <= tt.hi && got.ZstdRatio >= tt.lo && got.ZstdRatio <= tt.hi) {
				t.Errorf("s2 ratio %.3f and zstd ratio %.3f, want both in [%v, %v]", got.S2Ratio, got.ZstdRatio, tt.lo, tt.hi)
			}
		})
	}
}

func TestSample_stops_at_maxSpillBytes(t *testing.T) {
	// Each record is 8+1+1+10+80 = 100 bytes in spill format.
	all := func(yielded *int) iter.Seq[harness.Record] {
		return func(yield func(harness.Record) bool) {
			for i := range 50 {
				*yielded++
				if !yield(harness.Record{Side: harness.Right, Key: fmt.Appendf(nil, "key-%06d", i), Value: make([]byte, 80)}) {
					return
				}
			}
		}
	}
	tests := []struct {
		max  int64
		want int
	}{
		{1, 1},
		{100, 1},
		{101, 2},
		{1000, 10},
		{1 << 20, 50},
		{0, 50},
		{-1, 50},
	}
	for _, tt := range tests {
		var yielded int
		got := Sample(all(&yielded), tt.max)
		if got.Records != tt.want || yielded != tt.want || got.SpillBytes != int64(100*tt.want) {
			t.Errorf("maxSpillBytes %d: read %d records (%d yielded, %d bytes), want %d", tt.max, got.Records, yielded, got.SpillBytes, tt.want)
		}
	}
}

// protoBlocks splits spill-format records into blocks as the reference
// prototype's bench does: a block ends before the record that would take
// it over size, unless the block is empty.
func protoBlocks(data []byte, size int) [][]byte {
	var out [][]byte
	start, off := 0, 0
	for off < len(data) {
		kl, n := binary.Uvarint(data[off+8:])
		vl, m := binary.Uvarint(data[off+8+n:])
		next := off + 8 + n + m + int(kl) + int(vl)
		if next-start > size && off > start {
			out = append(out, data[start:off])
			start = off
		}
		off = next
	}
	return append(out, data[start:])
}

func TestSample_compresses_each_side_in_blocks_as_the_prototype_does(t *testing.T) {
	// Values of mixed sizes, some larger than a block, some compressible,
	// on both sides in an irregular order, and an empty key.
	r := rand.NewChaCha8([32]byte{4})
	pick := rand.New(r)
	var rs []harness.Record
	for i := range 3000 {
		n := pick.IntN(400)
		if i%500 == 7 {
			n = 70 << 10
		}
		v := random(r, n)
		if pick.IntN(2) == 0 {
			clear(v[:n/2])
		}
		key := fmt.Appendf(nil, "key-%012d", i)
		if i == 42 {
			key = nil
		}
		rs = append(rs, harness.Record{Side: harness.Side(1 + pick.IntN(2)), Key: key, Value: v})
	}

	// Encode each side's records as one stream, then split and compress it.
	var data [2][]byte
	for _, rec := range rs {
		var hash uint64
		if len(rec.Key) > 0 {
			hash = buffer.Hash(rec.Key)
		}
		data[rec.Side-1] = buffer.AppendRecord(data[rec.Side-1], hash, rec.Key, rec.Value)
	}
	zenc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer zenc.Close()
	var spill, s2Bytes, zstdBytes int
	for _, d := range data {
		spill += len(d)
		for _, b := range protoBlocks(d, 64<<10) {
			s2Bytes += len(s2.Encode(nil, b))
			zstdBytes += len(zenc.EncodeAll(b, nil))
		}
	}

	got := Sample(records(rs), 0)
	if got.SpillBytes != int64(spill) {
		t.Errorf("SpillBytes = %d, want %d", got.SpillBytes, spill)
	}
	if want := float64(spill) / float64(s2Bytes); got.S2Ratio != want {
		t.Errorf("S2Ratio = %v, want %v", got.S2Ratio, want)
	}
	if want := float64(spill) / float64(zstdBytes); got.ZstdRatio != want {
		t.Errorf("ZstdRatio = %v, want %v", got.ZstdRatio, want)
	}
}

// prototype is the mean value size and zstd level 1 ratio of each
// profile, in the order protobuf, Avro, JSON, as measured by the reference
// prototype this generator was ported from. Its bench sampled 96 MiB of
// left-side spill-format records per cell, in 64 KiB blocks, with seed 20
// and RF keys; the brief for the port gives them as its table of results.
var prototype = map[string]struct{ mean, zstd [3]float64 }{
	"Payment":        {[3]float64{261, 219, 613}, [3]float64{1.7, 1.6, 3.5}},
	"BankLine":       {[3]float64{191, 148, 405}, [3]float64{1.9, 1.8, 3.6}},
	"CardSettlement": {[3]float64{144, 104, 415}, [3]float64{1.6, 1.5, 4.1}},
	"Envelope":       {[3]float64{581, 578, 793}, [3]float64{1.1, 1.1, 1.5}},
	"Tiny":           {[3]float64{23, 14, 86}, [3]float64{1.4, 1.0, 2.8}},
}

// TestSample_profiles_stay_near_the_prototype keeps every profile and
// encoding near the prototype's sizes and ratios, so that the generator
// can't drift back to values like the old generator's, which compress
// about 52 times with zstd level 1. A 256 KiB sample is
// within a few percent of a 96 MiB one.
func TestSample_profiles_stay_near_the_prototype(t *testing.T) {
	const (
		sample   = 256 << 10
		maxRatio = 6.0 // for any profile, encoding and compressor
	)
	for _, p := range gen.Profiles() {
		want, ok := prototype[p.Name]
		if !ok {
			t.Errorf("%s: no prototype results", p.Name)
			continue
		}
		for _, enc := range []gen.Encoding{gen.Protobuf, gen.Avro, gen.JSON} {
			t.Run(p.Name+"/"+enc.String(), func(t *testing.T) {
				t.Parallel()
				v := gen.Values{Schema: p, Encoding: enc}
				c := gen.Config{Seed: 20, Keys: 1 << 20, Left: v, Right: v, Mix: gen.Typical, KeyFormat: gen.RFKeys}
				got := Sample(c.Records(0, 1), sample)
				mean, ratio := want.mean[enc], want.zstd[enc]
				if got.Value.Mean < 0.9*mean || got.Value.Mean > 1.1*mean {
					t.Errorf("mean value size %.1f bytes, want %.0f ± 10%%", got.Value.Mean, mean)
				}
				if got.ZstdRatio < 0.8*ratio || got.ZstdRatio > 1.25*ratio {
					t.Errorf("zstd ratio %.2f, want within [%.2f, %.2f], around the prototype's %.1f", got.ZstdRatio, 0.8*ratio, 1.25*ratio, ratio)
				}
				if got.ZstdRatio >= maxRatio || got.S2Ratio >= maxRatio {
					t.Errorf("zstd ratio %.2f and s2 ratio %.2f, want both below %v", got.ZstdRatio, got.S2Ratio, maxRatio)
				}
				t.Logf("%d records, mean %.1f bytes, s2 %.2fx, zstd %.2fx", got.Records, got.Value.Mean, got.S2Ratio, got.ZstdRatio)
			})
		}
	}
}
