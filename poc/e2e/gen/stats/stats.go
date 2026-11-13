// Package stats measures how large and how compressible records are in the
// sidecar's spill format, so that generated values can be calibrated
// against real ones.
//
// Sample encodes records as the sidecar's buffers hold them, keeping each
// side's records in a stream of their own as the sidecar spills each side
// separately. It packs each stream into blocks of whole records of at most
// 64 KiB, and compresses every block on its own with s2 and with zstd
// level 1. That is how the generator's reference prototype measured its
// profiles, so the ratios compare with its results.
package stats

import (
	"iter"
	"slices"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"

	"poc/e2e/harness"
	"poc/internal/buffer"
)

// blockSize is the most bytes of records Sample compresses at once. A
// record larger than that is compressed in a block of its own.
const blockSize = 64 << 10

// Report describes a sample of records as the sidecar would spill them.
type Report struct {
	Records    int     // records sampled
	KeyBytes   int64   // bytes of their keys
	ValueBytes int64   // bytes of their values
	Value      Sizes   // value sizes in bytes
	SpillBytes int64   // spill-format bytes in the sample
	S2Ratio    float64 // SpillBytes over their s2-compressed size, compressing 64 KiB blocks
	ZstdRatio  float64 // the same with zstd level 1
}

// Sizes is the distribution of a sample's sizes. A percentile is the size
// at index n*q/100 of the n sorted sizes, so P50 of 1 to 100 is 51.
type Sizes struct {
	Mean          float64
	P50, P90, P99 int
	Max           int
}

// Sample reads records until it has about maxSpillBytes of spill-format
// bytes, or the records end. It stops after the record that reaches
// maxSpillBytes, and with maxSpillBytes 0 or less it reads every record.
// Sample doesn't keep the records' keys or values, so the iterator may
// reuse them. Records of any side but Left count as Right, as in
// harness.Ingest. With no records, every field of the Report is zero.
func Sample(records iter.Seq[harness.Record], maxSpillBytes int64) Report {
	s := newSampler()
	defer s.zstd.Close()
	for rec := range records {
		s.add(rec)
		if maxSpillBytes > 0 && s.r.SpillBytes >= maxSpillBytes {
			break
		}
	}
	return s.report()
}

// sampler holds the state of one Sample.
type sampler struct {
	r      Report
	sizes  []int     // of each value, in order read
	blocks [2][]byte // the block being filled, of the left and right side
	zstd   *zstd.Encoder
	dst    []byte // compressed output, reused
	s2     int64  // compressed bytes of the blocks so far
	zstd1  int64
}

func newSampler() *sampler {
	// These are the prototype's settings. Level 1 is SpeedFastest; one
	// encoder goroutine and lower memory don't change the output.
	z, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true))
	if err != nil {
		panic("stats: " + err.Error())
	}
	return &sampler{zstd: z}
}

// add appends one record to its side's block, as the sidecar's buffers
// encode it: hashed with buffer.Hash, or with hash 0 for an empty key. A
// record that would overflow a non-empty block starts a new one.
func (s *sampler) add(rec harness.Record) {
	var hash uint64
	if len(rec.Key) > 0 {
		hash = buffer.Hash(rec.Key)
	}
	b := &s.blocks[0]
	if rec.Side != harness.Left {
		b = &s.blocks[1]
	}
	n := buffer.EncodedLen(rec.Key, rec.Value)
	if len(*b) > 0 && len(*b)+n > blockSize {
		s.compress(*b)
		*b = (*b)[:0]
	}
	*b = buffer.AppendRecord(*b, hash, rec.Key, rec.Value)

	s.r.Records++
	s.r.KeyBytes += int64(len(rec.Key))
	s.r.ValueBytes += int64(len(rec.Value))
	s.r.SpillBytes += int64(n)
	s.sizes = append(s.sizes, len(rec.Value))
}

// compress adds the compressed sizes of block b.
func (s *sampler) compress(b []byte) {
	s.dst = slices.Grow(s.dst[:0], s2.MaxEncodedLen(len(b)))
	s.s2 += int64(len(s2.Encode(s.dst[:cap(s.dst)], b)))
	s.dst = s.zstd.EncodeAll(b, s.dst[:0])
	s.zstd1 += int64(len(s.dst))
}

// report compresses the last blocks and summarizes the sample.
func (s *sampler) report() Report {
	for _, b := range s.blocks {
		if len(b) > 0 {
			s.compress(b)
		}
	}
	r := s.r
	if n := len(s.sizes); n > 0 {
		slices.Sort(s.sizes)
		r.Value = Sizes{
			Mean: float64(r.ValueBytes) / float64(n),
			P50:  s.sizes[n*50/100],
			P90:  s.sizes[n*90/100],
			P99:  s.sizes[n*99/100],
			Max:  s.sizes[n-1],
		}
	}
	if r.SpillBytes > 0 {
		r.S2Ratio = float64(r.SpillBytes) / float64(s.s2)
		r.ZstdRatio = float64(r.SpillBytes) / float64(s.zstd1)
	}
	return r
}
