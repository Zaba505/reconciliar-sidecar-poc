package join

import (
	"bytes"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/encoding/protowire"

	"poc/internal/buffer"
)

// MaxDepth is how many times a bucket's records are re-partitioned at most.
// Re-partitioning at depth d hashes keys with buffer.Seed + d, so that the
// records of one oversized bucket spread over its sub-buckets.
const MaxDepth = 3

const (
	minFanout = 16
	maxFanout = 64
	minWrite  = 256
	maxWrite  = 1 << 20

	// counterSize is the memory a counter of the counting scan takes: its
	// map entry and its share of the scratch slice.
	counterSize = 64
	// keyCountSize is the memory a key read back takes besides its bytes.
	keyCountSize = 128
)

// HotKeyError is the failure of a Join because one key's records exceed
// the unit budget or its Match would exceed the maximum match size (§8.4).
type HotKeyError struct {
	Key         []byte
	Left, Right uint64 // the key's records per side
	detail      string
}

func (e *HotKeyError) Error() string { return e.detail }

func hotBudget(key []byte, left, right uint64, size, budget int64) *HotKeyError {
	return &HotKeyError{Key: bytes.Clone(key), Left: left, Right: right, detail: fmt.Sprintf(
		"a key's %d left and %d right records take %d bytes, more than a join unit's budget of %d bytes holds",
		left, right, size, budget)}
}

func hotMatch(key []byte, left, right uint64, size, limit int64) *HotKeyError {
	return &HotKeyError{Key: bytes.Clone(key), Left: left, Right: right, detail: fmt.Sprintf(
		"a key's Match of %d left and %d right values is %d bytes, more than the maximum match size of %d bytes",
		left, right, size, limit)}
}

// matchKeySize and matchValueSize add up to the encoded size of a Match:
// its key, and each of its left and right values. Fields 2 and 3 have tags
// of one byte alike.
func matchKeySize(n int) int64   { return int64(protowire.SizeTag(1) + protowire.SizeBytes(n)) }
func matchValueSize(n int) int64 { return int64(protowire.SizeTag(2) + protowire.SizeBytes(n)) }

// oversized joins bucket i of parts, whose estimate alone exceeds the unit
// budget. It fails with a *HotKeyError if a single key is to blame, and
// otherwise re-partitions the bucket's records into sub-buckets of depth
// depth+1 and joins those. Their files are removed once they are joined.
func (j *joiner) oversized(parts *buffer.Partitions, i uint32, depth int) (err error) {
	bufs := bucketsOf(parts, i, i+1)
	if err := j.hotKey(bufs); err != nil {
		return err
	}

	dir := "join-" + strconv.FormatInt(j.subparts.Add(1)-1, 10)
	j.stats.Subpartitions++
	sub, err := j.repartition(bufs, dir, depth+1)
	defer func() {
		var written int64
		for _, b := range bucketsOf(sub, 0, sub.Buckets()) {
			written += b.b.Bytes()
		}
		if rerr := j.scratch.Remove(dir, written); err == nil && rerr != nil {
			err = fmt.Errorf("remove sub-partitions: %w", rerr)
		}
	}()
	if err != nil {
		return err
	}
	return j.joinBuckets(sub, depth+1)
}

// hotKey runs the counting scan over bufs, the two sides of an oversized
// bucket. It returns a *HotKeyError for the key with the lowest hash, then
// the first key of that hash, whose records exceed the unit budget, or
// whose records exceed the scan's threshold and whose Match would exceed
// the maximum match size.
//
// The scan counts record bytes by hash with a Misra-Gries summary, in
// memory proportional to the bucket's bytes over the threshold. Each hash
// that may weigh more than the threshold is read back and its records
// counted per key, so that a hash that several keys share isn't taken for
// a hot key: re-partitioning separates those keys.
func (j *joiner) hotKey(bufs []sided) error {
	var weight int64
	for _, b := range bufs {
		weight += b.b.Bytes()
	}
	threshold := max(j.budget/ExpansionFactor, 1) // the most bytes a key may take
	if j.maxMatch > 0 {
		// A Match takes fewer bytes than its records. Counting down to a
		// small maximum would take a counter per maxMatch bytes of the
		// bucket, so the summary counts no finer than fits in a quarter
		// of the unit budget. A key between the two is found by the Match
		// check when its sub-bucket is joined.
		kmax := max((j.budget/4/counterSize-1)/2, 1)
		threshold = min(threshold, max(j.maxMatch, weight/kmax+1))
	}
	k := int(weight/threshold) + 1
	counters := int64(2*k+1) * counterSize
	read := j.readSize()
	pins, reserved, err := j.pinFor(bufs, counters, read)
	if err != nil {
		return err
	}
	defer unpin(pins)
	defer func() { j.pool.Release(reserved) }()

	h := newHeavy(k)
	err = j.each(pins, read, func(c chunk) error {
		for off := 0; off < len(c.data); {
			hash, _, _, next := buffer.Decode(c.data, off)
			h.add(hash, int64(next-off))
			off = next
		}
		return nil
	})
	if err != nil {
		return err
	}
	cands := h.candidates(threshold)
	h = nil
	j.pool.Release(counters)
	reserved -= counters
	if len(cands) == 0 {
		return nil
	}

	type keyCount struct {
		key         []byte
		left, right uint64
		bytes       int64 // of its records
		match       int64 // of its Match
	}
	counts := make(map[uint64][]*keyCount, len(cands))
	for _, hash := range cands {
		counts[hash] = nil
	}
	var keys int64 // reserved for counts
	defer func() { j.pool.Release(keys) }()
	err = j.each(pins, read, func(c chunk) error {
		for off := 0; off < len(c.data); {
			hash, key, value, next := buffer.Decode(c.data, off)
			kcs, ok := counts[hash]
			if ok {
				i := slices.IndexFunc(kcs, func(kc *keyCount) bool { return bytes.Equal(kc.key, key) })
				if i < 0 {
					size := int64(len(key)) + keyCountSize
					if err := j.pool.Reserve(size); err != nil {
						return err
					}
					keys += size
					kcs = append(kcs, &keyCount{key: bytes.Clone(key), match: matchKeySize(len(key))})
					counts[hash] = kcs
					i = len(kcs) - 1
				}
				kc := kcs[i]
				if c.side == buffer.Left {
					kc.left++
				} else {
					kc.right++
				}
				kc.bytes += int64(next - off)
				kc.match += matchValueSize(len(value))
			}
			off = next
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, hash := range cands {
		for _, kc := range counts[hash] {
			switch {
			case kc.bytes*ExpansionFactor > j.budget:
				return hotBudget(kc.key, kc.left, kc.right, kc.bytes, j.budget)
			case j.maxMatch > 0 && kc.left > 0 && kc.right > 0 && kc.match > j.maxMatch:
				return hotMatch(kc.key, kc.left, kc.right, kc.match, j.maxMatch)
			}
		}
	}
	return nil
}

// heavy is a Misra-Gries summary of weighted hashes, with at most 2k
// counters. Every count is at most debt short of its hash's weight, and
// debt is at most the total weight over k+1, so a hash heavier than that
// always has a counter.
type heavy struct {
	k      int
	counts map[uint64]int64
	debt   int64
	vals   []int64 // scratch for purge
}

func newHeavy(k int) *heavy {
	return &heavy{k: k, counts: make(map[uint64]int64, 2*k+1), vals: make([]int64, 0, 2*k+1)}
}

func (h *heavy) add(hash uint64, w int64) {
	h.counts[hash] += w
	if len(h.counts) <= 2*h.k {
		return
	}
	// Subtract the (k+1)th largest count from every count and drop those
	// left at zero or below, at least k+1 of the 2k+1. Each purge takes at
	// least k+1 times what it subtracts from the total weight.
	h.vals = h.vals[:0]
	for _, v := range h.counts {
		h.vals = append(h.vals, v)
	}
	slices.Sort(h.vals)
	tau := h.vals[len(h.vals)-h.k-1]
	for hash, v := range h.counts {
		if v <= tau {
			delete(h.counts, hash)
		} else {
			h.counts[hash] = v - tau
		}
	}
	h.debt += tau
}

// candidates returns, in ascending order, the hashes that may weigh more
// than threshold.
func (h *heavy) candidates(threshold int64) []uint64 {
	var out []uint64
	for _, hash := range slices.Sorted(maps.Keys(h.counts)) {
		if h.counts[hash]+h.debt > threshold {
			out = append(out, hash)
		}
	}
	return out
}

// repartition writes the records of bufs, the two sides of an oversized
// bucket, into the sub-buckets of depth, under dir. It returns the
// sub-buckets, whose buffers hold only files, also when it fails part way.
//
// Records keep their hash; the sub-bucket is that of their key hashed with
// the depth's seed. The fanout aims for sub-buckets of half the unit
// budget, and each sub-bucket side collects records in a write buffer
// before they are written to a file, all of them within half the budget.
func (j *joiner) repartition(bufs []sided, dir string, depth int) (*buffer.Partitions, error) {
	var size, records int64
	for _, b := range bufs {
		size += b.b.Bytes()
		records += b.b.Records()
	}
	fanout := min(max((2*size*ExpansionFactor+j.budget-1)/j.budget, minFanout), maxFanout)
	write := min(max(j.budget/(4*fanout), minWrite), maxWrite)
	sub := buffer.NewPartitions(uint32(fanout))

	read := j.readSize()
	pins, reserved, err := j.pinFor(bufs, 2*fanout*write, read)
	if err != nil {
		return sub, err
	}
	defer unpin(pins)
	defer j.pool.Release(reserved)

	var out [2][][]byte
	var counts [2][]int64
	for s := range out {
		out[s] = make([][]byte, fanout)
		counts[s] = make([]int64, fanout)
	}
	flush := func(side buffer.Side, i uint64, data []byte, records int64) error {
		b := sub.Bucket(side, uint32(i))
		path := filepath.Join(dir, side.String(), strconv.FormatUint(i, 10), strconv.Itoa(len(b.Files()))+".rec")
		f, err := j.scratch.Write(path, records, [][]byte{data})
		if err != nil {
			return err
		}
		b.AddFile(f)
		return nil
	}
	seed := buffer.Seed + uint64(depth)
	err = j.each(pins, read, func(c chunk) error {
		for off := 0; off < len(c.data); {
			_, key, _, next := buffer.Decode(c.data, off)
			rec := c.data[off:next]
			i := xxh3.HashSeed(key, seed) % uint64(fanout)
			w := &out[c.side][i]
			if len(*w)+len(rec) > int(write) && len(*w) > 0 {
				if err := flush(c.side, i, *w, counts[c.side][i]); err != nil {
					return err
				}
				*w, counts[c.side][i] = (*w)[:0], 0
			}
			if len(rec) > int(write) { // a file of its own
				if err := flush(c.side, i, rec, 1); err != nil {
					return err
				}
			} else {
				if *w == nil {
					*w = make([]byte, 0, write)
				}
				*w = append(*w, rec...)
				counts[c.side][i]++
			}
			off = next
		}
		return nil
	})
	if err != nil {
		return sub, err
	}
	for s := range out {
		for i, w := range out[s] {
			if len(w) > 0 {
				if err := flush(buffer.Side(s), uint64(i), w, counts[s][i]); err != nil {
					return sub, err
				}
			}
		}
	}
	j.log.InfoContext(j.ctx, "bucket sub-partitioned",
		"recon.depth", depth,
		"recon.sub_buckets", fanout,
		"recon.records", records,
		"recon.bytes", size,
	)
	return sub, nil
}
