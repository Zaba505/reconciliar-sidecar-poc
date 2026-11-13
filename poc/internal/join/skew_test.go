package join

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/oracle"
)

// events records the log records a join writes.
type events struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (e *events) Enabled(context.Context, slog.Level) bool { return true }

func (e *events) Handle(_ context.Context, r slog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recs = append(e.recs, r.Clone())
	return nil
}

func (e *events) WithAttrs([]slog.Attr) slog.Handler { return e }
func (e *events) WithGroup(string) slog.Handler      { return e }

// depths returns the depth of each sub-partition event.
func (e *events) depths() []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []int64
	for _, r := range e.recs {
		if r.Message != "bucket sub-partitioned" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "recon.depth" {
				out = append(out, a.Value.Int64())
			}
			return true
		})
	}
	return out
}

// hashed is a record with the hash it is stored with, which a test may
// force, so that distinct keys collide.
type hashed struct {
	oracle.Record
	hash   uint64
	forced bool
}

// loadHashed appends records to new Partitions with n buckets.
func loadHashed(t interface{ Fatal(...any) }, pool *mem.Pool, n uint32, records []hashed) (*buffer.Partitions, [2]uint64) {
	p := buffer.NewPartitions(n)
	var totals [2]uint64
	for _, r := range records {
		var err error
		if r.forced {
			err = p.Bucket(buffer.Side(r.Side), uint32(r.hash%uint64(n))).Append(pool, r.hash, r.Key, r.Value)
		} else {
			err = p.Append(pool, buffer.Side(r.Side), r.Key, r.Value)
		}
		if err != nil {
			t.Fatal(err)
		}
		totals[r.Side]++
	}
	return p, totals
}

func plain(records []hashed) []oracle.Record {
	out := make([]oracle.Record, len(records))
	for i, r := range records {
		out[i] = r.Record
	}
	return out
}

// keyStats is what the model knows of a key: its records per side, their
// encoded bytes, and the size of its Match.
type keyStats struct {
	left, right uint64
	bytes       int64
	match       int64
}

// hotKeys returns the keys of records whose bytes take more than the unit
// budget, or whose Match exceeds maxMatch (0 for no limit), with their
// stats. Empty keys are never hot.
func hotKeys(records []oracle.Record, budget, maxMatch int64) map[string]keyStats {
	stats := map[string]*keyStats{}
	for _, r := range records {
		if len(r.Key) == 0 {
			continue
		}
		s := stats[string(r.Key)]
		if s == nil {
			s = &keyStats{match: matchKeySize(len(r.Key))}
			stats[string(r.Key)] = s
		}
		if r.Side == oracle.Left {
			s.left++
		} else {
			s.right++
		}
		s.bytes += int64(buffer.EncodedLen(r.Key, r.Value))
		s.match += matchValueSize(len(r.Value))
	}
	hot := map[string]keyStats{}
	for k, s := range stats {
		if s.bytes*ExpansionFactor > budget || maxMatch > 0 && s.left > 0 && s.right > 0 && s.match > maxMatch {
			hot[k] = *s
		}
	}
	return hot
}

// wantHotKey checks that err is a *HotKeyError naming one of hot, with its
// counts, and returns it.
func wantHotKey(t interface{ Fatalf(string, ...any) }, err error, hot map[string]keyStats) *HotKeyError {
	hk, ok := errors.AsType[*HotKeyError](err)
	if !ok {
		t.Fatalf("Run: %v, want a HotKeyError naming one of %d hot keys", err, len(hot))
	}
	s, ok := hot[string(hk.Key)]
	if !ok {
		t.Fatalf("HotKeyError names key %q, which isn't hot", hk.Key)
	}
	if hk.Left != s.left || hk.Right != s.right {
		t.Fatalf("HotKeyError counts %d left and %d right records of key %q, want %d and %d", hk.Left, hk.Right, hk.Key, s.left, s.right)
	}
	return hk
}

func TestMatchSize_is_the_encoded_size_of_a_Match(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := &reconv1.Match{
			Key:   rapid.SliceOfN(rapid.Byte(), 1, 300).Draw(t, "key"),
			Left:  rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 300), 1, 5).Draw(t, "left"),
			Right: rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 300), 1, 5).Draw(t, "right"),
		}
		size := matchKeySize(len(m.Key))
		for _, v := range slices.Concat(m.Left, m.Right) {
			size += matchValueSize(len(v))
		}
		if want := int64(proto.Size(m)); size != want {
			t.Fatalf("size %d, proto.Size %d", size, want)
		}
	})
}

func TestHeavy_property_bounds_every_count(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		k := rapid.IntRange(1, 8).Draw(t, "k")
		h := newHeavy(k)
		weights := map[uint64]int64{}
		var total int64
		for _, hash := range rapid.SliceOf(rapid.Uint64Range(0, 30)).Draw(t, "hashes") {
			w := rapid.Int64Range(1, 100).Draw(t, "weight")
			h.add(hash, w)
			weights[hash] += w
			total += w
			if len(h.counts) > 2*k {
				t.Fatalf("%d counters, at most %d", len(h.counts), 2*k)
			}
		}
		if h.debt*int64(k+1) > total {
			t.Fatalf("debt %d, above the total weight %d over %d", h.debt, total, k+1)
		}
		for hash, w := range weights {
			if c := h.counts[hash]; c > w || c+h.debt < w {
				t.Fatalf("hash %d weighs %d, counted %d with debt %d", hash, w, c, h.debt)
			}
		}
		// The scan sizes k so that the debt stays under the threshold,
		// which an untracked hash then can't exceed.
		threshold := rapid.Int64Range(total/int64(k+1), total+1).Draw(t, "threshold")
		cands := h.candidates(threshold)
		if !slices.IsSorted(cands) {
			t.Fatalf("candidates %v aren't sorted", cands)
		}
		for hash, w := range weights {
			if w > threshold && !slices.Contains(cands, hash) {
				t.Fatalf("hash %d weighs %d, above %d, but isn't a candidate", hash, w, threshold)
			}
		}
	})
}

// TestEach_streams_records_through_a_small_buffer reads spill files whose
// records are smaller and larger than the read buffer.
func TestEach_streams_records_through_a_small_buffer(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	b := &buffer.Buffer{}
	var want [][]byte
	for i, size := range []int{1, 600, 10, 3000, 0, 511, 512, 513, 40, 2000, 7} {
		key := fmt.Appendf(nil, "k%d", i)
		value := bytes.Repeat([]byte{byte(i)}, size)
		if err := b.Append(data, buffer.Hash(key), key, value); err != nil {
			t.Fatal(err)
		}
		want = append(want, buffer.AppendRecord(nil, buffer.Hash(key), key, value))
		if i%4 == 3 {
			if err := sp.spill(data, b); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(b.Files()) < 2 || len(b.Chunks()) == 0 {
		t.Fatalf("%d files, %d chunks; want records in both", len(b.Files()), len(b.Chunks()))
	}

	pool := mem.NewPool(1 << 20)
	j := &joiner{shared: &shared{pool: pool, disk: sp.disk}, ctx: t.Context()}
	pins, reserved, err := j.pinFor([]sided{{b, buffer.Left}}, 0, minRead)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	var streamed int
	err = j.each(pins, minRead, func(c chunk) error {
		if c.streamed {
			streamed++
		}
		for off := 0; off < len(c.data); {
			_, _, _, next := buffer.Decode(c.data, off)
			got = append(got, bytes.Clone(c.data[off:next]))
			off = next
		}
		return nil
	})
	unpin(pins)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(got, want, bytes.Equal) {
		t.Fatalf("streamed %d records, not the %d appended in order", len(got), len(want))
	}
	if streamed < len(b.Files()) {
		t.Errorf("%d streamed chunks from %d files", streamed, len(b.Files()))
	}
	if pool.Used() != reserved {
		t.Errorf("%d reserved after streaming, %d before; the grown buffer must be released", pool.Used(), reserved)
	}
	pool.Release(reserved)

	// A file cut short ends inside a record.
	f := b.Files()[0]
	if err := os.Truncate(f.Path, f.Bytes-1); err != nil {
		t.Fatal(err)
	}
	pins, reserved, _ = j.pinFor([]sided{{b, buffer.Left}}, 0, minRead)
	err = j.each(pins, minRead, func(chunk) error { return nil })
	unpin(pins)
	pool.Release(reserved)
	if err == nil {
		t.Error("streaming a truncated file succeeded")
	}
}

// fixedKey returns n records of key with values of size bytes, alternating
// sides, left first.
func fixedKey(key string, n, size int) []oracle.Record {
	out := make([]oracle.Record, n)
	for i := range out {
		out[i] = oracle.Record{Side: oracle.Side(i % 2), Key: []byte(key), Value: bytes.Repeat([]byte{byte(i)}, size)}
	}
	return out
}

func encoded(records []oracle.Record) int64 {
	var n int64
	for _, r := range records {
		n += int64(buffer.EncodedLen(r.Key, r.Value))
	}
	return n
}

func TestRun_HOT_KEY_at_the_unit_budget(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	records := fixedKey("hot", 40, 200)
	size := encoded(records)
	for _, tc := range []struct {
		name   string
		budget int64
		hot    bool
	}{
		{"fits exactly", size * ExpansionFactor, false},
		{"a byte over", size*ExpansionFactor - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, totals := load(t, data, 1, records)
			pool := mem.NewPool(tc.budget)
			opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals, Parallelism: 1, Scratch: sp}
			for range 2 { // a retry fails the same way
				batches, err := collect(t.Context(), pool, sp.disk, p, opts)
				if !tc.hot {
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
					if d := oracle.Diff(oracle.Join(records), results(batches)); d != "" {
						t.Fatal(d)
					}
					continue
				}
				hk, ok := errors.AsType[*HotKeyError](err)
				if !ok || string(hk.Key) != "hot" || hk.Left != 20 || hk.Right != 20 {
					t.Fatalf("Run: %v (%+v), want a HotKeyError for key hot with 20 left and 20 right records", err, hk)
				}
				if len(batches) != 0 {
					t.Errorf("%d batches sent before HOT_KEY", len(batches))
				}
			}
			if pool.Used() != 0 || len(sp.scratchFiles(t)) != 0 {
				t.Errorf("after the joins: %d bytes reserved, scratch files %v", pool.Used(), sp.scratchFiles(t))
			}
		})
	}
}

func TestRun_HOT_KEY_for_a_Match_above_the_maximum(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	hot := fixedKey("big", 6, 100)
	var others []oracle.Record
	for i := range 2000 {
		others = append(others, oracle.Record{Side: oracle.Side(i % 2), Key: fmt.Appendf(nil, "k%d", i/2), Value: make([]byte, 40)})
	}
	records := slices.Concat(others[:1000], hot, others[1000:])
	m := matchKeySize(3)
	for _, r := range hot {
		m += matchValueSize(len(r.Value))
	}
	for _, tc := range []struct {
		name   string
		budget int64 // holds the bucket's 116 KiB of records or not
	}{
		{"in a unit that fits", 1 << 20},
		{"in an oversized bucket", 128 << 10},
	} {
		for _, limit := range []int64{m, m - 1} {
			t.Run(fmt.Sprintf("%s/max %d", tc.name, limit), func(t *testing.T) {
				p, totals := load(t, data, 1, records)
				pool := mem.NewPool(tc.budget)
				opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 4096, Totals: totals, Parallelism: 1, MaxMatchBytes: limit, Scratch: sp}
				batches, err := collect(t.Context(), pool, sp.disk, p, opts)
				if limit == m {
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
					if d := oracle.Diff(oracle.Join(records), results(batches)); d != "" {
						t.Fatal(d)
					}
					return
				}
				wantHotKey(t, err, hotKeys(records, tc.budget, limit))
				// Filtered out, the Match still counts.
				opts.Filter = NewFilter([]reconv1.JoinState{reconv1.JoinState_JOIN_STATE_LEFT_ONLY})
				_, err = collect(t.Context(), pool, sp.disk, p, opts)
				wantHotKey(t, err, hotKeys(records, tc.budget, limit))
			})
		}
	}
}

// TestRun_subpartitions_an_oversized_bucket joins one bucket of 64 times
// the unit budget: re-partitioned 64 ways, about half its sub-buckets are
// oversized again.
func TestRun_subpartitions_an_oversized_bucket(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	var records []oracle.Record
	for i := range 6000 {
		records = append(records, oracle.Record{Side: oracle.Side(i % 3 % 2), Key: fmt.Appendf(nil, "key-%d", i/3), Value: make([]byte, 64+i%32)})
	}
	p, totals := load(t, data, 1, records)
	for _, b := range buffers(p)[:1] {
		if err := sp.spill(data, b); err != nil { // the left side streams from a file
			t.Fatal(err)
		}
	}
	const parallelism = 4
	budget := encoded(records) * ExpansionFactor / 64
	pool := mem.NewPool(budget * parallelism)
	ev := &events{}
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 2048, Totals: totals, Parallelism: parallelism, Scratch: sp, Log: slog.New(ev)}
	diskBefore := sp.disk.Used()
	var stats Stats
	first, err := func() ([]*reconv1.JoinResponse, error) {
		var batches []*reconv1.JoinResponse
		var err error
		stats, err = Run(t.Context(), pool, sp.disk, p, opts, func(r *reconv1.JoinResponse) error {
			if len(sp.scratchFiles(t)) == 0 {
				t.Error("no scratch files while sub-buckets are joined")
			}
			batches = append(batches, proto.Clone(r).(*reconv1.JoinResponse))
			return nil
		})
		return batches, err
	}()
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), results(first)); d != "" {
		t.Fatal(d)
	}
	depths := ev.depths()
	if stats.Subpartitions != len(depths) || !slices.Contains(depths, 1) || !slices.Contains(depths, 2) {
		t.Errorf("%d sub-partitions, events at depths %v; want depth 1 and some depth 2", stats.Subpartitions, depths)
	}
	if files := sp.scratchFiles(t); len(files) != 0 || sp.disk.Used() != diskBefore || pool.Used() != 0 {
		t.Errorf("after the join: scratch files %v, %d bytes on disk (%d before), %d reserved", files, sp.disk.Used(), diskBefore, pool.Used())
	}
	if pool.Peak() > pool.Limit() {
		t.Errorf("pool peak %d, above the limit %d", pool.Peak(), pool.Limit())
	}
	second, err := collect(t.Context(), pool, sp.disk, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameBatches(t, parallelism, first, second)
}

// collision returns records of keys that share one forced hash.
func collision(hash uint64, keys []string, perKey, size int) []hashed {
	var out []hashed
	for _, k := range keys {
		for i, r := range fixedKey(k, perKey, size) {
			r.Value = fmt.Appendf(r.Value, "%d", i)
			out = append(out, hashed{Record: r, hash: hash, forced: true})
		}
	}
	return out
}

func TestRun_a_hash_that_keys_share_is_no_hot_key(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	var keys []string
	for i := range 30 {
		keys = append(keys, fmt.Sprintf("c%d", i))
	}
	// Each key takes about 1 KiB, together 30, above the 16 KiB a key
	// may take under a 32 KiB unit budget.
	shared := collision(42, keys, 10, 90)
	var others []hashed
	for i := range 600 {
		others = append(others, hashed{Record: oracle.Record{Side: oracle.Side(i % 2), Key: fmt.Appendf(nil, "k%d", i/2), Value: make([]byte, 50)}})
	}
	for _, tc := range []struct {
		name    string
		records []hashed
		hot     string
	}{
		{"none hot", slices.Concat(others, shared), ""},
		{"one hot", slices.Concat(others, shared, collision(42, []string{"c-hot"}, 200, 90)), "c-hot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, totals := loadHashed(t, data, 4, tc.records)
			const budget = 32 << 10
			pool := mem.NewPool(budget)
			ev := &events{}
			opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 2048, Totals: totals, Parallelism: 1, Scratch: sp, Log: slog.New(ev)}
			batches, err := collect(t.Context(), pool, sp.disk, p, opts)
			records := plain(tc.records)
			if tc.hot != "" {
				if hk := wantHotKey(t, err, hotKeys(records, budget, 0)); string(hk.Key) != tc.hot {
					t.Errorf("HotKeyError names %q, want %q", hk.Key, tc.hot)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if d := oracle.Diff(oracle.Join(records), results(batches)); d != "" {
				t.Fatal(d)
			}
			if len(ev.depths()) == 0 {
				t.Error("the bucket of the shared hash wasn't sub-partitioned")
			}
		})
	}
}

func TestRun_streams_empty_keys_past_the_budget(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	var records []oracle.Record
	for i := range 4000 {
		records = append(records, oracle.Record{Side: oracle.Side(i / 100 % 2), Value: fmt.Appendf(nil, "value-%d", i)})
	}
	records = append(records, fixedKey("k", 4, 10)...)
	p := buffer.NewPartitions(1)
	var totals [2]uint64
	for i, r := range records { // spill as they come, so that the sides' files interleave
		if err := p.Append(data, buffer.Side(r.Side), r.Key, r.Value); err != nil {
			t.Fatal(err)
		}
		totals[r.Side]++
		if i%300 == 299 {
			if err := sp.spill(data, p.Empty(buffer.Side(r.Side))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(p.Empty(buffer.Left).Files()) < 2 || len(p.Empty(buffer.Right).Files()) < 2 {
		t.Fatal("want empty keys in several files per side")
	}
	empty := p.Empty(buffer.Left).Bytes() + p.Empty(buffer.Right).Bytes()
	pool := mem.NewPool(8 << 10)
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals, Parallelism: 1, Scratch: sp}
	batches, err := collect(t.Context(), pool, sp.disk, p, opts)
	if err != nil {
		t.Fatalf("Run with %d bytes of empty keys and an 8 KiB budget: %v", empty, err)
	}
	if d := oracle.Diff(oracle.Join(records), results(batches)); d != "" {
		t.Fatal(d)
	}
	if pool.Peak() > pool.Limit() || pool.Used() != 0 {
		t.Errorf("pool peak %d, %d reserved after", pool.Peak(), pool.Used())
	}
}

// TestRun_streams_small_files_through_a_buffer_of_their_size: a pass
// reserves no more stream buffer than its largest file, so a budget below
// minRead joins small spilled files as it did when they were read whole.
func TestRun_streams_small_files_through_a_buffer_of_their_size(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	records := []oracle.Record{rec(oracle.Left, "", ""), rec(oracle.Left, "", "v")}
	p, totals := load(t, data, 1, records)
	if err := sp.spill(data, p.Empty(buffer.Left)); err != nil {
		t.Fatal(err)
	}
	const batch = 1
	size := p.Empty(buffer.Left).Bytes()
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: batch, Totals: totals, Parallelism: 1, Scratch: sp}
	for _, tc := range []struct {
		limit int64
		fits  bool
	}{{batch + size, true}, {batch + size - 1, false}} {
		batches, err := collect(t.Context(), mem.NewPool(tc.limit), sp.disk, p, opts)
		if !tc.fits {
			if !errors.Is(err, mem.ErrExhausted) {
				t.Errorf("Run with a %d-byte budget: %v, want ErrExhausted", tc.limit, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Run with a %d-byte budget for a %d-byte file: %v", tc.limit, size, err)
		}
		if d := oracle.Diff(oracle.Join(records), results(batches)); d != "" {
			t.Fatal(d)
		}
	}
}

// TestRun_sends_a_streamed_record_larger_than_a_batch_from_the_read_buffer:
// an empty-key record larger than the output batch, which the batch's
// reservation can't cover a clone of, is sent in a batch of its own from
// the read buffer that holds it, without a copy.
func TestRun_sends_a_streamed_record_larger_than_a_batch_from_the_read_buffer(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	big := strings.Repeat("v", 8<<20)
	records := []oracle.Record{rec(oracle.Left, "", "a"), rec(oracle.Left, "", big), rec(oracle.Left, "", "b")}
	p, totals := load(t, data, 1, records)
	if err := sp.spill(data, p.Empty(buffer.Left)); err != nil {
		t.Fatal(err)
	}
	const batch = 64 << 10
	// The read buffer grows to the large record and is all a join needs
	// besides the batch.
	l := int64(buffer.EncodedLen(nil, []byte(big)))
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: batch, Totals: totals, Parallelism: 1, Scratch: sp}
	for _, tc := range []struct {
		limit int64
		fits  bool
	}{{batch + l, true}, {batch + l - 1, false}} {
		pool := mem.NewPool(tc.limit)
		var sizes []int
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := Run(t.Context(), pool, sp.disk, p, opts, func(r *reconv1.JoinResponse) error {
			for _, res := range r.GetResults() {
				sizes = append(sizes, len(res.GetLeftOnly().GetValue()))
			}
			return nil
		})
		runtime.ReadMemStats(&after)
		if !tc.fits {
			if !errors.Is(err, mem.ErrExhausted) {
				t.Errorf("Run with a %d-byte budget: %v, want ErrExhausted", tc.limit, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Run with a %d-byte budget: %v", tc.limit, err)
		}
		if !slices.Equal(sizes, []int{1, len(big), 1}) {
			t.Errorf("sent values of %v bytes, want 1, %d and 1", sizes, len(big))
		}
		// The read buffer is allocated twice, at its start size and grown
		// to the record; a copy of the record would be a third.
		if alloc := after.TotalAlloc - before.TotalAlloc; alloc > uint64(l+l/2) {
			t.Errorf("the join allocated %d bytes for a %d-byte record: it was copied", alloc, l)
		}
		if pool.Used() != 0 {
			t.Errorf("%d bytes reserved after the join", pool.Used())
		}
	}
}

// TestRun_counts_a_small_maximum_match_size_within_the_budget: counting an
// oversized bucket's hashes down to a small maximum match size would take
// more memory than the unit budget, so the scan counts no finer than fits
// in a quarter of it. The bucket is still sub-partitioned, and a lighter key
// whose Match is too large is found when its sub-bucket is joined.
func TestRun_counts_a_small_maximum_match_size_within_the_budget(t *testing.T) {
	sp := newSpiller(t)
	data := mem.NewPool(1 << 30)
	var records []oracle.Record
	for i := range 2000 {
		records = append(records, oracle.Record{Side: oracle.Side(i % 2), Key: fmt.Appendf(nil, "k%d", i), Value: make([]byte, 40)})
	}
	pair := []oracle.Record{rec(oracle.Left, "pair", strings.Repeat("l", 100)), rec(oracle.Right, "pair", strings.Repeat("r", 100))}
	const budget = 64 << 10
	const maxMatch = 64
	if w := encoded(records); w*ExpansionFactor <= budget || (2*(w/maxMatch)+1)*counterSize <= budget {
		t.Fatalf("%d bytes of records: want an oversized bucket whose counters at %d bytes exceed the budget", w, maxMatch)
	}
	for _, tc := range []struct {
		name    string
		records []oracle.Record
	}{
		{"no matches", records},
		{"a Match too large", slices.Concat(records[:1000], pair, records[1000:])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, totals := load(t, data, 1, tc.records)
			pool := mem.NewPool(budget)
			ev := &events{}
			opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals, Parallelism: 1, MaxMatchBytes: maxMatch, Scratch: sp, Log: slog.New(ev)}
			for range 2 { // a retry ends the same way
				batches, err := collect(t.Context(), pool, sp.disk, p, opts)
				if hot := hotKeys(tc.records, budget, maxMatch); len(hot) > 0 {
					if hk := wantHotKey(t, err, hot); string(hk.Key) != "pair" {
						t.Fatalf("HotKeyError names %q, want pair", hk.Key)
					}
				} else {
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
					if d := oracle.Diff(oracle.Join(tc.records), results(batches)); d != "" {
						t.Fatal(d)
					}
				}
			}
			if d := ev.depths(); len(d) != 2 || d[0] != 1 {
				t.Errorf("sub-partition events at depths %v, want depth 1 for each join", d)
			}
			if pool.Used() != 0 || len(sp.scratchFiles(t)) != 0 {
				t.Errorf("after the joins: %d bytes reserved, scratch files %v", pool.Used(), sp.scratchFiles(t))
			}
		})
	}
}

// genSkewed draws records for a unit budget: keys of a few records, a hot
// key, a key with a large Match, empty keys, and keys that share a forced
// hash, one of which may be hot. Every encoded record takes at least 32
// bytes, and all of them at most 8 times the budget.
func genSkewed(t *rapid.T, budget int64) []hashed {
	value := func(lo, hi int) []byte {
		return bytes.Repeat([]byte{rapid.Byte().Draw(t, "fill")}, rapid.IntRange(lo, hi).Draw(t, "value size"))
	}
	side := func() oracle.Side { return oracle.Side(rapid.IntRange(0, 1).Draw(t, "side")) }
	var out []hashed
	add := func(key []byte, size int64, lo, hi int, forced bool, hash uint64) {
		for n := int64(0); n < size; {
			r := hashed{Record: oracle.Record{Side: side(), Key: key, Value: value(lo, hi)}, hash: hash, forced: forced}
			n += int64(buffer.EncodedLen(r.Key, r.Value))
			out = append(out, r)
		}
	}
	// Keys of a few records, prefixed 0 so that no key of another kind
	// equals them; twins differ in their last byte.
	var keys [][]byte
	for _, k := range rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 1, 10), 1, 300).Draw(t, "keys") {
		k = append([]byte{0}, k...)
		twin := bytes.Clone(k)
		twin[len(twin)-1] ^= 1
		keys = append(keys, k, twin)
	}
	size := budget + rapid.Int64Range(0, 2*budget).Draw(t, "bytes")
	for n := int64(0); n < size; {
		r := hashed{Record: oracle.Record{Side: side(), Key: rapid.SampledFrom(keys).Draw(t, "key"), Value: value(22, 200)}}
		n += int64(buffer.EncodedLen(r.Key, r.Value))
		out = append(out, r)
	}
	for range rapid.IntRange(0, 300).Draw(t, "empty keys") {
		out = append(out, hashed{Record: oracle.Record{Side: side(), Value: value(0, 200)}})
	}
	if rapid.IntRange(0, 3).Draw(t, "hot key") == 0 {
		add([]byte{1}, rapid.Int64Range(budget*6/10, 2*budget).Draw(t, "hot bytes"), 100, 200, false, 0)
	}
	if rapid.Bool().Draw(t, "large Match") {
		key := []byte{2}
		out = append(out, hashed{Record: oracle.Record{Side: oracle.Left, Key: key, Value: value(22, 200)}})
		add(key, rapid.Int64Range(budget/4, budget*45/100).Draw(t, "Match bytes"), 100, 200, false, 0)
	}
	if rapid.Bool().Draw(t, "shared hash") {
		hash := rapid.Uint64().Draw(t, "hash")
		for i := range rapid.IntRange(2, 20).Draw(t, "sharing keys") {
			add([]byte{3, byte(i)}, rapid.Int64Range(1, budget/20).Draw(t, "sharing bytes"), 22, 200, true, hash)
		}
		if rapid.IntRange(0, 3).Draw(t, "hot sharing key") == 0 {
			add([]byte{3, 255}, rapid.Int64Range(budget*6/10, budget).Draw(t, "hot sharing bytes"), 100, 200, true, hash)
		}
	}
	return rapid.Permutation(out).Draw(t, "order")
}

// TestRun_property_skewed_data_joins_or_fails_with_HOT_KEY joins skewed
// records, some spilled, under unit budgets of 32 to 96 KiB, which their
// buckets may exceed many times over. The join must equal the oracle, or,
// exactly when a key is hot, fail with a HotKeyError naming one; and a
// second join must do the same.
//
// The budgets hold whatever the join reserves: an output batch of at most
// a sixteenth of the unit budget, a unit of at most half the budget in
// records of at least 32 bytes, with as many bytes again of index at most,
// and the counting scan and the write buffers of sub-partitioning, at most
// half the budget for records of 8 times the budget.
func TestRun_property_skewed_data_joins_or_fails_with_HOT_KEY(t *testing.T) {
	sp := newSpiller(t)
	var examples, subpartitioned, hot int
	rapid.Check(t, func(t *rapid.T) {
		examples++
		parallelism := rapid.IntRange(1, 4).Draw(t, "parallelism")
		budget := rapid.Int64Range(32<<10, 96<<10).Draw(t, "unit budget")
		var maxMatch int64
		if rapid.Bool().Draw(t, "match limit") {
			maxMatch = rapid.Int64Range(budget/4, 2*budget).Draw(t, "max match")
		}
		records := genSkewed(t, budget)
		// Mostly a few buckets, which the records then exceed.
		n := uint32(rapid.OneOf(rapid.IntRange(1, 4), rapid.IntRange(1, 64)).Draw(t, "buckets"))

		data := mem.NewPool(1 << 30)
		p, totals := loadHashed(t, data, n, records)
		all := buffers(p)
		for range rapid.IntRange(0, 10).Draw(t, "spills") {
			if err := sp.spill(data, rapid.SampledFrom(all).Draw(t, "spilled")); err != nil {
				t.Fatal(err)
			}
		}

		pool := mem.NewPool(budget * int64(parallelism))
		ev := &events{}
		opts := Options{
			Filter:           NewFilter(nil),
			OutputBatchBytes: rapid.Int64Range(64, budget/16).Draw(t, "batch"),
			Totals:           totals,
			Parallelism:      parallelism,
			MaxMatchBytes:    maxMatch,
			Scratch:          sp,
			Log:              slog.New(ev),
		}
		diskBefore := sp.disk.Used()
		first, err := collect(context.Background(), pool, sp.disk, p, opts)
		second, err2 := collect(context.Background(), pool, sp.disk, p, opts)
		if pool.Used() != 0 || pool.Peak() > pool.Limit() || sp.disk.Used() != diskBefore || len(sp.scratchFiles(t)) > 0 {
			t.Fatalf("after the joins: %d reserved, peak %d of %d, %d bytes on disk (%d before), scratch files %v",
				pool.Used(), pool.Peak(), pool.Limit(), sp.disk.Used(), diskBefore, sp.scratchFiles(t))
		}
		if len(ev.depths()) > 0 {
			subpartitioned++
		}

		plain := plain(records)
		if want := hotKeys(plain, budget, maxMatch); len(want) > 0 {
			hot++
			hk := wantHotKey(t, err, want)
			if hk2, ok := errors.AsType[*HotKeyError](err2); !ok || !bytes.Equal(hk.Key, hk2.Key) || hk.Error() != hk2.Error() {
				t.Fatalf("the second join failed with %v, the first with %v", err2, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if err2 != nil {
			t.Fatalf("second Run: %v", err2)
		}
		if d := oracle.Diff(oracle.Join(plain), results(first)); d != "" {
			t.Fatal(d)
		}
		sameBatches(t, parallelism, first, second)
	})
	t.Logf("%d of %d examples sub-partitioned, %d had a hot key", subpartitioned, examples, hot)
	if subpartitioned == 0 || hot == 0 || hot == examples {
		t.Errorf("of %d examples, %d sub-partitioned and %d had a hot key; want some of each, and some without", examples, subpartitioned, hot)
	}
}
