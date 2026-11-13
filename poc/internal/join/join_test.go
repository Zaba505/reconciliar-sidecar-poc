package join

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/z5labs/humus/humustest"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/oracle"
	"poc/internal/spill"
)

// load appends records to new Partitions with n buckets and returns them
// with their per-side totals.
func load(t interface {
	Helper()
	Fatal(...any)
}, pool *mem.Pool, n uint32, records []oracle.Record) (*buffer.Partitions, [2]uint64) {
	t.Helper()
	p := buffer.NewPartitions(n)
	var totals [2]uint64
	for _, r := range records {
		if err := p.Append(pool, buffer.Side(r.Side), r.Key, r.Value); err != nil {
			t.Fatal(err)
		}
		totals[r.Side]++
	}
	return p, totals
}

// collect runs a join and returns its batches.
func collect(ctx context.Context, pool *mem.Pool, disk *spill.Disk, p *buffer.Partitions, opts Options) ([]*reconv1.JoinResponse, error) {
	var batches []*reconv1.JoinResponse
	_, err := Run(ctx, pool, disk, p, opts, func(r *reconv1.JoinResponse) error {
		batches = append(batches, proto.Clone(r).(*reconv1.JoinResponse))
		return nil
	})
	return batches, err
}

// spiller writes spill files into a temporary directory.
type spiller struct {
	disk *spill.Disk
	dir  string
	n    int
}

func newSpiller(t testing.TB) *spiller {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"spill/.keep": nil}))
	return &spiller{disk: spill.NewDisk(h.FS(), 1<<30), dir: h.Path("spill")}
}

// spill spills b to the spiller's directory.
func (s *spiller) spill(pool *mem.Pool, b *buffer.Buffer) error {
	s.n++
	dir := filepath.Join(s.dir, fmt.Sprint(s.n))
	return b.Spill(pool, func(seq int, records int64, chunks [][]byte) (spill.File, error) {
		return s.disk.Write(filepath.Join(dir, fmt.Sprintf("%d.rec", seq)), records, chunks)
	})
}

// Write and Remove make the spiller the Scratch of a join, under its
// directory's scratch/.
func (s *spiller) Write(path string, records int64, chunks [][]byte) (spill.File, error) {
	return s.disk.Write(filepath.Join(s.dir, "scratch", path), records, chunks)
}

func (s *spiller) Remove(dir string, bytes int64) error {
	if err := os.RemoveAll(filepath.Join(s.dir, "scratch", dir)); err != nil {
		return err
	}
	s.disk.Release(bytes)
	return nil
}

// scratchFiles returns the files in the spiller's scratch directory.
func (s *spiller) scratchFiles(t interface{ Fatal(...any) }) []string {
	var files []string
	err := filepath.WalkDir(filepath.Join(s.dir, "scratch"), func(path string, d os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err == nil && !d.IsDir() {
			files = append(files, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// oversized reports whether a bucket of p would be oversized under the unit
// budget.
func oversized(p *buffer.Partitions, budget int64) bool {
	for i := range p.Buckets() {
		if (p.Bucket(buffer.Left, i).Bytes()+p.Bucket(buffer.Right, i).Bytes())*ExpansionFactor > budget {
			return true
		}
	}
	return false
}

// buffers returns every buffer of p.
func buffers(p *buffer.Partitions) []*buffer.Buffer {
	var out []*buffer.Buffer
	for s := range 2 {
		for i := range p.Buckets() {
			out = append(out, p.Bucket(buffer.Side(s), i))
		}
		out = append(out, p.Empty(buffer.Side(s)))
	}
	return out
}

// sameBatches fails unless two joins sent the same batches: in the same
// order with one worker, and in any order with more, whose units
// interleave.
func sameBatches(t interface{ Fatalf(string, ...any) }, parallelism int, first, second []*reconv1.JoinResponse) {
	if len(first) != len(second) {
		t.Fatalf("%d batches, then %d", len(first), len(second))
	}
	if parallelism <= 1 {
		for i := range first {
			if !proto.Equal(first[i], second[i]) {
				t.Fatalf("batch %d differs between two joins of the same data", i)
			}
		}
		return
	}
	a, b := marshalSorted(first), marshalSorted(second)
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("the batches of two joins of the same data differ: the %dth in sorted order", i)
		}
	}
}

// marshalSorted returns the encodings of batches, sorted.
func marshalSorted(batches []*reconv1.JoinResponse) [][]byte {
	out := make([][]byte, len(batches))
	for i, b := range batches {
		out[i], _ = proto.MarshalOptions{Deterministic: true}.Marshal(b)
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

func results(batches []*reconv1.JoinResponse) []*reconv1.JoinResult {
	var out []*reconv1.JoinResult
	for _, b := range batches {
		out = append(out, b.GetResults()...)
	}
	return out
}

func rec(side oracle.Side, key, value string) oracle.Record {
	return oracle.Record{Side: side, Key: []byte(key), Value: []byte(value)}
}

var sample = []oracle.Record{
	rec(oracle.Left, "a", "l1"),
	rec(oracle.Right, "a", "r1"),
	rec(oracle.Left, "a", "l2"),
	rec(oracle.Right, "a", "r2"),
	rec(oracle.Right, "a", "r3"),
	rec(oracle.Left, "b", "l3"),
	rec(oracle.Left, "b", "l4"),
	rec(oracle.Right, "c", "r4"),
	rec(oracle.Left, "", "l5"),
	rec(oracle.Right, "", "r5"),
	rec(oracle.Left, "", "l6"),
	rec(oracle.Left, "d\x00", "l7"),
	rec(oracle.Right, "d\x01", "r6"),
}

func TestRun_joins_like_the_oracle(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	p, totals := load(t, pool, 4, sample)
	batches, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1 << 20, Totals: totals})
	if err != nil {
		t.Fatal(err)
	}
	got := results(batches)
	if d := oracle.Diff(oracle.Join(sample), got); d != "" {
		t.Error(d)
	}
	for _, r := range got {
		if m := r.GetBoth(); m != nil && string(m.GetKey()) == "a" {
			if l := m.GetLeft(); string(l[0]) != "l1" || string(l[1]) != "l2" {
				t.Errorf("Match left = %q, want ingest order", l)
			}
			if r := m.GetRight(); string(r[0]) != "r1" || string(r[2]) != "r3" {
				t.Errorf("Match right = %q, want ingest order", r)
			}
		}
	}
	if pool.Used() != reservedBy(p) {
		t.Errorf("pool has %d reserved after the join, buffers hold %d", pool.Used(), reservedBy(p))
	}
}

// reservedBy returns the capacity of p's chunks.
func reservedBy(p *buffer.Partitions) int64 {
	var n int64
	for s := range 2 {
		for i := range p.Buckets() {
			for _, c := range p.Bucket(buffer.Side(s), i).Chunks() {
				n += int64(cap(c))
			}
		}
		for _, c := range p.Empty(buffer.Side(s)).Chunks() {
			n += int64(cap(c))
		}
	}
	return n
}

func TestRun_states_filter_still_counts(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	p, totals := load(t, pool, 3, sample)
	breaks := []reconv1.JoinState{reconv1.JoinState_JOIN_STATE_LEFT_ONLY, reconv1.JoinState_JOIN_STATE_RIGHT_ONLY}
	batches, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(breaks), OutputBatchBytes: 1 << 20, Totals: totals})
	if err != nil {
		t.Fatalf("a filtered join failed: %v", err)
	}
	if d := oracle.Diff(oracle.Filter(oracle.Join(sample), breaks...), results(batches)); d != "" {
		t.Error(d)
	}
}

func TestRun_batches_are_deterministic_and_bounded(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	var records []oracle.Record
	for i := range 3000 {
		records = append(records, oracle.Record{Side: oracle.Side(i % 2), Key: []byte{byte(i % 251), byte(i % 13)}, Value: bytes.Repeat([]byte{byte(i)}, i%40)})
	}
	p, totals := load(t, pool, 16, records)
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 2048, Totals: totals}
	first, err := collect(t.Context(), pool, nil, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := collect(t.Context(), pool, nil, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) < 10 || len(first) != len(second) {
		t.Fatalf("%d and %d batches; want the same number, at least 10", len(first), len(second))
	}
	for i := range first {
		if !proto.Equal(first[i], second[i]) {
			t.Fatalf("batch %d differs between two joins of the same data", i)
		}
		if n := proto.Size(first[i]); n > 2048 && len(first[i].GetResults()) > 1 {
			t.Errorf("batch %d is %d bytes with %d results, above the 2048 limit", i, n, len(first[i].GetResults()))
		}
	}
	if d := oracle.Diff(oracle.Join(records), results(first)); d != "" {
		t.Error(d)
	}
}

func TestRun_a_dropped_record_fails_the_control_totals(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	p, totals := load(t, pool, 2, sample)
	dropRecord.Store(true)
	_, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1 << 20, Totals: totals})
	if !errors.Is(err, ErrControlTotals) {
		t.Fatalf("Run: %v, want ErrControlTotals", err)
	}
	if _, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1 << 20, Totals: totals}); err != nil {
		t.Errorf("the next join: %v; the hook should drop only one record", err)
	}
}

func TestRun_fails_when_the_working_set_doesnt_fit(t *testing.T) {
	// The pool holds the buffers and the output batch, but is one byte
	// short of an index entry per keyed record.
	var keyed int64
	for _, r := range sample {
		if len(r.Key) > 0 {
			keyed++
		}
	}
	measure := mem.NewPool(1 << 30)
	load(t, measure, 1, sample)
	buffers := measure.Used()
	pool := mem.NewPool(buffers + 1024 + keyed*entrySize - 1)
	p, totals := load(t, pool, 1, sample)
	_, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals})
	if !errors.Is(err, mem.ErrExhausted) {
		t.Fatalf("Run: %v, want ErrExhausted", err)
	}
	if pool.Used() != buffers {
		t.Errorf("pool has %d reserved after the failed join, %d for the buffers", pool.Used(), buffers)
	}

	pool = mem.NewPool(buffers + 1024 + keyed*entrySize)
	p, totals = load(t, pool, 1, sample)
	if _, err := collect(t.Context(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals}); err != nil {
		t.Errorf("Run with one more byte: %v", err)
	}
}

func TestRun_stops_when_its_context_ends(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	p, totals := load(t, pool, 2, sample)
	cause := errors.New("deleted")
	ctx, cancel := context.WithCancelCause(t.Context())
	sent := 0
	_, err := Run(ctx, pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: 1, Totals: totals}, func(*reconv1.JoinResponse) error {
		sent++
		cancel(cause)
		return nil
	})
	if !errors.Is(err, cause) {
		t.Fatalf("Run: %v, want the context's cause", err)
	}
	if sent != 1 {
		t.Errorf("sent %d batches after the context ended, want 1", sent)
	}
}

// genRecords draws records with duplicates, skew, empty keys, binary keys
// and keys that differ only in their last byte.
func genRecords(t *rapid.T) []oracle.Record {
	var keys [][]byte
	for _, k := range rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 10), 1, 24).Draw(t, "keys") {
		keys = append(keys, k)
		if len(k) > 0 {
			twin := bytes.Clone(k)
			twin[len(twin)-1] ^= 1
			keys = append(keys, twin)
		}
	}
	n := rapid.IntRange(0, 600).Draw(t, "records")
	records := make([]oracle.Record, n)
	for i := range records {
		k := keys[0] // the hot key
		if rapid.IntRange(0, 3).Draw(t, "hot") > 0 {
			k = rapid.SampledFrom(keys).Draw(t, "key")
		}
		records[i] = oracle.Record{
			Side:  oracle.Side(rapid.IntRange(0, 1).Draw(t, "side")),
			Key:   k,
			Value: rapid.SliceOfN(rapid.Byte(), 0, 12).Draw(t, "value"),
		}
	}
	return records
}

func TestRun_property_matches_the_oracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		records := genRecords(t)
		n := uint32(rapid.IntRange(1, 64).Draw(t, "buckets"))
		var states []reconv1.JoinState
		for _, s := range []reconv1.JoinState{reconv1.JoinState_JOIN_STATE_LEFT_ONLY, reconv1.JoinState_JOIN_STATE_RIGHT_ONLY, reconv1.JoinState_JOIN_STATE_BOTH} {
			if rapid.Bool().Draw(t, "state") {
				states = append(states, s)
			}
		}
		opts := Options{
			Filter:           NewFilter(states),
			OutputBatchBytes: int64(rapid.IntRange(1, 4096).Draw(t, "batch")),
		}

		pool := mem.NewPool(1 << 30)
		p, totals := load(t, pool, n, records)
		opts.Totals = totals
		first, err := collect(context.Background(), pool, nil, p, opts)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if d := oracle.Diff(oracle.Filter(oracle.Join(records), states...), results(first)); d != "" {
			t.Fatal(d)
		}
		second, err := collect(context.Background(), pool, nil, p, opts)
		if err != nil {
			t.Fatalf("second Run: %v", err)
		}
		sameBatches(t, 1, first, second)
	})
}

func TestPlan_coalesces_contiguous_buckets_up_to_the_budget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sizes  []int64
		budget int64
		want   []Unit
	}{
		{"none", nil, 100, nil},
		{"all fit", []int64{1, 2, 3, 4}, 100, []Unit{{0, 4}}},
		{"greedy", []int64{10, 10, 10, 10, 10}, 40, []Unit{{0, 2}, {2, 4}, {4, 5}}},
		{"empty buckets join the unit", []int64{0, 0, 20, 0, 0, 1}, 40, []Unit{{0, 5}, {5, 6}}},
		{"oversized alone", []int64{1, 100, 1, 1}, 40, []Unit{{0, 1}, {1, 2}, {2, 4}}},
		{"oversized first", []int64{100, 1}, 40, []Unit{{0, 1}, {1, 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Plan(tc.sizes, tc.budget)
			if !slices.Equal(got, tc.want) {
				t.Errorf("Plan(%v, %d) = %v, want %v", tc.sizes, tc.budget, got, tc.want)
			}
		})
	}
}

func TestPlan_property_covers_every_bucket_once(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sizes := rapid.SliceOfN(rapid.Int64Range(0, 1000), 0, 300).Draw(t, "sizes")
		budget := rapid.Int64Range(1, 5000).Draw(t, "budget")
		units := Plan(sizes, budget)
		next := uint32(0)
		for i, u := range units {
			if u.Lo != next || u.Hi <= u.Lo {
				t.Fatalf("unit %d is %v after bucket %d", i, u, next)
			}
			var est int64
			for _, s := range sizes[u.Lo:u.Hi] {
				est += s * ExpansionFactor
			}
			if est > budget && u.Hi-u.Lo > 1 {
				t.Fatalf("unit %v estimates %d, above the budget %d", u, est, budget)
			}
			if i+1 < len(units) && est+sizes[u.Hi]*ExpansionFactor <= budget {
				t.Fatalf("unit %v stops before bucket %d, which fits", u, u.Hi)
			}
			next = u.Hi
		}
		if int(next) != len(sizes) {
			t.Fatalf("units cover %d of %d buckets", next, len(sizes))
		}
		if !slices.Equal(units, Plan(sizes, budget)) {
			t.Fatal("two plans of the same sizes differ")
		}
	})
}

func TestRun_reads_spill_files_and_gives_the_same_batches(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	sp := newSpiller(t)
	records := slices.Concat(sample, sample)
	inMemory, totals := load(t, pool, 3, records)
	spilled := buffer.NewPartitions(3)
	for i, r := range records {
		if err := spilled.Append(pool, buffer.Side(r.Side), r.Key, r.Value); err != nil {
			t.Fatal(err)
		}
		if i%5 == 4 {
			for _, b := range buffers(spilled) {
				if err := sp.spill(pool, b); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	var files int
	for _, b := range buffers(spilled) {
		files += len(b.Files())
	}
	if files == 0 {
		t.Fatal("nothing was spilled")
	}

	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 64, Totals: totals, Parallelism: 1}
	want, err := collect(t.Context(), pool, sp.disk, inMemory, opts)
	if err != nil {
		t.Fatal(err)
	}
	before := pool.Used()
	got, err := collect(t.Context(), pool, sp.disk, spilled, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameBatches(t, opts.Parallelism, want, got)
	if pool.Used() != before {
		t.Errorf("pool has %d reserved after the join, %d before", pool.Used(), before)
	}
	if d := oracle.Diff(oracle.Join(records), results(got)); d != "" {
		t.Error(d)
	}
}

func TestRun_a_missing_spill_file_fails_the_join(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	sp := newSpiller(t)
	p, totals := load(t, pool, 1, sample)
	if err := sp.spill(pool, p.Bucket(buffer.Left, 0)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p.Bucket(buffer.Left, 0).Files()[0].Path); err != nil {
		t.Fatal(err)
	}
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals}
	if _, err := collect(t.Context(), pool, sp.disk, p, opts); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Run: %v, want the file's ErrNotExist", err)
	}

	// The files of a deleted run are removed with it, so the abort is
	// reported, not the missing file.
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("deleted")
	cancel(cause)
	if _, err := collect(ctx, pool, sp.disk, p, opts); !errors.Is(err, cause) {
		t.Errorf("Run with its context done: %v, want the cause", err)
	}
}

// TestRun_property_spilling_changes_no_batch joins the same records twice,
// once in memory and once with buffers spilled at random points, under a
// budget that splits them into several units: the batches must be the
// same, and equal the oracle's results.
func TestRun_property_spilling_changes_no_batch(t *testing.T) {
	sp := newSpiller(t)
	rapid.Check(t, func(t *rapid.T) {
		records := genRecords(t)
		n := uint32(rapid.IntRange(1, 64).Draw(t, "buckets"))
		pool := mem.NewPool(1 << 30)
		inMemory, totals := load(t, pool, n, records)
		spilled := buffer.NewPartitions(n)
		all := buffers(spilled)
		for _, r := range records {
			if err := spilled.Append(pool, buffer.Side(r.Side), r.Key, r.Value); err != nil {
				t.Fatal(err)
			}
			if rapid.IntRange(0, 9).Draw(t, "spill") == 0 {
				if err := sp.spill(pool, rapid.SampledFrom(all).Draw(t, "buffer")); err != nil {
					t.Fatal(err)
				}
			}
		}

		var bytes, count int64
		for _, b := range all {
			bytes += b.Bytes()
			count += b.Records()
		}
		opts := Options{
			Filter:           NewFilter(nil),
			OutputBatchBytes: int64(rapid.IntRange(1, 4096).Draw(t, "batch")),
			Totals:           totals,
			Parallelism:      rapid.IntRange(1, 4).Draw(t, "parallelism"),
		}
		// The join reserves from its own pool, whose limit sets the unit
		// budget: it holds an output batch for each worker and the largest
		// working set possible, all of the records.
		// An oversized bucket is sub-partitioned, which takes write buffers
		// besides: 32 KiB a unit holds those, and then no key of these
		// records is hot.
		opts.Scratch = sp
		limit := int64(opts.Parallelism)*opts.OutputBatchBytes + bytes + count*entrySize + rapid.Int64Range(0, 4*bytes).Draw(t, "extra")
		if oversized(inMemory, limit/int64(opts.Parallelism)) {
			limit = max(limit, int64(opts.Parallelism)*32<<10)
		}
		joinPool := mem.NewPool(limit)
		want, err := collect(context.Background(), joinPool, sp.disk, inMemory, opts)
		if err != nil {
			t.Fatalf("Run in memory: %v", err)
		}
		got, err := collect(context.Background(), joinPool, sp.disk, spilled, opts)
		if err != nil {
			t.Fatalf("Run with spill files: %v", err)
		}
		sameBatches(t, opts.Parallelism, want, got)
		if d := oracle.Diff(oracle.Join(records), results(got)); d != "" {
			t.Fatal(d)
		}
		if joinPool.Used() != 0 {
			t.Fatalf("the join's pool has %d reserved after the joins", joinPool.Used())
		}
	})
}
