package run

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/z5labs/humus/humustest"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/oracle"
	"poc/internal/spill"
)

// message is one ingest message: records of one side.
type message struct {
	side    buffer.Side
	records []*reconv1.Record
	size    int64
}

// messages groups records into messages of at most per records, each of
// one side, and returns them with the records as the oracle takes them.
func messages(records []oracle.Record, per int) ([]message, []oracle.Record) {
	var msgs []message
	for i, r := range records {
		if i == 0 || r.Side != records[i-1].Side || len(msgs[len(msgs)-1].records) == per {
			msgs = append(msgs, message{side: buffer.Side(r.Side)})
		}
		m := &msgs[len(msgs)-1]
		m.records = append(m.records, &reconv1.Record{Key: r.Key, Value: r.Value})
	}
	for i := range msgs {
		msgs[i].size = int64(proto.Size(&reconv1.RecordBatch{Records: msgs[i].records}))
	}
	return msgs, records
}

// ingestStreams ingests msgs over streams streams open at once, dealing
// the messages out to them in turn.
func ingestStreams(r *Run, streams int, msgs []message) error {
	ins := make([]*Ingest, streams)
	for i := range ins {
		in, err := r.BeginIngest()
		if err != nil {
			return err
		}
		defer in.Abandon()
		ins[i] = in
	}
	for i, m := range msgs {
		if err := ins[i%streams].Append(m.side, m.records, m.size); err != nil {
			return err
		}
	}
	for _, in := range ins {
		if err := in.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func randomRecords(rng *rand.Rand, n, keys, valueSize int) []oracle.Record {
	out := make([]oracle.Record, n)
	side := oracle.Left
	for i := range out {
		if rng.IntN(50) == 0 {
			side = 1 - side
		}
		var key []byte
		if k := rng.IntN(keys + 1); k > 0 {
			key = fmt.Appendf(nil, "key-%d", k)
		}
		out[i] = oracle.Record{Side: side, Key: key, Value: make([]byte, rng.IntN(valueSize))}
		for j := range out[i].Value {
			out[i].Value[j] = byte(rng.Uint32())
		}
	}
	return out
}

func joinRun(r *Run) ([]*reconv1.JoinResult, error) {
	var out []*reconv1.JoinResult
	err := r.Join(context.Background(), join.NewFilter(nil), func(resp *reconv1.JoinResponse) error {
		out = append(out, proto.Clone(resp).(*reconv1.JoinResponse).GetResults()...)
		return nil
	})
	return out, err
}

var spillPath = regexp.MustCompile(`^(left|right)/([0-9]+|empty)/[0-9]+\.rec$`)

// spillFiles returns the spill files under a run's directory, relative to
// it, and fails the test on a file that doesn't follow the layout.
func spillFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if !spillPath.MatchString(rel) {
			t.Errorf("spill file %s isn't at <side>/<bucket>/<seq>.rec", rel)
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestRun_spills_past_the_budget_and_joins_like_the_oracle(t *testing.T) {
	e := newEnv(t, 256<<10)
	logs := &recorder{}
	e.m.log = slog.New(logs)
	id, r := e.create(t, 16)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(1, 1)), 20_000, 3000, 200), 200)
	if err := ingestStreams(r, 3, msgs); err != nil {
		t.Fatal(err)
	}
	files := spillFiles(t, r.dir)
	if len(files) == 0 || e.disk.Used() == 0 {
		t.Fatalf("%d spill files, %d bytes on disk; want 2 MiB of records to spill under a 256 KiB budget", len(files), e.disk.Used())
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	again, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(got, again); d != "" {
		t.Errorf("a second Join differs: %s", d)
	}
	if e.pool.Peak() > e.pool.Limit() {
		t.Errorf("pool peak %d, above the limit %d", e.pool.Peak(), e.pool.Limit())
	}
	finished := logs.find("join finished")
	if len(finished) != 2 {
		t.Fatalf("%d join finished records, want one per Join", len(finished))
	}
	attrs := finished[1]
	if attrs["recon.run_id"].String() != id || attrs["recon.join_units"].Int64() < 2 {
		t.Errorf("join finished record %v, want the run's ID and its units", attrs)
	}
	if attrs["recon.pool_peak_bytes"].Int64() != e.pool.Peak() || attrs["recon.disk_peak_bytes"].Int64() != e.disk.Peak() {
		t.Errorf("join finished record %v, want the pool's peak %d and the disk's %d", attrs, e.pool.Peak(), e.disk.Peak())
	}
	if attrs["recon.go_peak_bytes"].Int64() <= 0 {
		t.Errorf("join finished record %v, want the Go runtime's peak memory", attrs)
	}
	// Spilled records are read back at their size, in-memory ones take
	// their chunks' capacity, and each has an index entry besides.
	largest, working := attrs["recon.largest_unit_record_bytes"].Int64(), attrs["recon.largest_unit_working_set_bytes"].Int64()
	if f := attrs["recon.expansion_factor"].Float64(); f <= 1 || f > 2 || largest == 0 || working <= largest {
		t.Errorf("join finished record %v, want an expansion factor between 1 and 2, of the largest unit too", attrs)
	}

	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after Delete: %v", err)
	}
	if e.pool.Used() != 0 || e.disk.Used() != 0 {
		t.Errorf("after Delete: %d bytes reserved, %d on disk", e.pool.Used(), e.disk.Used())
	}
}

func TestRun_DISK_EXHAUSTED_during_ingest_fails_the_run(t *testing.T) {
	e := newEnvWithDisk(t, 64<<10, 128<<10)
	id, r := e.create(t, 4)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(2, 2)), 5000, 1000, 200), 50)
	err := ingestStreams(r, 1, msgs)
	if !errors.Is(err, spill.ErrDiskExhausted) {
		t.Fatalf("Ingest of 500 KiB with 128 KiB of disk: %v, want ErrDiskExhausted", err)
	}
	if e.disk.Used() > 128<<10 {
		t.Errorf("%d bytes on disk, above the limit", e.disk.Used())
	}
	_, err = r.Seal()
	wantReason(t, "Seal", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if e.pool.Used() != 0 || e.disk.Used() != 0 {
		t.Fatalf("after Delete: %d bytes reserved, %d on disk", e.pool.Used(), e.disk.Used())
	}

	// The disk is free again.
	_, r = e.create(t, 4)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(3, 3)), 1000, 300, 100), 50)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatalf("Ingest after the Delete: %v", err)
	}
	if e.disk.Used() == 0 {
		t.Error("the second run didn't spill")
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
}

func TestRun_DISK_EXHAUSTED_fails_the_reserving_run_only(t *testing.T) {
	e := newEnvWithDisk(t, 64<<10, 1)
	_, full := e.create(t, 1)
	msgs, want := messages(randomRecords(rand.New(rand.NewPCG(4, 4)), 100, 20, 200), 10)
	if err := ingestStreams(full, 1, msgs); err != nil {
		t.Fatal(err)
	}

	// Making room for this message means spilling the other run, which the
	// disk can't hold: the stream fails, and its run with it, though it
	// accepted no record.
	_, r := e.create(t, 1)
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	err = in.Append(buffer.Left, records(buffer.Left, "a"), e.pool.Limit()-e.pool.Used()+1)
	if !errors.Is(err, spill.ErrDiskExhausted) {
		t.Fatalf("Append: %v, want ErrDiskExhausted", err)
	}
	in.Abandon()
	_, err = r.Seal()
	wantReason(t, "Seal of the run whose stream hit the disk limit", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)

	if _, err := full.Seal(); err != nil {
		t.Fatalf("the other run: %v", err)
	}
	got, err := joinRun(full)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(want), got); d != "" {
		t.Error(d)
	}
}

func TestRun_DISK_EXHAUSTED_during_a_Join_leaves_the_run_SEALED(t *testing.T) {
	e := newEnvWithDisk(t, 64<<10, 1)
	// A unit takes the whole budget, so that the run's bucket isn't
	// oversized, which sub-partitioning would need the disk for.
	e.m.cfg.JoinParallelism = 1
	otherID, other := e.create(t, 1)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(5, 5)), 160, 50, 200), 10)
	if err := ingestStreams(other, 1, msgs); err != nil {
		t.Fatal(err)
	}
	otherBytes := e.pool.Used()

	// Many small records, so that the run's index takes the room the other
	// run's buffers hold.
	_, r := e.create(t, 1)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(6, 6)), 1000, 900, 8), 50)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatal(err)
	}
	need := e.m.cfg.OutputBatchBytes + int64(len(records))*16
	if e.disk.Used() != 0 || e.pool.Used()+need <= e.pool.Limit() || e.pool.Used()-otherBytes+need > e.pool.Limit() {
		t.Fatalf("%d bytes reserved, %d of them the other run's, %d on disk: want the Join's %d bytes to fit only once the other run is spilled",
			e.pool.Used(), otherBytes, e.disk.Used(), need)
	}
	sealed, err := r.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinRun(r); !errors.Is(err, spill.ErrDiskExhausted) {
		t.Fatalf("Join: %v, want ErrDiskExhausted", err)
	}
	if again, err := r.Seal(); err != nil || again != sealed {
		t.Errorf("Seal after the failed Join: %v, %v; want the run still SEALED", again, err)
	}

	// With the other run gone, the same Join fits.
	if err := e.m.Delete(otherID); err != nil {
		t.Fatal(err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatalf("Join after deleting the other run: %v", err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
}

// TestRun_Join_reads_its_buffers_where_they_are joins a run whose records
// fit in memory with their index and the output batch, but not twice over.
// The disk can't hold a byte, so the Join must not spill the buffers it is
// about to read.
func TestRun_Join_reads_its_buffers_where_they_are(t *testing.T) {
	e := newEnvWithDisk(t, 64<<10, 1)
	// A unit takes the whole budget, so that the bucket isn't oversized,
	// which sub-partitioning would need the disk for.
	e.m.cfg.JoinParallelism = 1
	_, r := e.create(t, 1)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(7, 7)), 270, 50, 200), 10)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatal(err)
	}
	size := r.parts.Bucket(buffer.Left, 0).Bytes() + r.parts.Bucket(buffer.Right, 0).Bytes()
	if used := e.pool.Used(); e.disk.Used() != 0 || used+size <= e.pool.Limit() || size*join.ExpansionFactor > e.pool.Limit() {
		t.Fatalf("%d bytes reserved, %d of records, %d on disk: want the records in memory, too many to load a copy, and within a unit's budget",
			used, size, e.disk.Used())
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	if files := spillFiles(t, r.dir); len(files) != 0 {
		t.Errorf("spill files after the Join: %v", files)
	}
}

// TestRun_concurrent_runs_spill_one_another ingests three runs at once,
// and then joins them one at a time: the unit budget is the pool's share of
// one Join's workers, so Joins of several runs at once may not fit.
func TestRun_concurrent_runs_spill_one_another(t *testing.T) {
	e := newEnv(t, 512<<10)
	var wg sync.WaitGroup
	var joins sync.Mutex
	for i := range 3 {
		wg.Go(func() {
			id, err := e.m.Create(uint32(8 << i))
			if err != nil {
				t.Error(err)
				return
			}
			r, _ := e.m.Get(id)
			msgs, records := messages(randomRecords(rand.New(rand.NewPCG(uint64(i), 9)), 10_000, 2000, 150), 100)
			var streams sync.WaitGroup
			for s := range 2 {
				streams.Go(func() {
					var part []message
					for j := s; j < len(msgs); j += 2 {
						part = append(part, msgs[j])
					}
					if err := ingestStreams(r, 1, part); err != nil {
						t.Errorf("run %d stream %d: %v", i, s, err)
					}
				})
			}
			streams.Wait()
			if _, err := r.Seal(); err != nil {
				t.Error(err)
				return
			}
			joins.Lock()
			got, err := joinRun(r)
			joins.Unlock()
			if err != nil {
				t.Errorf("run %d: Join: %v", i, err)
				return
			}
			if d := oracle.Diff(oracle.Join(records), got); d != "" {
				t.Errorf("run %d: %s", i, d)
			}
			if err := e.m.Delete(id); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if e.disk.Used() != 0 || e.pool.Used() != 0 {
		t.Errorf("after every Delete: %d bytes on disk, %d reserved", e.disk.Used(), e.pool.Used())
	}
}

// TestRun_concurrent_Joins_share_the_pool joins three sealed runs at once.
// Each Join plans its units for the whole pool, so Joins of several runs at
// once may not fit (§15 sizes join memory for one Join): a Join that fails
// must fail with ErrExhausted and leave its run SEALED, and joining it again
// on its own must match the oracle.
func TestRun_concurrent_Joins_share_the_pool(t *testing.T) {
	e := newEnv(t, 512<<10)
	type joined struct {
		id      string
		r       *Run
		records []oracle.Record
		sealed  *reconv1.SealRunResponse
		got     []*reconv1.JoinResult
		err     error
	}
	runs := make([]joined, 3)
	for i := range runs {
		id, r := e.create(t, uint32(8<<i))
		msgs, records := messages(randomRecords(rand.New(rand.NewPCG(uint64(i), 9)), 10_000, 2000, 150), 100)
		if err := ingestStreams(r, 2, msgs); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		sealed, err := r.Seal()
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = joined{id: id, r: r, records: records, sealed: sealed}
	}
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() { runs[i].got, runs[i].err = joinRun(runs[i].r) })
	}
	wg.Wait()
	var exhausted int
	for i, run := range runs {
		if run.err != nil {
			if !errors.Is(run.err, mem.ErrExhausted) {
				t.Errorf("run %d: Join: %v, want nil or %v", i, run.err, mem.ErrExhausted)
				continue
			}
			exhausted++
			if again, err := run.r.Seal(); err != nil || again != run.sealed {
				t.Errorf("run %d: Seal after a failed Join: %v, %v; want the run still SEALED", i, again, err)
				continue
			}
			if run.got, run.err = joinRun(run.r); run.err != nil {
				t.Errorf("run %d: Join on its own: %v", i, run.err)
				continue
			}
		}
		if d := oracle.Diff(oracle.Join(run.records), run.got); d != "" {
			t.Errorf("run %d: %s", i, d)
		}
		if err := e.m.Delete(run.id); err != nil {
			t.Error(err)
		}
	}
	t.Logf("%d of %d concurrent Joins ran out of memory", exhausted, len(runs))
	if e.disk.Used() != 0 || e.pool.Used() != 0 {
		t.Errorf("after every Delete: %d bytes on disk, %d reserved", e.disk.Used(), e.pool.Used())
	}
}

// genRecords draws records with duplicates, a hot key, empty keys and
// values of up to 300 bytes, so that a few thousand overflow a tiny
// budget.
func genRecords(t *rapid.T) []oracle.Record {
	keys := rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 12), 1, 200).Draw(t, "keys")
	n := rapid.IntRange(0, 1500).Draw(t, "records")
	records := make([]oracle.Record, n)
	side := oracle.Side(rapid.IntRange(0, 1).Draw(t, "first side"))
	for i := range records {
		if rapid.IntRange(0, 20).Draw(t, "switch") == 0 {
			side = 1 - side
		}
		k := keys[0] // the hot key
		if rapid.IntRange(0, 4).Draw(t, "hot") > 0 {
			k = rapid.SampledFrom(keys).Draw(t, "key")
		}
		records[i] = oracle.Record{Side: side, Key: k, Value: make([]byte, rapid.IntRange(0, 300).Draw(t, "value size"))}
		for j := range records[i].Value {
			records[i].Value[j] = byte(i + j)
		}
	}
	return records
}

// overFloor is the unit budget that sub-partitioning genRecords' records
// takes at most: half of it for a sub-unit's records, an index entry for
// each of at most 1500 records, an output batch of at most 4 KiB, a read
// buffer of a sixteenth of it, and the scan's counters or the write
// buffers, each within half of it.
const overFloor = 64 << 10

// minBudget returns the smallest budget the run can always be ingested
// and joined in by one worker, however its buffers are spilled. Below it, a
// message and its next chunk wouldn't fit even with everything else
// spilled, or a join unit wouldn't, or, if a bucket exceeds the unit
// budget, the unit budget would be under overFloor. The plan depends on the
// budget, so it is found by raising the budget until it holds.
func minBudget(records []oracle.Record, msgs []message, n uint32, parallelism int, batch, from int64) int64 {
	sizes := make([]int64, n)
	counts := make([]int64, n)
	var maxMsg int64
	for _, r := range records {
		if len(r.Key) == 0 {
			continue
		}
		b := buffer.Hash(r.Key) % uint64(n)
		sizes[b] += int64(buffer.EncodedLen(r.Key, r.Value))
		counts[b]++
	}
	for _, m := range msgs {
		maxMsg = max(maxMsg, m.size)
	}
	budget := max(from, 2*(maxMsg+max(4096, maxRecord(records))))
	for {
		need := int64(0)
		unit := budget / int64(parallelism)
		for _, u := range join.Plan(sizes, unit) {
			if u.Hi-u.Lo == 1 && sizes[u.Lo]*join.ExpansionFactor > unit {
				need = max(need, int64(parallelism)*overFloor)
				continue
			}
			var load int64
			for b := u.Lo; b < u.Hi; b++ {
				load += sizes[b] + counts[b]*16
			}
			need = max(need, batch+load)
		}
		if need <= budget {
			return budget
		}
		budget = need
	}
}

func maxRecord(records []oracle.Record) int64 {
	var n int64
	for _, r := range records {
		n = max(n, int64(buffer.EncodedLen(r.Key, r.Value)))
	}
	return n
}

// parallelNeed returns the most the Join of r, sealed in a pool of limit,
// takes with several workers, which join units at once: their output
// batches, and the most that as many units take together, since a unit
// that is loading can't spill what the others have pinned. A unit takes an
// index entry per record and its buffers' chunks at their capacity or
// their spill files read back, an oversized bucket its chunks and a unit
// budget, and the empty-key unit its chunks, a read buffer and a record.
// Spilling during the Join only turns chunks into smaller files, so the
// buffers as they are after Seal bound it. With one worker, minBudget's
// bound holds, and it returns 0.
func parallelNeed(r *Run, limit int64, parallelism int, batch, maxRecord int64) int64 {
	if parallelism == 1 {
		return 0
	}
	held := func(b *buffer.Buffer, files bool) int64 {
		var n int64
		for _, c := range b.Chunks() {
			n += int64(cap(c))
		}
		if files {
			for _, f := range b.Files() {
				n += f.Bytes
			}
		}
		return n
	}
	p := r.parts
	unit := limit / int64(parallelism)
	sizes := make([]int64, p.Buckets())
	for i := range sizes {
		sizes[i] = p.Bucket(buffer.Left, uint32(i)).Bytes() + p.Bucket(buffer.Right, uint32(i)).Bytes()
	}
	var peaks []int64
	for _, u := range join.Plan(sizes, unit) {
		if u.Hi-u.Lo == 1 && sizes[u.Lo]*join.ExpansionFactor > unit {
			peaks = append(peaks, unit+held(p.Bucket(buffer.Left, u.Lo), false)+held(p.Bucket(buffer.Right, u.Lo), false))
			continue
		}
		var peak int64
		for i := u.Lo; i < u.Hi; i++ {
			for s := range 2 {
				b := p.Bucket(buffer.Side(s), i)
				peak += held(b, true) + b.Records()*16
			}
		}
		peaks = append(peaks, peak)
	}
	if p.Empty(buffer.Left).Records()+p.Empty(buffer.Right).Records() > 0 {
		read := min(max(unit/16, 512), 1<<20)
		peaks = append(peaks, held(p.Empty(buffer.Left), false)+held(p.Empty(buffer.Right), false)+read+maxRecord)
	}
	slices.SortFunc(peaks, func(a, b int64) int { return cmp.Compare(b, a) })
	workers := min(parallelism, len(peaks))
	need := int64(workers) * batch
	for _, peak := range peaks[:workers] {
		need += peak
	}
	return need
}

// hotKeys returns the per-side counts of the keys of records whose bytes
// take more than the unit budget.
func hotKeys(records []oracle.Record, unit int64) map[string][2]uint64 {
	type stats struct {
		counts [2]uint64
		bytes  int64
	}
	all := map[string]*stats{}
	for _, r := range records {
		if len(r.Key) == 0 {
			continue
		}
		s := all[string(r.Key)]
		if s == nil {
			s = &stats{}
			all[string(r.Key)] = s
		}
		s.counts[r.Side]++
		s.bytes += int64(buffer.EncodedLen(r.Key, r.Value))
	}
	hot := map[string][2]uint64{}
	for k, s := range all {
		if s.bytes*join.ExpansionFactor > unit {
			hot[k] = s.counts
		}
	}
	return hot
}

func TestRun_property_tiny_budgets_spill_and_match_the_oracle(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	var examples, spilled, hot int
	var spilledBy [5]int // examples that spilled, by parallelism
	subpartitioned := &counter{msg: "bucket sub-partitioned"}
	rapid.Check(t, func(rt *rapid.T) {
		examples++
		records := genRecords(rt)
		n := uint32(rapid.IntRange(1, 64).Draw(rt, "buckets"))
		parallelism := rapid.IntRange(1, 4).Draw(rt, "parallelism")
		batch := rapid.Int64Range(64, 4096).Draw(rt, "batch")
		msgs, _ := messages(records, rapid.IntRange(1, 60).Draw(rt, "per message"))
		streams := rapid.IntRange(1, 3).Draw(rt, "streams")
		limit := minBudget(records, msgs, n, parallelism, batch, rapid.Int64Range(1, 1<<20).Draw(rt, "budget"))

		// With several workers, the budget is raised until the units that
		// may run at once fit, as the buffers turned out, and the run is
		// ingested again.
		var m *Manager
		var r *Run
		var id string
		var pool *mem.Pool
		var disk *spill.Disk
		for attempt := 0; ; attempt++ {
			pool = mem.NewPool(limit)
			disk = spill.NewDisk(h.FS(), 1<<30)
			m = NewManager(h.FS(), pool, disk, Config{
				DataDir:          h.Path(fmt.Sprintf("data/%d-%d", examples, attempt)),
				MaxRuns:          1,
				MaxBuckets:       64,
				OutputBatchBytes: batch,
				JoinParallelism:  parallelism,
				Logger:           slog.New(subpartitioned),
			})
			var err error
			if id, err = m.Create(n); err != nil {
				rt.Fatal(err)
			}
			r, _ = m.Get(id)
			if err := ingestStreams(r, streams, msgs); err != nil {
				rt.Fatalf("Ingest with a %d-byte budget: %v", limit, err)
			}
			need := parallelNeed(r, limit, parallelism, batch, maxRecord(records))
			if need <= limit {
				break
			}
			if err := m.Delete(id); err != nil {
				rt.Fatal(err)
			}
			limit = minBudget(records, msgs, n, parallelism, batch, need)
		}
		if disk.Used() > 0 {
			spilled++
			spilledBy[parallelism]++
		}
		sealed, err := r.Seal()
		if err != nil {
			rt.Fatal(err)
		}
		if got := sealed.GetLeft().GetRecords() + sealed.GetRight().GetRecords(); got != uint64(len(records)) {
			rt.Fatalf("sealed %d records, ingested %d", got, len(records))
		}
		got, err := joinRun(r)
		if want := hotKeys(records, limit/int64(parallelism)); len(want) > 0 {
			hot++
			hk, ok := errors.AsType[*join.HotKeyError](err)
			if !ok {
				rt.Fatalf("Join with a %d-byte budget: %v, want a HotKeyError naming one of %d hot keys", limit, err, len(want))
			}
			if counts, ok := want[string(hk.Key)]; !ok || counts != [2]uint64{hk.Left, hk.Right} {
				rt.Fatalf("HotKeyError for key %q with %d left and %d right records; it is hot: %v, with %v", hk.Key, hk.Left, hk.Right, ok, counts)
			}
			if again, err := r.Seal(); err != nil || again != sealed {
				rt.Fatalf("Seal after HOT_KEY: %v, %v; want the run still SEALED", again, err)
			}
			_, err = joinRun(r)
			if hk2, ok := errors.AsType[*join.HotKeyError](err); !ok || !bytes.Equal(hk2.Key, hk.Key) || hk2.Error() != hk.Error() {
				rt.Fatalf("the second Join: %v, the first: %v; want the same HOT_KEY", err, hk)
			}
		} else {
			if err != nil {
				rt.Fatalf("Join with a %d-byte budget: %v", limit, err)
			}
			if d := oracle.Diff(oracle.Join(records), got); d != "" {
				rt.Fatal(d)
			}
		}
		if err := m.Delete(id); err != nil {
			rt.Fatal(err)
		}
		if pool.Used() != 0 || disk.Used() != 0 {
			rt.Fatalf("after Delete: %d bytes reserved, %d on disk", pool.Used(), disk.Used())
		}
	})
	if spilledBy[1] == 0 || spilled == spilledBy[1] || hot == examples {
		t.Errorf("of %d examples, %d spilled (by parallelism 1 to 4: %v) and %d had a hot key; want some to spill with one worker and with more, and some without a hot key",
			examples, spilled, spilledBy[1:], hot)
	}
	// Few examples have a bucket of over 32 KiB, so sub-partitioning is
	// left to package join's property test to assert.
	t.Logf("%d of %d examples spilled (by parallelism 1 to 4: %v), %d had a hot key; %d sub-partitionings",
		spilled, examples, spilledBy[1:], hot, subpartitioned.n.Load())
}

// recorder keeps the attributes of every log record, by message.
type recorder struct {
	mu   sync.Mutex
	recs map[string][]map[string]slog.Value
}

func (h *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *recorder) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recorder) WithGroup(string) slog.Handler            { return h }

func (h *recorder) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]slog.Value{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.recs == nil {
		h.recs = map[string][]map[string]slog.Value{}
	}
	h.recs[r.Message] = append(h.recs[r.Message], attrs)
	return nil
}

func (h *recorder) find(msg string) []map[string]slog.Value {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.recs[msg]
}

// counter counts the log records with message msg.
type counter struct {
	msg string
	n   atomic.Int64
}

func (c *counter) Enabled(context.Context, slog.Level) bool { return true }
func (c *counter) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *counter) WithGroup(string) slog.Handler            { return c }

func (c *counter) Handle(_ context.Context, r slog.Record) error {
	if r.Message == c.msg {
		c.n.Add(1)
	}
	return nil
}
