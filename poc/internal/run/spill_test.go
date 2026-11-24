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
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/z5labs/humus/humustest"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/oracle"
	"poc/internal/record"
	"poc/internal/spill"
)

// message is one ingest message: records of one side.
type message struct {
	side    record.Side
	records []*reconv1.Record
	size    int64
}

// messages groups records into messages of at most per records, each of
// one side, and returns them with the records as the oracle takes them.
func messages(records []oracle.Record, per int) ([]message, []oracle.Record) {
	var msgs []message
	for i, r := range records {
		if i == 0 || r.Side != records[i-1].Side || len(msgs[len(msgs)-1].records) == per {
			msgs = append(msgs, message{side: record.Side(r.Side)})
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

func joinRun(r *Run) ([]*reconv1.JoinResult, error) { return joinAll(r) }

var runFilePath = regexp.MustCompile(`^[0-9]{6}\.run$`)

// runFiles returns the files in a run's directory, and fails the test on a
// file that doesn't follow the layout.
func runFileNames(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if !runFilePath.MatchString(rel) {
			t.Errorf("run file %s isn't at <seq>.run", rel)
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
	files := runFileNames(t, r.dir)
	if len(files) == 0 || e.disk.Used() == 0 {
		t.Fatalf("%d run files, %d bytes on disk; want 2 MiB of records to spill under a 256 KiB budget", len(files), e.disk.Used())
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	// Seal wrote the rest as one more file, and merged files until one
	// worker reads them.
	sealed := logs.find("run sealed")
	if len(sealed) != 1 || sealed[0]["recon.run_files"].Int64() != int64(len(r.buf.Files())) || sealed[0]["recon.spills"].Int64() < int64(len(files)+1) {
		t.Errorf("run sealed records %v, want one with the run's %d files, after %d spills or more", sealed, len(r.buf.Files()), len(files)+1)
	}
	if got := runFileNames(t, r.dir); len(got) != len(r.buf.Files()) {
		t.Errorf("%d files in the run's directory, the run has %d", len(got), len(r.buf.Files()))
	}
	if e.inUse() != spill.FilesMem(r.buf.Files()) {
		t.Errorf("a sealed run that spilled holds %d bytes, want its files' tables only", e.inUse())
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
	if attrs["recon.run_id"].String() != id || attrs["recon.join_files"].Int64() != int64(len(r.buf.Files())) || attrs["recon.join_workers"].Int64() < 1 || attrs["recon.join_tasks"].Int64() < 2 {
		t.Errorf("join finished record %v, want the run's ID, files, workers and tasks", attrs)
	}
	if attrs["recon.pool_peak_bytes"].Int64() != e.pool.Peak() || attrs["recon.disk_peak_bytes"].Int64() != e.disk.Peak() {
		t.Errorf("join finished record %v, want the pool's peak %d and the disk's %d", attrs, e.pool.Peak(), e.disk.Peak())
	}
	if attrs["recon.go_peak_bytes"].Int64() <= 0 || attrs["recon.join_read_buffer_bytes"].Int64() < spill.Align {
		t.Errorf("join finished record %v, want the Go runtime's peak memory and the read buffers' size", attrs)
	}

	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after Delete: %v", err)
	}
	if e.inUse() != 0 || e.disk.Used() != 0 {
		t.Errorf("after Delete: %d bytes reserved, %d on disk", e.inUse(), e.disk.Used())
	}
}

func TestRun_a_run_that_never_spilled_stays_in_memory(t *testing.T) {
	e := newEnvWithDisk(t, 1<<20, 1)
	_, r := e.create(t, 4)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(2, 1)), 2000, 300, 100), 50)
	if err := ingestStreams(r, 2, msgs); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatalf("Seal of a run in memory with no disk: %v", err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	if files := runFileNames(t, r.dir); len(files) != 0 {
		t.Errorf("run files of a run that fits in memory: %v", files)
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
	if e.inUse() != 0 || e.disk.Used() != 0 {
		t.Fatalf("after Delete: %d bytes reserved, %d on disk", e.inUse(), e.disk.Used())
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
	err = in.Append(record.Left, records(record.Left, "a"), e.pool.Limit()-e.pool.Used()+e.pool.Idle()+1)
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

// TestRun_DISK_EXHAUSTED_at_Seal_leaves_the_run_OPEN fills the disk with
// another run's files, so that a run that spilled can't write its last
// file. Nothing is lost, so the run stays OPEN, and once the disk has room
// Seal succeeds.
func TestRun_DISK_EXHAUSTED_at_Seal_leaves_the_run_OPEN(t *testing.T) {
	e := newEnvWithDisk(t, 128<<10, 1<<20)
	_, r := e.create(t, 4)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(5, 4)), 3000, 500, 100), 50)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatal(err)
	}
	if e.disk.Used() == 0 {
		t.Fatal("the run didn't spill")
	}
	// Another run takes what is left of the disk.
	otherID, _ := e.create(t, 1)
	filler := e.disk.Limit() - e.disk.Used()
	if err := e.disk.Reserve(filler); err != nil {
		t.Fatal(err)
	}
	_, err := r.Seal()
	if !errors.Is(err, spill.ErrDiskExhausted) {
		t.Fatalf("Seal with a full disk: %v, want ErrDiskExhausted", err)
	}
	if _, err := joinRun(r); reason(err) != reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE {
		t.Errorf("Join after a failed Seal: %v, want the run not sealed", err)
	}
	// More records may come, and the run seals once the disk has room.
	more, extra := messages(randomRecords(rand.New(rand.NewPCG(6, 4)), 50, 500, 100), 50)
	if err := ingestStreams(r, 1, more); err != nil {
		t.Fatalf("Ingest after a failed Seal: %v", err)
	}
	e.disk.Release(filler)
	if err := e.m.Delete(otherID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatalf("Seal once the disk has room: %v", err)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(append(records, extra...)), got); d != "" {
		t.Error(d)
	}
}

func TestRun_DISK_EXHAUSTED_during_a_Join_leaves_the_run_SEALED(t *testing.T) {
	e := newEnvWithDisk(t, 64<<10, 1)
	e.m.cfg.JoinParallelism = 1
	otherID, other := e.create(t, 1)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(5, 5)), 100, 50, 200), 10)
	if err := ingestStreams(other, 1, msgs); err != nil {
		t.Fatal(err)
	}
	_, r := e.create(t, 1)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(6, 6)), 60, 50, 200), 10)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatal(err)
	}
	if e.disk.Used() != 0 {
		t.Fatalf("%d bytes on disk before the Join", e.disk.Used())
	}
	sealed, err := r.Seal()
	if err != nil {
		t.Fatal(err)
	}
	// An output batch that fits only once the other run is spilled, which
	// the disk can't hold a byte of.
	room := e.pool.Limit() - e.pool.Used() + e.pool.Idle()
	e.m.cfg.OutputBatchBytes = room + other.buf.SpillSize()/2
	if _, err := joinRun(r); !errors.Is(err, spill.ErrDiskExhausted) {
		t.Fatalf("Join: %v, want ErrDiskExhausted", err)
	}
	if again, err := r.Seal(); err != nil || again != sealed {
		t.Errorf("Seal after the failed Join: %v, %v; want the run still SEALED", again, err)
	}

	// With the other run gone, a Join with the usual batches fits, from
	// memory.
	if err := e.m.Delete(otherID); err != nil {
		t.Fatal(err)
	}
	e.m.cfg.OutputBatchBytes = 4096
	got, err := joinRun(r)
	if err != nil {
		t.Fatalf("Join after deleting the other run: %v", err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
}

// TestRun_concurrent_runs_spill_one_another ingests and seals three runs
// at once, and then joins them one at a time.
func TestRun_concurrent_runs_spill_one_another(t *testing.T) {
	e := newEnv(t, 512<<10)
	type sealed struct {
		id      string
		r       *Run
		records []oracle.Record
	}
	runs := make([]sealed, 3)
	var wg sync.WaitGroup
	for i := range runs {
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
				t.Errorf("run %d: Seal: %v", i, err)
				return
			}
			runs[i] = sealed{id, r, records}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	for i, run := range runs {
		got, err := joinRun(run.r)
		if err != nil {
			t.Fatalf("run %d: Join: %v", i, err)
		}
		if d := oracle.Diff(oracle.Join(run.records), got); d != "" {
			t.Errorf("run %d: %s", i, d)
		}
		if err := e.m.Delete(run.id); err != nil {
			t.Error(err)
		}
	}
	if e.disk.Used() != 0 || e.inUse() != 0 {
		t.Errorf("after every Delete: %d bytes on disk, %d reserved", e.disk.Used(), e.inUse())
	}
}

// TestRun_concurrent_Joins_share_the_pool joins three sealed runs at once.
// Each Join sizes its read buffers from what the pool has free, so Joins
// of several runs at once read with smaller buffers: they must all match
// the oracle, or fail with ErrExhausted and leave the run SEALED.
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
	if e.disk.Used() != 0 || e.inUse() != 0 {
		t.Errorf("after every Delete: %d bytes on disk, %d reserved", e.disk.Used(), e.inUse())
	}
}

// TestRun_Delete_during_a_Join_of_files deletes a run whose Join is
// stuck in a send: Delete doesn't wait for it, the Join ends with
// ErrAborted once the send returns, and frees the run's memory.
func TestRun_Delete_during_a_Join_of_files(t *testing.T) {
	e := newEnv(t, 256<<10)
	id, r := e.create(t, 4)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(8, 8)), 10_000, 2000, 100), 100)
	if err := ingestStreams(r, 2, msgs); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	stuck := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	go func() {
		done <- r.Join(context.Background(), join.NewFilter(nil), func(*reconv1.JoinResponse) error {
			once.Do(func() { close(stuck) })
			<-release
			return nil
		})
	}()
	<-stuck
	deleted := make(chan error, 1)
	go func() { deleted <- e.m.Delete(id) }()
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Delete waits for a Join stuck in a send")
	}
	if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after Delete: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrAborted) {
		t.Errorf("Join: %v, want ErrAborted", err)
	}
	if e.inUse() != 0 || e.disk.Used() != 0 {
		t.Errorf("after the Join ended: %d bytes reserved, %d on disk", e.inUse(), e.disk.Used())
	}
}

// TestRun_Seal_merges_files_a_Join_couldnt_read_at_once ingests 20 times
// a small budget, so that the run has more files than one worker reads
// with read buffers of 16 KiB.
func TestRun_Seal_merges_files_a_Join_couldnt_read_at_once(t *testing.T) {
	e := newEnv(t, 512<<10)
	logs := &recorder{}
	e.m.log = slog.New(logs)
	_, r := e.create(t, 4)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(9, 9)), 100_000, 20_000, 200), 100)
	if err := ingestStreams(r, 2, msgs); err != nil {
		t.Fatal(err)
	}
	before := len(r.buf.Files())
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	sealed := logs.find("run sealed")
	if len(sealed) != 1 || sealed[0]["recon.merges"].Int64() == 0 || int(sealed[0]["recon.run_files"].Int64()) >= before {
		t.Fatalf("run sealed record %v after %d spills, want merges down to fewer files", sealed, before)
	}
	if names := runFileNames(t, r.dir); len(names) != len(r.buf.Files()) {
		t.Errorf("%d files in the directory, the run has %d: merged files must be removed", len(names), len(r.buf.Files()))
	}
	var size int64
	for _, f := range r.buf.Files() {
		size += f.Bytes
	}
	if e.disk.Used() != size {
		t.Errorf("%d bytes on disk, the run's files have %d", e.disk.Used(), size)
	}
	got, err := joinRun(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	t.Logf("%d files merged to %d in %d merges", before, len(r.buf.Files()), sealed[0]["recon.merges"].Int64())
}

// TestRun_Delete_during_Seal cancels Seal's merge.
func TestRun_Delete_during_Seal(t *testing.T) {
	e := newEnv(t, 512<<10)
	id, r := e.create(t, 4)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(10, 9)), 100_000, 20_000, 200), 100)
	if err := ingestStreams(r, 2, msgs); err != nil {
		t.Fatal(err)
	}
	sealed := make(chan error, 1)
	go func() {
		_, err := r.Seal()
		sealed <- err
	}()
	for {
		r.mu.Lock()
		sealing := r.sealing
		r.mu.Unlock()
		if sealing {
			break
		}
		runtime.Gosched()
	}
	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	err := <-sealed
	if err != nil && reason(err) != reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND {
		t.Errorf("Seal of a deleted run: %v, want nil or RUN_NOT_FOUND", err)
	}
	if e.inUse() != 0 || e.disk.Used() != 0 {
		t.Errorf("after Delete: %d bytes reserved, %d on disk", e.inUse(), e.disk.Used())
	}
	if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after Delete: %v", err)
	}
}

// genRecords draws records with duplicates, a hot key, empty keys and
// values of up to 300 bytes, so that a few thousand overflow a tiny
// budget.
func genRecords(t *rapid.T) []oracle.Record {
	keys := rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 12), 1, 200).Draw(t, "keys")
	n := rapid.IntRange(0, 4000).Draw(t, "records")
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

// firstHotKey returns the Match of results larger than maxMatch that comes
// first in the plan: the lowest hash, then key.
func firstHotKey(results []*reconv1.JoinResult, maxMatch int64) *reconv1.Match {
	var first *reconv1.Match
	for _, r := range results {
		m := r.GetBoth()
		if m == nil || int64(proto.Size(m)) <= maxMatch {
			continue
		}
		if first == nil || cmp.Or(cmp.Compare(record.Hash(m.GetKey()), record.Hash(first.GetKey())), bytes.Compare(m.GetKey(), first.GetKey())) < 0 {
			first = m
		}
	}
	return first
}

// TestRun_property_tiny_budgets_spill_and_match_the_oracle ingests on
// several streams under budgets from 128 KiB, joins with up to four
// workers, and checks the results against the oracle: HOT_KEY exactly when
// a Match is over the maximum, and the first such key in the plan.
func TestRun_property_tiny_budgets_spill_and_match_the_oracle(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	var examples, spilled, hot int
	rapid.Check(t, func(rt *rapid.T) {
		examples++
		records := genRecords(rt)
		parallelism := rapid.IntRange(1, 4).Draw(rt, "parallelism")
		batch := rapid.Int64Range(64, 4096).Draw(rt, "batch")
		msgs, _ := messages(records, rapid.IntRange(1, 60).Draw(rt, "per message"))
		streams := rapid.IntRange(1, 3).Draw(rt, "streams")
		limit := rapid.Int64Range(128<<10, 512<<10).Draw(rt, "budget")
		pool := mem.NewPool(limit)
		disk := spill.NewDisk(h.FS(), 1<<30)
		m, err := NewManager(h.FS(), pool, disk, Config{
			DataDir:          h.Path(fmt.Sprintf("data/%d", examples)),
			MaxRuns:          1,
			MaxBuckets:       64,
			OutputBatchBytes: batch,
			JoinParallelism:  parallelism,
			SpillCodec:       testCodec,
		})
		if err != nil {
			rt.Fatal(err)
		}
		id, err := m.Create(uint32(rapid.IntRange(1, 64).Draw(rt, "buckets")))
		if err != nil {
			rt.Fatal(err)
		}
		r, _ := m.Get(id)
		if err := ingestStreams(r, streams, msgs); err != nil {
			rt.Fatalf("Ingest with a %d-byte budget: %v", limit, err)
		}
		if disk.Used() > 0 {
			spilled++
		}
		sealed, err := r.Seal()
		if err != nil {
			rt.Fatal(err)
		}
		if got := sealed.GetLeft().GetRecords() + sealed.GetRight().GetRecords(); got != uint64(len(records)) {
			rt.Fatalf("sealed %d records, ingested %d", got, len(records))
		}
		want := oracle.Join(records)
		maxMatch := join.MaxMatch(limit, r.joinOptions(join.Filter{}, [2]uint64{}))
		got, err := joinRun(r)
		if hk := firstHotKey(want, maxMatch); hk != nil {
			hot++
			for range 2 {
				he, ok := errors.AsType[*join.HotKeyError](err)
				if !ok || !bytes.Equal(he.Key, hk.GetKey()) || he.Left != uint64(len(hk.GetLeft())) || he.Right != uint64(len(hk.GetRight())) {
					rt.Fatalf("Join with a %d-byte budget: %v, want HOT_KEY for %q with %d and %d records", limit, err, hk.GetKey(), len(hk.GetLeft()), len(hk.GetRight()))
				}
				if again, err := r.Seal(); err != nil || again != sealed {
					rt.Fatalf("Seal after HOT_KEY: %v, %v; want the run still SEALED", again, err)
				}
				_, err = joinRun(r)
			}
		} else {
			if err != nil {
				rt.Fatalf("Join with a %d-byte budget: %v", limit, err)
			}
			if d := oracle.Diff(want, got); d != "" {
				rt.Fatal(d)
			}
		}
		if pool.Peak() > pool.Limit() {
			rt.Fatalf("pool peak %d above its limit", pool.Peak())
		}
		if err := m.Delete(id); err != nil {
			rt.Fatal(err)
		}
		if pool.Used()-pool.Idle() != m.reserved || disk.Used() != 0 {
			rt.Fatalf("after Delete: %d bytes reserved, %d on disk", pool.Used()-pool.Idle()-m.reserved, disk.Used())
		}
	})
	if spilled == 0 || hot == examples || hot == 0 {
		t.Errorf("of %d examples, %d spilled and %d had a hot key; want some to spill, some with a hot key and some without", examples, spilled, hot)
	}
	t.Logf("%d of %d examples spilled, %d had a hot key", spilled, examples, hot)
}

// TestRun_live_heap_stays_near_the_pool joins 64 MiB of records under an
// 8 MiB budget and samples the live heap while it runs: what the Join
// allocates is reserved, so the heap grows by about the budget, not the
// data.
func TestRun_live_heap_stays_near_the_pool(t *testing.T) {
	if testing.Short() {
		t.Skip("joins 64 MiB")
	}
	e := newEnv(t, 8<<20)
	_, r := e.create(t, 4)
	rng := rand.New(rand.NewPCG(11, 11))
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	var left, right uint64
	for sent := 0; sent < 64<<20; {
		recs := randomRecords(rng, 500, 200_000, 250)
		msgs, _ := messages(recs, 500)
		for _, m := range msgs {
			if err := in.Append(m.side, m.records, m.size); err != nil {
				t.Fatal(err)
			}
			sent += int(m.size)
			if m.side == record.Left {
				left += uint64(len(m.records))
			} else {
				right += uint64(len(m.records))
			}
		}
	}
	if err := in.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	// Idle blocks are reserved, live memory, which the Join takes over:
	// drop them, so that the heap grows by what the Join adds.
	e.pool.DropIdle()
	defer debug.SetGCPercent(debug.SetGCPercent(10))
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	base := int64(sample[0].Value.Uint64())
	var peak atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
				metrics.Read(sample)
				if v := int64(sample[0].Value.Uint64()); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()
	var results uint64
	err = r.Join(t.Context(), join.NewFilter(nil), func(resp *reconv1.JoinResponse) error {
		results += uint64(len(resp.GetResults()))
		return nil
	})
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	growth := peak.Load() - base
	t.Logf("live heap grew by %d MiB over %d MiB during the Join of %d records, %d results, pool peak %d MiB", growth>>20, base>>20, left+right, results, e.pool.Peak()>>20)
	if growth > 2*e.pool.Limit() {
		t.Errorf("the live heap grew by %d bytes during the Join, more than twice the %d-byte budget", growth, e.pool.Limit())
	}
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
