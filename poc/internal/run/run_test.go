package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/z5labs/humus/humustest"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/oracle"
	"poc/internal/spill"
)

type env struct {
	host *humustest.Host
	pool *mem.Pool
	disk *spill.Disk
	m    *Manager
}

func newEnv(t *testing.T, limit int64) *env {
	t.Helper()
	return newEnvWithDisk(t, limit, 1<<30)
}

func newEnvWithDisk(t *testing.T, limit, diskLimit int64) *env {
	t.Helper()
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	pool := mem.NewPool(limit)
	disk := spill.NewDisk(h.FS(), diskLimit)
	return &env{host: h, pool: pool, disk: disk, m: NewManager(h.FS(), pool, disk, Config{
		DataDir:          h.Path("data"),
		MaxRuns:          3,
		MaxBuckets:       64,
		OutputBatchBytes: 4096,
		JoinParallelism:  2,
	})}
}

func (e *env) create(t *testing.T, buckets uint32) (string, *Run) {
	t.Helper()
	id, err := e.m.Create(buckets)
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.m.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return id, r
}

func records(side buffer.Side, keys ...string) []*reconv1.Record {
	var out []*reconv1.Record
	for i, k := range keys {
		out = append(out, &reconv1.Record{Key: []byte(k), Value: fmt.Appendf(nil, "%v-%d", side, i)})
	}
	return out
}

// ingest runs one committed stream of batches, alternating sides.
func ingest(t *testing.T, r *Run, batches ...[]*reconv1.Record) {
	t.Helper()
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Abandon()
	for i, b := range batches {
		if err := in.Append(buffer.Side(i%2), b, 100); err != nil {
			t.Fatal(err)
		}
	}
	if err := in.Commit(); err != nil {
		t.Fatal(err)
	}
}

func reason(err error) reconv1.ErrorReason {
	if re, ok := errors.AsType[*Error](err); ok {
		return re.Reason
	}
	return reconv1.ErrorReason_ERROR_REASON_UNSPECIFIED
}

func wantReason(t *testing.T, what string, err error, want reconv1.ErrorReason) {
	t.Helper()
	if got := reason(err); got != want {
		t.Errorf("%s: %v (%v), want %v", what, err, got, want)
	}
}

func joinAll(r *Run) ([]*reconv1.JoinResult, error) {
	var out []*reconv1.JoinResult
	err := r.Join(context.Background(), join.NewFilter(nil), func(resp *reconv1.JoinResponse) error {
		out = append(out, resp.GetResults()...)
		return nil
	})
	return out, err
}

func TestParseID(t *testing.T) {
	v7 := uuid.Must(uuid.NewV7()).String()
	if got, err := ParseID(&reconv1.RunId{Value: v7}); err != nil || got != v7 {
		t.Errorf("ParseID(%q) = %q, %v", v7, got, err)
	}
	for _, bad := range []string{
		"",
		"not-a-uuid",
		strings.ToUpper(v7),
		"{" + v7 + "}",
		"urn:uuid:" + v7,
		strings.ReplaceAll(v7, "-", ""),
		uuid.New().String(),     // version 4
		v7[:19] + "c" + v7[20:], // a variant other than RFC 9562's
	} {
		_, err := ParseID(&reconv1.RunId{Value: bad})
		wantReason(t, fmt.Sprintf("ParseID(%q)", bad), err, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
	}
	_, err := ParseID(nil)
	wantReason(t, "ParseID(nil)", err, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
}

func TestManager_Create_validates_and_limits(t *testing.T) {
	e := newEnv(t, 1<<30)
	for _, n := range []uint32{0, 65} {
		_, err := e.m.Create(n)
		wantReason(t, fmt.Sprintf("Create(%d)", n), err, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
	}
	var ids []string
	for range 3 {
		id, err := e.m.Create(64)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseID(&reconv1.RunId{Value: id}); err != nil {
			t.Errorf("Create returned %q, which ParseID rejects", id)
		}
		if _, err := os.Stat(e.host.Path("data/runs/" + id)); err != nil {
			t.Errorf("run directory: %v", err)
		}
		ids = append(ids, id)
	}
	_, err := e.m.Create(1)
	wantReason(t, "Create past RECON_MAX_RUNS", err, reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS)
	if err := e.m.Delete(ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := e.m.Create(1); err != nil {
		t.Errorf("Create after a Delete: %v", err)
	}
}

func TestRun_lifecycle(t *testing.T) {
	e := newEnv(t, 1<<30)
	id, r := e.create(t, 8)

	_, err := joinAll(r)
	wantReason(t, "Join before Seal", err, reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE)

	ingest(t, r, records(buffer.Left, "a", "b", "", "a"), records(buffer.Right, "a", "c", ""))
	ingest(t, r, records(buffer.Left, "d"))
	sealed, err := r.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if l, rt := sealed.GetLeft(), sealed.GetRight(); l.GetRecords() != 5 || l.GetEmptyKeyRecords() != 1 || rt.GetRecords() != 3 || rt.GetEmptyKeyRecords() != 1 || sealed.GetCommittedIngests() != 2 {
		t.Errorf("Seal = %v", sealed)
	}
	again, err := r.Seal()
	if err != nil || again != sealed {
		t.Errorf("second Seal = %v, %v; want the same totals", again, err)
	}
	_, err = r.BeginIngest()
	wantReason(t, "Ingest after Seal", err, reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE)

	got, err := joinAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 { // both a, left b, left "", right c, right "", left d
		t.Errorf("Join returned %d results, want 6", len(got))
	}

	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if e.pool.Used() != 0 {
		t.Errorf("pool has %d reserved after Delete", e.pool.Used())
	}
	if _, err := os.Stat(e.host.Path("data/runs/" + id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after Delete: %v", err)
	}
	_, err = e.m.Get(id)
	wantReason(t, "Get after Delete", err, reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND)
	_, err = r.Seal()
	wantReason(t, "Seal of a deleted run", err, reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND)
	if err := e.m.Delete(id); err != nil {
		t.Errorf("deleting again: %v", err)
	}
}

func TestRun_Seal_waits_for_open_streams(t *testing.T) {
	e := newEnv(t, 1<<30)
	_, r := e.create(t, 1)
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Seal()
	wantReason(t, "Seal with a stream open", err, reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS)
	if err := in.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Errorf("Seal after the commit: %v", err)
	}
}

func TestRun_a_failed_stream_fails_the_run_only_once_records_were_accepted(t *testing.T) {
	e := newEnv(t, 1<<30)
	_, r := e.create(t, 4)

	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	in.Abandon()
	if _, err := r.Seal(); err != nil {
		t.Fatalf("Seal after a stream that accepted nothing: %v", err)
	}

	_, r = e.create(t, 4)
	in, err = r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	other, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Append(buffer.Left, records(buffer.Left, "a"), 10); err != nil {
		t.Fatal(err)
	}
	in.Abandon()
	err = other.Append(buffer.Right, records(buffer.Right, "a"), 10)
	wantReason(t, "Append to a failed run", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	err = other.Commit()
	wantReason(t, "Commit to a failed run", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	other.Abandon()
	_, err = r.Seal()
	wantReason(t, "Seal", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	_, err = joinAll(r)
	wantReason(t, "Join", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	_, err = r.BeginIngest()
	wantReason(t, "Ingest", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
}

func TestRun_memory_exhausted(t *testing.T) {
	e := newEnv(t, 64<<10)
	_, r := e.create(t, 1)
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	if err := in.Append(buffer.Left, records(buffer.Left, "a"), 128<<10); !errors.Is(err, mem.ErrExhausted) {
		t.Fatalf("Append of a message larger than the budget: %v, want ErrExhausted", err)
	}
	in.Abandon()
	if _, err := r.Seal(); err != nil {
		t.Fatalf("a message rejected before any record must leave the run OPEN: %v", err)
	}

	// A record that doesn't fit next to its message fails once spilling
	// can't make room. A record was accepted before it, so the run fails.
	_, r = e.create(t, 1)
	in, err = r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Abandon()
	big := []*reconv1.Record{{Key: []byte("a")}, {Key: []byte("b"), Value: make([]byte, 8<<10)}}
	if err := in.Append(buffer.Left, big, 58<<10); !errors.Is(err, mem.ErrExhausted) {
		t.Fatalf("Append past the budget: %v, want ErrExhausted", err)
	}
	if e.pool.Used() > e.pool.Limit() {
		t.Errorf("pool has %d reserved, above its limit", e.pool.Used())
	}
	in.Abandon()
	_, err = r.Seal()
	wantReason(t, "Seal after records were accepted and the stream failed", err, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
}

func TestRun_one_Join_at_a_time(t *testing.T) {
	e := newEnv(t, 1<<30)
	_, r := e.create(t, 4)
	ingest(t, r, records(buffer.Left, "a", "b"), records(buffer.Right, "b", "c"))
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	inside := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error)
	go func() {
		done <- r.Join(context.Background(), join.NewFilter(nil), func(*reconv1.JoinResponse) error {
			close(inside)
			<-release
			return nil
		})
	}()
	<-inside
	_, err := joinAll(r)
	wantReason(t, "a second Join", err, reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := joinAll(r); err != nil {
		t.Errorf("a Join after the first ended: %v", err)
	}
}

func TestDelete_aborts_a_Join(t *testing.T) {
	e := newEnv(t, 1<<30)
	id, r := e.create(t, 4)
	var batch []*reconv1.Record
	for i := range 1000 {
		batch = append(batch, &reconv1.Record{Key: fmt.Appendf(nil, "%d", i), Value: make([]byte, 100)})
	}
	ingest(t, r, batch)
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	buffered := e.pool.Used()
	sent := 0
	err := r.Join(context.Background(), join.NewFilter(nil), func(*reconv1.JoinResponse) error {
		if sent++; sent == 1 {
			if err := e.m.Delete(id); err != nil {
				t.Error(err)
			}
			// The Join still reads the buffers, so they stay reserved.
			if e.pool.Used() <= buffered {
				t.Errorf("pool has %d reserved after Delete with the Join running, want more than the buffers' %d", e.pool.Used(), buffered)
			}
		}
		return nil
	})
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("Join: %v, want ErrAborted", err)
	}
	if sent != 1 {
		t.Errorf("sent %d batches, want the Join to stop after the first", sent)
	}
	if e.pool.Used() != 0 {
		t.Errorf("pool has %d reserved after Delete", e.pool.Used())
	}
}

func TestDelete_aborts_ingest(t *testing.T) {
	e := newEnv(t, 1<<30)
	id, r := e.create(t, 4)
	in, err := r.BeginIngest()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := r.WithContext(context.Background())
	defer cancel()
	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	if !errors.Is(context.Cause(ctx), ErrAborted) {
		t.Errorf("cause = %v, want ErrAborted", context.Cause(ctx))
	}
	if err := in.Append(buffer.Left, records(buffer.Left, "a"), 10); !errors.Is(err, ErrAborted) {
		t.Errorf("Append after Delete: %v, want ErrAborted", err)
	}
	if err := in.Commit(); !errors.Is(err, ErrAborted) {
		t.Errorf("Commit after Delete: %v, want ErrAborted", err)
	}
	in.Abandon()
	if e.pool.Used() != 0 {
		t.Errorf("pool has %d reserved", e.pool.Used())
	}
}

func TestRun_concurrent_streams_match_the_oracle(t *testing.T) {
	e := newEnv(t, 1<<30)
	_, r := e.create(t, 16)
	var (
		mu  sync.Mutex
		all []oracle.Record
		wg  sync.WaitGroup
	)
	for s := range 8 {
		wg.Go(func() {
			in, err := r.BeginIngest()
			if err != nil {
				t.Error(err)
				return
			}
			defer in.Abandon()
			for b := range 20 {
				side := buffer.Side((s + b) % 2)
				var batch []*reconv1.Record
				for i := range 50 {
					key := fmt.Appendf(nil, "k%d", (s*7+b*3+i)%300)
					if i%17 == 0 {
						key = nil
					}
					batch = append(batch, &reconv1.Record{Key: key, Value: fmt.Appendf(nil, "%d/%d/%d", s, b, i)})
				}
				if err := in.Append(side, batch, 1000); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, rec := range batch {
					all = append(all, oracle.Record{Side: oracle.Side(side), Key: rec.Key, Value: rec.Value})
				}
				mu.Unlock()
			}
			if err := in.Commit(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	sealed, err := r.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if got := sealed.GetLeft().GetRecords() + sealed.GetRight().GetRecords(); got != uint64(len(all)) || sealed.GetCommittedIngests() != 8 {
		t.Errorf("Seal = %v, want %d records from 8 streams", sealed, len(all))
	}
	got, err := joinAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(all), got); d != "" {
		t.Error(d)
	}
}

func TestManager_Close_deletes_every_run(t *testing.T) {
	e := newEnv(t, 1<<30)
	_, r := e.create(t, 2)
	ingest(t, r, records(buffer.Left, "a"))
	e.create(t, 2)
	if err := e.m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(e.host.Path("data/runs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || e.pool.Used() != 0 {
		t.Errorf("after Close: %d run directories, %d bytes reserved", len(entries), e.pool.Used())
	}
}

func TestManager_Delete_waits_for_a_Delete_in_progress(t *testing.T) {
	e := newEnv(t, 1<<30)
	id, r := e.create(t, 2)
	ingest(t, r, records(buffer.Left, "a"))

	// Holding the run's files for reading, as a spill does, stops the first
	// Delete before it removes them.
	r.files.RLock()
	first := make(chan error)
	go func() { first <- e.m.Delete(id) }()
	for !r.isDeleted() {
		runtime.Gosched()
	}
	second := make(chan error)
	go func() {
		err := e.m.Delete(id)
		if _, statErr := os.Stat(r.dir); !errors.Is(statErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("the second Delete returned with the run directory still there: %v", statErr))
		}
		second <- err
	}()
	select {
	case err := <-second:
		t.Errorf("the second Delete returned before the first was done: %v", err)
		r.files.RUnlock()
		<-first
		return
	case <-time.After(50 * time.Millisecond):
	}
	r.files.RUnlock()
	if err := <-first; err != nil {
		t.Error(err)
	}
	if err := <-second; err != nil {
		t.Error(err)
	}
	if _, err := e.m.Get(id); reason(err) != reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND {
		t.Errorf("Get after Delete: %v, want RUN_NOT_FOUND", err)
	}
}
