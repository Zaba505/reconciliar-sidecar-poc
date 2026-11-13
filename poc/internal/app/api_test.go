package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/grpc/grpcserver/grpcservertest"
	"github.com/z5labs/humus/humustest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

// testApp is the App running in-process on a humustest Host, inside the
// calling test's synctest bubble.
type testApp struct {
	env    *testEnv
	client reconv1.ReconcilerClient
	stop   func(t *testing.T)
}

func startApp(t *testing.T, changes map[string]string) *testApp {
	t.Helper()
	e := newTestEnv(t)
	env := map[string]string{"RECON_LISTEN": ":7443"}
	maps.Copy(env, changes)
	p, exit := humustest.Start(t, initForTest(e.cgroup),
		humustest.WithApp(humus.WithHost(e.host.Host()), humus.WithTelemetry(humustest.NewTelemetry().Builder())),
		humustest.WithEnv(e.with(env)),
	)
	synctest.Wait()
	return &testApp{
		env:    e,
		client: reconv1.NewReconcilerClient(grpcservertest.NewClient(t, e.host, "localhost:7443", e.ca.Pool())),
		stop: func(t *testing.T) {
			t.Helper()
			p.Signal(syscall.SIGTERM)
			if st := <-exit; st.Code() != 0 {
				t.Errorf("exit %d: %v", st.Code(), st.Cause())
			}
		},
	}
}

func (a *testApp) create(t *testing.T, buckets uint32) *reconv1.RunId {
	t.Helper()
	resp, err := a.client.CreateRun(t.Context(), &reconv1.CreateRunRequest{BucketCount: buckets})
	if err != nil {
		t.Fatal(err)
	}
	return resp.GetRunId()
}

func (a *testApp) runDir(id *reconv1.RunId) string {
	return a.env.host.Path("data/runs/" + id.GetValue())
}

// batch returns an IngestRequest of records for side.
func batch(id *reconv1.RunId, side oracle.Side, records ...oracle.Record) *reconv1.IngestRequest {
	rb := &reconv1.RecordBatch{}
	for _, r := range records {
		rb.Records = append(rb.Records, &reconv1.Record{Key: r.Key, Value: r.Value})
	}
	if side == oracle.Left {
		return &reconv1.IngestRequest{RunId: id, Batch: &reconv1.IngestRequest_Left{Left: rb}}
	}
	return &reconv1.IngestRequest{RunId: id, Batch: &reconv1.IngestRequest_Right{Right: rb}}
}

// batches splits records into IngestRequests of up to n records of one
// side each.
func batches(id *reconv1.RunId, records []oracle.Record, n int) []*reconv1.IngestRequest {
	var out []*reconv1.IngestRequest
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && end-start < n && records[end].Side == records[start].Side {
			end++
		}
		out = append(out, batch(id, records[start].Side, records[start:end]...))
		start = end
	}
	return out
}

// ingest sends msgs over one Ingest stream and returns its status.
func (a *testApp) ingest(ctx context.Context, msgs ...*reconv1.IngestRequest) error {
	stream, err := a.client.Ingest(ctx)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

func (a *testApp) join(ctx context.Context, req *reconv1.JoinRequest) ([]*reconv1.JoinResult, error) {
	stream, err := a.client.Join(ctx, req)
	if err != nil {
		return nil, err
	}
	var out []*reconv1.JoinResult
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, resp.GetResults()...)
	}
}

// wantErr checks err's code and, unless reason is UNSPECIFIED, its
// ReconError reason.
func wantErr(t *testing.T, what string, err error, code codes.Code, reason reconv1.ErrorReason) {
	t.Helper()
	st := status.Convert(err)
	var got *reconv1.ReconError
	for _, d := range st.Details() {
		if re, ok := d.(*reconv1.ReconError); ok {
			got = re
		}
	}
	if st.Code() != code || got.GetReason() != reason {
		t.Errorf("%s: %v with reason %v, want %v with %v", what, err, got.GetReason(), code, reason)
	}
	if reason != reconv1.ErrorReason_ERROR_REASON_UNSPECIFIED && got.GetDetail() == "" {
		t.Errorf("%s: ReconError has no detail", what)
	}
}

// testRecords returns records with duplicates on both sides, one-sided
// keys and empty keys.
func testRecords(n int) []oracle.Record {
	var out []oracle.Record
	for i := range n {
		side := oracle.Side(i % 3 % 2)
		key := fmt.Appendf(nil, "key-%d", i%(n/3+1))
		if i%11 == 0 {
			key = nil
		}
		out = append(out, oracle.Record{Side: side, Key: key, Value: fmt.Appendf(nil, "value-%d", i)})
	}
	return out
}

func TestAPI_lifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, nil)
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 16)
		if _, err := os.Stat(a.runDir(id)); err != nil {
			t.Fatalf("run directory: %v", err)
		}

		records := testRecords(3000)
		msgs := batches(id, records, 50)
		errs := make(chan error, 2)
		go func() { errs <- a.ingest(ctx, msgs[:len(msgs)/2]...) }()
		go func() { errs <- a.ingest(ctx, msgs[len(msgs)/2:]...) }()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("Ingest: %v", err)
			}
		}

		var want reconv1.SealRunResponse
		want.Left, want.Right, want.CommittedIngests = &reconv1.SideTotals{}, &reconv1.SideTotals{}, 2
		for _, r := range records {
			st := want.Left
			if r.Side == oracle.Right {
				st = want.Right
			}
			st.Records++
			if len(r.Key) == 0 {
				st.EmptyKeyRecords++
			}
		}
		for i := range 2 {
			sealed, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(sealed, &want) {
				t.Errorf("Seal %d = %v, want %v", i+1, sealed, &want)
			}
		}

		expected := oracle.Join(records)
		for i := range 2 {
			got, err := a.join(ctx, &reconv1.JoinRequest{RunId: id})
			if err != nil {
				t.Fatalf("Join %d: %v", i+1, err)
			}
			if d := oracle.Diff(expected, got); d != "" {
				t.Errorf("Join %d: %s", i+1, d)
			}
		}

		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(a.runDir(id)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("run directory after DeleteRun: %v", err)
		}
		_, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		wantErr(t, "Seal after DeleteRun", err, codes.NotFound, reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND)
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Errorf("DeleteRun again: %v", err)
		}
	})
}

func TestAPI_states_filter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, nil)
		defer a.stop(t)
		id := a.create(t, 4)
		records := testRecords(500)
		if err := a.ingest(t.Context(), batches(id, records, 40)...); err != nil {
			t.Fatal(err)
		}
		if _, err := a.client.SealRun(t.Context(), &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		breaks := []reconv1.JoinState{reconv1.JoinState_JOIN_STATE_LEFT_ONLY, reconv1.JoinState_JOIN_STATE_RIGHT_ONLY}
		got, err := a.join(t.Context(), &reconv1.JoinRequest{RunId: id, States: breaks})
		if err != nil {
			t.Fatalf("a filtered Join must still end OK: %v", err)
		}
		if d := oracle.Diff(oracle.Filter(oracle.Join(records), breaks...), got); d != "" {
			t.Error(d)
		}
		_, err = a.join(t.Context(), &reconv1.JoinRequest{RunId: id, States: []reconv1.JoinState{reconv1.JoinState_JOIN_STATE_UNSPECIFIED}})
		wantErr(t, "Join of UNSPECIFIED", err, codes.InvalidArgument, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
		_, err = a.join(t.Context(), &reconv1.JoinRequest{RunId: id, States: []reconv1.JoinState{7}})
		wantErr(t, "Join of an unknown state", err, codes.InvalidArgument, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
	})
}

func TestAPI_errors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, map[string]string{"RECON_MAX_RUNS": "2", "RECON_MAX_BUCKETS": "64"})
		defer a.stop(t)
		ctx := t.Context()
		const (
			invalid  = reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST
			notFound = reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND
			wrong    = reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE
			failed   = reconv1.ErrorReason_ERROR_REASON_RUN_FAILED
		)
		rec := oracle.Record{Key: []byte("k"), Value: []byte("v")}

		for _, n := range []uint32{0, 65} {
			_, err := a.client.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: n})
			wantErr(t, fmt.Sprintf("CreateRun(%d)", n), err, codes.InvalidArgument, invalid)
		}

		id := a.create(t, 64)
		for _, bad := range []*reconv1.RunId{nil, {Value: "not-a-uuid"}, {Value: fmt.Sprintf("%X", id.GetValue())}} {
			_, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: bad})
			wantErr(t, fmt.Sprintf("SealRun(%q)", bad.GetValue()), err, codes.InvalidArgument, invalid)
			_, err = a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: bad})
			wantErr(t, fmt.Sprintf("DeleteRun(%q)", bad.GetValue()), err, codes.InvalidArgument, invalid)
			_, err = a.join(ctx, &reconv1.JoinRequest{RunId: bad})
			wantErr(t, fmt.Sprintf("Join(%q)", bad.GetValue()), err, codes.InvalidArgument, invalid)
			err = a.ingest(ctx, batch(bad, oracle.Left, rec))
			wantErr(t, fmt.Sprintf("Ingest(%q)", bad.GetValue()), err, codes.InvalidArgument, invalid)
		}
		unknown := &reconv1.RunId{Value: "019a7a3e-5c4b-7d2e-8f10-3b6c9e2a4d51"}
		_, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: unknown})
		wantErr(t, "SealRun of an unknown run", err, codes.NotFound, notFound)
		err = a.ingest(ctx, batch(unknown, oracle.Left, rec))
		wantErr(t, "Ingest to an unknown run", err, codes.NotFound, notFound)
		_, err = a.join(ctx, &reconv1.JoinRequest{RunId: unknown})
		wantErr(t, "Join of an unknown run", err, codes.NotFound, notFound)

		err = a.ingest(ctx, &reconv1.IngestRequest{RunId: id})
		wantErr(t, "Ingest with no batch", err, codes.InvalidArgument, invalid)
		_, err = a.join(ctx, &reconv1.JoinRequest{RunId: id})
		wantErr(t, "Join before Seal", err, codes.FailedPrecondition, wrong)

		// A stream that is open, with its first message received, holds the
		// Seal off.
		stream, err := a.client.Ingest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(batch(id, oracle.Left, rec)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		_, err = a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		wantErr(t, "SealRun with a stream open", err, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS)
		if _, err := stream.CloseAndRecv(); err != nil {
			t.Fatal(err)
		}
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		err = a.ingest(ctx, batch(id, oracle.Left, rec))
		wantErr(t, "Ingest after Seal", err, codes.FailedPrecondition, wrong)

		other := a.create(t, 1)
		_, err = a.client.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: 1})
		wantErr(t, "a third CreateRun", err, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS)

		err = a.ingest(ctx, batch(other, oracle.Left, rec), batch(id, oracle.Right, rec))
		wantErr(t, "Ingest naming two runs", err, codes.InvalidArgument, invalid)
		_, err = a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: other})
		wantErr(t, "SealRun of the failed run", err, codes.FailedPrecondition, failed)
		_, err = a.join(ctx, &reconv1.JoinRequest{RunId: other})
		wantErr(t, "Join of the failed run", err, codes.FailedPrecondition, failed)
		err = a.ingest(ctx, batch(other, oracle.Left, rec))
		wantErr(t, "Ingest to the failed run", err, codes.FailedPrecondition, failed)
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: other}); err != nil {
			t.Errorf("DeleteRun of the failed run: %v", err)
		}
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Errorf("the run the stream named second must be unaffected: %v", err)
		}
	})
}

func TestAPI_JOIN_IN_PROGRESS(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, nil)
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 8)
		var records []oracle.Record
		for i := range 8000 {
			records = append(records, oracle.Record{Key: fmt.Appendf(nil, "%d", i), Value: make([]byte, 1000)})
		}
		if err := a.ingest(ctx, batches(id, records, 500)...); err != nil {
			t.Fatal(err)
		}
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		jctx, cancel := context.WithCancel(ctx)
		first, err := a.client.Join(jctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.Recv(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		_, err = a.join(ctx, &reconv1.JoinRequest{RunId: id})
		wantErr(t, "a second Join", err, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS)
		cancel()
		synctest.Wait()
		got, err := a.join(ctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatalf("a Join after the first was cancelled: %v", err)
		}
		if len(got) != len(records) {
			t.Errorf("Join returned %d results, want %d", len(got), len(records))
		}
	})
}

func TestAPI_ingest_failures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, nil)
		defer a.stop(t)
		ctx := t.Context()
		rec := oracle.Record{Key: []byte("k"), Value: []byte("v")}

		// Cancelled after a batch was accepted: the run fails.
		id := a.create(t, 4)
		sctx, cancel := context.WithCancel(ctx)
		stream, err := a.client.Ingest(sctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(batch(id, oracle.Left, rec)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		cancel()
		synctest.Wait()
		_, err = a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		wantErr(t, "SealRun after a cancelled stream", err, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(a.runDir(id)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("run directory after DeleteRun: %v", err)
		}

		// Rejected before any record: the run stays OPEN.
		id = a.create(t, 4)
		err = a.ingest(ctx, &reconv1.IngestRequest{RunId: id})
		wantErr(t, "Ingest with no batch", err, codes.InvalidArgument, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST)
		if err := a.ingest(ctx); err != nil {
			t.Errorf("an empty Ingest stream: %v", err)
		}
		sealed, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		if err != nil {
			t.Fatalf("SealRun after rejected streams: %v", err)
		}
		if sealed.GetCommittedIngests() != 0 || sealed.GetLeft().GetRecords() != 0 {
			t.Errorf("Seal = %v, want nothing committed", sealed)
		}
	})
}

func TestAPI_DeleteRun_aborts_streams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, nil)
		defer a.stop(t)
		ctx := t.Context()

		// An idle Ingest stream, waiting for its next message.
		id := a.create(t, 4)
		stream, err := a.client.Ingest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(batch(id, oracle.Left, oracle.Record{Key: []byte("k")})); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		_, err = stream.CloseAndRecv()
		wantErr(t, "the idle Ingest stream", err, codes.Aborted, reconv1.ErrorReason_ERROR_REASON_UNSPECIFIED)

		// An active Join, blocked on flow control.
		id = a.create(t, 4)
		var records []oracle.Record
		for i := range 8000 {
			records = append(records, oracle.Record{Side: oracle.Side(i % 2), Key: fmt.Appendf(nil, "%d", i/2), Value: make([]byte, 1000)})
		}
		if err := a.ingest(ctx, batches(id, records, 500)...); err != nil {
			t.Fatal(err)
		}
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		join, err := a.client.Join(ctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := join.Recv(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		for err == nil {
			_, err = join.Recv()
		}
		wantErr(t, "the active Join", err, codes.Aborted, reconv1.ErrorReason_ERROR_REASON_UNSPECIFIED)
	})
}

func TestAPI_memory_exhausted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, map[string]string{"RECON_MEMORY_LIMIT_BYTES": "64KiB"})
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 1)
		err := a.ingest(ctx, batch(id, oracle.Left, oracle.Record{Key: []byte("k"), Value: make([]byte, 100<<10)}))
		wantErr(t, "Ingest of a message larger than the budget", err, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED)
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Errorf("a message rejected before any record must leave the run OPEN: %v", err)
		}

		// A record that doesn't fit next to its message, even with every
		// buffer spilled, fails the stream after a record was accepted.
		id = a.create(t, 1)
		big := batch(id, oracle.Left, oracle.Record{Key: []byte("a")}, oracle.Record{Key: []byte("b"), Value: make([]byte, 32<<10)})
		err = a.ingest(ctx, big)
		wantErr(t, "Ingest past the budget", err, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED)
		_, err = a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		wantErr(t, "SealRun once records were accepted", err, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
	})
}

// spillFiles counts the .rec files under dir.
func spillFiles(t *testing.T, dir string) int {
	t.Helper()
	var n int
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(path, ".rec") {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAPI_ingest_past_the_budget_spills(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// One join unit takes the whole budget, which a key's 8 KiB of
		// records then fit, not a quarter.
		a := startApp(t, map[string]string{"RECON_MEMORY_LIMIT_BYTES": "64KiB", "RECON_OUTPUT_BATCH_BYTES": "4KiB", "RECON_JOIN_PARALLELISM": "1"})
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 16)
		var records []oracle.Record
		for i := range 200 {
			records = append(records, oracle.Record{Side: oracle.Side(i / 10 % 2), Key: fmt.Appendf(nil, "%d", i%60), Value: make([]byte, 2<<10)})
		}
		if err := a.ingest(ctx, batches(id, records, 4)...); err != nil {
			t.Fatalf("Ingest of 400 KiB with a 64 KiB budget: %v", err)
		}
		if n := spillFiles(t, a.runDir(id)); n == 0 {
			t.Fatal("no spill files after ingesting past the budget")
		}
		if _, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		got, err := a.join(ctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatalf("Join: %v", err)
		}
		if d := oracle.Diff(oracle.Join(records), got); d != "" {
			t.Error(d)
		}
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(a.runDir(id)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("run directory after DeleteRun: %v", err)
		}
	})
}

func TestAPI_disk_exhausted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, map[string]string{"RECON_MEMORY_LIMIT_BYTES": "64KiB", "RECON_DISK_LIMIT_BYTES": "128KiB"})
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 4)
		var records []oracle.Record
		for i := range 200 {
			records = append(records, oracle.Record{Key: fmt.Appendf(nil, "%d", i), Value: make([]byte, 2<<10)})
		}
		err := a.ingest(ctx, batches(id, records, 4)...)
		wantErr(t, "Ingest of 400 KiB with 128 KiB of disk", err, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED)
		_, err = a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		wantErr(t, "SealRun of the run", err, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED)
		if _, err := a.client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(a.runDir(id)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("run directory after DeleteRun: %v", err)
		}

		// Deleting the run freed its disk.
		id = a.create(t, 4)
		if err := a.ingest(ctx, batches(id, records[:50], 4)...); err != nil {
			t.Fatalf("Ingest of 100 KiB after the Delete: %v", err)
		}
	})
}

func TestAPI_hot_key(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := startApp(t, map[string]string{"RECON_MAX_MATCH_BYTES": "1KiB"})
		defer a.stop(t)
		ctx := t.Context()
		id := a.create(t, 4)
		records := testRecords(30)
		for i := range 21 {
			records = append(records, oracle.Record{Side: oracle.Side(min(i, 1)), Key: []byte("N/A"), Value: make([]byte, 50)})
		}
		if err := a.ingest(ctx, batches(id, records, 3)...); err != nil {
			t.Fatal(err)
		}
		sealed, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		if err != nil {
			t.Fatal(err)
		}
		for range 2 { // a retry fails the same way
			_, err := a.join(ctx, &reconv1.JoinRequest{RunId: id})
			wantErr(t, "Join of a key whose Match exceeds 1 KiB", err, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_HOT_KEY)
			var hot *reconv1.HotKey
			for _, d := range status.Convert(err).Details() {
				if re, ok := d.(*reconv1.ReconError); ok {
					hot = re.GetHotKey()
				}
			}
			if want := (&reconv1.HotKey{Key: []byte("N/A"), LeftRecords: 1, RightRecords: 20}); !proto.Equal(hot, want) {
				t.Errorf("HotKey %v, want %v", hot, want)
			}
		}
		again, err := a.client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		if err != nil || !proto.Equal(again, sealed) {
			t.Errorf("SealRun after HOT_KEY: %v, %v; want the run still SEALED with %v", again, err, sealed)
		}
	})
}
