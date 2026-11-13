//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

const (
	reasonNone        = reconv1.ErrorReason_ERROR_REASON_UNSPECIFIED
	reasonInvalid     = reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST
	reasonNotFound    = reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND
	reasonWrongState  = reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE
	reasonFailed      = reconv1.ErrorReason_ERROR_REASON_RUN_FAILED
	reasonIngesting   = reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS
	reasonJoining     = reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS
	reasonMemory      = reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED
	reasonTooManyRuns = reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS
	stateLeftOnly     = reconv1.JoinState_JOIN_STATE_LEFT_ONLY
	stateRightOnly    = reconv1.JoinState_JOIN_STATE_RIGHT_ONLY
	testTimeout       = 5 * time.Minute
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// wantErr checks err's code and, unless reason is reasonNone, its
// ReconError reason.
func wantErr(t *testing.T, what string, err error, code codes.Code, reason reconv1.ErrorReason) {
	t.Helper()
	got := harness.ReconError(err)
	if status.Code(err) != code || got.GetReason() != reason {
		t.Errorf("%s: %v with reason %v, want %v with %v", what, err, got.GetReason(), code, reason)
	}
}

func create(ctx context.Context, t *testing.T, c *harness.Container, buckets uint32) *reconv1.RunId {
	t.Helper()
	resp, err := c.Client.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: buckets})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return resp.GetRunId()
}

func seal(ctx context.Context, t *testing.T, c *harness.Container, id *reconv1.RunId) *reconv1.SealRunResponse {
	t.Helper()
	resp, err := c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
	if err != nil {
		t.Fatalf("SealRun: %v", err)
	}
	return resp
}

func deleteRun(ctx context.Context, t *testing.T, c *harness.Container, id *reconv1.RunId) {
	t.Helper()
	if _, err := c.Client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: id}); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}
}

func runDir(c *harness.Container, id *reconv1.RunId) string {
	return filepath.Join(c.DataDir, "runs", id.GetValue())
}

func wantNoRunDir(t *testing.T, c *harness.Container, id *reconv1.RunId) {
	t.Helper()
	if _, err := os.Stat(runDir(c, id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("runs/%s after DeleteRun: %v, want it gone", id.GetValue(), err)
	}
}

// randomRecords returns n records in blocks of one side: duplicates on
// both sides, one-sided keys, a hot key, empty keys, binary keys and keys
// that differ only in their last byte.
func randomRecords(seed uint64, n int) []oracle.Record {
	rng := rand.New(rand.NewPCG(seed, 0))
	keys := make([][]byte, n/3+1)
	for i := range keys {
		if i%5 == 4 {
			k := []byte(string(keys[i-1]))
			k[len(k)-1] ^= 1
			keys[i] = k
			continue
		}
		k := make([]byte, 1+rng.IntN(16))
		for j := range k {
			k[j] = byte(rng.Uint32())
		}
		keys[i] = k
	}
	var out []oracle.Record
	side := oracle.Left
	for len(out) < n {
		for range min(1+rng.IntN(300), n-len(out)) {
			var key []byte
			switch x := rng.IntN(20); {
			case x == 0:
				key = nil
			case x == 1:
				key = keys[0]
			default:
				key = keys[rng.IntN(len(keys))]
			}
			value := make([]byte, rng.IntN(64))
			for j := range value {
				value[j] = byte(rng.Uint32())
			}
			out = append(out, oracle.Record{Side: side, Key: key, Value: value})
		}
		side = 1 - side
	}
	return out
}

func seq(records []oracle.Record) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for _, r := range records {
			if !yield(harness.Record{Side: harness.Side(r.Side + 1), Key: r.Key, Value: r.Value}) {
				return
			}
		}
	}
}

func totals(records []oracle.Record) (left, right *reconv1.SideTotals) {
	left, right = &reconv1.SideTotals{}, &reconv1.SideTotals{}
	for _, r := range records {
		t := left
		if r.Side == oracle.Right {
			t = right
		}
		t.Records++
		if len(r.Key) == 0 {
			t.EmptyKeyRecords++
		}
	}
	return left, right
}

func wantTotals(t *testing.T, got *reconv1.SealRunResponse, records []oracle.Record, streams uint32) {
	t.Helper()
	left, right := totals(records)
	if got.GetLeft().GetRecords() != left.Records || got.GetLeft().GetEmptyKeyRecords() != left.EmptyKeyRecords ||
		got.GetRight().GetRecords() != right.Records || got.GetRight().GetEmptyKeyRecords() != right.EmptyKeyRecords ||
		got.GetCommittedIngests() != streams {
		t.Errorf("Seal = %v, want left %v, right %v, %d committed", got, left, right, streams)
	}
}

// ingestParallel ingests parts over one stream each, concurrently.
func ingestParallel(ctx context.Context, t *testing.T, c *harness.Container, id *reconv1.RunId, parts [][]oracle.Record) {
	t.Helper()
	var wg sync.WaitGroup
	for i, part := range parts {
		wg.Go(func() {
			if err := harness.Ingest(ctx, c.Client, id, seq(part)); err != nil {
				t.Errorf("Ingest stream %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
}

// split deals the side blocks of records out to n parts in turn, so that
// each part interleaves both sides.
func split(records []oracle.Record, n int) [][]oracle.Record {
	parts := make([][]oracle.Record, n)
	p := 0
	for i, r := range records {
		if i > 0 && r.Side != records[i-1].Side {
			p = (p + 1) % n
		}
		parts[p] = append(parts[p], r)
	}
	return parts
}

// openStream returns an OPEN run with an Ingest stream on it that the
// sidecar has accepted, its first batch received. The API gives a client no
// acknowledgement of a received message, but a Seal of the run fails with
// INGEST_IN_PROGRESS once the sidecar has it. A Seal that comes first seals
// the run instead, so that run is deleted and the attempt made again with a
// longer wait before the Seal, until ctx ends. Every attempt is decided by
// the sidecar's answer; the wait only makes success likelier.
func openStream(ctx context.Context, t *testing.T, c *harness.Container) (*reconv1.RunId, grpc.ClientStreamingClient[reconv1.IngestRequest, reconv1.IngestResponse], context.CancelFunc) {
	t.Helper()
	const maxWait = 2 * time.Second
	wait := 50 * time.Millisecond
	for attempt := 1; ; attempt++ {
		id := create(ctx, t, c, 4)
		sctx, cancel := context.WithCancel(ctx)
		stream, err := c.Client.Ingest(sctx)
		if err != nil {
			t.Fatal(err)
		}
		batch := &reconv1.RecordBatch{Records: []*reconv1.Record{{Key: []byte("k"), Value: []byte("v")}}}
		if err := stream.Send(&reconv1.IngestRequest{RunId: id, Batch: &reconv1.IngestRequest_Left{Left: batch}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the sidecar never received an Ingest stream's first message (%d attempts): %v", attempt, context.Cause(ctx))
		case <-time.After(wait):
		}
		_, err = c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
		if harness.ReconError(err).GetReason() == reasonIngesting {
			return id, stream, cancel
		}
		if err != nil {
			t.Fatalf("SealRun with a stream open: %v", err)
		}
		t.Logf("openStream: Seal won the race after %v (attempt %d); retrying", wait, attempt)
		cancel()
		deleteRun(ctx, t, c, id)
		wait = min(2*wait, maxWait)
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	id := create(ctx, t, c, 16)
	if _, err := os.Stat(runDir(c, id)); err != nil {
		t.Fatalf("run directory: %v", err)
	}
	records := randomRecords(1, 50_000)
	ingestParallel(ctx, t, c, id, split(records, 2))
	wantTotals(t, seal(ctx, t, c, id), records, 2)

	want := oracle.Join(records)
	first, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if d := oracle.Diff(want, first); d != "" {
		t.Errorf("Join: %s", d)
	}
	second, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatalf("second Join: %v", err)
	}
	if d := oracle.Diff(first, second); d != "" {
		t.Errorf("second Join differs from the first: %s", d)
	}

	deleteRun(ctx, t, c, id)
	wantNoRunDir(t, c, id)
}

func TestStatesFilter(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	id := create(ctx, t, c, 8)
	records := randomRecords(2, 20_000)
	if err := harness.Ingest(ctx, c.Client, id, seq(records)); err != nil {
		t.Fatal(err)
	}
	seal(ctx, t, c, id)
	breaks := []reconv1.JoinState{stateLeftOnly, stateRightOnly}
	got, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id, States: breaks})
	if err != nil {
		t.Fatalf("a breaks-only Join must still end OK, with the filtered results counted: %v", err)
	}
	for _, r := range got {
		if r.GetBoth() != nil {
			t.Fatal("a breaks-only Join returned a Match")
		}
	}
	if d := oracle.Diff(oracle.Filter(oracle.Join(records), breaks...), got); d != "" {
		t.Error(d)
	}
}

func TestEmptyKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []harness.Option
	}{
		{"defaults", nil},
		{"16MiB budget", []harness.Option{harness.Env("RECON_MEMORY_LIMIT_BYTES", "16MiB")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := harness.Start(t, tc.opts...)
			ctx := testContext(t)

			var records []oracle.Record
			for i := range 40_000 {
				key := []byte(nil)
				if i%4 == 0 {
					key = fmt.Appendf(nil, "k%d", i%1000)
				}
				records = append(records, oracle.Record{Side: oracle.Side(i / 500 % 2), Key: key, Value: fmt.Appendf(nil, "v%d", i)})
			}
			id := create(ctx, t, c, 4)
			if err := harness.Ingest(ctx, c.Client, id, seq(records)); err != nil {
				t.Fatal(err)
			}
			wantTotals(t, seal(ctx, t, c, id), records, 1)

			got, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
			if err != nil {
				t.Fatalf("Join: %v; empty keys must never cause HOT_KEY", err)
			}
			for _, r := range got {
				if m := r.GetBoth(); m != nil && len(m.GetKey()) == 0 {
					t.Fatal("empty-key records matched one another")
				}
			}
			if d := oracle.Diff(oracle.Join(records), got); d != "" {
				t.Error(d)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MAX_RUNS", "2"), harness.Env("RECON_MAX_BUCKETS", "64"))
	ctx := testContext(t)
	rec := []oracle.Record{{Key: []byte("k"), Value: []byte("v")}}

	for _, n := range []uint32{0, 65} {
		_, err := c.Client.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: n})
		wantErr(t, fmt.Sprintf("CreateRun(%d)", n), err, codes.InvalidArgument, reasonInvalid)
	}

	id := create(ctx, t, c, 64)
	for _, bad := range []string{"", "not-a-uuid", strings.ToUpper(id.GetValue())} {
		badID := &reconv1.RunId{Value: bad}
		_, err := c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: badID})
		wantErr(t, fmt.Sprintf("SealRun(%q)", bad), err, codes.InvalidArgument, reasonInvalid)
		_, err = c.Client.DeleteRun(ctx, &reconv1.DeleteRunRequest{RunId: badID})
		wantErr(t, fmt.Sprintf("DeleteRun(%q)", bad), err, codes.InvalidArgument, reasonInvalid)
		err = harness.Ingest(ctx, c.Client, badID, seq(rec))
		wantErr(t, fmt.Sprintf("Ingest(%q)", bad), err, codes.InvalidArgument, reasonInvalid)
		_, err = harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: badID})
		wantErr(t, fmt.Sprintf("Join(%q)", bad), err, codes.InvalidArgument, reasonInvalid)
	}

	unknown := &reconv1.RunId{Value: uuid.Must(uuid.NewV7()).String()}
	_, err := c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: unknown})
	wantErr(t, "SealRun of an unknown run", err, codes.NotFound, reasonNotFound)
	err = harness.Ingest(ctx, c.Client, unknown, seq(rec))
	wantErr(t, "Ingest to an unknown run", err, codes.NotFound, reasonNotFound)
	_, err = harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: unknown})
	wantErr(t, "Join of an unknown run", err, codes.NotFound, reasonNotFound)

	stream, err := c.Client.Ingest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reconv1.IngestRequest{RunId: id}); err != nil {
		t.Fatal(err)
	}
	_, err = stream.CloseAndRecv()
	wantErr(t, "Ingest with no batch", err, codes.InvalidArgument, reasonInvalid)

	_, err = harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	wantErr(t, "Join before Seal", err, codes.FailedPrecondition, reasonWrongState)

	// A Join that the client stops reading stays in progress.
	var big []oracle.Record
	for i := range 40_000 {
		big = append(big, oracle.Record{Side: oracle.Side(i / 1000 % 2), Key: fmt.Appendf(nil, "%d", i), Value: make([]byte, 1024)})
	}
	if err := harness.Ingest(ctx, c.Client, id, seq(big)); err != nil {
		t.Fatal(err)
	}
	seal(ctx, t, c, id)
	err = harness.Ingest(ctx, c.Client, id, seq(rec))
	wantErr(t, "Ingest after Seal", err, codes.FailedPrecondition, reasonWrongState)
	jctx, cancelJoin := context.WithCancel(ctx)
	first, err := c.Client.Join(jctx, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Recv(); err != nil {
		t.Fatal(err)
	}
	_, err = harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	wantErr(t, "a second Join", err, codes.FailedPrecondition, reasonJoining)
	cancelJoin()

	other := create(ctx, t, c, 1)
	_, err = c.Client.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: 1})
	wantErr(t, "a third CreateRun", err, codes.ResourceExhausted, reasonTooManyRuns)

	err = harness.Ingest(ctx, c.Client, other, seq(rec))
	if err != nil {
		t.Fatal(err)
	}
	two, err := c.Client.Ingest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, runID := range []*reconv1.RunId{other, unknown} {
		batch := &reconv1.RecordBatch{Records: []*reconv1.Record{{Key: []byte("k")}}}
		if err := two.Send(&reconv1.IngestRequest{RunId: runID, Batch: &reconv1.IngestRequest_Right{Right: batch}}); err != nil {
			break
		}
	}
	_, err = two.CloseAndRecv()
	wantErr(t, "Ingest naming two runs", err, codes.InvalidArgument, reasonInvalid)
	_, err = c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: other})
	wantErr(t, "SealRun of the run that stream failed", err, codes.FailedPrecondition, reasonFailed)
	deleteRun(ctx, t, c, other)
	deleteRun(ctx, t, c, id)

	id, stream, cancel := openStream(ctx, t, c)
	defer cancel()
	_, err = c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
	wantErr(t, "SealRun with a stream open", err, codes.FailedPrecondition, reasonIngesting)
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if got := seal(ctx, t, c, id); got.GetCommittedIngests() != 1 || got.GetLeft().GetRecords() != 1 {
		t.Errorf("Seal once the stream committed = %v", got)
	}
}

func TestIngestCancel(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	id, _, cancel := openStream(ctx, t, c)
	cancel()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
		if harness.ReconError(err).GetReason() == reasonFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Join after the cancelled stream: %v, want RUN_FAILED", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, err := c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
	wantErr(t, "SealRun of the failed run", err, codes.FailedPrecondition, reasonFailed)
	err = harness.Ingest(ctx, c.Client, id, seq([]oracle.Record{{Key: []byte("k")}}))
	wantErr(t, "Ingest to the failed run", err, codes.FailedPrecondition, reasonFailed)
	deleteRun(ctx, t, c, id)
	wantNoRunDir(t, c, id)

	// A stream rejected before any record leaves the run OPEN.
	id = create(ctx, t, c, 4)
	stream, err := c.Client.Ingest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reconv1.IngestRequest{RunId: id}); err != nil {
		t.Fatal(err)
	}
	_, err = stream.CloseAndRecv()
	wantErr(t, "Ingest with no batch", err, codes.InvalidArgument, reasonInvalid)
	if got := seal(ctx, t, c, id); got.GetCommittedIngests() != 0 {
		t.Errorf("Seal = %v, want nothing committed", got)
	}
}

func TestConcurrentIngest(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	id := create(ctx, t, c, 32)
	records := randomRecords(3, 100_000)
	ingestParallel(ctx, t, c, id, split(records, 16))
	wantTotals(t, seal(ctx, t, c, id), records, 16)
	got, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
}

func TestDeleteAbortsStreams(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	t.Run("idle Ingest", func(t *testing.T) {
		id, stream, cancel := openStream(ctx, t, c)
		defer cancel()
		deleteRun(ctx, t, c, id)
		_, err := stream.CloseAndRecv()
		wantErr(t, "the idle Ingest stream", err, codes.Aborted, reasonNone)
		wantNoRunDir(t, c, id)
	})

	t.Run("active Join", func(t *testing.T) {
		id := create(ctx, t, c, 8)
		cfg := gen.Config{Seed: 4, Keys: 40_000, ValueSize: 512}
		if err := harness.Ingest(ctx, c.Client, id, cfg.Records(0, 1)); err != nil {
			t.Fatal(err)
		}
		seal(ctx, t, c, id)
		join, err := c.Client.Join(ctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := join.Recv(); err != nil {
			t.Fatal(err)
		}
		deleteRun(ctx, t, c, id)
		for err == nil {
			_, err = join.Recv()
		}
		wantErr(t, "the active Join", err, codes.Aborted, reasonNone)
	})
}

func TestJoinRetry(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	cfg := gen.Config{Seed: 5, Keys: 100_000, ValueSize: 64}
	id := create(ctx, t, c, 64)
	var wg sync.WaitGroup
	for p := range 4 {
		wg.Go(func() {
			if err := harness.Ingest(ctx, c.Client, id, cfg.Records(p, 4)); err != nil {
				t.Errorf("Ingest: %v", err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	left, right := cfg.Totals()
	if got := seal(ctx, t, c, id); got.GetLeft().GetRecords() != left || got.GetRight().GetRecords() != right {
		t.Fatalf("Seal = %v, want %d left and %d right", got, left, right)
	}

	var whole gen.Fingerprint
	v := gen.NewVerifier(cfg)
	err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(r *reconv1.JoinResult) error {
		whole.Add(r)
		return v.Add(r)
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := v.Finish(); err != nil {
		t.Fatal(err)
	}

	halfway := errors.New("halfway")
	var n uint64
	err = harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(*reconv1.JoinResult) error {
		if n++; n == whole.Count/2 {
			return halfway
		}
		return nil
	})
	if !errors.Is(err, halfway) {
		t.Fatalf("the interrupted Join: %v", err)
	}

	// The sidecar notices the cancelled Join a moment later, so the retry
	// may first find it still in progress.
	for {
		var retry gen.Fingerprint
		err = harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(r *reconv1.JoinResult) error {
			retry.Add(r)
			return nil
		})
		if harness.ReconError(err).GetReason() == reasonJoining {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("the retried Join: %v", err)
		}
		if retry != whole {
			t.Errorf("the retried Join's fingerprint %+v differs from the uninterrupted one's %+v", retry, whole)
		}
		break
	}
}

func TestMemoryExhausted(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "256KiB"))
	ctx := testContext(t)

	id := create(ctx, t, c, 4)
	var records []oracle.Record
	for i := range 2000 {
		records = append(records, oracle.Record{Key: fmt.Appendf(nil, "%d", i), Value: make([]byte, 512)})
	}
	err := harness.Ingest(ctx, c.Client, id, seq(records))
	wantErr(t, "Ingest of a message larger than the budget", err, codes.ResourceExhausted, reasonMemory)
	if st := c.State(); !st.Running {
		t.Errorf("container state %+v, want it still running", st)
	}
	if got := seal(ctx, t, c, id); got.GetCommittedIngests() != 0 {
		t.Errorf("Seal = %v; the rejected message must leave the run OPEN and empty", got)
	}
}
