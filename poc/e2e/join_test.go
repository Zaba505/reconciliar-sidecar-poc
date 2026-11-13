//go:build e2e

package e2e

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

func TestBackpressure(t *testing.T) {
	t.Parallel()
	const (
		budget = 64 << 20
		rate   = 5 << 20 // bytes a second the client reads
	)
	// A sidecar that buffered the results instead of waiting for the
	// client would need far more than 256 MiB.
	c := harness.Start(t, harness.Memory("256m"), harness.Env("RECON_MEMORY_LIMIT_BYTES", "64MiB"))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()

	// About 1.1 GiB of results: 380k keys of 3 records each on average,
	// of 1 KB values.
	cfg := gen.Config{Seed: 12, Keys: 380_000, ValueSize: 1000}
	id := create(ctx, t, c, 256)
	ingestGen(ctx, t, c, id, cfg, 4)

	stream, err := c.Client.Join(ctx, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	v := gen.NewVerifier(cfg)
	start := time.Now()
	var received int64
	var checked bool
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Join after %d MiB: %v", received>>20, err)
		}
		for _, r := range resp.GetResults() {
			if err := v.Add(r); err != nil {
				t.Fatal(err)
			}
		}
		received += int64(proto.Size(resp))
		// Halfway through, the sidecar must still be joining: it can't
		// be further ahead of the client than its batches and gRPC's
		// flow-control windows.
		if !checked && received > 512<<20 {
			checked = true
			if _, done := lastJoinFinished(c, id); done {
				t.Errorf("the sidecar finished the Join with the client %d MiB in", received>>20)
			}
		}
		if ahead := time.Until(start.Add(time.Duration(received * int64(time.Second) / rate))); ahead > 0 {
			time.Sleep(ahead)
		}
	}
	if err := v.Finish(); err != nil {
		t.Fatal(err)
	}
	if received < 1<<30 {
		t.Errorf("the Join sent %d MiB, want at least 1 GiB", received>>20)
	}
	if st := c.State(); st.OOMKilled || !st.Running {
		t.Fatalf("container state %+v, want it running and never OOM-killed", st)
	}
	rec := joinFinished(t, c, id)
	if peak, ok := rec.Int("recon.pool_peak_bytes"); !ok || peak > budget {
		t.Errorf("join finished record reports pool peak %d (%v), want at most %d", peak, ok, budget)
	}
	t.Logf("read %d MiB in %v; join finished record %v", received>>20, time.Since(start).Round(time.Second), rec.Attributes)
}

func TestSIGTERMDuringJoin(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	// About 60 MiB, which the client reads at 20 MiB/s.
	id := create(ctx, t, c, 8)
	cfg := gen.Config{Seed: 13, Keys: 40_000, ValueSize: 512}
	ingestGen(ctx, t, c, id, cfg, 1)
	join, err := c.Client.Join(ctx, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := join.Recv(); err != nil {
		t.Fatal(err)
	}
	// The client keeps reading: a Join blocked in a send that flow
	// control holds up sees the shutdown only once the client reads
	// (PLAN §6.4).
	ended := make(chan error, 1)
	go func() {
		for {
			if _, err := join.Recv(); err != nil {
				ended <- err
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	code := c.Stop(30 * time.Second)
	err = <-ended
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "server is shutting down" {
		t.Errorf("the Join SIGTERM cut off ended with %v, want UNAVAILABLE \"server is shutting down\"", err)
	}
	if code != 0 {
		t.Errorf("exit code %d after SIGTERM, want 0", code)
	}
}
