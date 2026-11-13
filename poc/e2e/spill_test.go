//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

const reasonDisk = reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED

// spillFiles returns the number and total size of the spill files in a
// run's directory.
func spillFiles(t *testing.T, c *harness.Container, id *reconv1.RunId) (files int, bytes int64) {
	t.Helper()
	err := filepath.WalkDir(runDir(c, id), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".rec") {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files++
		bytes += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("walk runs/%s: %v", id.GetValue(), err)
	}
	return files, bytes
}

// ingestGen ingests cfg's run over streams parallel streams.
func ingestGen(ctx context.Context, t *testing.T, c *harness.Container, id *reconv1.RunId, cfg gen.Config, streams int) {
	t.Helper()
	var wg sync.WaitGroup
	for p := range streams {
		wg.Go(func() {
			if err := harness.Ingest(ctx, c.Client, id, cfg.Records(p, streams)); err != nil {
				t.Errorf("Ingest stream %d: %v", p, err)
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
}

// verifiedJoin joins the run, checks the results against cfg and returns
// their fingerprint.
func verifiedJoin(ctx context.Context, t *testing.T, c *harness.Container, id *reconv1.RunId, cfg gen.Config) gen.Fingerprint {
	t.Helper()
	var fp gen.Fingerprint
	v := gen.NewVerifier(cfg)
	err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(r *reconv1.JoinResult) error {
		fp.Add(r)
		return v.Add(r)
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := v.Finish(); err != nil {
		t.Fatal(err)
	}
	return fp
}

// joinFinished returns the join-finished log record of the run's last
// Join.
func joinFinished(t *testing.T, c *harness.Container, id *reconv1.RunId) harness.LogRecord {
	t.Helper()
	rec, ok := lastJoinFinished(c, id)
	if !ok {
		t.Fatalf("no join finished record for run %s", id.GetValue())
	}
	return rec
}

// lastJoinFinished returns the join-finished log record of the run's last
// Join, if a Join of the run finished.
func lastJoinFinished(c *harness.Container, id *reconv1.RunId) (rec harness.LogRecord, found bool) {
	for _, r := range c.Logs() {
		if r.Body == "join finished" && r.Attributes["recon.run_id"] == id.GetValue() {
			rec, found = r, true
		}
	}
	return rec, found
}

func TestLargerThanMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 2 GiB; skipped by -short")
	}
	const budget = 128 << 20
	c := harness.Start(t, harness.Memory("512m"), harness.Env("RECON_MEMORY_LIMIT_BYTES", "128MiB"))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()

	// About 2 GiB: 700k keys of 3 records each on average, of 1 KiB values.
	cfg := gen.Config{Seed: 7, Keys: 700_000, ValueSize: 1000}
	id := create(ctx, t, c, 1024)
	start := time.Now()
	ingestGen(ctx, t, c, id, cfg, 4)
	files, bytes := spillFiles(t, c, id)
	t.Logf("ingested in %v: %d spill files, %d MiB", time.Since(start).Round(time.Second), files, bytes>>20)
	if files == 0 || bytes < 1<<30 {
		t.Fatalf("%d spill files of %d MiB after Seal; most of 2 GiB must spill under a 128 MiB budget", files, bytes>>20)
	}

	start = time.Now()
	verifiedJoin(ctx, t, c, id, cfg)
	t.Logf("joined in %v", time.Since(start).Round(time.Second))
	if st := c.State(); st.OOMKilled || !st.Running {
		t.Fatalf("container state %+v, want it running and never OOM-killed", st)
	}
	rec := joinFinished(t, c, id)
	peak, ok := rec.Int("recon.pool_peak_bytes")
	if !ok || peak > budget || peak == 0 {
		t.Errorf("join finished record reports pool peak %d (%v), want 1 to %d", peak, ok, budget)
	}
	t.Logf("pool peak %d MiB, %v units", peak>>20, rec.Attributes["recon.join_units"])
	deleteRun(ctx, t, c, id)
	wantNoRunDir(t, c, id)
}

func TestBudgetInvariance(t *testing.T) {
	t.Parallel()
	ctx := testContext(t)
	// About 20 MiB, so that every bucket count joins within 32 MiB, while
	// 4096 buckets of 4 KiB first chunks per side don't fit in it.
	cfg := gen.Config{Seed: 8, Keys: 6_800, ValueSize: 1000}
	type result struct {
		name string
		fp   gen.Fingerprint
	}
	var results []result
	for _, b := range []struct {
		budget string
		memory string
		tiny   bool
	}{
		{"32MiB", "1g", true},
		{"4GiB", "5g", false},
	} {
		for _, parallelism := range []string{"1", "4"} {
			c := harness.Start(t, harness.Memory(b.memory), harness.Env("RECON_MEMORY_LIMIT_BYTES", b.budget), harness.Env("RECON_JOIN_PARALLELISM", parallelism))
			for _, n := range []uint32{1, 64, 4096} {
				name := fmt.Sprintf("budget %s, parallelism %s, %d buckets", b.budget, parallelism, n)
				id := create(ctx, t, c, n)
				ingestGen(ctx, t, c, id, cfg, 2)
				files, bytes := spillFiles(t, c, id)
				if b.tiny && n == 4096 && files == 0 {
					t.Errorf("%s: nothing spilled during ingest", name)
				}
				fp := verifiedJoin(ctx, t, c, id, cfg)
				units, _ := joinFinished(t, c, id).Int("recon.join_units")
				if b.tiny && n == 4096 && (units < 2 || units >= int64(n)) {
					t.Errorf("%s: %d join units, want the buckets coalesced into a few", name, units)
				}
				t.Logf("%s: %d spill files (%d KiB) after Seal, %d units", name, files, bytes>>10, units)
				results = append(results, result{name, fp})
				deleteRun(ctx, t, c, id)
			}
			if st := c.State(); st.OOMKilled || !st.Running {
				t.Errorf("budget %s, parallelism %s: container state %+v", b.budget, parallelism, st)
			}
		}
	}
	for _, r := range results[1:] {
		if r.fp != results[0].fp {
			t.Errorf("fingerprint of %s is %+v, of %s %+v; want them equal", r.name, r.fp, results[0].name, results[0].fp)
		}
	}
}

func TestDiskExhausted(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB"), harness.Env("RECON_DISK_LIMIT_BYTES", "64MiB"))
	ctx := testContext(t)

	// About 256 MiB.
	cfg := gen.Config{Seed: 9, Keys: 87_000, ValueSize: 1000}
	id := create(ctx, t, c, 64)
	err := harness.Ingest(ctx, c.Client, id, cfg.Records(0, 1))
	wantErr(t, "Ingest of 256 MiB with 64 MiB of disk", err, codes.ResourceExhausted, reasonDisk)
	_, err = c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
	wantErr(t, "SealRun of the run", err, codes.FailedPrecondition, reasonFailed)
	files, bytes := spillFiles(t, c, id)
	if files == 0 || bytes > 64<<20 {
		t.Errorf("%d spill files of %d bytes, want some, within the 64 MiB limit", files, bytes)
	}
	deleteRun(ctx, t, c, id)
	wantNoRunDir(t, c, id)

	// Deleting the run freed its share of the disk limit: a run that spills
	// most of it succeeds.
	small := gen.Config{Seed: 10, Keys: 16_000, ValueSize: 1000}
	id = create(ctx, t, c, 64)
	ingestGen(ctx, t, c, id, small, 1)
	if files, bytes := spillFiles(t, c, id); files == 0 || bytes < 8<<20 {
		t.Errorf("the second run spilled %d files of %d bytes; want it to need the freed disk", files, bytes)
	}
	verifiedJoin(ctx, t, c, id, small)
}

func TestKillDuringJoin(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx := testContext(t)

	id := create(ctx, t, c, 8)
	cfg := gen.Config{Seed: 11, Keys: 40_000, ValueSize: 512}
	ingestGen(ctx, t, c, id, cfg, 1)
	join, err := c.Client.Join(ctx, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := join.Recv(); err != nil {
		t.Fatal(err)
	}
	// Otherwise the check that the restart wiped the data directory would
	// pass vacuously.
	if _, err := os.Stat(runDir(c, id)); err != nil {
		t.Fatalf("runs/%s before the kill: %v", id.GetValue(), err)
	}
	c.Kill()
	for err == nil {
		_, err = join.Recv()
	}
	wantErr(t, "the Join the kill cut off", err, codes.Unavailable, reasonNone)

	c.Restart()
	_, err = c.Client.SealRun(ctx, &reconv1.SealRunRequest{RunId: id})
	wantErr(t, "SealRun after the restart", err, codes.NotFound, reasonNotFound)
	_, err = harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	wantErr(t, "Join after the restart", err, codes.NotFound, reasonNotFound)
	entries, err := os.ReadDir(c.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("data directory after the restart holds %d entries, want it wiped", len(entries))
	}
}
