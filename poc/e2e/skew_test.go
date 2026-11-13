//go:build e2e

package e2e

import (
	"bytes"
	"iter"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

const reasonHotKey = reconv1.ErrorReason_ERROR_REASON_HOT_KEY

// joinDirs returns the run's join-<n> directories.
func joinDirs(t *testing.T, c *harness.Container, id *reconv1.RunId) []string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(runDir(c, id), "join-*"))
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

func TestSubpartition(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB"))
	ctx := testContext(t)

	// About 96 MiB of distinct keys in one bucket: three times the budget,
	// and more than ten times a unit's.
	cfg := gen.Config{Seed: 12, Keys: 32_000, ValueSize: 1000}
	id := create(ctx, t, c, 1)
	ingestGen(ctx, t, c, id, cfg, 2)

	v := gen.NewVerifier(cfg)
	var during []string
	var first bool
	err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(r *reconv1.JoinResult) error {
		if !first { // the bucket is sub-partitioned before its first result
			first, during = true, joinDirs(t, c, id)
		}
		return v.Add(r)
	})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := v.Finish(); err != nil {
		t.Fatal(err)
	}
	if len(during) == 0 {
		t.Error("no join-<n> directory while the Join ran")
	}
	if dirs := joinDirs(t, c, id); len(dirs) != 0 {
		t.Errorf("join directories after the Join: %v", dirs)
	}

	var events []harness.LogRecord
	for _, r := range c.Logs() {
		if r.Body == "bucket sub-partitioned" && r.Attributes["recon.run_id"] == id.GetValue() {
			events = append(events, r)
		}
	}
	if len(events) == 0 {
		t.Fatal("no sub-partition event logged for the run")
	}
	for _, e := range events {
		depth, _ := e.Int("recon.depth")
		subs, _ := e.Int("recon.sub_buckets")
		t.Logf("sub-partitioned at depth %d into %d sub-buckets", depth, subs)
	}
	if depth, _ := events[0].Int("recon.depth"); depth != 1 {
		t.Errorf("the first sub-partition event is at depth %d, want 1", depth)
	}
	units, _ := joinFinished(t, c, id).Int("recon.join_units")
	t.Logf("%d join units", units)
	if units < 2 {
		t.Errorf("%d join units, want the sub-buckets joined in several", units)
	}
	if st := c.State(); st.OOMKilled || !st.Running {
		t.Errorf("container state %+v", st)
	}
	deleteRun(ctx, t, c, id)
	wantNoRunDir(t, c, id)
}

// hotRecords returns left records, then right records, of key, with values
// of size bytes.
func hotRecords(key []byte, left, right, size int) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for i := range left + right {
			side := harness.Left
			if i >= left {
				side = harness.Right
			}
			v := bytes.Repeat([]byte{byte(i)}, size)
			if !yield(harness.Record{Side: side, Key: key, Value: v}) {
				return
			}
		}
	}
}

func concat(seqs ...iter.Seq[harness.Record]) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for _, s := range seqs {
			for r := range s {
				if !yield(r) {
					return
				}
			}
		}
	}
}

func TestHotKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		env         []harness.Option
		left, right int
		size        int
	}{
		// A million records of about 30 bytes, several times the 4 MiB a
		// key may take of a unit's 8 MiB.
		{"budget", []harness.Option{harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB")}, 600_000, 400_000, 16},
		// A Match of about 3 MiB, whose 6 MiB estimate fits a unit.
		{"match", []harness.Option{harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB"), harness.Env("RECON_MAX_MATCH_BYTES", "1MiB")}, 2, 3000, 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := harness.Start(t, tc.env...)
			ctx := testContext(t)
			key := []byte("N/A")
			cfg := gen.Config{Seed: 13, Keys: 10_000, ValueSize: 100}
			id := create(ctx, t, c, 64)
			if err := harness.Ingest(ctx, c.Client, id, concat(cfg.Records(0, 2), hotRecords(key, tc.left, tc.right, tc.size), cfg.Records(1, 2))); err != nil {
				t.Fatal(err)
			}
			sealed := seal(ctx, t, c, id)
			left, right := cfg.Totals()
			if sealed.GetLeft().GetRecords() != left+uint64(tc.left) || sealed.GetRight().GetRecords() != right+uint64(tc.right) {
				t.Fatalf("Seal = %v", sealed)
			}

			want := &reconv1.HotKey{Key: key, LeftRecords: uint64(tc.left), RightRecords: uint64(tc.right)}
			var first *reconv1.ReconError
			for i := range 2 { // a retry fails the same way
				err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(*reconv1.JoinResult) error { return nil })
				wantErr(t, "Join", err, codes.ResourceExhausted, reasonHotKey)
				re := harness.ReconError(err)
				if !proto.Equal(re.GetHotKey(), want) {
					t.Errorf("HotKey %v, want %v", re.GetHotKey(), want)
				}
				t.Logf("Join %d: %s", i+1, re.GetDetail())
				if first == nil {
					first = re
				} else if !proto.Equal(re, first) {
					t.Errorf("the retry failed with %v, the first Join with %v", re, first)
				}
				if again := seal(ctx, t, c, id); !proto.Equal(again, sealed) {
					t.Errorf("SealRun after HOT_KEY = %v, want the run still SEALED with %v", again, sealed)
				}
			}
			if dirs := joinDirs(t, c, id); len(dirs) != 0 {
				t.Errorf("join directories after the Joins: %v", dirs)
			}
			deleteRun(ctx, t, c, id)
		})
	}
}
