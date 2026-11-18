//go:build e2e

package e2e

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

const reasonHotKey = reconv1.ErrorReason_ERROR_REASON_HOT_KEY

// hotRecords returns left records, then right records, of key, with values
// of size bytes, each starting with its number.
func hotRecords(key []byte, left, right, size int) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for i := range left + right {
			side := harness.Left
			if i >= left {
				side = harness.Right
			}
			v := make([]byte, max(size, 8))
			binary.BigEndian.PutUint64(v, uint64(i))
			if !yield(harness.Record{Side: side, Key: key, Value: v[:max(size, 8)]}) {
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

// TestOneSidedHotKey ingests 64 MiB of one key, on one side, under a
// 32 MiB budget and a 3 MiB maximum Match: it needs no Match, so the Join
// streams it, in ingest order, however large it is.
func TestOneSidedHotKey(t *testing.T) {
	t.Parallel()
	for _, side := range []harness.Side{harness.Left, harness.Right} {
		t.Run(fmt.Sprint(side), func(t *testing.T) {
			t.Parallel()
			c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB"))
			ctx := testContext(t)
			key := []byte("one-sided")
			const n = 64_000
			left, right := n, 0
			if side == harness.Right {
				left, right = 0, n
			}
			cfg := gen.Config{Seed: 12, Keys: 10_000, ValueSize: 100}
			id := create(ctx, t, c, 64)
			if err := harness.Ingest(ctx, c.Client, id, concat(cfg.Records(0, 2), hotRecords(key, left, right, 1000), cfg.Records(1, 2))); err != nil {
				t.Fatal(err)
			}
			seal(ctx, t, c, id)
			if files, _ := spillFiles(t, c, id); files < 2 {
				t.Fatalf("%d run files, want the key spread over several", files)
			}

			v := gen.NewVerifier(cfg)
			var next uint64
			err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, func(r *reconv1.JoinResult) error {
				rec := r.GetLeftOnly()
				if side == harness.Right {
					rec = r.GetRightOnly()
				}
				if !bytes.Equal(rec.GetKey(), key) {
					if m := r.GetBoth(); m != nil && bytes.Equal(m.GetKey(), key) {
						return fmt.Errorf("a Match of the one-sided key")
					}
					return v.Add(r)
				}
				if i := binary.BigEndian.Uint64(rec.GetValue()); i != next {
					return fmt.Errorf("the key's value %d came after %d: want ingest order", i, next-1)
				}
				next++
				return nil
			})
			if err != nil {
				t.Fatalf("Join: %v", err)
			}
			if err := v.Finish(); err != nil {
				t.Fatal(err)
			}
			if next != n {
				t.Errorf("%d results of the key, want %d", next, n)
			}
			rec := joinFinished(t, c, id)
			streamed, _ := rec.Int("recon.join_streamed_keys")
			if want := int64(1); side == harness.Left && streamed != want {
				t.Errorf("%d keys streamed, want the left key streamed once it outgrew a Match", streamed)
			}
			if peak, _ := rec.Int("recon.pool_peak_bytes"); peak > 32<<20 {
				t.Errorf("pool peak %d above the budget", peak)
			}
			if st := c.State(); st.OOMKilled || !st.Running {
				t.Errorf("container state %+v", st)
			}
			deleteRun(ctx, t, c, id)
			wantNoRunDir(t, c, id)
		})
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
		// A million records of about 30 bytes, a Match of about 22 MiB:
		// several times the default maximum of 3 MiB.
		{"default", []harness.Option{harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB")}, 600_000, 400_000, 16},
		// A Match of about 3 MiB, over a maximum of 1 MiB.
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
			deleteRun(ctx, t, c, id)
		})
	}
}

// TestLargeValues joins values of 1 to 1.8 MiB under a 32 MiB budget,
// from run files whose read buffers are smaller than the values: each is
// read when its result is emitted. A Match holds two of them, which keeps
// each JoinResponse within the 4 MiB a gRPC client receives by default.
func TestLargeValues(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "32MiB"), harness.Env("RECON_MAX_MATCH_BYTES", "12MiB"))
	ctx := testContext(t)
	records := randomRecords(14, 20_000)
	for i := range 40 {
		v := make([]byte, 1<<20+(i*97<<10)%(800<<10))
		for j := 0; j < len(v); j += 4096 {
			v[j] = byte(i + j)
		}
		records = append(records, oracle.Record{Side: oracle.Side(i % 2), Key: fmt.Appendf(nil, "large-%d", i/2), Value: v})
	}
	records = append(records, randomRecords(15, 20_000)...)
	id := create(ctx, t, c, 16)
	if err := harness.Ingest(ctx, c.Client, id, seq(records)); err != nil {
		t.Fatal(err)
	}
	wantTotals(t, seal(ctx, t, c, id), records, 1)
	got, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	// One stream: each Match's values are in ingest order.
	if d := oracle.DiffOrdered(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	rec := joinFinished(t, c, id)
	readBuf, _ := rec.Int("recon.join_read_buffer_bytes")
	files, _ := rec.Int("recon.join_files")
	t.Logf("%d files, read buffers of %d bytes", files, readBuf)
	if files == 0 || readBuf >= 1<<20 {
		t.Errorf("%d files read with buffers of %d bytes, want values larger than the buffers", files, readBuf)
	}
	if st := c.State(); st.OOMKilled || !st.Running {
		t.Errorf("container state %+v", st)
	}
	deleteRun(ctx, t, c, id)
}

// TestValueOrder ingests on one stream under a small budget, so that keys'
// values are spread over several run files, and checks that each Match
// holds each side's values in ingest order.
func TestValueOrder(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "8MiB"))
	ctx := testContext(t)
	records := randomRecords(16, 400_000)
	id := create(ctx, t, c, 1)
	if err := harness.Ingest(ctx, c.Client, id, seq(records)); err != nil {
		t.Fatal(err)
	}
	wantTotals(t, seal(ctx, t, c, id), records, 1)
	if files, _ := spillFiles(t, c, id); files < 2 {
		t.Fatalf("%d run files, want several", files)
	}
	got, err := harness.JoinAll(ctx, c.Client, &reconv1.JoinRequest{RunId: id})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if d := oracle.DiffOrdered(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	deleteRun(ctx, t, c, id)
}
