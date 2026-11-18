//go:build e2e

package e2e

import (
	"testing"

	"poc/e2e/gen"
	"poc/e2e/harness"
	"poc/internal/record"
)

// spillSizes returns the encoded bytes of cfg's records, as the sidecar
// holds and spills them, and the size of the largest.
func spillSizes(cfg gen.Config) (bytes int64, largest int) {
	for r := range cfg.Records(0, 1) {
		l := record.Len(len(r.Key), len(r.Value))
		bytes += int64(l)
		largest = max(largest, l)
	}
	return bytes, largest
}

// TestRealisticSpill joins realistic records, of variable size, under a
// tiny budget, and checks that the results are those of a sidecar with
// memory to spare. The run spills several times its budget into run files,
// which the Join reads with one worker, as many as the budget leaves read
// buffers for.
func TestRealisticSpill(t *testing.T) {
	t.Parallel()
	ctx := testContext(t)
	// 4 MiB holds the two Ingest streams' messages of about 1 MiB each.
	// The join parallelism asks for more workers than the budget holds.
	const budget = 4 << 20
	tiny := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "4MiB"), harness.Env("RECON_JOIN_PARALLELISM", "64"))
	roomy := harness.Start(t)

	payments := func(seed uint64, keys int, left, right gen.Encoding) gen.Config {
		return gen.Config{
			Seed:      seed,
			Keys:      keys,
			Left:      gen.Values{Schema: gen.Payment, Encoding: left},
			Right:     gen.Values{Schema: gen.BankLine, Encoding: right},
			Mix:       gen.Typical,
			KeyFormat: gen.RFKeys,
		}
	}
	for _, tc := range []struct {
		name string
		cfg  gen.Config
	}{
		{"protobuf", payments(21, 40_000, gen.Protobuf, gen.Protobuf)}, // about 19 MiB
		{"json-avro", payments(22, 20_000, gen.JSON, gen.Avro)},        // about 15 MiB
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytes, largest := spillSizes(tc.cfg)
			t.Logf("%d keys, %d MiB of records, the largest of %d bytes", tc.cfg.Keys, bytes>>20, largest)

			id := create(ctx, t, tiny, 1)
			ingestGen(ctx, t, tiny, id, tc.cfg, 2)
			// A run that spilled keeps nothing in memory once sealed.
			if files, spilled := spillFiles(t, tiny, id); files < 2 || spilled != bytes {
				t.Fatalf("%d run files of %d bytes after Seal, want the run's %d bytes in several", files, spilled, bytes)
			}
			got := verifiedJoin(ctx, t, tiny, id, tc.cfg)
			rec := joinFinished(t, tiny, id)
			workers, _ := rec.Int("recon.join_workers")
			files, _ := rec.Int("recon.join_files")
			readBuf, _ := rec.Int("recon.join_read_buffer_bytes")
			t.Logf("joined %d files with %d workers and read buffers of %d bytes", files, workers, readBuf)
			if workers < 1 || workers >= 64 {
				t.Errorf("%d workers under a %d-byte budget, want fewer than the 64 asked for", workers, budget)
			}

			id2 := create(ctx, t, roomy, 64)
			ingestGen(ctx, t, roomy, id2, tc.cfg, 2)
			if want := verifiedJoin(ctx, t, roomy, id2, tc.cfg); got != want {
				t.Errorf("fingerprint under the tiny budget is %+v, with memory to spare %+v; want them equal", got, want)
			}
			deleteRun(ctx, t, tiny, id)
			wantNoRunDir(t, tiny, id)
			deleteRun(ctx, t, roomy, id2)
		})
	}
	if st := tiny.State(); st.OOMKilled || !st.Running {
		t.Errorf("container state %+v, want it running and never OOM-killed", st)
	}
}
