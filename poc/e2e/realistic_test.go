//go:build e2e

package e2e

import (
	"testing"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
)

// readSize returns the size of the buffer c's joins stream spill files
// through: a sixteenth of the unit budget, the memory limit over the join
// parallelism, within 512 bytes and 1 MiB. It mirrors readSize in
// internal/join/stream.go. A record larger than the buffer makes it grow.
func readSize(t *testing.T, c *harness.Container) int {
	t.Helper()
	limits, ok := c.Log("effective limits")
	limit, ok1 := limits.Int("recon.memory_limit_bytes")
	parallelism, ok2 := limits.Int("recon.join_parallelism")
	if !ok || !ok1 || !ok2 || parallelism < 1 {
		t.Fatalf("no memory limit and join parallelism in the effective limits record %+v", limits)
	}
	return int(min(max(limit/parallelism/16, 512), 1<<20))
}

// spillSizes returns the encoded bytes of cfg's records, as the sidecar
// buffers and spills them, and how many of them are larger than n bytes.
func spillSizes(cfg gen.Config, n int) (bytes int64, larger int) {
	for r := range cfg.Records(0, 1) {
		l := buffer.EncodedLen(r.Key, r.Value)
		bytes += int64(l)
		if l > n {
			larger++
		}
	}
	return bytes, larger
}

// subpartitions returns the number of the run's "bucket sub-partitioned"
// records of each depth, and the sub-buckets the first of depth 1 made.
func subpartitions(c *harness.Container, id *reconv1.RunId) (byDepth map[int64]int, subBuckets int64) {
	byDepth = make(map[int64]int)
	for _, r := range c.Logs() {
		if r.Body != "bucket sub-partitioned" || r.Attributes["recon.run_id"] != id.GetValue() {
			continue
		}
		depth, _ := r.Int("recon.depth")
		if depth == 1 && byDepth[1] == 0 {
			subBuckets, _ = r.Int("recon.sub_buckets")
		}
		byDepth[depth]++
	}
	return byDepth, subBuckets
}

// TestRealisticSpill joins realistic records, of variable size, under a
// tiny budget, and checks that the results are those of a sidecar with
// memory to spare. The run's one bucket is many times the unit budget, so
// the join sub-partitions it twice, streaming every record from a spill
// file through a buffer smaller than the largest of Payment's records,
// whose metadata has a long tail. That buffer must grow to hold them.
func TestRealisticSpill(t *testing.T) {
	t.Parallel()
	ctx := testContext(t)
	// 4 MiB holds the two Ingest streams' messages of about 1 MiB each,
	// and the join parallelism, which only divides the budget into units
	// here, makes the unit budget 64 KiB and the read buffer 4 KiB.
	const budget = 4 << 20
	tiny := harness.Start(t, harness.Env("RECON_MEMORY_LIMIT_BYTES", "4MiB"), harness.Env("RECON_JOIN_PARALLELISM", "64"))
	roomy := harness.Start(t)
	read := readSize(t, tiny)

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
			// Otherwise the test would pass without the buffer growing.
			const minLarger = 8
			bytes, larger := spillSizes(tc.cfg, read)
			t.Logf("%d keys, %d MiB of records, %d of them larger than the %d-byte read buffer", tc.cfg.Keys, bytes>>20, larger, read)
			if larger < minLarger {
				t.Fatalf("%d records are larger than the %d-byte read buffer, want at least %d", larger, read, minLarger)
			}

			id := create(ctx, t, tiny, 1)
			ingestGen(ctx, t, tiny, id, tc.cfg, 2)
			// No more than the budget stays in memory.
			if files, spilled := spillFiles(t, tiny, id); spilled < bytes-budget {
				t.Fatalf("%d spill files of %d bytes after Seal, want all but %d of the run's %d bytes spilled", files, spilled, budget, bytes)
			}
			got := verifiedJoin(ctx, t, tiny, id, tc.cfg)

			// Every sub-bucket of depth 1 is sub-partitioned again, which
			// streams all of its records from its files.
			byDepth, subBuckets := subpartitions(tiny, id)
			units, _ := joinFinished(t, tiny, id).Int("recon.join_units")
			t.Logf("sub-partitioned %v times by depth, into %d sub-buckets at depth 1; %d join units", byDepth, subBuckets, units)
			if byDepth[1] != 1 || subBuckets == 0 || int64(byDepth[2]) != subBuckets {
				t.Errorf("sub-partitioned %v times by depth, the first time into %d sub-buckets; want the bucket once and each of its sub-buckets again", byDepth, subBuckets)
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
