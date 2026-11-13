# POC results

These are the measurements PLAN.md §8.5 asks for. Every number below was measured on the POC as built, by `TestMeasure` (`e2e/measure_test.go`), unless it says otherwise. The suite ran twice on 2026-11-13, one pass after the other. The tables come from the first pass. The second pass agreed with it, where a difference is the gap between the two passes over their mean: the pool peak was the same to 0.1 MiB in every run, VmHWM was within 3% (5% for the Tiny runs), the Go peak within 5%, the expansion factor within 0.005 (the largest unit's factor within 0.03, and 0.05 for Tiny), the number of spill files within 6%, the bytes spilled within 5%, the disk peak within 2%, join times within 14%, and ingest times within 20%. The exception was one Tiny run, whose ingest took 2.5 s in the first pass and 1.3 s in the second. Both default-budget scenarios without `GOMEMLIMIT` were OOM-killed again, 1.1 s into ingest. The data, and so the value sizes and compression ratios, were the same in both passes. So treat differences of a few percent in memory, and of up to about 20% in time, as noise. Runs of a few seconds are noisier.

## Summary

- **Earlier results used unrealistic data on a disk that compressed it.** Their values were 1000 bytes of a header and a repeated pattern, which compress 52.0 times with zstd level 1 and 38.4 times with s2 in 64 KiB spill-format blocks. Their data directory was on btrfs mounted `compress=zstd:1`, which compressed the spill files. These results use marshaled records that compress 1.1 to 3.5 times with zstd level 1, on a directory with compression turned off. Rerunning the old code on both directories moved its times by about 5%, within the noise. See [Earlier results](#earlier-results-and-what-moved-them).
- **The earlier join throughput was limited by the test client, not the sidecar.** The old client verified each result on the goroutine that read the Join stream. On the same data, image and directory, in the 10 GiB default-budget scenario, the new client, which verifies on 8 goroutines, measured the join at 771 to 844 MiB/s, against 544 to 554 MiB/s for the old one. Ingest didn't change.
- **The reservation accounting holds.** In every run the pool's peak reservation reached the budget and never went above it. The accounted working set of a join unit was at most 1.17 times its record bytes for the realistic profiles, and 1.40 times for the smallest records.
- **Go's garbage collector is the gap.** With `GOMEMLIMIT` unset, the sidecar's peak resident set was 2.4 to 2.9 times the pool's peak. With the default budget of 70% of the container limit, the container was OOM-killed 1.0 to 1.6 s into ingest, at both 1 GiB and 10 GiB.
- **`GOMEMLIMIT` at 90% of the container limit closes the gap.** All 14 default-budget runs with it completed, with a peak resident set of 1.26 to 1.30 times the budget, or 88 to 91% of the container limit. No throughput cost was measurable.
- **§8.4's expansion factor of 2 is conservative for the planner.** The measured factor was 1.03 to 1.11 for the realistic profiles, 1.02 for the old 1 KB values and 1.34 for Tiny records of 41 bytes of key and value. It does not cover garbage-collector headroom, which is a separate multiplier.
- **Throughput on this machine,** in the first pass, in key and value bytes, for Payment and BankLine records as protobuf: ingest ran at 133 to 210 MiB/s and the join at 381 to 472 MiB/s. Across the other profiles at 10 GiB, ingest ran at 140 to 223 MiB/s and the join at 356 to 670 MiB/s, or 1.2 to 2.3 M records/s. The disk peak was 0.94 to 1.06 times the key and value bytes, and 1.13 to 1.14 times for Tiny records.

## Method

| | |
|---|---|
| Machine | AMD Ryzen 9 5950X (32 CPUs), 31 GB RAM, NVMe, Linux 7.3.3 (Fedora 44), 8 GiB zram swap |
| Engine | Rootless podman 5.8.9, image `recon-sidecar:dev` (image ID `21c7441554d7`) built by `make image` with go1.27.4. The sidecar's code and dependencies are unchanged since the earlier results; only the test client changed |
| Container | `--memory=512m --memory-swap=512m` (no swap), no `--cpus`, so `GOMAXPROCS` is 32 and the default join parallelism is 4 |
| Data directory | `/var/tmp/recon-e2e-nocomp`, chosen with `RECON_E2E_DATA_ROOT`: btrfs on NVMe, mounted `compress=zstd:1`, but with the directory's no-compress attribute set (`chattr +m`) before any file was written in it. New files and directories inherit the attribute. Every RESULT records the attribute as set (`NoCompress`) and the files as not compressed (`Compresses` false) |
| Run | `bucket_count` 1024. Ingested over 4 parallel streams in batches of about 1 MiB, then joined. Every result was checked by `gen.Verifier` |
| Data | `e2e/gen`: marshaled records, see [Data](#data) |
| Client | The `go test` process on the same machine, over TLS on loopback. It generates the data while it sends, and checks the Join's results on 8 goroutines (`min(GOMAXPROCS, 8)`), sharded by key block |
| Repeats | Two complete passes of every scenario, of 723 s and 707 s |

**Proof that the directory doesn't compress.** Before measuring, a compressible 64 MiB file was written under the directory and in `/var/tmp`. `filefrag -v` showed it as one plain extent under the directory, and as 510 compressed ("encoded") extents in `/var/tmp`. After the runs, `compsize` on the same two files agreed: 64 MiB stored without compression, and 64 MiB stored with zstd in 2.0 MiB of disk (3%).

### Data

A value is a record marshaled the way a client would marshal it. `e2e/gen` builds each record from a schema, an ordered list of typed fields, and writes it as protobuf, Avro or JSON, byte for byte as `protoc-gen-go`, `avroc-gen-go` and `encoding/json` would for the same struct. Tests check this against code generated from the schemas, checked in under `e2e/gen/reference`. Each value is a function of the seed, the side, the key and the record's index, so the verifier can regenerate it and compare the bytes.

There are five profiles. These are their sizes and ratios as `datastats` measures them, for about 96 MiB of one side's records in spill format, in the Typical mix with RF keys. The ratios are of 64 KiB blocks of those records:

| Profile | What it holds | Mean value, protobuf / Avro / JSON (bytes) | 99th percentile, protobuf (bytes) | Largest, protobuf (bytes) | zstd level 1, protobuf / Avro / JSON | s2, protobuf / Avro / JSON |
|---|---|---|---|---|---|---|
| Payment | An internal ledger's payment: a nested counterparty, fees in the payment's currency, tags, and a rare metadata list with a long tail | 261.2 / 219.5 / 614.4 | 808 | 12,985 | 1.68 / 1.65 / 3.46× | 1.42 / 1.38 / 2.60× |
| BankLine | A bank statement line | 191.1 / 148.2 / 405.4 | 292 | 324 | 1.90 / 1.84 / 3.65× | 1.63 / 1.56 / 2.70× |
| CardSettlement | A card network's settlement record, mostly numbers | 143.8 / 104.4 / 415.1 | 147 | 149 | 1.62 / 1.50 / 4.13× | 1.30 / 1.18 / 2.57× |
| Envelope | A value a client encrypted: a key ID, a nonce and the ciphertext, which doesn't compress | 581.7 / 578.5 / 793.9 | 1,702 | 6,210 | 1.14 / 1.13 / 1.46× | 1.13 / 1.13 / 1.14× |
| Tiny | A minimal record, for the cost per record | 23.3 / 14.3 / 86.1 | 24 | 25 | 1.43 / 1.00 / 2.77× | 1.19 / 1.00 / 1.93× |

- **Match mix:** the Typical mix. About 93% of keys have one record on each side, 3% are left-only, 3% are right-only, and 1% are 1:N or N:1 with N of 2 or 3.
- **Keys:** "RF" and 16 hex digits of a scramble of the key's number, 18 bytes.
- **Sizing:** each scenario names a target in key and value bytes, and the generator picks the number of keys from a sampled mean record size. Every throughput below divides the key and value bytes the client actually sent: 1,023 MiB for the 1 GiB scenarios, 10,226 to 10,242 MiB for the 10 GiB ones and 275 MiB for Tiny. The old generator's number of keys is fixed, which gives 10,177 MiB.
- **The old generator** ran once more for comparison, with its old settings: 16-byte keys (`key-%012d`), 1000-byte values of a header and a repeated pattern, and the Stress mix of 0 to 3 records per side per key.

### Scenarios

All ran in a 512 MiB container without swap, with `bucket_count` 1024 and 4 Ingest streams.

- **Payment on the left and BankLine on the right, as protobuf:** 1 GiB and 10 GiB, each with a budget of 128 MiB (`RECON_MEMORY_LIMIT_BYTES=128MiB`) or the default of 70% of 512 MiB (358 MiB), each with `GOMEMLIMIT` unset or `483183820`, which is 90% of 512 MiB. This is the same eight-scenario grid as the earlier results, so the memory figures stay comparable.
- **The same pair as Avro, and as JSON;** CardSettlement on both sides as protobuf; Envelope on both sides as protobuf: 10 GiB, default budget, `GOMEMLIMIT` 90%.
- **Tiny on both sides as protobuf:** 275 MiB, 128 MiB budget, `GOMEMLIMIT` unset and 90%. This replaces the earlier 32-byte small-record scenario.
- **The old generator with its old settings:** 10 GiB, default budget, `GOMEMLIMIT` 90%.

### What was measured

- **Pool peak:** `recon.pool_peak_bytes`, from the join-finished log record. It is the largest reservation since the process started.
- **Go peak:** `recon.go_peak_bytes`, from the same record. It reads `/memory/classes/total:bytes` from `runtime/metrics`. This counts all the memory the runtime has mapped, and it never decreases, because the runtime returns freed pages to the OS but never unmaps them. That makes it the high-water mark of Go's memory.
- **VmHWM:** the sidecar process's peak resident set, read from the host's `/proc/<pid>/status` after the Join. This is the number the OOM killer compares against the limit.
- **Expansion factor:** `recon.expansion_factor`, the sum over units of their working set divided by their record bytes. A unit's working set is its in-memory chunks at their capacity, plus its spill files once read, plus a 16-byte index entry per record. The record also gives the largest unit's record bytes and working set.
- **Disk peak:** `recon.disk_peak_bytes`, the most bytes of spill files on disk at once, as the sidecar counts the bytes it wrote.
- **Spill files and bytes:** counted in the data directory after Seal.
- **Throughput:** the key and value bytes the client sent, divided by wall-clock time as the client saw it.
- **Sample ratios and value sizes:** before each run, the client takes the run's first 64 MiB or so of records in spill format, and reports their s2 and zstd level 1 ratios in 64 KiB blocks and the percentiles of their value sizes.
- **Data filesystem:** the type, mount options and no-compress attribute of the container's data directory, and whether the filesystem may compress its files.

The cgroup's `memory.peak` reached about 512 MiB in every run, because page cache from spill writes fills the cgroup up to its limit. That cache can be reclaimed, so `memory.peak` says nothing about the sidecar and is left out below.

### Reproducing

Make a directory that btrfs won't compress, while it is still empty, then run `make image` and the suite. A pass takes about 12 minutes and needs about 11 GiB of free disk in the data directory.

```
mkdir /var/tmp/recon-e2e-nocomp && chattr +m /var/tmp/recon-e2e-nocomp
make image
RECON_E2E_IMAGE=recon-sidecar:dev RECON_E2E_DATA_ROOT=/var/tmp/recon-e2e-nocomp go test -tags e2e,measure -run TestMeasure -timeout 3h -v ./e2e/
```

Each scenario prints a `RESULT` line of JSON. `go run ./e2e/gen/cmd/datastats` prints the profile table above. It also takes `-profile` and `-encoding` for one row, `-schema` for a schema of your own as JSON, or `-values` with `-framing varint` or `-framing lines` for a file of real values, so the profiles can be checked against real data.

## Earlier results, and what moved them

The results published before this rerun (commit `3ec5059`) differed in four ways:

- **Their values compressed far better than real records.** Every value was 1000 bytes (32 in the small-record scenario) of a header and a repeated pattern. In 64 KiB spill-format blocks they compress 52.0 times with zstd level 1 and 38.4 times with s2. The realistic records here compress 1.1 to 3.5 times.
- **Their data directory compressed the spill files.** It was under `/var/tmp`, on btrfs mounted `compress=zstd:1`. How much btrfs compressed those spill files was not measured.
- **They used the Stress mix:** 0 to 3 records per side per key, uniformly, so only 6% of keys were 1:1.
- **Their client verified each result on the goroutine that read the Join stream.**

To separate these, the old test code (commit `3ec5059`, changed only to read `RECON_E2E_DATA_ROOT`) ran its 10 GiB, default-budget, `GOMEMLIMIT` 90% scenario twice on each directory, with the same image, right after the two passes and with the machine otherwise idle. The new client ran the same old data on the uncompressed directory in each pass:

| Client | Data directory | Ingest (s) | Ingest (MiB/s) | Join (s) | Join (MiB/s) | VmHWM (MiB) | Disk peak (MiB) |
|---|---|---|---|---|---|---|---|
| Old, as published | Compressed | 36.0 | 283 | 22.1 | 460 | 461 | 10,175 |
| Old | Compressed | 37.4 | 272 | 19.9 | 511 | 465 | 10,171 |
| Old | Compressed | 34.3 | 296 | 18.9 | 538 | 466 | 10,174 |
| Old | Not compressed | 35.2 | 289 | 18.7 | 544 | 459 | 10,177 |
| Old | Not compressed | 32.0 | 318 | 18.4 | 554 | 460 | 10,175 |
| New, pass 1 | Not compressed | 36.3 | 281 | 12.1 | 844 | 465 | 10,171 |
| New, pass 2 | Not compressed | 32.6 | 312 | 13.2 | 771 | 462 | 10,170 |

Every run handled the same 10,177 MiB of keys and values in 10.50 M records.

- **The filesystem's compression made little difference to these runs.** On average, the old code's ingest took 6% less time on the uncompressed directory, and its join 4% less. That is about as large as the difference between two repeats on the same directory. Memory and the disk peak didn't change; the disk peak counts the bytes the sidecar wrote, before any compression by the filesystem.
- **The test client limited the earlier join throughput.** On the same data, image and directory, the old client joined at 544 and 554 MiB/s and the new client at 844 and 771 MiB/s: the join took about a third less time. Ingest didn't change. A benchmark of the old verifier alone checked the old 1000-byte values at about 1.2 GB/s on one goroutine, about twice the old join's rate. So the limit was the loop that received each result and then verified it on the same goroutine, not the verification by itself. The earlier join figures for 1 KB values, 390 to 540 MiB/s, therefore most likely measured the test client, not the sidecar. Only the 10 GiB default-budget scenario was compared; the other 1 KB scenarios used the same client and values.
- **Whether the joins of realistic records below are limited by the new client was not measured.**
- Today's runs of the old code joined in 18.4 to 19.9 s, against 22.1 s published. That is within the 22% the earlier results gave as the noise in times.

## Memory: pool accounting against Go's real use (§20 Q1)

Sizes are the target key and value bytes; budgets are 128 MiB or the default of 358 MiB.

| Data | Size, budget, `GOMEMLIMIT` | Budget (MiB) | Pool peak (MiB) | Go peak (MiB) | VmHWM (MiB) | Go peak / pool | VmHWM / pool | VmHWM / 512 MiB limit |
|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 128 | 128.0 | 322 | 314 | 2.51 | 2.46 | 61% |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 128 | 128.0 | 329 | 325 | 2.57 | 2.54 | 64% |
| Payment/BankLine, protobuf | 1 GiB, default, unset | 358 | **OOM-killed 1.6 s into ingest** | | | | | |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 358 | 358.4 | 559 | 454 | 1.56 | 1.27 | 89% |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 128 | 128.0 | 375 | 366 | 2.93 | 2.86 | 71% |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 128 | 128.0 | 366 | 360 | 2.86 | 2.82 | 70% |
| Payment/BankLine, protobuf | 10 GiB, default, unset | 358 | **OOM-killed 1.0 s into ingest** | | | | | |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 578 | 458 | 1.61 | 1.28 | 89% |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 358 | 358.4 | 583 | 459 | 1.63 | 1.28 | 90% |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 358 | 358.4 | 578 | 459 | 1.61 | 1.28 | 90% |
| CardSettlement, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 583 | 458 | 1.63 | 1.28 | 90% |
| Envelope, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 569 | 459 | 1.59 | 1.28 | 90% |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 128 | 128.0 | 339 | 322 | 2.65 | 2.51 | 63% |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 128 | 128.0 | 338 | 337 | 2.64 | 2.63 | 66% |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 358 | 358.4 | 573 | 465 | 1.60 | 1.30 | 91% |

**Swap hides the problem.** An earlier run of the same scenarios, with the old generator, used podman's default, which allows as much swap again as `--memory`, here zram. In that run the two default-budget scenarios without `GOMEMLIMIT` were not OOM-killed:

| Scenario | Go peak (MiB) | VmHWM (MiB) | Ingest |
|---|---|---|---|
| 1 GiB, default budget, `GOMEMLIMIT` unset | 816 | 522 | 5.2 s |
| 10 GiB, default budget, `GOMEMLIMIT` unset | 897 | 523 | 64.5 s |

The 10 GiB ingest took 64.5 s, against 29.3 s with `GOMEMLIMIT` set in the same run, because the sidecar was swapping. Kubernetes nodes usually run without swap, so the e2e harness now passes `--memory-swap` equal to `--memory` for every test. With swap allowed, a test's "never OOM-killed" check could pass on a container that would have been killed in production.

**What this means for §20 Q1 (Rust or Go):**

- **The accounting is sound.** Every buffer, message, unit and batch reserves before it allocates. The pool never went over its budget, and the accounted working set stayed within 1.40 times the record bytes. The unaccounted memory is almost entirely garbage-collector headroom. With `GOGC=100` the heap goal is about twice the live heap, and the measurements match that: 2.4 to 2.9 times the pool peak, across both passes.
- **Without `GOMEMLIMIT`, Go cannot use the proposal's default.** A 70% budget would need a container 1.7 to 2 times its limit. To fit with `GOMEMLIMIT` unset, the budget would have to be at most about 35% of the limit (1 / 2.86), with no headroom left.
- **With `GOMEMLIMIT` at 90%, the 70% default works.** The garbage collector holds the process at about the soft limit: across the 14 default-budget runs with it, the resident set was 453 to 465 MiB against a soft limit of 460.8 MiB. That leaves about 10% of the container for what Go doesn't count: stacks, the binary and gRPC's buffers. No run came close to being killed, but the margin is set by the soft limit, not by the budget. Throughput did not drop measurably. The 10 GiB default-budget run with the limit ingested at 167 MiB/s and joined at 444 MiB/s, as fast as the 128 MiB run without it (145 and 437 MiB/s). At the 128 MiB budget, the runs with and without the limit took about as long.
- **So Go is viable if `GOMEMLIMIT` is always set** to about 90% of the cgroup limit alongside the 70% budget. The sidecar could set it itself from the cgroup limit; the POC doesn't. The cost is a soft limit the runtime enforces by working the garbage collector harder, and a gap of about 30% above the budget that the planner can't see.
- **What this doesn't measure:** Rust's equivalent gap (allocator fragmentation and gRPC buffers) was not measured, so this result shows that Go can work, not that it matches Rust. A Go sidecar that gets a container without `GOMEMLIMIT` set will be OOM-killed under load at the default budget.

## Expansion factor (§8.4)

| Data | Size, budget, `GOMEMLIMIT` | Average record (key + value bytes) | Units | Expansion factor (all units) | Largest unit: record bytes (MiB) | Largest unit: working set (MiB) | Largest unit's factor |
|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 244 | 69 | 1.076 | 15.7 | 17.4 | 1.108 |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 244 | 69 | 1.077 | 15.8 | 17.3 | 1.098 |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 244 | 25 | 1.106 | 43.8 | 51.3 | 1.172 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 244 | 1024 | 1.065 | 10.7 | 11.4 | 1.065 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 244 | 1024 | 1.065 | 10.7 | 11.4 | 1.064 |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 244 | 256 | 1.067 | 42.0 | 45.1 | 1.073 |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 202 | 256 | 1.079 | 42.3 | 46.0 | 1.089 |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 528 | 256 | 1.034 | 41.3 | 43.0 | 1.040 |
| CardSettlement, protobuf | 10 GiB, default, 90% | 162 | 256 | 1.096 | 42.9 | 47.3 | 1.104 |
| Envelope, protobuf | 10 GiB, default, 90% | 600 | 256 | 1.031 | 41.5 | 42.8 | 1.032 |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 41 | 22 | 1.340 | 16.0 | 22.4 | 1.405 |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 41 | 22 | 1.340 | 16.0 | 22.4 | 1.404 |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 1016 | 256 | 1.020 | 41.4 | 42.4 | 1.025 |

The factor has two parts:

- **The index:** 16 bytes per record. Records are encoded with an 8-byte hash and two varint lengths ahead of the key and value, 10 bytes for Tiny's records and 11 for most others. So the index adds about 2% for the old 1 KB values, 3 to 9% for the realistic profiles, and 31% for Tiny's records of about 51 encoded bytes. The factor follows it: the smaller the records, the larger the factor.
- **Chunk slack:** buffers still in memory hold their chunks at capacity, not at the bytes they contain. Slack is largest when more of the data stays in memory: the default-budget 1 GiB run had the largest unit factor of the realistic runs, 1.17 (1.14 in the second pass).

**What this means for §8.4's factor of 2:**

- **The planner over-provisions.** A unit is planned at up to half the unit budget, but it used at most 70% of the budget (1.40 / 2), and 52 to 59% for the realistic profiles. So a factor of 2 is safe for these records, and a factor near `1 + 16 / average encoded record + 0.15` would fit more data in each unit.
- **Tiny records can exceed 2.** The index alone is `1 + 16 / r` for an encoded record of `r` bytes, so records under about 16 encoded bytes (a key and value of about 6 bytes together) exceed a factor of 2 before any chunk slack. Calibrating to the record size, rather than using a constant, would cover both cases.
- **Parallel units can need more than their estimate.** Units that run at once each hold their pinned chunks at capacity, and one unit's pinned chunks can't be spilled to make room for another unit. For many small in-memory buffers that is up to about twice the bytes plus 4 KiB each. The in-process property test (`TestRun_property_tiny_budgets_spill_and_match_the_oracle`) therefore sizes its parallel budgets from the run's actual buffers after Seal. At realistic budgets, where ingest has already spilled most buffers, this did not show up.
- **What the factor does not cover:** garbage-collector headroom (see the memory section), which is a separate multiplier on top of it.

## Throughput, spill and disk

| Data | Size, budget, `GOMEMLIMIT` | Key + value bytes (MiB) | Records | Ingest (s) | Ingest (MiB/s) | Join (s) | Join (MiB/s) | Join (M records/s) | Spill files after Seal | Spilled after Seal (MiB) | Disk peak (MiB) | Disk peak / key + value |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 1,023 | 4.39 M | 7.7 | 133 | 2.2 | 472 | 2.03 | 14,125 | 990 | 1,032 | 1.01 |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 1,023 | 4.39 M | 6.6 | 155 | 2.7 | 381 | 1.63 | 14,063 | 986 | 1,032 | 1.01 |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 1,023 | 4.39 M | 4.9 | 210 | 2.5 | 405 | 1.74 | 4,707 | 861 | 965 | 0.94 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 10,227 | 43.93 M | 70.7 | 145 | 23.4 | 437 | 1.88 | 149,652 | 10,610 | 10,633 | 1.04 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 10,227 | 43.93 M | 64.8 | 158 | 22.3 | 458 | 1.97 | 149,750 | 10,610 | 10,635 | 1.04 |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 10,227 | 43.93 M | 61.1 | 167 | 23.1 | 444 | 1.91 | 51,782 | 10,465 | 10,581 | 1.03 |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 10,226 | 53.14 M | 65.9 | 155 | 24.5 | 417 | 2.17 | 52,083 | 10,544 | 10,664 | 1.04 |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 10,237 | 20.33 M | 49.3 | 208 | 15.9 | 645 | 1.28 | 50,983 | 10,251 | 10,337 | 1.01 |
| CardSettlement, protobuf | 10 GiB, default, 90% | 10,240 | 66.34 M | 73.1 | 140 | 28.8 | 356 | 2.30 | 51,701 | 10,715 | 10,833 | 1.06 |
| Envelope, protobuf | 10 GiB, default, 90% | 10,242 | 17.90 M | 46.0 | 223 | 15.3 | 670 | 1.17 | 49,615 | 10,203 | 10,317 | 1.01 |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 275 | 6.98 M | 2.5 | 111 | 2.3 | 120 | 3.04 | 3,635 | 260 | 314 | 1.14 |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 275 | 6.98 M | 2.5 | 109 | 2.2 | 125 | 3.16 | 3,632 | 260 | 310 | 1.13 |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 10,177 | 10.50 M | 36.3 | 281 | 12.1 | 844 | 0.87 | 49,221 | 10,055 | 10,171 | 1.00 |

The data in each run, from the RESULT's sample of its first records (value sizes in bytes, ratios of 64 KiB blocks in spill format):

| Data | Mean value | Median | 90th percentile | 99th percentile | Largest | s2 | zstd level 1 |
|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 226 | 210 | 286 | 485 | 12,110 | 1.50× | 1.77× |
| Payment/BankLine, Avro | 184 | 169 | 244 | 406 | 10,089 | 1.45× | 1.72× |
| Payment/BankLine, JSON | 510 | 478 | 653 | 953 | 16,589 | 2.64× | 3.54× |
| CardSettlement, protobuf | 144 | 144 | 146 | 147 | 149 | 1.30× | 1.62× |
| Envelope, protobuf | 582 | 503 | 966 | 1,708 | 4,448 | 1.13× | 1.14× |
| Tiny, protobuf | 23 | 23 | 24 | 24 | 25 | 1.19× | 1.43× |
| Old generator | 1000 | 1000 | 1000 | 1000 | 1000 | 38.4× | 52.0× |

- **Ingest** ran at 133 to 210 MiB/s for Payment and BankLine as protobuf, and 140 to 223 MiB/s for the other profiles at 10 GiB: 0.4 to 0.9 M records/s. The client generated the data and ran on the same 32 CPUs, and was not profiled, so these are lower bounds for the sidecar. A larger budget spilled less: about a third as many spill files. It ingested faster at 1 GiB (4.9 s against 6.6 s, and 4.0 s against 7.0 s in the second pass), but not measurably at 10 GiB (61.1 s against 64.8 s, and 64.6 s against 62.6 s in the second pass).
- **The join** read every spill file back and ran at 381 to 472 MiB/s for Payment and BankLine as protobuf, and 356 to 670 MiB/s for the other profiles at 10 GiB, with 4 units in parallel, including the client's verification of every result. In records it ran at 1.2 to 2.3 M records/s: fastest in MiB/s for the largest records (JSON and Envelope), and in records/s for the smallest. With the old 1 KB values it ran at 844 MiB/s. Whether the client limited these joins was not measured.
- **For Tiny records** both ingest and join drop to 109 to 125 MiB/s, because the cost per record dominates: about 3 M records/s. In the second pass one Tiny run ingested in 1.3 s, 5.2 M records/s; runs this short are noisy.
- **Disk:** the sidecar doesn't compress its spill files, and here the data directory didn't either. The files hold the encoded records. For the realistic profiles the disk peak was 0.94 to 1.06 times the key and value bytes, lower with the larger budget at 1 GiB. For Tiny it was 1.13 to 1.14 times, because each record's 10-byte header is large next to 41 bytes of key and value. It is somewhat above the bytes spilled after Seal, because a Join spills the buffers still in memory to make room for its units. No bucket here was oversized (the largest unit's record bytes were at most half the unit budget), so these runs had no sub-partitioning rewrites. §15's 1.5 times budget for those rewrites is not exercised by these numbers.
- **These are starting points for M3's targets, not targets:** a single machine, two runs of each scenario, synthetic data, and no spill compression.

## Other results from the tests

These were not rerun with the realistic data.

- **Backpressure (`TestBackpressure`):** the client read 1101 MiB at 5 MiB/s, which took 3 min 40 s, from a 256 MiB container with a 64 MiB budget and no swap. The sidecar was still joining when the client was halfway through, and it was never OOM-killed. Its join-finished record gave a pool peak of 67,108,016 bytes (within the 64 MiB budget) and a Go peak of 190,392,632 bytes (182 MiB). podman's sampled peak, including page cache, was 169 MiB. gRPC flow control, through the single sender, held the 4 workers back.
- **Concurrent Joins of different runs share one pool,** and each Join plans its units for the whole budget, so two Joins at once can fail with `MEMORY_EXHAUSTED`. That is retryable, and the run stays SEALED. The design (§8.4, §15) sizes for one Join at a time. In the in-process test `TestRun_concurrent_Joins_share_the_pool` (three sealed runs of 10,000 records each, joined at once in a 512 KiB pool with parallelism 2), 2 or 3 of the 3 Joins failed in 299 of 300 repetitions, and every one of them then succeeded when joined alone.
- **Joins also hold memory that another run's ingest can't spill.** In a variant of `TestRun_concurrent_runs_spill_one_another` that let all three runs join as soon as each was sealed, 1 of about 700 repetitions failed a still-ingesting run with `memory exhausted: reserving 8192 bytes with 520357 of 524288 in use`: the other runs' Joins held nearly the whole pool. Under §6.2 that fails the run. With one Join at a time, as the test now does, it did not happen in 1,800 repetitions. So with several runs, a client should join one run at a time: §15 sizes join memory for a single Join.
