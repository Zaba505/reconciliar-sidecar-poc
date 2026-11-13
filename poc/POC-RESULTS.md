# POC results

These are the measurements PLAN.md §8.5 asks for. Every number below was measured on the POC as built, by `TestMeasure` (`e2e/measure_test.go`). The tables come from one run of each scenario. A second, independent run of every scenario agreed with them within 2% on the pool peak and VmHWM, within 5% on the Go peak, within 0.02 on the expansion factor, within 6% on the bytes spilled and the disk peak, and within 22% on ingest and join times. Both default-budget scenarios without `GOMEMLIMIT` were OOM-killed again, 0.6 s and 0.8 s into ingest. So treat differences of a few percent in memory, and of up to about 20% in time, as noise.

## Summary

- **The reservation accounting holds.** In every run the pool's peak reservation reached the budget exactly and never went above it. The accounted working set of a join unit was at most 1.35 times its record bytes.
- **Go's garbage collector is the gap.** With `GOMEMLIMIT` unset, the sidecar's peak resident set was 2.4 to 2.8 times the pool's peak. With the default budget of 70% of the container limit, the container was OOM-killed within the first second of ingest, at both 1 GiB and 10 GiB.
- **`GOMEMLIMIT` at 90% of the container limit closes the gap.** The same runs then completed, with a peak resident set of 1.29 times the budget, or 90% of the container limit. No throughput cost was measurable.
- **§8.4's expansion factor of 2 is conservative for the planner.** The measured factor was 1.02 to 1.13 for 1 KB values and 1.35 for 58-byte records. It does not cover garbage-collector headroom, which is a separate multiplier.
- **Throughput on this machine:** ingest ran at 190 to 310 MiB/s and the join at 390 to 540 MiB/s, measured in key and value bytes. The disk peak was at most 1.01 times the key and value bytes for 1 KB values.

## Method

| | |
|---|---|
| Machine | AMD Ryzen 9 5950X (32 CPUs), 31 GB RAM, NVMe, Linux 7.3.3 (Fedora 44), 8 GiB zram swap |
| Engine | Rootless podman 5.8.9, image `recon-sidecar:dev` built by `make image` (Go 1.27.0) |
| Container | `--memory=512m --memory-swap=512m` (no swap), no `--cpus`, so `GOMAXPROCS` is 32 and the default join parallelism is 4 |
| Data directory | A bind mount on btrfs under `/var/tmp` (not a tmpfs) |
| Run | `bucket_count` 1024. Ingested over 4 parallel streams in batches of about 1 MiB, then joined. Every result was checked by `gen.Verifier` |
| Data | `e2e/gen`: 16-byte keys, 0 to 3 records per side per key. Values are 1000 bytes, or 32 bytes for the small-record scenario |
| Client | The `go test` process on the same machine, over TLS on loopback. It generates the data while it sends |

The variables were the data size (1 GiB, 10 GiB, and 275 MiB of small records), the budget (`RECON_MEMORY_LIMIT_BYTES=128MiB`, or the default of 70% of 512 MiB, which is 358 MiB), and `GOMEMLIMIT` (unset, or `483183820`, which is 90% of 512 MiB).

What was measured:

- **Pool peak:** `recon.pool_peak_bytes`, from the join-finished log record. It is the largest reservation since the process started.
- **Go peak:** `recon.go_peak_bytes`, from the same record. It reads `/memory/classes/total:bytes` from `runtime/metrics`. This counts all the memory the runtime has mapped, and it never decreases, because the runtime returns freed pages to the OS but never unmaps them. That makes it the high-water mark of Go's memory.
- **VmHWM:** the sidecar process's peak resident set, read from the host's `/proc/<pid>/status` after the Join. This is the number the OOM killer compares against the limit.
- **Expansion factor:** `recon.expansion_factor`, the sum over units of their working set divided by their record bytes. A unit's working set is its in-memory chunks at their capacity, plus its spill files once read, plus a 16-byte index entry per record. The record also gives the largest unit's record bytes and working set.
- **Disk peak:** `recon.disk_peak_bytes`, the most bytes of spill files on disk at once.
- **Spill files and bytes:** counted in the data directory after Seal.
- **Throughput:** key and value bytes divided by wall-clock time, as the client saw it.

The cgroup's `memory.peak` reached about 512 MiB in every run, because page cache from spill writes fills the cgroup up to its limit. That cache can be reclaimed, so `memory.peak` says nothing about the sidecar and is left out below.

To reproduce, run `make image`, then:

```
RECON_E2E_IMAGE=recon-sidecar:dev go test -tags e2e,measure -run TestMeasure -timeout 2h -v ./e2e/
```

Each scenario prints a `RESULT` line of JSON.

## Memory: pool accounting against Go's real use (§20 Q1)

| Scenario | Budget (MiB) | Pool peak (MiB) | Go peak (MiB) | VmHWM (MiB) | Go peak / pool | VmHWM / pool | VmHWM / 512 MiB limit |
|---|---|---|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 | 128.0 | 320 | 319 | 2.50 | 2.49 | 62% |
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 128 | 128.0 | 328 | 326 | 2.56 | 2.55 | 64% |
| 1 GiB, default budget, `GOMEMLIMIT` unset | 358 | **OOM-killed 0.7 s into ingest** | | | | | |
| 1 GiB, default budget, `GOMEMLIMIT` 90% | 358 | 358.4 | 548 | 463 | 1.53 | 1.29 | 90% |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 | 128.0 | 389 | 360 | 3.04 | 2.81 | 70% |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 128 | 128.0 | 366 | 354 | 2.86 | 2.76 | 69% |
| 10 GiB, default budget, `GOMEMLIMIT` unset | 358 | **OOM-killed 1.1 s into ingest** | | | | | |
| 10 GiB, default budget, `GOMEMLIMIT` 90% | 358 | 358.4 | 589 | 461 | 1.64 | 1.29 | 90% |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` unset | 128 | 128.0 | 347 | 315 | 2.71 | 2.46 | 62% |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` 90% | 128 | 128.0 | 329 | 310 | 2.57 | 2.43 | 61% |

**Swap hides the problem.** An earlier run of the same scenarios used podman's default, which allows as much swap again as `--memory`, here zram. In that run the two default-budget scenarios without `GOMEMLIMIT` were not OOM-killed:

| Scenario | Go peak (MiB) | VmHWM (MiB) | Ingest |
|---|---|---|---|
| 1 GiB, default budget, `GOMEMLIMIT` unset | 816 | 522 | 5.2 s |
| 10 GiB, default budget, `GOMEMLIMIT` unset | 897 | 523 | 64.5 s |

The 10 GiB ingest took 64.5 s, against 29.3 s with `GOMEMLIMIT` set in the same run, because the sidecar was swapping. Kubernetes nodes usually run without swap, so the e2e harness now passes `--memory-swap` equal to `--memory` for every test. With swap allowed, a test's "never OOM-killed" check could pass on a container that would have been killed in production.

**What this means for §20 Q1 (Rust or Go):**

- **The accounting is sound.** Every buffer, message, unit and batch reserves before it allocates. The pool never went over its budget, and the accounted working set stayed within 1.35 times the record bytes. The unaccounted memory is almost entirely garbage-collector headroom. With `GOGC=100` the heap goal is about twice the live heap, and the measurements match that: 2.4 to 2.8 times the pool peak.
- **Without `GOMEMLIMIT`, Go cannot use the proposal's default.** A 70% budget would need a container 1.75 to 2 times its limit. To fit with `GOMEMLIMIT` unset, the budget would have to be at most about 35% of the limit (1 / 2.81), with no headroom left.
- **With `GOMEMLIMIT` at 90%, the 70% default works.** The garbage collector holds the process at about the soft limit: the resident set was 461 to 463 MiB against a soft limit of 460.8 MiB (468 MiB in the second 10 GiB run). That leaves about 10% of the container for what Go doesn't count: stacks, the binary and gRPC's buffers. No run came close to being killed, but the margin is set by the soft limit, not by the budget. Throughput did not drop measurably. The 10 GiB default-budget run with the limit ingested at 283 MiB/s and joined at 460 MiB/s, as fast as the 128 MiB runs without it.
- **So Go is viable if `GOMEMLIMIT` is always set** to about 90% of the cgroup limit alongside the 70% budget. The sidecar could set it itself from the cgroup limit; the POC doesn't. The cost is a soft limit the runtime enforces by working the garbage collector harder, and a gap of about 30% above the budget that the planner can't see.
- **What this doesn't measure:** Rust's equivalent gap (allocator fragmentation and gRPC buffers) was not measured, so this result shows that Go can work, not that it matches Rust. A Go sidecar that gets a container without `GOMEMLIMIT` set will be OOM-killed under load at the default budget.

## Expansion factor (§8.4)

| Scenario | Average record (key + value bytes) | Units | Expansion factor (all units) | Largest unit: record bytes (MiB) | Largest unit: working set (MiB) | Largest unit's factor |
|---|---|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 1016 | 67 | 1.033 | 15.9 | 16.6 | 1.046 |
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 1016 | 67 | 1.033 | 16.0 | 16.7 | 1.042 |
| 1 GiB, default budget, `GOMEMLIMIT` 90% | 1016 | 24 | 1.061 | 44.2 | 49.9 | 1.129 |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 1016 | 1024 | 1.018 | 10.7 | 10.9 | 1.018 |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 1016 | 1024 | 1.018 | 10.7 | 10.9 | 1.019 |
| 10 GiB, default budget, `GOMEMLIMIT` 90% | 1016 | 256 | 1.020 | 41.4 | 42.2 | 1.019 |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` unset | 48 | 21 | 1.313 | 15.9 | 21.4 | 1.351 |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` 90% | 48 | 21 | 1.306 | 15.9 | 21.3 | 1.344 |

The factor has two parts:

- **The index:** 16 bytes per record. Records are encoded with a 10-byte header (an 8-byte hash and two lengths), so this is 1.6% for 1 KB values and 28% for the 58-byte encoded small records.
- **Chunk slack:** buffers still in memory hold their chunks at capacity, not at the bytes they contain. Slack is largest when more of the data stays in memory: the default-budget 1 GiB run had the largest unit factor, 1.13.

**What this means for §8.4's factor of 2:**

- **The planner over-provisions.** A unit is planned at up to half the unit budget, but it used at most 68% of the budget (1.35 / 2), and 52 to 56% for 1 KB values. So a factor of 2 is safe for these records, and a factor near `1 + 16 / average encoded record + 0.15` would fit more data in each unit.
- **Tiny records can exceed 2.** The index alone is `1 + 16 / r` for an encoded record of `r` bytes, so records under about 16 encoded bytes (a key and value of about 6 bytes together) exceed a factor of 2 before any chunk slack. Calibrating to the record size, rather than using a constant, would cover both cases.
- **Parallel units can need more than their estimate.** Units that run at once each hold their pinned chunks at capacity, and one unit's pinned chunks can't be spilled to make room for another unit. For many small in-memory buffers that is up to about twice the bytes plus 4 KiB each. The in-process property test (`TestRun_property_tiny_budgets_spill_and_match_the_oracle`) therefore sizes its parallel budgets from the run's actual buffers after Seal. At realistic budgets, where ingest has already spilled most buffers, this did not show up.
- **What the factor does not cover:** garbage-collector headroom (see the memory section), which is a separate multiplier on top of it.

## Throughput, spill and disk

| Scenario | Key + value bytes (MiB) | Records | Ingest (s) | Ingest (MiB/s) | Join (s) | Join (MiB/s) | Spill files after Seal | Spilled after Seal (MiB) | Disk peak (MiB) |
|---|---|---|---|---|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 1016 | 1.05 M | 5.4 | 187 | 2.6 | 394 | 13,894 | 954 | 991 |
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 1016 | 1.05 M | 4.5 | 227 | 2.4 | 419 | 13,704 | 949 | 991 |
| 1 GiB, default budget, `GOMEMLIMIT` 90% | 1016 | 1.05 M | 3.3 | 308 | 2.2 | 472 | 4,180 | 818 | 906 |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 10,177 | 10.50 M | 44.9 | 227 | 19.0 | 536 | 146,473 | 10,208 | 10,234 |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 10,177 | 10.50 M | 45.3 | 225 | 22.1 | 461 | 146,533 | 10,215 | 10,242 |
| 10 GiB, default budget, `GOMEMLIMIT` 90% | 10,177 | 10.50 M | 36.0 | 283 | 22.1 | 460 | 49,633 | 10,077 | 10,175 |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` unset | 275 | 6.00 M | 1.8 | 152 | 2.0 | 137 | 3,461 | 244 | 303 |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` 90% | 275 | 6.00 M | 1.8 | 154 | 2.1 | 134 | 3,480 | 246 | 301 |

- **Ingest** ran at 190 to 310 MiB/s for 1 KB values. The client generated the data and ran on the same 32 CPUs, and was not profiled, so these are lower bounds for the sidecar. A larger budget spills less and ingests faster.
- **The join** read every spill file back and ran at 390 to 540 MiB/s with 4 units in parallel, including the client's verification of every result. For small records both ingest and join drop to about 135 to 155 MiB/s, because the cost per record dominates: about 3 M records/s.
- **Disk:** spill files are uncompressed and hold the encoded records, which are about 1% larger than the key and value bytes for 1 KB values. For 1 KB values the disk peak was 0.89 to 1.01 times the key and value bytes, lower with the larger budget. For the small records it was 1.10 times, because each record's 10-byte header is large next to 48 bytes of key and value. It is somewhat above the bytes spilled after Seal, because a Join spills the buffers still in memory to make room for its units. No bucket here was oversized, so these runs had no sub-partitioning rewrites. §15's 1.5 times budget for those rewrites is not exercised by these numbers.
- **These are starting points for M3's targets, not targets:** a single machine, two runs of each scenario, and no compression.

## Other results from the tests

- **Backpressure (`TestBackpressure`):** the client read 1101 MiB at 5 MiB/s, which took 3 min 40 s, from a 256 MiB container with a 64 MiB budget and no swap. The sidecar was still joining when the client was halfway through, and it was never OOM-killed. Its join-finished record gave a pool peak of 67,108,016 bytes (within the 64 MiB budget) and a Go peak of 190,392,632 bytes (182 MiB). podman's sampled peak, including page cache, was 169 MiB. gRPC flow control, through the single sender, held the 4 workers back.
- **Concurrent Joins of different runs share one pool,** and each Join plans its units for the whole budget, so two Joins at once can fail with `MEMORY_EXHAUSTED`. That is retryable, and the run stays SEALED. The design (§8.4, §15) sizes for one Join at a time. In the in-process test `TestRun_concurrent_Joins_share_the_pool` (three sealed runs of 10,000 records each, joined at once in a 512 KiB pool with parallelism 2), 2 or 3 of the 3 Joins failed in 299 of 300 repetitions, and every one of them then succeeded when joined alone.
- **Joins also hold memory that another run's ingest can't spill.** In a variant of `TestRun_concurrent_runs_spill_one_another` that let all three runs join as soon as each was sealed, 1 of about 700 repetitions failed a still-ingesting run with `memory exhausted: reserving 8192 bytes with 520357 of 524288 in use`: the other runs' Joins held nearly the whole pool. Under §6.2 that fails the run. With one Join at a time, as the test now does, it did not happen in 1,800 repetitions. So with several runs, a client should join one run at a time: §15 sizes join memory for a single Join.
