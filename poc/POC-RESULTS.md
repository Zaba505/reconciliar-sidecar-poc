# POC results

These are the measurements PLAN.md §8.5 asks for, of the POC as built since it adopted sorted runs and a streaming merge join ([BUCKET_ARCHITECTURE_EVALUATION.md](../BUCKET_ARCHITECTURE_EVALUATION.md)). Every number below was measured by `TestMeasure` (`e2e/measure_test.go`), unless it says otherwise. The suite ran twice on 2026-11-18, one pass after the other. The tables come from the first pass.

The second pass agreed with it, where a difference is the gap between the two passes over their mean. The pool peak, the disk peak and the number of run files were the same in every run. VmHWM was within 8%, the Go peak within 4%, ingest times within 3% and join times within 13%, except the old generator's join, which took 11.4 s in the first pass and 14.6 s in the second. Seal took the same time to within 31 ms. Both default-budget scenarios without `GOMEMLIMIT` were OOM-killed again, 1.6 s into ingest. So treat differences of a few percent in memory, and of up to about 25% in join times, as noise.

The previous measurements, of the hash-bucket design this replaced, are in this file's history (`git show 670db13:poc/POC-RESULTS.md`). [Against the bucket design](#against-the-bucket-design) compares the two.

## Summary

- **Ingest is faster, and spills are a hundred to a thousand times fewer.** At 10 GiB under a 128 MiB budget, ingest took 46.9 s against 70.7 s for the bucket design, and wrote 92 run files against 149,652 spill files. Across the scenarios ingest took 16 to 42% less time, except the 1 GiB default-budget run, which fits in about three budgets and took the same time.
- **The join took about as long,** within the noise of these runs: 16% less to 15% more, depending on the profile. The evaluation's finding holds here too: the client, which checks every result, limits the join more than the sidecar does.
- **Ingest and join together took 13 to 32% less time** in every 10 GiB scenario, and 20 to 27% less at 1 GiB with a 128 MiB budget.
- **Seal now writes, and took at most 0.55 s.** A run that spilled writes what it still holds as its last file. No run had so many files that Seal merged any: one worker reads 92 files at a 128 MiB budget.
- **The reservation accounting holds.** In every run the pool's peak reached the budget and never went above it.
- **The join holds read buffers, not the data:** 63 MiB of read buffers for 92 files at 10 GiB under 128 MiB, and 30 to 34 MiB at the default budget, against a budget's worth of loaded units before.
- **Go's garbage collector is still the gap.** With `GOMEMLIMIT` unset, the peak resident set was 2.3 to 2.9 times the pool's peak, and with the default budget of 70% of the container limit the container was OOM-killed 1.6 to 1.9 s into ingest, at both 1 GiB and 10 GiB.
- **`GOMEMLIMIT` at 90% of the container limit closes it,** as before. All 7 default-budget runs with it completed, at 1.29 to 1.31 times the budget, or 90 to 91% of the container limit.
- **Throughput on this machine,** in key and value bytes, for Payment and BankLine records as protobuf: ingest ran at 213 to 218 MiB/s and the join at 374 to 493 MiB/s. Across the other profiles at 10 GiB, ingest ran at 173 to 355 MiB/s and the join at 424 to 583 MiB/s, or 1.0 to 2.8 M records/s. The disk peak was 1.02 to 1.07 times the key and value bytes, and 1.24 times for Tiny records.

## Method

| | |
|---|---|
| Machine | AMD Ryzen 9 5950X (32 CPUs), 31 GB RAM, NVMe, Linux 7.3.4 (Fedora 44), 8 GiB zram swap |
| Engine | Rootless podman 5.8.9, image `recon-sidecar:dev` (image ID `9df1feeded0d`) built by `make image` from commit `fdedbb0`. The test code adds the Seal time and the run-sealed record to each RESULT |
| Container | `--memory=512m --memory-swap=512m` (no swap), no `--cpus`, so `GOMAXPROCS` is 32 and the default join parallelism is 4 |
| Data directory | `/var/tmp/recon-e2e-nocomp`, chosen with `RECON_E2E_DATA_ROOT`: btrfs on NVMe, mounted `compress=zstd:1`, but with the directory's no-compress attribute set (`chattr +m`) while it was empty. New files and directories inherit the attribute. Every RESULT of both passes records the attribute as set (`NoCompress`) and the files as not compressed (`Compresses` false) |
| Run | `bucket_count` 1024, which the sidecar accepts and ignores. Ingested over 4 parallel streams in batches of about 1 MiB, sealed, then joined. Every result was checked by `gen.Verifier` |
| Data | `e2e/gen`: marshaled records, see [Data](#data) |
| Client | The `go test` process on the same machine, over TLS on loopback. It generates the data while it sends, and checks the Join's results on 8 goroutines (`min(GOMAXPROCS, 8)`), sharded by key block |
| Repeats | Two complete passes of every scenario, of 563 s and 562 s |

The proof that this directory doesn't compress, with `filefrag` and `compsize`, was made for the previous measurements on a directory made the same way, and wasn't repeated; the RESULT fields above come from `lsattr` and the mount options.

### Data

A value is a record marshaled the way a client would marshal it. `e2e/gen` builds each record from a schema, an ordered list of typed fields, and writes it as protobuf, Avro or JSON, byte for byte as `protoc-gen-go`, `avroc-gen-go` and `encoding/json` would for the same struct. Tests check this against code generated from the schemas, checked in under `e2e/gen/reference`. Each value is a function of the seed, the side, the key and the record's index, so the verifier can regenerate it and compare the bytes. The data is the same as in the previous measurements, byte for byte.

There are five profiles. These are their sizes and ratios as `datastats` measures them, for about 96 MiB of one side's records in the run-file encoding, in the Typical mix with RF keys. The ratios are of 64 KiB blocks of those records:

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

All ran in a 512 MiB container without swap, with 4 Ingest streams.

- **Payment on the left and BankLine on the right, as protobuf:** 1 GiB and 10 GiB, each with a budget of 128 MiB (`RECON_MEMORY_LIMIT_BYTES=128MiB`) or the default of 70% of 512 MiB (358 MiB), each with `GOMEMLIMIT` unset or `483183820`, which is 90% of 512 MiB.
- **The same pair as Avro, and as JSON;** CardSettlement on both sides as protobuf; Envelope on both sides as protobuf: 10 GiB, default budget, `GOMEMLIMIT` 90%.
- **Tiny on both sides as protobuf:** 275 MiB, 128 MiB budget, `GOMEMLIMIT` unset and 90%.
- **The old generator with its old settings:** 10 GiB, default budget, `GOMEMLIMIT` 90%.

### What was measured

- **Pool peak:** `recon.pool_peak_bytes`, from the join-finished log record. It is the largest reservation since the process started.
- **Go peak:** `recon.go_peak_bytes`, from the same record: `/memory/classes/total:bytes` from `runtime/metrics`, all the memory the runtime has mapped. It never decreases, because the runtime returns freed pages to the OS but never unmaps them, so it is the high-water mark of Go's memory.
- **VmHWM:** the sidecar process's peak resident set, read from the host's `/proc/<pid>/status` after the Join. This is the number the OOM killer compares against the limit.
- **Join plan:** the join-finished record's files, workers, read buffer size, `pread` calls and batches.
- **Spills and merges:** the run-sealed record's spills (run files written, the last at Seal included) and Seal's merges of files.
- **Disk peak:** `recon.disk_peak_bytes`, the most bytes of run files on disk at once, as the sidecar counts them.
- **Run files and bytes:** counted in the data directory after Seal.
- **Throughput:** the key and value bytes the client sent, divided by wall-clock time as the client saw it. Ingest is timed until every stream has closed, Seal from the SealRun call to its reply, and the join from the Join call to the end of its stream.
- **Sample ratios and value sizes:** before each run, the client takes the run's first 64 MiB or so of records in the run-file encoding, and reports their s2 and zstd level 1 ratios in 64 KiB blocks and the percentiles of their value sizes.

The cgroup's `memory.peak` reached about 512 MiB in every run, because page cache fills the cgroup up to its limit. Run files are written with direct I/O, but read through the page cache. That cache can be reclaimed, so `memory.peak` says nothing about the sidecar and is left out below.

### Reproducing

Make a directory that btrfs won't compress, while it is still empty, then run `make image` and the suite. A pass takes about 10 minutes and needs about 11 GiB of free disk in the data directory.

```
mkdir /var/tmp/recon-e2e-nocomp && chattr +m /var/tmp/recon-e2e-nocomp
make image
RECON_E2E_IMAGE=recon-sidecar:dev RECON_E2E_DATA_ROOT=/var/tmp/recon-e2e-nocomp go test -tags e2e,measure -run TestMeasure -timeout 3h -v ./e2e/
```

Each scenario prints a `RESULT` line of JSON. `go run ./e2e/gen/cmd/datastats` prints the profile table above. It also takes `-profile` and `-encoding` for one row, `-schema` for a schema of your own as JSON, or `-values` with `-framing varint` or `-framing lines` for a file of real values, so the profiles can be checked against real data.

## Against the bucket design

The bucket design's numbers are the first pass of its published measurements (commit `670db13`), with the same data, client, container, data directory and machine, five days earlier, on Linux 7.3.3 instead of 7.3.4. They weren't run back to back with these, so differences of up to about 20% in time are within the two sets' noise. The ingest time of sorted runs here includes Seal's in the "+ Seal" column, since the bucket design's Seal did no work.

| Data | Size, budget, `GOMEMLIMIT` | Ingest (s), buckets → sorted runs | + Seal (s) | Join (s), buckets → sorted runs | Total (s), buckets → sorted runs | Files, buckets → sorted runs | VmHWM (MiB), buckets → sorted runs |
|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 7.7 → 4.7 | 0.02 | 2.2 → 2.5 | 9.9 → 7.3 (−27%) | 14,125 → 10 | 314 → 317 |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 6.6 → 4.7 | 0.01 | 2.7 → 2.7 | 9.3 → 7.4 (−20%) | 14,063 → 10 | 325 → 320 |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 4.9 → 4.8 | 0.10 | 2.5 → 2.6 | 7.4 → 7.5 (+1%) | 4,707 → 4 | 454 → 465 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 70.7 → 46.9 | 0.03 | 23.4 → 23.3 | 94.1 → 70.2 (−25%) | 149,652 → 92 | 366 → 366 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 64.8 → 47.6 | 0.02 | 22.3 → 23.7 | 87.1 → 71.2 (−18%) | 149,750 → 92 | 360 → 333 |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 61.1 → 48.0 | 0.55 | 23.1 → 20.7 | 84.2 → 69.3 (−18%) | 51,782 → 32 | 458 → 462 |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 65.9 → 53.8 | 0.36 | 24.5 → 24.1 | 90.4 → 78.3 (−13%) | 52,083 → 33 | 459 → 466 |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 49.3 → 32.1 | 0.13 | 15.9 → 17.9 | 65.2 → 50.1 (−23%) | 50,983 → 31 | 459 → 468 |
| CardSettlement, protobuf | 10 GiB, default, 90% | 73.1 → 59.1 | 0.39 | 28.8 → 24.1 | 101.9 → 83.6 (−18%) | 51,701 → 34 | 458 → 468 |
| Envelope, protobuf | 10 GiB, default, 90% | 46.0 → 28.8 | 0.06 | 15.3 → 17.6 | 61.3 → 46.5 (−24%) | 49,615 → 31 | 459 → 465 |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 2.5 → 2.1 | 0.21 | 2.3 → 2.2 | 4.8 → 4.5 (−6%) | 3,635 → 4 | 322 → 292 |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 2.5 → 2.1 | 0.23 | 2.2 → 2.1 | 4.7 → 4.5 (−5%) | 3,632 → 4 | 337 → 291 |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 36.3 → 21.2 | 0.15 | 12.1 → 11.4 | 48.4 → 32.7 (−32%) | 49,221 → 30 | 465 → 465 |

- **Ingest gained most where the bucket design spilled most often:** by 34% and 27% at 10 GiB under 128 MiB, and by 38% and 29% at 1 GiB under 128 MiB. A spill is now one sequential write of about a budget of sorted records instead of many small per-bucket pieces. At 1 GiB under the default budget the data is only three budgets, and the times were the same.
- **The join is the same, within the noise.** It ranged from 16% less time (CardSettlement) to 15% more (Envelope). The evaluation measured the join's CPU, not its time, as what separates the designs, and found that the client's single receiving goroutine floors join time; that wasn't measured here.
- **Files went from tens of thousands to tens:** one file per spill, about one per budget of data. That is what matters on a network volume, where each file costs round trips ([SPILL_STORAGE_RESEARCH.md](../SPILL_STORAGE_RESEARCH.md)).
- **Memory is the same,** because what sets the resident set is the budget and the garbage collector's headroom, not the design. Tiny records' peak dropped by about 30 MiB, from 322 to 337 MiB to 291 to 292 MiB.
- **These match the evaluation's lab,** which measured 92 files and about 50 s for 10 GiB at 128 MiB (35 s ingest and Seal, 15 s join) on its prototype, with a driver that didn't check results. Here the join includes the client's verification of every result.

## Memory: pool accounting against Go's real use (§20 Q1)

Sizes are the target key and value bytes; budgets are 128 MiB or the default of 358 MiB.

| Data | Size, budget, `GOMEMLIMIT` | Budget (MiB) | Pool peak (MiB) | Go peak (MiB) | VmHWM (MiB) | Go peak / pool | VmHWM / pool | VmHWM / 512 MiB limit |
|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 128 | 127.7 | 317 | 317 | 2.48 | 2.48 | 62% |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 128 | 127.7 | 321 | 320 | 2.52 | 2.51 | 63% |
| Payment/BankLine, protobuf | 1 GiB, default, unset | 358 | **OOM-killed 1.7 s into ingest** | | | | | |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 358 | 358.4 | 483 | 465 | 1.35 | 1.30 | 91% |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 128 | 128.0 | 385 | 366 | 3.01 | 2.86 | 72% |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 128 | 128.0 | 385 | 333 | 3.01 | 2.60 | 65% |
| Payment/BankLine, protobuf | 10 GiB, default, unset | 358 | **OOM-killed 1.9 s into ingest** | | | | | |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 530 | 462 | 1.48 | 1.29 | 90% |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 358 | 358.4 | 512 | 466 | 1.43 | 1.30 | 91% |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 358 | 358.4 | 531 | 468 | 1.48 | 1.30 | 91% |
| CardSettlement, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 536 | 468 | 1.50 | 1.31 | 91% |
| Envelope, protobuf | 10 GiB, default, 90% | 358 | 358.4 | 511 | 465 | 1.43 | 1.30 | 91% |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 128 | 127.9 | 294 | 292 | 2.30 | 2.28 | 57% |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 128 | 127.9 | 294 | 291 | 2.30 | 2.28 | 57% |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 358 | 358.3 | 511 | 465 | 1.43 | 1.30 | 91% |

**What this means for §20 Q1 (Rust or Go):**

- **The accounting is sound.** Every block, message, read buffer, arena and batch reserves before it allocates, idle blocks stay reserved, and the pool never went over its budget. The unaccounted memory is almost entirely garbage-collector headroom: with `GOGC=100` the heap goal is about twice the live heap, and the resident set was 2.3 to 2.9 times the pool peak.
- **Without `GOMEMLIMIT`, Go cannot use the proposal's default.** A 70% budget would need a container 1.6 to 2 times its limit. To fit with `GOMEMLIMIT` unset, the budget would have to be at most about 35% of the limit (1 / 2.86).
- **With `GOMEMLIMIT` at 90%, the 70% default works.** The garbage collector holds the process at about the soft limit: the seven default-budget runs with it peaked at 462 to 468 MiB against a soft limit of 460.8 MiB, which leaves about 9% of the container for what Go doesn't count: stacks, the binary and gRPC's buffers. No run came close to being killed, but the margin is set by the soft limit, not by the budget. Throughput didn't drop measurably: at 10 GiB the default-budget run with the limit took 69.3 s in all, against 70.2 s at 128 MiB without it.
- **So Go is viable if `GOMEMLIMIT` is always set** to about 90% of the cgroup limit alongside the 70% budget. The sidecar could set it itself from the cgroup limit; the POC doesn't.
- **What this doesn't measure:** Rust's equivalent gap (allocator fragmentation and gRPC buffers), so this shows that Go can work, not that it matches Rust.

**Swap hides the problem.** An earlier run of the bucket design with podman's default, which allows as much swap again as `--memory`, wasn't OOM-killed in the default-budget scenarios without `GOMEMLIMIT`; it swapped, and its 10 GiB ingest took twice as long. Kubernetes nodes usually run without swap, so the e2e harness passes `--memory-swap` equal to `--memory` for every test.

## Join memory and plan

| Data | Size, budget, `GOMEMLIMIT` | Spills | Seal's merges | Files joined | Workers | Read buffer | Read buffers in all (MiB) | `pread` calls | Batches |
|---|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 10 | 0 | 10 | 4 | 256 KiB | 10.0 | 4,458 | 1,024 |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 10 | 0 | 10 | 4 | 256 KiB | 10.0 | 4,481 | 1,024 |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 4 | 0 | 4 | 4 | 256 KiB | 4.0 | 4,417 | 1,024 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 92 | 0 | 92 | 4 | 176 KiB | 63.2 | 64,812 | 10,171 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 92 | 0 | 92 | 4 | 176 KiB | 63.2 | 64,840 | 10,171 |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 32 | 0 | 32 | 4 | 256 KiB | 32.0 | 43,921 | 10,171 |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 33 | 0 | 33 | 4 | 256 KiB | 33.0 | 44,297 | 10,151 |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 31 | 0 | 31 | 4 | 256 KiB | 31.0 | 42,921 | 10,228 |
| CardSettlement, protobuf | 10 GiB, default, 90% | 34 | 0 | 34 | 4 | 256 KiB | 34.0 | 45,039 | 10,151 |
| Envelope, protobuf | 10 GiB, default, 90% | 31 | 0 | 31 | 4 | 256 KiB | 31.0 | 42,859 | 10,233 |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 4 | 0 | 4 | 4 | 256 KiB | 4.0 | 1,459 | 640 |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 4 | 0 | 4 | 4 | 256 KiB | 4.0 | 1,473 | 640 |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 30 | 0 | 30 | 4 | 256 KiB | 30.0 | 42,250 | 10,186 |

- **Every run joined from files, with 4 workers and 32 tasks.** Each run spilled before Seal, so Seal wrote the rest as one more file and the Join found no blocks in memory.
- **A spill is about 0.9 times the budget of encoded records:** 92 files of 116 MiB on average under 128 MiB, and 30 to 34 files of 322 to 343 MiB on average under 358 MiB.
- **Read buffers shrink before workers are given up.** At 10 GiB under 128 MiB, 4 workers each read 92 files through buffers of 176 KiB: 63 MiB, under the half of the budget read buffers may take. With fewer files the buffers stay at their largest, 256 KiB. The evaluation's sizing rule, `budget ≥ sqrt(0.3 MiB × workers × data)`, gives 110 MiB for 10 GiB with 4 workers, and this ran at full speed.
- **Each byte of a file is read once,** in about 65,000 `pread` calls for 10.4 GiB, and no Seal needed to merge.
- **Not measured:** the join's own CPU time, and a run large enough over its budget to make Seal merge or the Join give up workers; the e2e and unit tests exercise both.

## Throughput, spill and disk

| Data | Size, budget, `GOMEMLIMIT` | Key + value bytes (MiB) | Records | Ingest (s) | Ingest (MiB/s) | Seal (s) | Join (s) | Join (MiB/s) | Join (M records/s) | Run files after Seal | Run file bytes after Seal (MiB) | Disk peak (MiB) | Disk peak / key + value |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, unset | 1,023 | 4.39 M | 4.7 | 216 | 0.02 | 2.5 | 409 | 1.76 | 10 | 1,069 | 1,069 | 1.05 |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB, 90% | 1,023 | 4.39 M | 4.7 | 218 | 0.01 | 2.7 | 374 | 1.61 | 10 | 1,069 | 1,069 | 1.05 |
| Payment/BankLine, protobuf | 1 GiB, default, 90% | 1,023 | 4.39 M | 4.8 | 213 | 0.10 | 2.6 | 400 | 1.72 | 4 | 1,069 | 1,069 | 1.05 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, unset | 10,227 | 43.93 M | 46.9 | 218 | 0.03 | 23.3 | 440 | 1.89 | 92 | 10,688 | 10,688 | 1.05 |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB, 90% | 10,227 | 43.93 M | 47.6 | 215 | 0.02 | 23.7 | 432 | 1.86 | 92 | 10,688 | 10,688 | 1.05 |
| Payment/BankLine, protobuf | 10 GiB, default, 90% | 10,227 | 43.93 M | 48.0 | 213 | 0.55 | 20.7 | 493 | 2.12 | 32 | 10,688 | 10,688 | 1.05 |
| Payment/BankLine, Avro | 10 GiB, default, 90% | 10,226 | 53.14 M | 53.8 | 190 | 0.36 | 24.1 | 424 | 2.20 | 33 | 10,777 | 10,777 | 1.05 |
| Payment/BankLine, JSON | 10 GiB, default, 90% | 10,237 | 20.33 M | 32.1 | 319 | 0.13 | 17.9 | 572 | 1.14 | 31 | 10,450 | 10,450 | 1.02 |
| CardSettlement, protobuf | 10 GiB, default, 90% | 10,240 | 66.34 M | 59.1 | 173 | 0.39 | 24.1 | 425 | 2.75 | 34 | 10,935 | 10,935 | 1.07 |
| Envelope, protobuf | 10 GiB, default, 90% | 10,242 | 17.90 M | 28.8 | 355 | 0.06 | 17.6 | 583 | 1.02 | 31 | 10,430 | 10,430 | 1.02 |
| Tiny, protobuf | 275 MiB, 128 MiB, unset | 275 | 6.98 M | 2.1 | 132 | 0.21 | 2.2 | 124 | 3.15 | 4 | 342 | 342 | 1.24 |
| Tiny, protobuf | 275 MiB, 128 MiB, 90% | 275 | 6.98 M | 2.1 | 130 | 0.23 | 2.1 | 129 | 3.28 | 4 | 342 | 342 | 1.24 |
| Old generator, 1000-byte values | 10 GiB, default, 90% | 10,177 | 10.50 M | 21.2 | 481 | 0.15 | 11.4 | 893 | 0.92 | 30 | 10,287 | 10,287 | 1.01 |

The data in each run, from the RESULT's sample of its first records (value sizes in bytes, ratios of 64 KiB blocks in the run-file encoding), was the same as in the previous measurements:

| Data | Mean value | Median | 90th percentile | 99th percentile | Largest | s2 | zstd level 1 |
|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 226 | 210 | 286 | 485 | 12,110 | 1.50× | 1.77× |
| Payment/BankLine, Avro | 184 | 169 | 244 | 406 | 10,089 | 1.45× | 1.72× |
| Payment/BankLine, JSON | 510 | 478 | 653 | 953 | 16,589 | 2.64× | 3.54× |
| CardSettlement, protobuf | 144 | 144 | 146 | 147 | 149 | 1.30× | 1.62× |
| Envelope, protobuf | 582 | 503 | 966 | 1,708 | 4,448 | 1.13× | 1.14× |
| Tiny, protobuf | 23 | 23 | 24 | 24 | 25 | 1.19× | 1.43× |
| Old generator | 1000 | 1000 | 1000 | 1000 | 1000 | 38.4× | 52.0× |

- **Ingest** ran at 213 to 218 MiB/s for Payment and BankLine as protobuf, whatever the budget, and 173 to 355 MiB/s for the other profiles at 10 GiB: 0.6 to 1.1 M records/s. The client generated the data on the same 32 CPUs, so these are lower bounds for the sidecar. A larger budget wrote about a third as many files and didn't change the time.
- **Seal** took 0.01 to 0.55 s: writing what was left in memory as the last file, which is more at the larger budget.
- **The join** ran at 374 to 493 MiB/s for Payment and BankLine as protobuf and 424 to 583 MiB/s for the other profiles at 10 GiB, with 4 workers, including the client's verification of every result: 1.0 to 2.8 M records/s, fastest in MiB/s for the largest records and in records/s for the smallest. With the old 1 KB values it ran at 893 MiB/s.
- **For Tiny records** ingest and join drop to 124 to 132 MiB/s, about 3.2 M records/s, because the cost per record dominates.
- **Disk:** the sidecar doesn't compress run files, and here the data directory didn't either. Every byte was in a run file by Seal, so the disk peak equals the bytes after Seal: 1.02 to 1.07 times the key and value bytes for the realistic profiles, the record headers, and 1.24 times for Tiny, whose 10-byte header is large next to 41 bytes of key and value. A Join writes nothing.
- **These are starting points for M3's targets, not targets:** a single machine, two runs of each scenario, synthetic data, and no spill compression.

## Other results from the tests

- **Backpressure (`TestBackpressure`):** the client read 1101 MiB at 5 MiB/s, which took 3 min 40 s, from a 256 MiB container with a 64 MiB budget and no swap, on 2026-11-18. The sidecar was never OOM-killed. Its join-finished record gave a pool peak of 66,957,192 bytes (within the 64 MiB budget), a Go peak of 211,810,600 bytes (202 MiB), 19 files read by 4 workers through 252 KiB buffers. gRPC flow control, through the single sender, held the workers back.
- **Concurrent Joins of different runs share one pool.** Each Join sizes its read buffers from what the pool has free, so Joins at once read with smaller buffers, and can still fail with `MEMORY_EXHAUSTED` under a small budget. That is retryable, and the run stays SEALED. In the in-process test `TestRun_concurrent_Joins_share_the_pool` (three sealed runs of 10,000 records each, joined at once in a 512 KiB pool with parallelism 2), 1 of the 3 Joins failed in each of 300 repetitions, and every one of them then succeeded when joined alone. The bucket design failed 2 or 3 of the 3.
