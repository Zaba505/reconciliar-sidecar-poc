# recon-sidecar POC

This is a proof of concept of the reconciliation sidecar described in [PROPOSAL.md](PROPOSAL.md). It is written in Go on [humus](https://github.com/z5labs/humus), and [PLAN.md](PLAN.md) is the plan it was built from.

The sidecar accepts two sides of records over the `recon.v1` gRPC API (defined in [`proto/recon/v1/reconciler.proto`](proto/recon/v1/reconciler.proto)) and runs a full outer join on them. It returns each key as `left_only`, `right_only` or `both`, and checks the results against control totals. It holds to a fixed memory budget: when the data is larger than the budget it spills to disk, and it splits up skewed buckets. A key too large to join within limits fails with `HOT_KEY`.

The POC set out to answer two questions:

1. **Does the design hold?** Yes. It passes the full end-to-end suite of PLAN.md §8.4, run against the container image. Its output is identical under tiny and huge memory budgets, at every bucket count and parallelism tested.
2. **Can Go keep to a memory budget?** Yes, but only with `GOMEMLIMIT` set. See [Performance](#performance).

## High-level design

The sidecar runs next to your application, in the same pod. Your application streams records for a left and a right side into a **run**, seals it, then streams back the reconciled results. The sidecar never holds more than its memory budget. Anything that doesn't fit goes to spill files in its data directory, and the join reads it back one slice at a time.

### Components

```mermaid
flowchart TB
  App(["Your application"])

  subgraph Sidecar["recon-sidecar container"]
    API["gRPC API<br/>TLS :7443"]
    Runs["Run manager"]
    Buckets["Hash buckets<br/>left / right"]
    Join["Join engine"]
    Pool["Memory pool<br/>one budget"]
    Disk[("Spill files")]
  end

  App -- "CreateRun · Ingest<br/>SealRun · Join · DeleteRun" --> API
  API --> Runs
  Runs -- "Ingest" --> Buckets
  Runs -- "Join" --> Join
  Buckets -- "reserve" --> Pool
  Join -- "reserve" --> Pool
  Join -- "read" --> Buckets
  Pool -. "spill when full" .-> Disk
  Join -- "read" --> Disk
  Join == "results" ==> App
```

Inside the join engine:

```mermaid
flowchart LR
  Planner["Planner<br/>buckets → units"] --> Workers["Workers<br/>× parallelism"] --> Sender["Single<br/>sender"] --> Client(["Your application"])
```

| Component | Code | Job |
|---|---|---|
| gRPC API | `internal/api` | Validates every request, checks the run's state, and maps every failure to a gRPC code plus a `ReconError` detail |
| Run manager | `internal/run` | Creates runs with UUIDv7 IDs, enforces `RECON_MAX_RUNS`, tracks each run's state and its per-side record totals, and cancels a run's streams on DeleteRun |
| Partitions and buffers | `internal/buffer` | Hashes each non-empty key with XXH3 and appends the record to bucket `hash % N` for its side. Buffers are lists of chunks (4 KiB doubling to 1 MiB) already in spill-file encoding, so a spill is one sequential write |
| Memory pool | `internal/mem` | Holds the single memory budget. Every chunk, decoded message, join unit and output batch reserves before it allocates. When a reservation doesn't fit, the goroutine asking for memory spills the largest unpinned buffers (of any run) to 80% of the budget and retries once. There is no background work |
| Spill files | `internal/spill` | Writes and tracks spill files against `RECON_DISK_LIMIT_BYTES`, and empties the data directory at startup |
| Join engine | `internal/join` | Plans units, runs them in parallel, handles skew, streams results through a single sender, and checks control totals |
| Health | `grpchealth` (humus) | Serves `grpc.health.v1` in plaintext on `:8081` for the startup and readiness probes |

### Lifecycle of a run

```mermaid
sequenceDiagram
  autonumber
  participant App as Your application
  participant API as Sidecar API
  participant Pool as Memory pool
  participant Disk as Spill files
  participant Join as Join engine

  App->>API: CreateRun(bucket_count N)
  API-->>App: run_id (OPEN)

  par one or more Ingest streams, both sides mixed
    loop each batch (about 1 MiB)
      App->>API: IngestRequest(run_id, side, records)
      API->>Pool: reserve message + chunks
      opt budget full
        Pool->>Disk: spill largest buffers
      end
      Note over API: records appended to bucket hash(key) % N
    end
    App->>API: close stream
    API-->>App: IngestResponse (stream's counts committed)
  end

  App->>API: SealRun(run_id)
  API-->>App: per-side totals (SEALED, now read-only)

  App->>API: Join(run_id, states filter)
  API->>Join: plan units from bucket sizes
  loop each unit, up to RECON_JOIN_PARALLELISM at once
    Join->>Pool: reserve unit working set
    Join->>Disk: read the unit's spill files
    Note over Join: sort by (hash, key, side), then one pass<br/>emits left_only / right_only / both
    Join-->>App: JoinResponse batches (single sender)
  end
  Note over Join: joined counts must equal Seal totals
  API-->>App: stream ends OK (or INTERNAL on a mismatch)

  opt Join again
    App->>API: Join(run_id) gives the same results
  end

  App->>API: DeleteRun(run_id)
  API->>Disk: remove the run's directory
  API-->>App: OK
```

### Run states

```mermaid
stateDiagram-v2
  [*] --> OPEN: CreateRun
  OPEN --> OPEN: Ingest stream commits
  OPEN --> SEALED: SealRun (no stream open)
  OPEN --> FAILED: stream ends uncommitted after records were accepted, or DISK_EXHAUSTED
  SEALED --> SEALED: Join (repeatable, read-only)
  OPEN --> [*]: DeleteRun
  SEALED --> [*]: DeleteRun
  FAILED --> [*]: DeleteRun
```

A Join that fails, for example with `HOT_KEY` or a cancelled client, leaves the run `SEALED`, so it can be retried. A `FAILED` run can only be deleted.

### How the join works

- **Partitioning.** At ingest, every record with a non-empty key goes to bucket `xxh3(key) % N` for its side, so a key's left and right records always land in the same bucket number.
- **Planning.** At Join time, the planner estimates each bucket's working set as its record bytes times 2, and groups neighbouring buckets into **units** that fit in `budget / parallelism`. Small buckets are coalesced into one unit, and the plan depends only on the data, not on timing.
- **Joining a unit.** A worker reserves the unit's memory, pins its in-memory chunks so they can't be spilled from under it, and reads its spill files. It builds a 16-byte index entry per record, sorts the entries by (hash, key, side), and walks them once. A key with records on only one side is `left_only` or `right_only`, and a key with both is `both`. No hash map is needed, and the output order is deterministic.
- **Skew.** If a single bucket is too big for a unit, the worker counts records per hash. If one key alone is too big for the budget or for `RECON_MAX_MATCH_BYTES`, the Join fails with `HOT_KEY`, naming the key and its per-side counts. Otherwise it re-partitions the bucket with a new seed into a scratch directory (`runs/<id>/join-<n>/`), up to 3 levels deep, and removes the scratch directory when the Join ends.
- **Empty keys** never match one another. They are streamed back as one-sided results without sorting.
- **Output and backpressure.** Workers hand finished batches to a single sender, because gRPC allows one send at a time per stream. When your application reads slowly, gRPC flow control blocks the sender, which blocks the workers, so memory stays within budget.
- **Control totals.** The engine counts every record it joined on each side, including results removed by the `states` filter. The stream ends `OK` only if those counts equal the totals returned by SealRun.

### Memory and spilling

The sidecar has one memory pool with one budget (`RECON_MEMORY_LIMIT_BYTES`), shared by every run. Everything that takes real memory reserves from the pool before it allocates, and releases afterwards:

| What reserves | Released | Can the pool spill it? |
|---|---|---|
| An Ingest message, once decoded | As soon as its records are copied into buckets | No, but it lasts one message |
| Bucket chunks (the run's records) | When the buffer is spilled, or on DeleteRun. Not when Ingest or Seal finishes | **Yes**, unless a running Join has pinned it |
| A join unit's working set: its spill files read back, plus a 16-byte index entry per record | When the unit's results have been sent | No |
| Join output batches, one per worker | When the Join ends | No |
| Per-key counters while scanning an oversized bucket for hot keys | When the scan ends | No |

When a reservation doesn't fit, the goroutine that asked for it frees memory itself. There is no background spiller:

```mermaid
flowchart TB
  R["Reserve n bytes"] --> F{"Fits in<br/>the budget?"}
  F -- yes --> OK(["Reserved"])
  F -- no --> S["Spill the largest unpinned buffers,<br/>from any run, to disk until usage<br/>is at most 80% of the budget"]
  S --> F2{"Fits now?"}
  F2 -- yes --> OK
  F2 -- no --> E(["MEMORY_EXHAUSTED"])
```

**During Ingest,** bucket chunks fill the pool. Once it is full, each new chunk forces a spill down to 80%, so the pool stays between about 80% and 100% full and everything beyond that goes to disk. In the 10 GiB run with a 128 MiB budget, the pool never went above 128 MiB, and 10.2 GiB was on disk by Seal.

**After Ingest,** the chunks that weren't spilled stay in memory and stay reserved, even after Seal. This isn't a leak: they are unpinned, so the pool can spill them whenever something else needs room. Delete runs when you are done with them, because only DeleteRun gives back their memory and their disk.

**At Join time,** the pool is usually still full of ingest chunks. The Join gets its memory the same way Ingest does: its reservations force those chunks to disk. If the chunks in the way belong to the unit it is about to join, the worker unpins them, reserves as if all the unit's records were on disk (which may spill them), then pins them again and reads them back. While a unit is being joined, its buffers are pinned, so nothing can spill them out from under it.

**The pool runs out** only when the memory that can't be spilled leaves no room: pinned buffers, units being joined, output batches and in-flight messages. In practice that means two Joins at once, a Join next to another run's heavy Ingest, or an Ingest message larger than the budget. The request then fails with `MEMORY_EXHAUSTED` instead of the container being OOM-killed.

#### Sizing: more memory means less spilling

Because the pool works as a cache in front of disk, a bigger budget changes how much is spilled and how fast Ingest runs, not just whether the sidecar survives. These are measured runs with `GOMEMLIMIT` at 90% of the container limit (see [Performance](#performance)):

| Data | Budget | Ingest | Join | Spill files at Seal | Spilled at Seal |
|---|---|---|---|---|---|
| 1 GiB | 128 MiB | 4.5 s | 2.4 s | 13,704 | 949 MiB |
| 1 GiB | 358 MiB | 3.3 s | 2.2 s | 4,180 | 818 MiB |
| 10 GiB | 128 MiB | 45.3 s | 22.1 s | 146,533 | 10,215 MiB |
| 10 GiB | 358 MiB | 36.0 s | 22.1 s | 49,633 | 10,077 MiB |

- **Ingest got 20 to 27% faster** with about 2.8 times the budget, and wrote about a third as many spill files. Each spill frees about 20% of the budget at once, so a bigger budget means fewer, larger writes.
- **The Join barely changed** in these runs, because with data 30 to 80 times the budget almost all of it has to be read back from disk either way. A bigger budget does give each unit more room (`budget / parallelism`), so fewer, larger units are planned.
- **A run that fits within the budget doesn't spill at all.** That case wasn't measured.
- **Raise three things together:** the container's memory limit, the budget (by default 70% of that limit) and `GOMEMLIMIT` (about 90% of it). Raising the budget without `GOMEMLIMIT` gets the container OOM-killed. Raising the container limit alone does nothing unless the budget follows it, which it does by default.
- **On slow or network-attached disks, the number of files may matter more than the bytes.** Spilling largest first across 2 × N buffers produces many small files: about 70 KiB each in the 10 GiB, 128 MiB run. A larger budget or a smaller `bucket_count` gives fewer, larger files.

## Repository layout

```
PROPOSAL.md        design proposal (§ references point here)
PLAN.md            implementation plan for this POC
proto/             recon.v1 API (shared with the proposal)
poc/               Go module "poc"
  cmd/recon-sidecar/   main
  internal/app/        Init, settings, telemetry
  internal/api/        gRPC server: validation, state checks, error mapping
  internal/run/        run manager and state machine
  internal/mem/        memory reservation pool and spill trigger
  internal/buffer/     per-(side, bucket) record buffers
  internal/spill/      spill files, disk accounting, startup wipe
  internal/join/       planner, loader, sort-based join, sub-partitioning
  internal/oracle/     naive full outer join used by tests
  gen/recon/v1/        generated gRPC code (committed)
  e2e/                 container tests, harness and data generator
  POC-RESULTS.md       full measurement report
```

## Requirements

- Go 1.27
- Podman or Docker. Rootless podman needs the `memory` cgroup controller delegated, otherwise the memory tests mean nothing. `make e2e` checks this before it runs.
- Only to regenerate the gRPC code: `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc`

All commands below run from `poc/`.

## Running it

### Build and run the container

```sh
cd poc
make run
```

This builds `recon-sidecar:dev` and generates a test CA and server certificate into `poc/.local/tls/`. It then runs the container with a 1 GiB memory limit (change it with `RUN_MEMORY=2g`), publishing these ports on `127.0.0.1`:

| Port | Serves |
|---|---|
| 7443 | `recon.v1.Reconciler`, TLS only |
| 8081 | `grpc.health.v1.Health`, plaintext (the startup and readiness probe) |

The data directory is `poc/.local/data`. The sidecar empties it at startup.

### Call the API by hand

The server has no reflection, so pass [grpcurl](https://github.com/fullstorydev/grpcurl) the proto file:

```sh
grpcurl -cacert .local/tls/ca.pem -import-path ../proto -proto recon/v1/reconciler.proto \
  -d '{"bucket_count":64}' localhost:7443 recon.v1.Reconciler/CreateRun
```

A run goes `CreateRun` → one or more `Ingest` streams → `SealRun` → `Join` (repeatable) → `DeleteRun`.

### Configuration

The sidecar is configured through environment variables. A bad value stops it at startup with exit code 78 and a log record naming the variable. Byte values take a plain integer or an IEC suffix (`128MiB`, `4GiB`).

| Variable | Default | Meaning |
|---|---|---|
| `RECON_TLS_CERT_FILE`, `RECON_TLS_KEY_FILE` | required | PEM server certificate and key |
| `RECON_LISTEN` | `127.0.0.1:7443` (image: `:7443`) | API address |
| `RECON_HEALTH_ADDR` | `:8081` | Health probe address |
| `RECON_DATA_DIR` | `/var/lib/recon` | Spill directory. It must be writable and must not be a tmpfs. |
| `RECON_MEMORY_LIMIT_BYTES` | 70% of the cgroup's `memory.max` | Memory budget. Required when the cgroup has no limit. |
| `RECON_DISK_LIMIT_BYTES` | Free space in the data directory | Spill disk budget |
| `RECON_JOIN_PARALLELISM` | `min(GOMAXPROCS, 4)` | Join units run at once |
| `RECON_MAX_RUNS` | `8` | Runs that can exist at once |
| `RECON_MAX_BUCKETS` | `4096` | Largest `bucket_count` |
| `RECON_MAX_RECV_MESSAGE_BYTES` | `16MiB` | Largest Ingest message |
| `RECON_OUTPUT_BATCH_BYTES` | `1MiB` | Join output batch size |
| `RECON_MAX_MATCH_BYTES` | `3MiB` | Largest single `Match` before `HOT_KEY` |
| `RECON_LOG_KEYS` | `false` | Include keys in logs and `HOT_KEY` errors |
| `RECON_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Logs go to stdout as JSON. |
| `RECON_METRICS_EXPORTER` | `none` | `none`, `console` or `otlp` (OpenTelemetry) |

**Always set `GOMEMLIMIT` to about 90% of the container's memory limit** when you run the sidecar at the default budget. Without it the container is OOM-killed under load (see below).

### Tests

| Command | Runs |
|---|---|
| `make test` | Unit, property-based (against the oracle) and in-process gRPC tests, with `-race` |
| `make e2e` | Builds the image, then runs every container test in PLAN.md §8.4. About 4 minutes on the machine below. |
| `make e2e-short` | The same, skipping the multi-GiB tests |
| `make proto` | Regenerates `gen/` from `../proto` |

The e2e tests drive the image through the engine's CLI. Set `RECON_E2E_ENGINE` to choose `podman` or `docker`, and `RECON_E2E_IMAGE` to choose the image (the Makefile sets both). Every container runs with swap disabled (`--memory-swap` equal to `--memory`), so an OOM kill can't be hidden by swapping.

To reproduce the performance numbers (this takes a while, and needs about 11 GiB of free disk under `/var/tmp`):

```sh
make image
RECON_E2E_IMAGE=recon-sidecar:dev go test -tags e2e,measure -run TestMeasure -timeout 2h -v ./e2e/
```

## Performance

Measured on an AMD Ryzen 9 5950X (32 CPUs, 31 GB RAM, NVMe, Fedora 44) under rootless podman. The container had `--memory=512m` with no swap, `bucket_count` was 1024, and the data had 16-byte keys and 1000-byte values (32-byte values in the small-record scenario). Every result was checked by a streaming verifier. The full method and tables are in [poc/POC-RESULTS.md](poc/POC-RESULTS.md).

### Throughput

| Scenario | Records | Ingest | Join | Ingest + join | Disk peak |
|---|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 1.05 M | 5.4 s (187 MiB/s) | 2.6 s (394 MiB/s) | 8.0 s | 991 MiB |
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 1.05 M | 4.5 s (227 MiB/s) | 2.4 s (419 MiB/s) | 6.9 s | 991 MiB |
| 1 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 1.05 M | 3.3 s (308 MiB/s) | 2.2 s (472 MiB/s) | 5.5 s | 906 MiB |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 10.5 M | 44.9 s (227 MiB/s) | 19.0 s (536 MiB/s) | 63.9 s | 10,234 MiB |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 10.5 M | 45.3 s (225 MiB/s) | 22.1 s (461 MiB/s) | 67.4 s | 10,242 MiB |
| 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 10.5 M | 36.0 s (283 MiB/s) | 22.1 s (460 MiB/s) | 58.1 s | 10,175 MiB |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` unset | 6.0 M | 1.8 s (152 MiB/s) | 2.0 s (137 MiB/s) | 3.8 s | 303 MiB |
| 275 MiB of small records, 128 MiB budget, `GOMEMLIMIT` 90% | 6.0 M | 1.8 s (154 MiB/s) | 2.1 s (134 MiB/s) | 3.9 s | 301 MiB |

Times are wall-clock as the client saw them, and throughput is key and value bytes over that time. Ingest + join is the sidecar's share of a run. SealRun, which runs between the two, wasn't timed, and neither were CreateRun and DeleteRun. The client generated the data and verified every result on the same machine, and the join time includes that verification, so these times are upper bounds for the sidecar. With small records the cost per record dominates, at about 3 M records/s. Spill files are uncompressed, so disk use is about 1× the data for 1 KB values.

In a real pipeline, reading the source can overlap with Ingest, and processing results can overlap with Join, because both stream in batches. Each step then takes about as long as the slower of the two sides.

### Memory

| Scenario | Budget | Pool peak | Peak RSS (VmHWM) | RSS / container limit |
|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 MiB | 128.0 MiB | 319 MiB | 62% |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 MiB | 128.0 MiB | 360 MiB | 70% |
| 1 GiB, default budget, `GOMEMLIMIT` unset | 358 MiB | **OOM-killed 0.7 s into ingest** | | |
| 10 GiB, default budget, `GOMEMLIMIT` unset | 358 MiB | **OOM-killed 1.1 s into ingest** | | |
| 1 GiB, default budget, `GOMEMLIMIT` 90% | 358 MiB | 358.4 MiB | 463 MiB | 90% |
| 10 GiB, default budget, `GOMEMLIMIT` 90% | 358 MiB | 358.4 MiB | 461 MiB | 90% |

- **The reservation accounting holds.** The pool's peak reached the budget exactly in every run and never went over it.
- **Go's garbage collector is the gap.** With `GOMEMLIMIT` unset, the peak resident set was 2.4 to 2.8 times the pool peak. That is the GC's default heap headroom (`GOGC=100`), and it makes the proposal's default budget of 70% of the container limit unworkable.
- **`GOMEMLIMIT` at 90% of the limit closes the gap,** with no measurable loss of throughput. The runtime holds the process at about the soft limit, and about 10% of the container is left for stacks, the binary and gRPC buffers. The POC doesn't set `GOMEMLIMIT` itself.
- **Backpressure works.** A client reading at 5 MiB/s pulled 1.1 GiB of results from a 256 MiB container with a 64 MiB budget, over 3 min 40 s. The container was never OOM-killed.

### Expansion factor

A join unit's working set was 1.02 to 1.13 times its record bytes for 1 KB values, and 1.35 times for 58-byte encoded records. The planner's factor of 2 is therefore safe but conservative for these records. Records under about 16 encoded bytes would exceed it on the per-record index alone.

## Known limitations

- **Spill compression is not implemented.** The proposal chooses the codec by benchmark in a later milestone.
- **Joins of different runs share one pool,** and each plans for the whole budget, so two Joins at once can fail with `MEMORY_EXHAUSTED`. That is retryable, and the run stays `SEALED`. A Join holding most of the pool can also fail another run's ingest, which marks that run `FAILED` (seen in 1 of about 700 stress repetitions). Clients should join one run at a time; the proposal sizes join memory for a single Join.
- **Parallelism above 1 needs more headroom at very tight budgets,** because the pinned chunks of units running at once can't be spilled.
- **A Join blocked by a client that stopped reading** is aborted by `DeleteRun` only once the client reads again or disconnects.

## Deviations from the proposal

These are listed in [PLAN.md §2](PLAN.md#2-deviations-from-the-proposal). In short: the API uses TCP with TLS instead of a Unix socket, metrics go to OpenTelemetry instead of Prometheus, the test oracle is in Go instead of DuckDB, and spill blocks are uncompressed.
