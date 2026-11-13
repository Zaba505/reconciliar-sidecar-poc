# recon-sidecar POC

This is a proof of concept of the reconciliation sidecar described in [PROPOSAL.md](PROPOSAL.md). It is written in Go on [humus](https://github.com/z5labs/humus), and [PLAN.md](PLAN.md) is the plan it was built from.

The sidecar accepts two sides of records over the `recon.v1` gRPC API (defined in [`proto/recon/v1/reconciler.proto`](proto/recon/v1/reconciler.proto)) and runs a full outer join on them. It returns each key as `left_only`, `right_only` or `both`, and checks the results against control totals. It holds to a fixed memory budget: when the data is larger than the budget it spills to disk, and it splits up skewed buckets. A key too large to join within limits fails with `HOT_KEY`.

The POC set out to answer two questions:

1. **Does the design hold?** Yes. It passes the full end-to-end suite of PLAN.md §8.4, run against the container image. Its output is identical under tiny and huge memory budgets, at every bucket count and parallelism tested.
2. **Can Go keep to a memory budget?** Yes, but only with `GOMEMLIMIT` set. See [Performance](#performance).

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

| Scenario | Data | Ingest | Join | Disk peak |
|---|---|---|---|---|
| 1 GiB, 128 MiB budget | 1.05 M records | 187–227 MiB/s | 394–419 MiB/s | 991 MiB |
| 1 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 1.05 M records | 308 MiB/s | 472 MiB/s | 906 MiB |
| 10 GiB, 128 MiB budget | 10.5 M records | 225–227 MiB/s | 461–536 MiB/s | 10.0 GiB |
| 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 10.5 M records | 283 MiB/s | 460 MiB/s | 9.9 GiB |
| 275 MiB of small records, 128 MiB budget | 6.0 M records | 152–154 MiB/s | 134–137 MiB/s | 303 MiB |

Throughput is key and value bytes over wall-clock time as the client saw it. The client generated and verified the data on the same machine, so these numbers are lower bounds for the sidecar. With small records the cost per record dominates, at about 3 M records/s. Spill files are uncompressed, so disk use is about 1× the data for 1 KB values.

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
