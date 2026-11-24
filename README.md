# recon-sidecar POC

This is a proof of concept of the reconciliation sidecar described in [PROPOSAL.md](PROPOSAL.md). It is written in Go on [humus](https://github.com/z5labs/humus), and [PLAN.md](PLAN.md) is the plan it was built from.

The sidecar accepts two sides of records over the `recon.v1` gRPC API (defined in [`proto/recon/v1/reconciler.proto`](proto/recon/v1/reconciler.proto)) and runs a full outer join on them. It returns each key as `left_only`, `right_only` or `both`, and checks the results against control totals. It holds to a fixed memory budget: when the data is larger than the budget it spills sorted runs to disk, and the join merges them as they stream by. A key is limited only by its `Match`: a `Match` larger than `RECON_MAX_MATCH_BYTES` fails with `HOT_KEY`, and a key on one side streams at any size.

The POC set out to answer two questions:

1. **Does the design hold?** Yes. It passes the full end-to-end suite of PLAN.md §8.4, run against the container image. Its output is identical under tiny and huge memory budgets, at every bucket count and parallelism tested.
2. **Can Go keep to a memory budget?** Yes, but only with `GOMEMLIMIT` set. See [Performance](#performance).

The storage and join layer follows the sorted-runs architecture recommended by [BUCKET_ARCHITECTURE_EVALUATION.md](BUCKET_ARCHITECTURE_EVALUATION.md), with its six conditions for adoption built in (see [Sorted runs](#sorted-runs-and-the-conditions-for-adoption)). The hash-bucket design it replaced is described in that report.

## High-level design

The sidecar runs next to your application, in the same pod. Your application streams records for a left and a right side into a **run**, seals it, then streams back the reconciled results. The sidecar never holds more than its memory budget. When the budget is full it sorts what it holds and writes it as one run file, and the join merges a run's files, holding one key at a time.

### Components

```mermaid
flowchart TB
  App(["Your application"])

  subgraph Sidecar["recon-sidecar container"]
    API["gRPC API<br/>TLS :7443"]
    Runs["Run manager"]
    Blocks["Blocks<br/>records + sort entries"]
    Join["Join engine<br/>merge + emit"]
    Pool["Memory pool<br/>one budget"]
    Disk[("Sorted run files")]
  end

  App -- "CreateRun · Ingest<br/>SealRun · Join · DeleteRun" --> API
  API --> Runs
  Runs -- "Ingest" --> Blocks
  Runs -- "Join" --> Join
  Blocks -- "reserve" --> Pool
  Join -- "reserve" --> Pool
  Pool -. "spill when full:<br/>all of a run's blocks, one file" .-> Disk
  Join -- "merge" --> Blocks
  Join -- "merge" --> Disk
  Join == "results" ==> App
```

Inside the join engine (step by step in [How a Join runs](#how-a-join-runs-step-by-step)):

```mermaid
flowchart LR
  Plan["Plan<br/>fences → tasks"] --> Workers["Workers × parallelism<br/>loser-tree merge"] --> Emitter["Emitter per worker<br/>one key at a time"] --> Sender["Single<br/>sender"] --> Client(["Your application"])
```

| Component | Code | Job |
|---|---|---|
| gRPC API | `internal/api` | Validates every request, checks the run's state, and maps every failure to a gRPC code plus a `ReconError` detail |
| Run manager | `internal/run` | Creates runs with UUIDv7 IDs, enforces `RECON_MAX_RUNS`, tracks each run's state and its per-side record totals, cancels a run's streams and Join on DeleteRun, and owns the one aligned write buffer every spill goes through |
| Records | `internal/record` | The one encoding of a record, in memory and on disk: hash, key and side, value. A key's hash is XXH3, its top byte the record's fence |
| Block memtable | `internal/buffer` | Copies each record once into a 1 MiB block: records from the front, a 16-byte sort entry (hash, offset, ingest ticket) from the back. The goroutine that fills a block sorts its entries. A spill merges every block of the run into one file |
| Merge | `internal/merge` | A loser tree over sorted sources, in (hash, key, side, ingest) order |
| Memory pool | `internal/mem` | Holds the single memory budget. Every block, decoded message, read buffer, arena and output batch reserves before it allocates. Blocks a spill empties stay reserved as idle buffers for the next records, until a reservation needs the room. When a reservation doesn't fit, the goroutine asking for memory drops idle buffers, then spills the largest unpinned runs to 80% of the budget, and retries once. There is no background work |
| Run files | `internal/spill` | Writes sorted run files with direct I/O through an aligned buffer, keeps each file's table of fences, reads segments of them with `pread` through cursors that leave large values on disk until they are emitted, tracks them against `RECON_DISK_LIMIT_BYTES`, and empties the data directory at startup |
| Join engine | `internal/join` | Plans fences into tasks, merges each task's segment of every file and block on a worker, emits results through an arena of chunks and a single sender, checks control totals, and merges files at Seal when there are too many |
| Health | `grpchealth` (humus) | Serves `grpc.health.v1` in plaintext on `:8081` for the startup and readiness probes |

### Lifecycle of a run

```mermaid
sequenceDiagram
  autonumber
  participant App as Your application
  participant API as Sidecar API
  participant Pool as Memory pool
  participant Disk as Run files
  participant Join as Join engine

  App->>API: CreateRun(bucket_count, accepted and ignored)
  API-->>App: run_id (OPEN)

  par one or more Ingest streams, both sides mixed
    loop each batch (about 1 MiB)
      App->>API: IngestRequest(run_id, side, records)
      API->>Pool: reserve message + blocks
      opt budget full
        Pool->>Disk: sort the largest run's blocks into one new file
      end
      Note over API: records copied into blocks, sort entries beside them
    end
    App->>API: close stream
    API-->>App: IngestResponse (stream's counts committed)
  end

  App->>API: SealRun(run_id)
  opt the run spilled
    API->>Disk: write the rest as the last file, merge files if too many
  end
  API-->>App: per-side totals (SEALED, now read-only)

  App->>API: Join(run_id, states filter)
  API->>Join: plan fences into tasks of equal bytes
  loop each task, up to RECON_JOIN_PARALLELISM at once
    Join->>Disk: pread the task's segment of every file
    Note over Join: loser-tree merge in (hash, key, side, ingest) order,<br/>one key at a time: left_only / right_only / both
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
  OPEN --> OPEN: Ingest stream commits, or SealRun fails writing
  OPEN --> SEALED: SealRun (no stream open)
  OPEN --> FAILED: stream ends uncommitted after records were accepted, or DISK_EXHAUSTED during Ingest
  SEALED --> SEALED: Join (repeatable, read-only)
  OPEN --> [*]: DeleteRun
  SEALED --> [*]: DeleteRun
  FAILED --> [*]: DeleteRun
```

A Join that fails, for example with `HOT_KEY` or a cancelled client, leaves the run `SEALED`, so it can be retried. A `FAILED` run can only be deleted. SealRun of a run that spilled writes a file, and if that fails, with `DISK_EXHAUSTED` say, the run stays `OPEN` with every record it had: SealRun can be retried once there is room.

### How the join works

- **Ingest.** A record is hashed and copied once into a block of the run. Nothing is partitioned: `bucket_count` is checked against `RECON_MAX_BUCKETS` and then ignored. Each Append section takes an ingest ticket, so a stream's records keep their order.
- **Spill.** When a reservation doesn't fit, the pool spills the largest run: every one of its blocks is merged by its sorted entries into one new file, `runs/<id>/<seq>.run`, in sequential writes of the 1 MiB write buffer. Ingest streams of that run wait while it is written. A run is a few large sorted files, about one per budget of data, whatever its keys.
- **Seal.** A run that spilled writes what it still holds as its last file, so a sealed run holds only its files' fence tables (about 2 KiB each) in memory. If a Join couldn't read all the files with one worker and read buffers of 16 KiB, Seal merges neighbouring files first, unless the pool has no memory for the merge, in which case the Join reads more files. A run that never spilled stays in memory, sorted, and is joined from there.
- **Planning.** The hash's top byte is its fence: 256 equal ranges, which every file records the offsets of. The Join cuts the fences into tasks of about equal bytes, eight per worker, and gives each worker one read buffer per file, as large as the budget allows. It runs fewer workers rather than shrink buffers below 32 KiB, or let their largest Matches take more than three quarters of the budget. The plan depends only on the data, so the batches do too.
- **Merging.** A worker merges its task's segment of every file and block with a loser tree, in (hash, key, side, ingest) order, so each key's left records come before its right ones, each side in ingest order. It holds a key's left records until its first right record, or the next key, shows whether they are a `Match` or `left_only` results. A right record with no left ones before it is `right_only` at once. Nothing is loaded: each byte of a file is read once, sequentially.
- **Hot keys.** A `Match` is held in its worker's arena, a list of chunks that never move, so it needs its size once. A `Match` larger than `RECON_MAX_MATCH_BYTES` fails the Join with `HOT_KEY`, naming the key and its per-side counts. A key on one side only needs no `Match`: once its held records reach the limit, the worker scans the rest of the key without keeping it, finds no right record, emits what it held, goes back, and streams the rest as one-sided results, however many.
- **Large values.** A record larger than its cursor's read buffer is not read whole: the cursor holds its key, and the value is read straight into the arena when its result is emitted. A result larger than `RECON_OUTPUT_BATCH_BYTES` is sent in a batch of its own, so a client whose Matches can exceed 4 MiB must raise its gRPC receive limit (grpc-go's default).
- **Empty keys** hash to 0, sort first and never match one another. Each is a one-sided result.
- **Output and backpressure.** Workers hand finished batches to a single sender, because gRPC allows one send at a time per stream. When your application reads slowly, gRPC flow control blocks the sender, which blocks the workers, so memory stays within budget. A batch's arena and structs are used again once it is sent.
- **Control totals.** The engine counts every record it joined on each side, including results removed by the `states` filter. The stream ends `OK` only if those counts equal the totals returned by SealRun.

### How records are laid out in memory and on disk

Blocks exist only in memory. A spill doesn't write blocks out: it merges all of a run's blocks into one new run file, in the order a Join reads. A record is the same bytes in both places:

```
┌─────────┬───────────────────────┬──────────────┬───────┬─────────┐
│ hash    │ uvarint               │ uvarint      │ key   │ value   │
│ u64, LE │ key length × 2 + side │ value length │       │         │
└─────────┴───────────────────────┴──────────────┴───────┴─────────┘
  8 bytes   1 byte if the key is    1 byte if the value
            under 64 bytes          is under 128 bytes
```

The hash is the key's XXH3, or 0 for an empty key. Its top byte is the record's **fence**, one of 256 equal ranges of the hash, which is how a Join divides its work.

A block is one allocation of 1 MiB, or a 32nd of a small budget. Records fill it from the front, and a 16-byte sort entry per record fills it from the back, until the next record and its entry don't fit:

```
offset 0                                                           end of the block
┌──────────────┬──────────────┬──────────────┬────────────┬───────┬───────┬───────┐
│ record A     │ record B     │ record C     │            │ entry │ entry │ entry │
│ hash c2…     │ hash 1a…     │ hash 3f…     │    free    │ → B   │ → C   │ → A   │
└──────────────┴──────────────┴──────────────┴────────────┴───────┴───────┴───────┘
 records, from the front:                                  sort entries, from the back:
 copied once, in arrival order,                            {hash u64, offset u32,
 never moved                                                ticket u32}, sorted in place
```

The goroutine that fills a block then sorts its entries in place, by (hash, key, side, ticket). Only the entries move: the drawing shows them sorted, pointing at B, C, A, the order the records will have on disk. The ticket is that of the Append section that copied the record, so a key's records on one side keep the order they were ingested in.

A spill merges the sorted entries of all the run's blocks, and writes the records in that order:

```mermaid
flowchart LR
  B0["Block 0<br/>sorted entries"] --> LT
  B1["Block 1<br/>sorted entries"] --> LT
  Bk["Block k<br/>sorted entries"] --> LT
  LT["Loser tree<br/>(hash, key, side, ticket)"] -- "records, in order" --> WB["Write buffer<br/>1 MiB, aligned,<br/>one for every spill"]
  WB -- "O_DIRECT writes<br/>of the whole buffer" --> F[("runs/#lt;id#gt;/000003.run<br/>records back to back:<br/>no header, no index")]
  LT -. "the offset where<br/>each fence starts" .-> FT["Fence table, in memory<br/>257 offsets, about 2 KiB"]
```

- The loser tree compares the hashes held in the entries, and reads keys only when two hashes are equal.
- A run file is written once, front to back, through the one aligned write buffer every spill shares. Every write but the last is the whole buffer. With `O_DIRECT` the last is padded to a 4 KiB page and the file cut back. Where the filesystem refuses `O_DIRECT`, the file is written through the page cache.
- As records go by, the writer notes the offset at which each fence starts. That table of 257 offsets, the last of them the file's size, stays in memory with the run, and is the file's only index.
- The emptied blocks go to the pool's idle list, still reserved, to take the next records.

A Join's task is a range of fences, which it reads from every file. The fence tables give the range's bytes in each file, at its own offset in each:

```
task 3 is fences 64–95, a segment of every file:

file 0    ├─ fences 0–63 ──┼▓▓ 64–95 ▓▓┼─────────── fences 96–255 ───────────┤
file 1    ├── 0–63 ──┼▓ 64–95 ▓┼───────── 96–255 ────────┤
file 2    ├─── fences 0–63 ────┼▓▓▓ 64–95 ▓▓▓┼─────────────── fences 96–255 ────────────────┤
                               ▲             ▲
                               Fences[64]    Fences[96] of file 2

one worker on task 3, with a cursor per file:

file 0, Fences[64] to Fences[96]  ── pread ──►  read buffer 0  ──┐
file 1, Fences[64] to Fences[96]  ── pread ──►  read buffer 1  ──┼──►  loser tree  ──►  emitter
file 2, Fences[64] to Fences[96]  ── pread ──►  read buffer 2  ──┘
```

- Each worker has its own cursor and read buffer for each file. The files are opened once and shared, because `pread` doesn't move a file's offset.
- A cursor reads its segment front to back, a buffer at a time, ending each read on a 4 KiB page where it can, so that no page is read twice. The kernel's readahead is off (`FADV_RANDOM`), since the buffers ask for exactly what the merge needs. Each byte of a file is read once per Join.
- Read buffers are as large as the budget allows, up to 256 KiB, in 4 KiB steps. Below 32 KiB the Join runs fewer workers instead.
- A record larger than its cursor's read buffer is not read whole. The cursor holds its header and key, and its value stays on disk until the worker needs it for a result, when one `pread` reads it straight into the worker's arena.
- A run that never spilled is joined from its sorted blocks instead. A binary search of a block's entries finds the task's fences, and the merge reads the records where they lie.

### How a Join runs, step by step

#### The merge, by example

Every source is sorted, so the loser tree's output is too, in (hash, key, side, ord) order. A file's ord is its place in the run, and every record in a file was ingested before every record in a later one. So a key's left records come first, in ingest order, then its right ones. Here are three files of a run, with made-up hashes of which only the top byte is shown:

```
 file 0                    file 1                    file 2
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│ 1a inv-42 left  a │     │ 1a inv-42 right b │     │ 1a inv-42 left  c │
│ 3f inv-07 right d │     │ 8c inv-19 left  e │     │ c2 inv-55 right g │
│ c2 inv-55 left  f │     └─────────┬─────────┘     └─────────┬─────────┘
└─────────┬─────────┘               │                         │
          └─────────────────────────┼─────────────────────────┘
                                    ▼
               loser tree, in (hash, key, side, ord) order
```

The worker takes the merged records one at a time, and holds at most one key's:

| Merged record | From | What the worker does | Result |
|---|---|---|---|
| `1a inv-42 left a` | file 0 | A new key, and a left record: starts a group, holds `a` | |
| `1a inv-42 left c` | file 2 | The same key: holds `c` | |
| `1a inv-42 right b` | file 1 | The same key, and a right record, so the group is a `Match`: holds `b` | |
| `3f inv-07 right d` | file 0 | A new key: ends the `inv-42` group | `both`: `inv-42`, left `[a, c]`, right `[b]` |
| | | A right record with no left record before it | `right_only`: `inv-07`, `d` |
| `8c inv-19 left e` | file 1 | A new key, and a left record: starts a group, holds `e` | |
| `c2 inv-55 left f` | file 0 | A new key: ends the `inv-19` group, which had no right record | `left_only`: `inv-19`, `e` |
| | | Starts a group, holds `f` | |
| `c2 inv-55 right g` | file 2 | The same key, and a right record: holds `g` | |
| (end of the task) | | Ends the `inv-55` group | `both`: `inv-55`, left `[f]`, right `[g]` |

#### What a worker does with each record

The example takes the common path. Every record goes through these checks, which also cover empty and hot keys:

```mermaid
flowchart TB
  N["The merge's next record"] --> E{"Empty key?"}
  E -- yes --> One["A one-sided result:<br/>left_only or right_only"]
  E -- no --> S{"Same key as<br/>the group held?"}
  S -- no --> Fin["End the group held:<br/>both sides → one both result, a Match<br/>left only → a left_only result per value"]
  Fin --> R{"A right record?"}
  R -- "yes: the key has<br/>no left records" --> One
  R -- no --> Start["Start a group for the key,<br/>and hold the value"]
  S -- yes --> L{"Would the group pass<br/>RECON_MAX_MATCH_BYTES,<br/>or take twice that in memory?"}
  L -- no --> Hold["Hold the value"]
  L -- yes --> Scan["Scan to the end of the key without holding it,<br/>counting each side, then rewind the cursors"]
  Scan --> Any{"Any right<br/>records?"}
  Any -- "yes, and the Match is<br/>over the limit" --> Hot(["HOT_KEY: the Join fails"])
  Any -- "yes, within the limit:<br/>many tiny values" --> Hold
  Any -- no --> Stream["Emit the values held as left_only,<br/>then stream the rest of the key,<br/>a left_only result per record"]
```

#### How the workers run at once

```mermaid
flowchart LR
  Plan["Plan<br/>256 fences → tasks<br/>of about equal bytes,<br/>8 per worker"] --> Sched["Schedule<br/>hands out tasks<br/>in plan order"]
  Files[("Run files<br/>opened once, shared")]
  subgraph Workers["Workers, at most RECON_JOIN_PARALLELISM"]
    direction TB
    W1["Worker 1<br/>a cursor per file → loser tree → emitter"]
    W2["Worker 2<br/>the same, on its own task"]
    WN["Worker N"]
  end
  Sched -- "next task" --> Workers
  Files -- "pread: the task's<br/>segment of every file" --> Workers
  Workers -- "a batch" --> Ch{{"Unbuffered<br/>channel"}}
  Ch --> Sender["Sender<br/>the Join's goroutine,<br/>one SendMsg at a time"]
  Sender -- "JoinResponse" --> App(["Your application"])
  Sender -. "sent: the worker reuses<br/>the batch's memory" .-> Workers
```

- The plan has eight tasks per worker, so a worker held up by a slow task leaves the rest to the others. A worker takes the next task in plan order as soon as it finishes one.
- Each worker merges with its own cursors, loser tree and emitter. What the workers share, the open files and the pinned run, they only read.
- A worker sends a batch when it fills, and at the end of every task. So a task's batches depend only on the data and the plan, not on which worker took it, though batches of different tasks interleave on the stream.
- A worker hands a batch to the sender over an unbuffered channel, and waits for the send before it reuses the batch's memory. So each worker has at most one batch in flight, and a client that reads slowly stops the workers instead of letting batches pile up.
- When a task fails, with `HOT_KEY` say, no further task starts and the tasks after it in the plan are cancelled, while those before it run on. The Join returns the error of the first failed task in plan order, so the error doesn't depend on which worker got there first.

#### A Join over time

With two workers the plan has 16 tasks, of about 16 fences each when the hashes spread evenly:

```mermaid
sequenceDiagram
  participant W1 as Worker 1
  participant W2 as Worker 2
  participant Q as Schedule
  participant S as Sender
  participant App as Your application

  W1->>Q: take a task
  Q-->>W1: task 0: fences 0–15
  W2->>Q: take a task
  Q-->>W2: task 1: fences 16–31
  Note over W1,W2: both merge at once, each<br/>its task's segment of every file
  W1->>S: a full batch
  Note over W1: waits until it is sent
  S->>App: JoinResponse
  Note over S,App: a slow reader blocks SendMsg,<br/>and so every waiting worker
  Note over W2: task 1 is done: its last<br/>batch waits for the sender
  S-->>W1: sent
  Note over W1: reuses the batch's<br/>memory, merges on
  W2->>S: task 1's last batch
  S->>App: JoinResponse
  S-->>W2: sent
  W2->>Q: take a task
  Q-->>W2: task 2: fences 32–47
  Note over W1,App: … until no task is left
  W1->>Q: take a task
  Q-->>W1: none left
  W2->>Q: take a task
  Q-->>W2: none left
  Note over S: every worker has ended:<br/>the channel closes, and each<br/>side's count must equal<br/>its Seal total
  S-->>App: the stream ends OK
```

### Memory and spilling

The sidecar has one memory pool with one budget (`RECON_MEMORY_LIMIT_BYTES`), shared by every run. Everything that takes real memory reserves from the pool before it allocates, and releases afterwards:

| What reserves | Released | Can the pool reclaim it? |
|---|---|---|
| The spill write buffer: 1 MiB, or a 32nd of a small budget | When the sidecar stops | No |
| An Ingest message, once decoded | As soon as its records are copied into blocks | No, but it lasts one message |
| A run's blocks (its records in memory) | When the run is spilled, or on DeleteRun | **Yes**, by spilling the run, unless a running Join is reading it |
| Idle blocks, emptied by a spill | When a block is taken for new records, of any run | **Yes**, by dropping them |
| A run file's fence table, about 2 KiB | On DeleteRun, or when Seal merges the file | No |
| A Join's read buffers, one per file per worker | When the Join ends | No |
| A Join worker's arena, output batch structs and the key it holds | When the batch is sent, or the Join ends | No |

When a reservation doesn't fit, the goroutine that asked for it frees memory itself. There is no background spiller:

```mermaid
flowchart TB
  R["Reserve n bytes"] --> F{"Fits in<br/>the budget?"}
  F -- yes --> OK(["Reserved"])
  F -- no --> D["Drop idle blocks"] --> F1{"Fits now?"}
  F1 -- yes --> OK
  F1 -- no --> S["Spill the largest unpinned runs,<br/>each as one sorted file, until usage<br/>is at most 80% of the budget"]
  S --> F2{"Fits now?"}
  F2 -- yes --> OK
  F2 -- no --> E(["MEMORY_EXHAUSTED"])
```

**During Ingest,** blocks fill the pool. Once it is full, the next block forces the largest run to spill, all of it, as one file, so a run of 80 times the budget makes about 90 files. The emptied blocks stay reserved as idle buffers and take the next records, so the garbage collector doesn't churn through a budget of blocks per spill.

**At Seal,** a run that spilled writes its last file. A run that didn't keeps its blocks in memory, and stays spillable: if another run needs the room, the sealed run is spilled like any other.

**At Join time,** the workers reserve their memory before the run is pinned, so if a run still in memory doesn't fit next to it, the reservation spills it and the Join reads the file instead. Once pinned, a run's blocks can't be spilled from under the Join. Then the Join holds one key at a time per worker, plus read buffers: 63 MiB of them for the 92 files of a 10 GiB run under a 128 MiB budget (see [Performance](#performance)).

**The pool runs out** only when what it can't reclaim leaves no room: in-flight messages, read buffers, arenas, fence tables and pinned runs. In practice that means several Joins at once under a small budget, a `Match` within the limit whose many tiny values take more than twice the limit to hold, or an Ingest message larger than the budget. The request then fails with `MEMORY_EXHAUSTED` instead of the container being OOM-killed.

#### Sizing: the budget a join runs at full speed in

Join read memory is files × workers × read buffer, and the number of files grows with data over budget. A join runs at full speed while:

```
budget ≥ sqrt(0.3 MiB × workers × data)
```

That is about 110 MiB for 10 GiB with 4 workers. Below it, read buffers shrink first, then the join gives up workers, then Seal merges files. It slows down instead of failing. Measured here, 10 GiB at 128 MiB joined 92 files with 4 workers and read buffers of 176 KiB, at full speed. Measured on the evaluation's prototype, which this code follows: 10 GiB at 32 MiB (403 files, one worker, 67.5 s join) and 4 GiB at 16 MiB (326 files, one worker, 53.4 s join).

- **Raise three things together:** the container's memory limit, the budget (by default 70% of that limit) and `GOMEMLIMIT` (about 90% of it). Raising the budget without `GOMEMLIMIT` gets the container OOM-killed. Raising the container limit alone does nothing unless the budget follows it, which it does by default.
- **Several runs share the pool.** Eight runs in a 128 MiB pool leave each about 16 MiB, which is the regime where the join slows.
- **Files are few and large,** so network volumes see sequential writes of the 1 MiB buffer and one file per spill, about 92 for 10 GiB at a 128 MiB budget. See [SPILL_STORAGE_RESEARCH.md](SPILL_STORAGE_RESEARCH.md) for the storage classes they suit.
- **Below about 128 KiB** the fence tables of many small spills, and the read buffers of a Join, leave too little of the budget, and runs fail with `MEMORY_EXHAUSTED`.

### Sorted runs and the conditions for adoption

The evaluation made adoption depend on six conditions. Each is built and tested here:

| Condition | How it is met | Tests |
|---|---|---|
| 1. Direct I/O for run writes | Run files are opened with `O_DIRECT` and written in whole aligned buffers; the last write is padded and the file cut back. A filesystem that refuses direct I/O falls back to the page cache | `internal/spill`: `TestWriter_writes_directly_where_it_can`, `TestRunFile_property_round_trips_by_fence` |
| 2. Large values read at emit time | A cursor holds only the header and key of a record larger than its read buffer; the value is read with one `pread` into the arena when its result is emitted, and copied through the cursor when Seal merges files | `internal/join`: `TestRun_large_values_are_read_when_emitted`; `internal/spill`: `TestCursor_*`, `TestCopy_copies_large_values_through_the_cursor`; e2e `TestLargeValues` |
| 3. A chunked arena | A worker's arena is a list of chunks that never move, so a `Match` needs its size once. A group that would outgrow the limit, or twice it in memory, is scanned without being held: `HOT_KEY` if it is a `Match` over the limit, streamed if it is one-sided | `internal/join`: `TestRun_a_Match_needs_its_size_once`, `TestRun_streams_a_one_sided_key_at_any_size`; e2e `TestOneSidedHotKey`, `TestHotKey` |
| 4. A pool-owned free list | Blocks a spill empties go to the pool's idle list, still reserved, and are dropped as soon as any reservation needs the room, so several runs share them exactly | `internal/mem`: `TestPool_idle_buffers_*`, `TestPool_spilling_doesnt_count_idle_buffers`; `internal/buffer`: `TestBuffer_reuses_idle_blocks` |
| 5. Close cancels a Join | DeleteRun cancels the Join's context and doesn't wait for it; the Join frees the run's memory when it ends, and sends nothing more once cancelled | `internal/run`: `TestRun_Delete_during_a_Join_of_files`, `TestDelete_aborts_a_Join`, `TestRun_Delete_during_Seal` |
| 6. A wider conformance suite | The oracle compares value order (`oracle.DiffOrdered`); property tests cover tiny budgets from 128 KiB, hot keys, large values, cancellation, the states filter, spilling at any point, and the live heap during a Join | `internal/join`, `internal/run` property tests; e2e `TestValueOrder` |

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
  internal/mem/        memory reservation pool, idle blocks and spill trigger
  internal/record/     the record encoding, hashes and fences
  internal/buffer/     a run's blocks and sorted run files
  internal/merge/      the loser-tree merge
  internal/spill/      run file writer and cursor, disk accounting, startup wipe
  internal/join/       plan, merge workers, emitter, hot keys, merge at Seal
  internal/oracle/     naive full outer join used by tests
  gen/recon/v1/        generated gRPC code (committed)
  e2e/                 container tests, harness and data generator
  POC-RESULTS.md       full measurement report
```

## Requirements

- Go 1.27
- Podman or Docker. Rootless podman needs the `memory` cgroup controller delegated, otherwise the memory tests mean nothing. `make e2e` checks this before it runs.
- Only to regenerate the gRPC code: `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc`
- Only to regenerate the test generator's reference code (`make reference`): `protoc`, able to find `google/protobuf/timestamp.proto`. A `protoc` built from source needs `PROTOBUF_INCLUDE` set to its `src` directory. The target installs its own pinned `avroc` and `avroc-gen-go` v0.3.0 and `protoc-gen-go` v1.36.6 into `poc/.local/tools`.

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
| `RECON_JOIN_PARALLELISM` | `min(GOMAXPROCS, 4)` | Join merge workers, at most |
| `RECON_MAX_RUNS` | `8` | Runs that can exist at once |
| `RECON_MAX_BUCKETS` | `4096` | Largest `bucket_count`, which is otherwise ignored |
| `RECON_MAX_RECV_MESSAGE_BYTES` | `16MiB` | Largest Ingest message |
| `RECON_OUTPUT_BATCH_BYTES` | `1MiB` | Join output batch size |
| `RECON_MAX_MATCH_BYTES` | `3MiB` | Largest single `Match` before `HOT_KEY`. A one-sided key has no limit |
| `RECON_LOG_KEYS` | `false` | Include keys in logs and `HOT_KEY` errors |
| `RECON_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Logs go to stdout as JSON. |
| `RECON_METRICS_EXPORTER` | `none` | `none`, `console` or `otlp` (OpenTelemetry) |

**Always set `GOMEMLIMIT` to about 90% of the container's memory limit** when you run the sidecar at the default budget. Without it the container is OOM-killed under load (see below).

### Tests

| Command | Runs |
|---|---|
| `make test` | Unit, property-based (against the oracle) and in-process gRPC tests, with `-race` |
| `make e2e` | Builds the image, then runs every container test in PLAN.md §8.4; `TestRealisticSpill`, which joins realistic records of varying size under a 4 MiB budget; and the sorted-runs tests of hot keys, large values and value order. About 4 minutes on the machine below. |
| `make e2e-short` | The same, skipping the multi-GiB tests |
| `make proto` | Regenerates `gen/` from `../proto` |
| `make reference` | Re-emits the test generator's reference `.proto` and `.avdl` schemas and regenerates the Go code the byte-exact tests in `e2e/gen/reference` compare values against |

The e2e tests drive the image through the engine's CLI. Set `RECON_E2E_ENGINE` to choose `podman` or `docker`, and `RECON_E2E_IMAGE` to choose the image (the Makefile sets both). Set `RECON_E2E_DATA_ROOT` to choose where the containers' data directories go (`/var/tmp` by default; not a tmpfs). Every container runs with swap disabled (`--memory-swap` equal to `--memory`), so an OOM kill can't be hidden by swapping.

The test data comes from `e2e/gen`, which marshals realistic records as protobuf, Avro or JSON. To see how large and how compressible its profiles are, or to check them against a file of your own values, run `go run ./e2e/gen/cmd/datastats` (`-h` lists the options).

To reproduce the performance numbers, use a data root the filesystem doesn't compress. On btrfs, make an empty directory and run `chattr +m` on it before anything is written there. A pass takes about 10 minutes and needs about 11 GiB of free disk in the data root:

```sh
mkdir /var/tmp/recon-e2e-nocomp && chattr +m /var/tmp/recon-e2e-nocomp
make image
RECON_E2E_IMAGE=recon-sidecar:dev RECON_E2E_DATA_ROOT=/var/tmp/recon-e2e-nocomp go test -tags e2e,measure -run TestMeasure -timeout 3h -v ./e2e/
```

## Performance

Measured on an AMD Ryzen 9 5950X (32 CPUs, 31 GB RAM, NVMe, Fedora 44) under rootless podman, on 2026-11-18, in two passes that agreed to within a few percent in memory and 25% in time. The container had `--memory=512m` with no swap. The data was marshaled records, the way a client sends them. Unless a row says otherwise, the left side was Payment records and the right side BankLine records, both as protobuf, with 18-byte keys. Their values averaged 226 bytes and compress about 1.8 times with zstd. About 93% of keys had one record on each side. A scenario's size is the key and value bytes of both sides together; the Left and Right columns show how it splits. Both sides have the same number of records, so a Payment/BankLine run puts more of its bytes on the left, where the records are larger. The data directory was on btrfs with compression turned off. Every result was checked by a streaming verifier. The full method and tables, the other profiles, and the comparison with the hash-bucket design this replaced are in [poc/POC-RESULTS.md](poc/POC-RESULTS.md).

### Throughput

| Data (left / right) | Scenario (total size, budget, `GOMEMLIMIT`) | Left side | Right side | Ingest | Seal | Join | Ingest + Seal + join | Run files | Disk peak |
|---|---|---|---|---|---|---|---|---|---|
| Payment/BankLine, protobuf | 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 2.20 M records, 585 MiB (57%) | 2.20 M records, 438 MiB (43%) | 4.7 s (216 MiB/s) | 0.02 s | 2.5 s (409 MiB/s) | 7.3 s | 10 | 1,069 MiB |
| Payment/BankLine, protobuf | 1 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 2.20 M records, 585 MiB (57%) | 2.20 M records, 438 MiB (43%) | 4.7 s (218 MiB/s) | 0.01 s | 2.7 s (374 MiB/s) | 7.4 s | 10 | 1,069 MiB |
| Payment/BankLine, protobuf | 1 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 2.20 M records, 585 MiB (57%) | 2.20 M records, 438 MiB (43%) | 4.8 s (213 MiB/s) | 0.10 s | 2.6 s (400 MiB/s) | 7.5 s | 4 | 1,069 MiB |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 21.96 M records, 5.71 GiB (57%) | 21.97 M records, 4.28 GiB (43%) | 46.9 s (218 MiB/s) | 0.03 s | 23.3 s (440 MiB/s) | 70.2 s | 92 | 10,688 MiB |
| Payment/BankLine, protobuf | 10 GiB, 128 MiB budget, `GOMEMLIMIT` 90% | 21.96 M records, 5.71 GiB (57%) | 21.97 M records, 4.28 GiB (43%) | 47.6 s (215 MiB/s) | 0.02 s | 23.7 s (432 MiB/s) | 71.2 s | 92 | 10,688 MiB |
| Payment/BankLine, protobuf | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 21.96 M records, 5.71 GiB (57%) | 21.97 M records, 4.28 GiB (43%) | 48.0 s (213 MiB/s) | 0.55 s | 20.7 s (493 MiB/s) | 69.3 s | 32 | 10,688 MiB |
| Payment/BankLine, Avro | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 26.57 M records, 5.88 GiB (59%) | 26.57 M records, 4.11 GiB (41%) | 53.8 s (190 MiB/s) | 0.36 s | 24.1 s (424 MiB/s) | 78.3 s | 33 | 10,777 MiB |
| Payment/BankLine, JSON | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 10.16 M records, 5.99 GiB (60%) | 10.16 M records, 4.01 GiB (40%) | 32.1 s (319 MiB/s) | 0.13 s | 17.9 s (572 MiB/s) | 50.1 s | 31 | 10,450 MiB |
| CardSettlement, protobuf | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 33.17 M records, 5.00 GiB (50%) | 33.17 M records, 5.00 GiB (50%) | 59.1 s (173 MiB/s) | 0.39 s | 24.1 s (425 MiB/s) | 83.6 s | 34 | 10,935 MiB |
| Envelope, protobuf | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 8.95 M records, 5.00 GiB (50%) | 8.95 M records, 5.00 GiB (50%) | 28.8 s (355 MiB/s) | 0.06 s | 17.6 s (583 MiB/s) | 46.5 s | 31 | 10,430 MiB |
| Tiny, protobuf | 275 MiB, 128 MiB budget, `GOMEMLIMIT` unset | 3.49 M records, 138 MiB (50%) | 3.49 M records, 138 MiB (50%) | 2.1 s (132 MiB/s) | 0.21 s | 2.2 s (124 MiB/s) | 4.5 s | 4 | 342 MiB |
| Tiny, protobuf | 275 MiB, 128 MiB budget, `GOMEMLIMIT` 90% | 3.49 M records, 138 MiB (50%) | 3.49 M records, 138 MiB (50%) | 2.1 s (130 MiB/s) | 0.23 s | 2.1 s (129 MiB/s) | 4.5 s | 4 | 342 MiB |
| Old generator, 1000-byte values | 10 GiB, 358 MiB budget (default), `GOMEMLIMIT` 90% | 5.25 M records, 4.97 GiB (50%) | 5.25 M records, 4.97 GiB (50%) | 21.2 s (481 MiB/s) | 0.15 s | 11.4 s (893 MiB/s) | 32.7 s | 30 | 10,287 MiB |

Times are wall-clock as the client saw them, and throughput is the key and value bytes sent over that time. CreateRun and DeleteRun weren't timed. The client generated the data and verified every result on the same machine, and the join time includes that verification, so these times are upper bounds for the sidecar.

- **Against the hash-bucket design,** on the same data and machine, ingest plus join took 13 to 32% less time at 10 GiB. Ingest took 16 to 42% less, most where the bucket design spilled most often (70.7 s against 46.9 s at 10 GiB under 128 MiB), except at 1 GiB under the default budget, which is only three budgets of data and took the same time. The join took about as long, within the noise. Files went from 149,652 to 92 at 10 GiB under 128 MiB, and from about 50,000 to about 32 at the default budget.
- **Seal now writes** what a spilled run still holds as its last file: at most 0.55 s here. No run had enough files for Seal to merge any.
- **The budget barely changed the time,** only the number of files: about one per 0.9 budgets of data.
- **Smaller records cost more per byte.** The join ran at 1.0 to 2.8 M records/s for the realistic profiles. With Tiny records of 41 bytes of key and value, the cost per record dominates, at about 3.2 M records/s.
- **The sidecar doesn't compress its run files,** and the data directory here didn't either, so the disk peak is the space they took: 1.02 to 1.07 times the key and value bytes, and 1.24 times for Tiny records. Every byte is on disk by Seal, and a Join writes nothing.

In a real pipeline, reading the source can overlap with Ingest, and processing results can overlap with Join, because both stream in batches. Each step then takes about as long as the slower of the two sides.

### Memory

| Scenario | Budget | Pool peak | Peak RSS (VmHWM) | RSS / container limit |
|---|---|---|---|---|
| 1 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 MiB | 127.7 MiB | 317 MiB | 62% |
| 10 GiB, 128 MiB budget, `GOMEMLIMIT` unset | 128 MiB | 128.0 MiB | 366 MiB | 72% |
| 1 GiB, default budget, `GOMEMLIMIT` unset | 358 MiB | **OOM-killed 1.7 s into ingest** | | |
| 10 GiB, default budget, `GOMEMLIMIT` unset | 358 MiB | **OOM-killed 1.9 s into ingest** | | |
| 1 GiB, default budget, `GOMEMLIMIT` 90% | 358 MiB | 358.4 MiB | 465 MiB | 91% |
| 10 GiB, default budget, `GOMEMLIMIT` 90% | 358 MiB | 358.4 MiB | 462 MiB | 90% |

- **The reservation accounting holds.** The pool's peak reached the budget in every run and never went over it.
- **Go's garbage collector is the gap.** With `GOMEMLIMIT` unset, the peak resident set was 2.3 to 2.9 times the pool peak. That is the GC's default heap headroom (`GOGC=100`), and it makes the proposal's default budget of 70% of the container limit unworkable. The design doesn't change this; the bucket design measured the same.
- **`GOMEMLIMIT` at 90% of the limit closes the gap,** with no measurable loss of throughput. The runtime holds the process at about the soft limit, and about 9% of the container is left for stacks, the binary and gRPC buffers. Every default-budget run with it completed, for every profile and encoding, at 90 to 91% of the container limit. The POC doesn't set `GOMEMLIMIT` itself.
- **The join holds read buffers, not the data:** 63 MiB for 92 files at 10 GiB under 128 MiB, and 30 to 34 MiB for about 32 files at the default budget, with 4 workers.
- **Backpressure works.** A client reading at 5 MiB/s pulled 1.1 GiB of results from a 256 MiB container with a 64 MiB budget, over 3 min 40 s. The container was never OOM-killed, and the pool peaked within the budget.

## Known limitations

- **Spill compression is not implemented.** The proposal chooses the codec by benchmark in a later milestone. Run files are large sequential blocks, which suit it.
- **The join slows when data far outgrows the budget.** Read buffers shrink, then workers are given up, then Seal merges files. Read-ahead on the cursor that runs dry first is the known remedy, and isn't built.
- **A spill stalls every stream of its run.** A spill is one write of about the budget: about 0.2 s on NVMe, about 1 s at 125 MiB/s. An overlapped or half-budget spill would hide it, and isn't built.
- **Joins of different runs share one pool.** Each sizes its read buffers from what the pool has free, so Joins at once read with smaller buffers, and can still fail with `MEMORY_EXHAUSTED` under a small budget. That is retryable, and the run stays `SEALED`.
- **A `Match` of many tiny values** within `RECON_MAX_MATCH_BYTES` can take more memory than its encoded size: 24 bytes per value besides the value. One that doesn't fit the pool fails with `MEMORY_EXHAUSTED`.
- **A Join blocked by a client that stopped reading** is cancelled by `DeleteRun` at once, and DeleteRun doesn't wait for it, but the Join's goroutine ends only once the client reads again or disconnects.
- **Not measured yet:** several runs sharing a pool, a real network volume, skewed keys at benchmark scale, and data far enough over the budget for Seal to merge files or the Join to give up workers.

## Deviations from the proposal

These are listed in [PLAN.md §2](PLAN.md#2-deviations-from-the-proposal). In short: the API uses TCP with TLS instead of a Unix socket, metrics go to OpenTelemetry instead of Prometheus, the test oracle is in Go instead of DuckDB, and spill blocks are uncompressed.
