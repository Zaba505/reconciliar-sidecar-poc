# Spill compression

`RECON_SPILL_CODEC` compresses run files in blocks. It is a prototype, and `none` by default. This document describes the format, what it keeps of a Join's random access, what it costs, and what it measured.

## In short

- **Disk:** s2 makes run files 0.67 times the size of their records, zstd 0.57 times, for Payment and BankLine records as protobuf. JSON compresses to 0.40 and 0.30 times. Values a client encrypted (Envelope) don't compress, and take 0.88 times with either codec.
- **Joins on a 125 MiB/s volume** read that many fewer bytes, and are I/O-bound, so they run 1.5 times faster with s2 and 1.7 times with zstd (17.0 s, 11.4 s and 9.8 s for 2 GiB). On a disk that reads 1.2 GB/s they take as long with either codec as without, and on tmpfs, where only the CPU counts, they are 23% (s2) and 38% (zstd) slower.
- **Spills pay on the stall path.** A spill compresses on its one goroutine, and the run's streams wait for it. s2 adds about 3.4 s of CPU per 2 GiB, and zstd about 13.6 s: zstd compresses at 110 to 155 MiB/s on one core here, about as fast as a gp3 volume writes. On a fast disk, s2 makes spills 1.7 times slower, and zstd 3.7 times.
- **Seeking still works.** Each fence starts a block, so the fence table locates every Join task's segment. A cursor's position names a block and an offset in it, so a hot key's rewind decodes at most the blocks it went back over. A record larger than a block is stored raw, so its value is read with one `pread`, or skipped.

**Recommendation.** Use s2 on network block storage (EBS, Persistent Disk, Hyperdisk) and keep `none` on local NVMe. zstd's ratio is better, but it needs compression off the stall path before it pays: in parallel with the merge, on several cores. That isn't built; [Compressing off the stall path](#compressing-off-the-stall-path) estimates what it would buy and cost.

## The format

A file of blocks holds its records in blocks, each an 8-byte header and a body:

```
┌──────────────────────────────────────┬─────────────────────────────────┐
│ header, u64 LE                       │ body                            │
│ kind: 2 bits                         │ raw: records, as they are       │
│ raw, stored: 31 bits each            │ compressed: records, s2 or zstd │
│ (large: the record's size, 62 bits)  │ large: one record, as it is     │
└──────────────────────────────────────┴─────────────────────────────────┘
```

- **Records are encoded as before,** back to back, and no record spans two blocks. A block holds at most the block size of records: a 1024th of the budget, between 4 KiB and 32 KiB. That is 32 KiB for budgets from 32 MiB.
- **A fence starts a block.** The fence table, still 257 offsets in memory, then holds the offset of each fence's first block, so a Join task's segment is whole blocks.
- **A record larger than a block is a block of its own,** stored raw. Its value stays where a `pread` can read it into the arena when it is emitted, or leave it unread when the `states` filter drops it.
- **A block is stored compressed only if that saves a sixteenth of it.** Otherwise it is stored raw, and costs nothing to read but a copy.
- **Each block is compressed on its own,** with no dictionary and no checksum. Run files had no checksums before either: they live only as long as the process.
- **The file's `File` keeps each fence's bytes of records too:** 256 `uint32`s, 1 KiB, or 2 KiB of `int64` offsets if a fence holds 4 GiB or more, as a hot key's may. A Join plans its tasks by records, as it does for runs still in memory, so whether a run spilled still doesn't change its batches. A file without blocks keeps nothing more.
- **Disk accounting** reserves the most a spill can take, its records plus a header per block, and gives back what the file didn't take once it is written.

## How a cursor reads it

```
the cursor's read buffer, as the Join gives it:

┌──────────────── Block bytes ───────────────┬──────────── the rest ──────────────┐
│ the block being decoded: the merge's heads │ the file, read ahead: compressed   │
│ point into it                              │ blocks, as many as fit, one pread  │
└────────────────────────────────────────────┴────────────────────────────────────┘
```

- **A cursor decodes one block at a time** into the first Block bytes of its read buffer, and reads the file ahead into the rest, as many blocks as fit, in one `pread`. It ends each read on a page, as before, so a Join still reads each byte of a file once.
- **If the rest can't hold a block,** the cursor reads each block into its worker's scratch buffer, one `pread` per block. Each read takes the next block's header too, so the next read is exactly that block.
- **A Join's read buffers can't be smaller than a block.** The Join gives up workers before its buffers fall below 32 KiB, as it did, and fails with `MEMORY_EXHAUSTED` only below a block: 4 KiB for budgets under 4 MiB, 32 KiB from 32 MiB. Compact merges files until one worker could read them all with buffers of 16 KiB or a block, if that is larger.
- **A position** is the offset of the head's block, shifted left 21 bits, and the head's offset in the block. A rewind into the block being decoded costs nothing. One further back decodes the block again, from what the cursor read ahead if it is still there, and otherwise from the file.
- **Each worker has one decoder,** shared by its cursors, since a worker reads one at a time. With zstd, it holds 24 KiB and twice the block size, besides its scratch buffer. The Join counts it with the worker's read buffers.

### Memory

| What | Without blocks | s2 | zstd |
|---|---|---|---|
| A `File` | 2.2 KiB | 3.2 KiB | 3.2 KiB |
| The spill encoder, once per sidecar | none | 70 KiB | 450 KiB |
| A Join worker's decoder | none | 32 KiB | 120 KiB |

The figures are for 32 KiB blocks. zstd's encoder holds about 300 KiB whatever the block size, so `zstd` needs a budget of at least 8 MiB. The setting is rejected below that.

## Measurements

### Method

- **Machine:** a cloud container with 4 vCPUs of an Intel Xeon at 2.1 GHz and 16 GB of RAM, with ext4 on a virtio disk that reads about 1.2 GB/s with direct I/O. Go 1.27, klauspost/compress 1.20.3. This is not the machine of [poc/POC-RESULTS.md](poc/POC-RESULTS.md), so compare times only within these tables.
- **Data:** the e2e generator's records, seed 20, Typical mix, RF keys: Payment and BankLine records as protobuf unless a table says otherwise. They were generated before the clock started, and dropped before each Join so that the garbage collector's work on them wouldn't count.
- **Run:** `TestCodecs_measure` in `internal/run`, which drives a `run.Manager` in-process, with no gRPC. It ingests over 4 streams at once, seals, empties the page cache, and joins with 4 workers, keeping a count of the results only. Every Join had the same number of results with every codec, and checked the control totals.
- **Budget:** 128 MiB, so 32 KiB blocks, and about 0.9 budgets of records per file.
- **Three data directories:**
  - tmpfs, which shows the CPU's cost alone;
  - the virtio disk as it is;
  - the same disk throttled to 125 MiB/s and 3,000 IOPS, reads and writes, by a cgroup v1 `blkio` throttle on the test process. That is a gp3 volume's baseline, as in the evaluation's slow-disk runs.
- **Columns:** "Spill stall" is the time spent writing spill files, including Seal's last spill, while the run's streams waited. CPU is the process's user and system time.

The throttle lets a process catch up on bandwidth it left unused within a time slice. So time spent compressing partly overlaps the writes that follow it, which a real volume's synchronous direct writes wouldn't allow. The throttled spill times are therefore a best case. A serial model, the tmpfs spill time plus the file's bytes at 125 MiB/s, is the worst case, and is shown next to them. The Join's reads overlap across its 4 workers on a real volume too, so its throttled times stand as they are.

### Ratio and speed by block size

`TestBlocks_measure` in `internal/spill` writes 267 MiB of records, sorted as a spill writes them, through the real writer at each block size. It then times one core compressing and decoding the file's blocks. Compression MiB/s is of records in; decoding MiB/s is of records out.

| Codec | Block | Records / file bytes | Compress MiB/s | Decode MiB/s |
|---|---|---|---|---|
| s2 | 4 KiB | 1.29 | 524 | 1,698 |
| s2 | 16 KiB | 1.42 | 565 | 1,504 |
| s2 | **32 KiB** | **1.49** | 531 | 1,303 |
| s2 | 128 KiB | 1.62 | 474 | 1,375 |
| zstd | 4 KiB | 1.42 | 121 | 423 |
| zstd | 16 KiB | 1.64 | 150 | 635 |
| zstd | **32 KiB** | **1.75** | 110 | 577 |
| zstd | 128 KiB | 1.93 | 154 | 819 |

Larger blocks compress better, but a cursor needs a block of its read buffer to decode into. 32 KiB is the Join's preferred smallest buffer, so a Join with many files keeps its workers.

### 2 GiB, Payment and BankLine, protobuf

2,137 MiB of records in 19 files.

| Codec | Data directory | On disk / records | Ingest s | Spill stall s | Spill, serial model s | Join s | Join read MiB | Join preads | Ingest and Seal CPU s | Join CPU s |
|---|---|---|---|---|---|---|---|---|---|---|
| none | tmpfs | 1.00 | 6.1 | 5.4 | | 1.3 | 2,137 | 8,956 | 7.8 | 4.6 |
| s2 | tmpfs | 0.67 | 9.5 | 8.8 | | 1.6 | 1,431 | 7,128 | 11.7 | 5.4 |
| zstd | tmpfs | 0.57 | 19.6 | 19.0 | | 1.8 | 1,225 | 6,196 | 21.7 | 6.6 |
| none | disk | 1.00 | 6.0 | 5.4 | | 2.4 | 2,137 | 8,947 | 6.7 | 6.0 |
| s2 | disk | 0.67 | 9.9 | 9.2 | | 2.4 | 1,431 | 7,141 | 10.9 | 6.8 |
| zstd | disk | 0.57 | 20.4 | 19.9 | | 2.5 | 1,226 | 6,166 | 21.2 | 7.7 |
| none | 125 MiB/s | 1.00 | 17.0 | 16.5 | 22.5 | **17.0** | 2,137 | 8,951 | 7.3 | 6.2 |
| s2 | 125 MiB/s | 0.67 | 11.4 | 10.8 | 20.2 | **11.4** | 1,431 | 7,136 | 11.3 | 7.0 |
| zstd | 125 MiB/s | 0.57 | 20.4 | 19.9 | 28.8 | **9.8** | 1,226 | 6,206 | 21.4 | 8.4 |

An earlier pass, which kept the generated records through the Join, agreed to within 10% in every wall-clock time.

- **Spills are CPU-bound before compression.** Without it, a spill merges and writes at about 400 MiB/s on tmpfs, and the disk didn't slow it. The spills' extra time on tmpfs puts s2 at about 630 MiB/s of records and zstd at about 160 MiB/s, on top of that, on the same goroutine.
- **At 125 MiB/s,** s2 still shortens the stall, in the best case by a third and in the worst by a tenth. zstd lengthens it in both: 19.9 s at best and 28.8 s at worst, against 16.5 s and 22.5 s.
- **The Join reads 33% and 43% fewer bytes,** in 20% and 31% fewer preads. On the throttled disk it runs at the speed of its bytes: 2,137, 1,431 and 1,226 MiB at 125 MiB/s take 17.1, 11.4 and 9.8 s.

### 1 GiB of values that compress little, and much

Envelope records, which hold ciphertext, as protobuf (1,042 MiB of records), and Payment and BankLine as JSON (1,045 MiB), in 9 files.

| Data | Codec | On disk / records | Spill stall s, tmpfs | at 125 MiB/s | serial model | Join s, tmpfs | at 125 MiB/s |
|---|---|---|---|---|---|---|---|
| Envelope | none | 1.00 | 1.5 | 8.2 | 9.8 | 0.4 | 8.3 |
| Envelope | s2 | 0.88 | 2.1 | 7.4 | 9.4 | 0.4 | 7.3 |
| Envelope | zstd | 0.88 | 3.4 | 7.2 | 10.7 | 0.4 | 7.3 |
| JSON | none | 1.00 | 1.6 | 8.2 | 10.0 | 0.3 | 8.3 |
| JSON | s2 | 0.40 | 2.9 | 3.3 | 6.2 | 0.4 | 3.3 |
| JSON | zstd | 0.30 | 6.1 | 5.9 | 8.6 | 0.6 | 2.4 |

- **Ciphertext saves 12%,** whether with s2 or zstd. Most likely that is the keys, key IDs and lengths around the ciphertext, which doesn't compress. It is just past the sixteenth a block must save to be stored compressed, so these blocks are decoded for little.
- **JSON is where compression pays most:** s2's Join at 125 MiB/s takes 2.5 times less time, and its spill at least 1.6 times less.

## Compressing off the stall path

A spill now runs on one goroutine, and the run's streams wait for all of it:

```
merge the next records → fill a block → compress it → copy it into the write buffer → O_DIRECT write → …
```

So a spill takes the merge's time, plus the compression's, plus the write's. The merge has to stay on one goroutine, since it produces one sorted stream. Compression doesn't: each block is compressed on its own, with nothing carried from one block to the next. A spill could be a pipeline:

```
merge goroutine ──raw blocks──► N compressor goroutines ──compressed blocks──► writer goroutine
  one sorted stream               in any order                                  in the blocks' order
```

The merge hands each full block on and goes on merging, the compressors work at once, and the writer writes the blocks in order. A spill then takes about as long as its slowest stage.

### What it would buy

These are estimates, not measurements. They take each stage's cost from the 2 GiB tables above:

- the merge: 5.4 s, the uncompressed spill on tmpfs;
- compression: 3.4 s with s2 and 13.6 s with zstd on one core, shared by 4 compressors;
- the write: the file's bytes at 125 MiB/s, or nothing extra on the fast disk, which didn't slow the uncompressed spill.

They assume the stages overlap fully. On 4 cores the merge takes one, so the compressors get about 3. zstd's compression then takes about 4.5 s, which is still shorter than the merge.

| Codec | Disk | Now | Pipelined, estimated | Slowest stage |
|---|---|---|---|---|
| none | 125 MiB/s | 22.5 s, serial model | 17.1 s | the write |
| s2 | 125 MiB/s | 20.2 s, serial model | 11.4 s | the write |
| zstd | 125 MiB/s | 28.8 s, serial model | 9.8 s | the write |
| none | fast disk | 5.4 s, measured | 5.4 s | the merge |
| s2 | fast disk | 9.2 s, measured | about 5.4 s | the merge |
| zstd | fast disk | 19.9 s, measured | about 5.4 s | the merge |

On a slow disk, zstd's spill would then be the shortest, since it writes the fewest bytes. On a fast disk, compression would cost the run's streams almost no time. So this change, more than the codec, decides whether zstd is worth its CPU.

### What it would cost

- **CPU.** The streams wait less, but the CPU does the same work: 13.6 s per 2 GiB with zstd, and 3.4 s with s2. That CPU is shared with decoding Ingest messages and with other runs' Joins. While a run spills its own streams wait, so some cores are idle, but other runs' aren't.
- **Memory, reserved for the Manager's life.** A spill runs inside the pool's `Reserve`, so it can't reserve anything itself. The pipeline's memory would have to be held from the start, as the write buffer is:
  - each block in flight needs its records and its compressed bytes, about 70 KiB at 32 KiB blocks;
  - each compressor needs an encoder: nothing for s2, about 384 KiB for zstd.

  With 4 compressors and 8 blocks in flight, that is about 0.6 MiB with s2 and 2 MiB with zstd. That is little against 128 MiB, but too much for small budgets, which would run fewer compressors or none.
- **The fence table, filled in later.** The writer now records a fence's file offset when the fence's first record arrives, which works because every earlier block has been written by then. In a pipeline, the merge would record that fence i starts at block k, and the writer would fill in the offset when it writes block k.
- **Order and back-pressure.** Compressors finish out of order. The writer holds finished blocks until the next one in order arrives, and the merge waits for a free block, so the blocks in flight bound the memory.
- **Failure.** A failed write must stop the merge and the compressors, and `Abort` must wait for them before it removes the file.

### Writes in flight too

The same writer goroutine could keep more than one write in flight. [SPILL_STORAGE_RESEARCH.md](SPILL_STORAGE_RESEARCH.md) notes that a gp3 volume needs about three requests outstanding to reach its baseline, and a spill now issues one at a time. Two or three 1 MiB write buffers, each reserved, would cover it, with or without compression.

## What isn't built

- **Compression off the stall path,** as described above. It is the change that decides between s2 and zstd.
- **Choosing per spill.** A spill whose first blocks don't compress could stop compressing, so that data like Envelope's costs nothing.
- **A real network volume.** These measurements throttle a local disk. A volume's latency per request, and its burst credits, aren't modelled. See [SPILL_STORAGE_RESEARCH.md](SPILL_STORAGE_RESEARCH.md).
- **The e2e suite on blocks.** The unit tests of `internal/join` and `internal/run` run on files of any codec with `RECON_TEST_SPILL_CODEC=s2` or `zstd`. The container suite and its RESULT records don't set a codec yet.
- **LZ4.** The proposal names LZ4 and zstd. s2 is in the same class of fast codecs, and comes with klauspost/compress, which the repository already uses for zstd.

## Reproducing

```sh
cd poc
# Ratio and speed by block size:
RECON_BLOCK_BENCH_MIB=256 go test ./internal/spill -run TestBlocks_measure -v
# Ingest, Seal and Join with each codec (as root, to empty the page cache):
go test -c -o run.test ./internal/run
RECON_CODEC_BENCH_MIB=2048 RECON_CODEC_BENCH_DIR=/path/on/the/disk RECON_CODEC_BENCH_DROP_CACHES=1 \
  ./run.test -test.run TestCodecs_measure -test.v
```

`RECON_CODEC_BENCH_DATA` picks other data, as `Envelope/Envelope/protobuf` or `Payment/BankLine/json`. To throttle the disk as here, on cgroup v1:

```sh
mkdir /sys/fs/cgroup/blkio/recon-bench && cd /sys/fs/cgroup/blkio/recon-bench
for f in read_bps write_bps; do echo "MAJ:MIN 131072000" > blkio.throttle.${f}_device; done
for f in read_iops write_iops; do echo "MAJ:MIN 3000" > blkio.throttle.${f}_device; done
sh -c 'echo $$ > cgroup.procs; exec env RECON_CODEC_BENCH_MIB=2048 ... ./run.test -test.run TestCodecs_measure -test.v'
```

`MAJ:MIN` is the data directory's block device, from `lsblk`.
