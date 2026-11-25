# Spill storage on Kubernetes volumes

Nov 17, 2026

The volumes a sidecar pod is likely to get reward three things: few passes over the data, large requests, and few file operations. Sorted runs written to one append-only file per run meets all three. The per-bucket designs in the [bucket architecture evaluation](BUCKET_ARCHITECTURE_EVALUATION.md) miss at least one.

## Why the lab result needs this

The evaluation's slow-disk runs used a cgroup I/O throttle on btrfs at 3,000 IOPS and 125 MiB/s. That is exactly an EBS gp3 volume's baseline ([AWS][ebs-gp]), so the rate limits were realistic. The throttle did not reproduce five things that the published material below shows matter:

- **Request latency.** Cloud block storage takes milliseconds per request, against microseconds for the lab's NVMe.
- **How requests are counted.** EBS counts in 256 KiB units and merges only physically sequential requests.
- **Limits on the node, not the volume.** Small EC2 instances sustain less EBS bandwidth than a gp3 volume's baseline.
- **File operations on NFS.** Every create, open and close is a network round trip.
- **The node's kernel.** The lab's memory-cgroup OOM kills ran on a kernel with MGLRU enabled (see [Kernel caveat](#kernel-caveat)).

Pods in large organizations rarely choose their storage. The cluster owner decides which storage classes exist, and the one on offer may be NFS (EFS, Filestore) or the node's root disk. The design therefore has to run acceptably on all of them, not only on the best one.

## What the volumes provide

| Storage | Baseline | Limits that matter for spilling |
| --- | --- | --- |
| **EBS gp3** | 3,000 IOPS and 125 MiB/s per volume, provisionable to 80,000 IOPS and 2,000 MiB/s ([AWS][ebs-gp]) | No burst. Latency is "single-digit millisecond" ([AWS][ebs-gp]). Requests count in units of up to 256 KiB, so a 1 MiB write counts as 4 ([AWS][ebs-io]) |
| **EC2 node EBS bandwidth**, m5/m6i/m7i.large | 650 Mbps, 81.25 MB/s sustained ([AWS][ec2-ebs]) | Bursts to 593.75 MB/s (m5) or 1,250 MB/s (m6i, m7i) "for 30 minutes at least once every 24 hours". Shared by every EBS volume on the node, including the root disk behind `emptyDir` |
| **GCP pd-balanced** | 3,000 IOPS and 140 MiB/s per instance, plus 6 IOPS and 0.28 MiB/s per GiB ([Google][pd-perf]) | Writes count against the VM's network egress cap with a multiplier of about 1.16 for zonal disks ([Google][pd-opt]). VMs with 2 to 7 vCPUs cap pd-balanced writes at 240 MiB/s ([Google][pd-perf]) |
| **GCP Hyperdisk Balanced** | 3,000 IOPS and 140 MiB/s ([Google][hdb]) | Per-VM limits are "shared across all Hyperdisk Balanced volumes attached to the same instance". Small C3 and C4 machines cap at 400 MiB/s. Reads and writes share the limits ([Google][hdb]) |
| **Amazon EFS (NFS)** | Depends on throughput mode | "Each round trip (file open, file close) can take as much time as reading or writing megabytes of bulk data." Open, close and metadata calls "generally cannot be made asynchronously or through a pipeline" ([AWS][efs-tips]) |
| **GCP Filestore (NFS)** | Depends on tier and capacity ([Google][fs-perf]) | Tiers are "performance optimized for multiple client VMs, not a single client VM". A single client's reads and writes "are only performed from a single Filestore cluster VM" ([Google][fs-perf]) |
| **Local NVMe** (EC2 instance store, GKE Local SSD) | Highest throughput and lowest latency of the options | Lost with the node. On GKE, `--ephemeral-storage-local-ssd` puts `emptyDir` volumes on Local SSD, and it is the default on third-generation machine series ([Google][gke-ssd]) |

### How requests are counted and queued

- **EBS caps a counted request at 256 KiB on SSD volumes** and splits larger ones: "I/O size is capped at 256 KiB for SSD volumes" ([AWS][ebs-io]).
- **EBS merges small requests only when they are next to each other on disk:** "When small I/O operations are physically sequential, Amazon EBS attempts to merge them into a single I/O operation up to the maximum I/O size." Random requests count one each ([AWS][ebs-io]).
- **Both providers expect several requests in flight.**
  - AWS: "a volume must maintain an average queue depth ... of one for every 1,000 provisioned IOPS" ([AWS][ebs-io]).
  - Google: one outstanding request for every 400 to 800 IOPS ([Google][pd-opt]).
- **Throughput needs large requests.**
  - Google, for throughput-bound workloads: "Use an I/O size of 256 KB or larger" ([Google][pd-opt]).
  - Hyperdisk Balanced reaches its maximum throughput only with requests of at least 256 KB ([Google][hdb]).

## What the research measured

- **Block storage is tens of times slower per request than a local SSD.**
  - Wang and Yang (CUHK) measured AWS and Alibaba Cloud block storage ([arXiv 2607.20319][wang-yang]). The paper is under review and extends a DAC 2025 paper. They found latency "tens of times higher" than a local SSD for small requests, and 13.9 times higher on average at 256 KB.
  - Their advice: avoid "small and outstanding I/Os on the critical path", and keep I/O evenly spread over time and within the guaranteed budget.
- **Node burst credits run out mid-run.**
  - The same paper ran a 4-stream random-write test on io2 from an m6in.xlarge. Throughput fell after about 27 minutes, "eventually stabilizing around 305MB/s".
  - The authors attribute the drop "to the exhaustion of instance-level EBS burst credits" ([arXiv 2607.20319][wang-yang]).
- **Every provider's block storage adds write latency.** BtrLog, by Kuschewski, Leis and others, accepted to VLDB 2026, reports that "remote block storage like EBS is easy to adopt but adds substantial write latency and cost" ([arXiv 2606.27051][btrlog]). Its measurements are small log appends, so it supports the per-request cost, not large-transfer behaviour.
- **Page cache from spill files counts against the pod.** AWS's Data on EKS guide for Spark says this page cache "counts against the container's memory limit", and that "Reading those spill files back generates even more page cache" ([Data on EKS][doeks-oom]). Its mitigations:
  - Use cgroup v2 nodes.
  - Enable the kubelet's MemoryQoS feature gate, which sets `memory.high` below `memory.max`.
  - Run pods with Burstable QoS.
- **Other spilling systems prefer local disk.**
  - AWS's Spark-on-EKS blueprints put shuffle and spill on NVMe instance store ([Data on EKS][doeks-nvme]).
  - Snowflake spills to local disk first, and to remote cloud storage when a query needs even more memory. Its documentation says remote spilling "results in even worse performance" ([Snowflake][snowflake]).

## When NFS is the only option

NFS works for this design if the design avoids what NFS charges for. NFS charges per file operation and per client, not per byte:

- **Create, open, close and metadata calls are synchronous round trips** ([AWS][efs-tips]). Google's troubleshooting guide says Filestore shares use the `sync` export option, so "when many files are involved in an operation, the client makes a long series of synchronous operations" and "the cumulative latency adds up" ([Google][fs-trouble]). One user measured about 300 file creates per second on Filestore, even on a high tier ([Google Cloud Community][fs-community]).
- **AWS's advice is to open once and hold the file open:** "It's more efficient to open an input or output file once, at the beginning of your compute job, and hold it open for the entire length of the job" ([AWS][efs-tips]).
- **A single client gets one server's worth of Filestore** ([Google][fs-perf]). The sidecar is a single client, so plan for one server's throughput.

File operations per run at 10 GiB under a 128 MiB budget, from the evaluation's measurements:

| Design | Files created and deleted per run | Descriptors open at Join | Time to create the files at 300 per second |
| --- | ---: | ---: | ---: |
| Current | 150,716 | At most 5, reopened per file | About 8.4 minutes |
| Current, patched | 2,048 | At most 5, reopened per file | About 7 seconds |
| Sorted runs, one file per spill | 92 | 92 | Under 1 second |
| **Sorted runs, one file per run** | **1** | **1** | **Negligible** |

The 300-per-second rate is one user's report, so the last column is arithmetic, not a measurement. One file per run follows AWS's advice exactly: one create, one open held for the run's life, large sequential appends, positional reads on that one descriptor, and one unlink at `DeleteRun`.

## What this means for the design

1. **Count passes over the data, because each pass is long.**
   - One pass over 500 GiB takes about 68 minutes at gp3's 125 MiB/s baseline.
   - It takes about 110 minutes at the 81.25 MB/s that a small EC2 node sustains once its burst is spent.
   - Re-partitioning or an intermediate merge adds a whole pass. Sorted runs with one file per run needs neither.
   - Spill compression, which the README measures at 1.8 times with zstd, cuts every pass by the same factor, if it compresses as fast as the disk writes. The prototype in [SPILL_COMPRESSION.md](SPILL_COMPRESSION.md) cuts a Join's pass at 125 MiB/s by 1.5 times with s2 and 1.7 times with zstd, but zstd compresses on one core at about the disk's speed, so it doesn't yet shorten the spills.
2. **Keep requests at 256 KiB or more in both directions.**
   - **Writes:** sorted runs averaged about 1 MB per write call at 10 GiB (10,734 calls). The current design averaged about 14 KiB (768,760 calls) and the patched design about 12 KiB (914,278 calls), scattered across thousands of files.
   - **Join reads:** the prototype gives up workers only when read buffers would fall below 32 KiB. At 3,000 IOPS a 32 KiB read caps at about 94 MiB/s. On network volumes the floor should be near 256 KiB, accepting fewer workers.
3. **Keep several requests in flight.**
   - A direct-I/O spill that issues one write at a time has one request outstanding. AWS's rule asks for three at gp3's baseline. Two to four writes in flight, from a few goroutines or io_uring, would cover it.
   - Join reads have one request outstanding per worker. A Join that falls back to one worker needs read-ahead.
4. **Spread spills over time.** Today a spill stops every stream and writes a whole budget at once, which is the bursty pattern Wang and Yang warn against. An overlapped or half-budget spill, listed in the evaluation as not built, now matters for tail latency as well as for the stall.
5. **Use one file per run.** It removes per-file round trips on NFS, descriptor limits at Join, and the 3.7 s it took to delete 150,716 files. It needs no merge and no extra pass. Each run starts on a 4 KiB boundary, so direct I/O and buffered appends stay page-aligned.
6. **Recommend storage in this order, and work on all of it:**
   1. Local NVMe through `emptyDir`, where nodes have it.
   2. A dedicated block volume per pod: gp3, pd-balanced or Hyperdisk Balanced.
   3. `emptyDir` on the node's root disk, which is shared with container images, logs and other pods.
   4. NFS, when that is the only storage class offered. One file per run keeps it workable.

   Never use tmpfs: its pages count against the pod's memory limit, as the implementation plan already notes.
7. **Write runs with direct I/O.** Buffered spills leave dirty pages that the pod's memory limit has to absorb until writeback catches up ([Data on EKS][doeks-oom]). Direct I/O is already condition 1 of the evaluation. On NFS it has not been measured.

## Kernel caveat

The lab machine runs Fedora's 7.3.4 kernel with MGLRU fully enabled (`/sys/kernel/mm/lru_gen/enabled` reads `0x0007`). MGLRU is a newer page-reclaim algorithm. In 2024 a kernel developer reported memory-cgroup OOM kills under MGLRU while writing to a slow disk with `memory.max` set, and another called it trivially reproducible ([LKML][lkml-mglru]).

The thread could not be opened for this note, so whether a fix was merged is unknown. The evaluation's OOM kills of buffered writers on the throttled disk may therefore be partly specific to this kernel. Check `/sys/kernel/mm/lru_gen/enabled` on the target EKS or GKE nodes. Direct I/O avoids the dependence either way.

## Gaps and next measurements

- **Run on a real node:** an m6i.large with a gp3 volume, and an n2-standard-2 with pd-balanced. Measure ingest and join at 10 GiB and at 100 GiB, past the node's 30-minute burst.
- **Add per-request latency to the lab** with `dm-delay`, as a cheaper stand-in for the above.
- **Run once on EFS and once on Filestore**, comparing one file per run against one file per spill, and buffered against direct writes.
- **Read the node kernels' MGLRU setting** before drawing conclusions from the lab's OOM kills.

## Sources

AWS documentation:
- [Amazon EBS I/O characteristics and monitoring][ebs-io]: the 256 KiB counting rule, merging of sequential requests, queue depth.
- [Amazon EBS General Purpose SSD volumes][ebs-gp]: gp3 baseline and maximums, no burst, latency.
- [Amazon EBS-optimized instance types][ec2-ebs]: node-level EBS bandwidth and the 30-minute burst rule.
- [Amazon EFS performance tips][efs-tips]: per-operation round trips and the advice to hold files open.

Google Cloud documentation:
- [Persistent Disk performance][pd-perf]: pd-balanced baselines, per-GiB scaling, per-vCPU write limits.
- [Optimize Persistent Disk performance][pd-opt]: write egress multiplier, queue depth, request size.
- [Hyperdisk Balanced][hdb]: baselines, shared per-VM limits, request size.
- [Filestore performance][fs-perf]: single-client limits.
- [Troubleshoot Filestore performance][fs-trouble]: the `sync` export option and many-file operations.
- [About Local SSD for GKE][gke-ssd]: Local SSD-backed `emptyDir`.

Research:
- [Wang and Yang, Black-Box Performance Evaluation of Elastic Block Storage, arXiv 2607.20319][wang-yang]: latency against local SSD, node burst-credit exhaustion. Under review.
- [Kuschewski et al., BtrLog: Low-Latency Logging for Cloud Database Systems, arXiv 2606.27051][btrlog]: block storage write latency on AWS, GCP and Azure. Accepted to VLDB 2026.

Practice:
- [Data on EKS: Preventing OOM Kills in Spark on EKS][doeks-oom]: page cache counted against pod limits, MemoryQoS.
- [Data on EKS: Spark with NVMe instance storage][doeks-nvme]: shuffle and spill on instance store.
- [Snowflake: Queries too large to fit in memory][snowflake]: local before remote spilling.
- [Google Cloud Community: Filestore much slower than Persistent Disk for some operations][fs-community]: one user's file-creation rate.
- [LKML: MGLRU premature memcg OOM on slow writes][lkml-mglru]: the kernel report. lore.kernel.org serves automated clients a bot check, so the thread itself could not be read for this note. Its details come from search results.

[ebs-io]: https://docs.aws.amazon.com/ebs/latest/userguide/ebs-io-characteristics.html
[ebs-gp]: https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html
[ec2-ebs]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-optimized.html
[efs-tips]: https://docs.aws.amazon.com/efs/latest/ug/performance-tips.html
[pd-perf]: https://docs.cloud.google.com/compute/docs/disks/performance
[pd-opt]: https://docs.cloud.google.com/compute/docs/disks/optimizing-pd-performance
[hdb]: https://docs.cloud.google.com/compute/docs/disks/hd-types/hyperdisk-balanced
[fs-perf]: https://docs.cloud.google.com/filestore/docs/performance
[fs-trouble]: https://docs.cloud.google.com/filestore/docs/troubleshooting
[gke-ssd]: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/local-ssd
[wang-yang]: https://arxiv.org/abs/2607.20319
[btrlog]: https://arxiv.org/abs/2606.27051
[doeks-oom]: https://awslabs.github.io/data-on-eks/docs/bestpractices/analytics/spark-oom-kills
[doeks-nvme]: https://awslabs.github.io/data-on-eks/docs/datastacks/processing/spark-on-eks/nvme-storage
[snowflake]: https://docs.snowflake.com/en/user-guide/performance-query-warehouse-memory
[fs-community]: https://www.googlecloudcommunity.com/gc/Infrastructure-Compute-Storage/Filestore-much-slower-than-Persistent-Disk-for-some-operations/m-p/611750
[lkml-mglru]: https://lore.kernel.org/all/ZfC2612ZYwwxpOmR@google.com/T/
