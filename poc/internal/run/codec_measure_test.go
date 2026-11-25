package run

import (
	"cmp"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/z5labs/humus"
	"google.golang.org/protobuf/proto"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/record"
	"poc/internal/spill"
)

// TestCodecs_measure ingests, seals and joins realistic records with each
// spill codec, on a real filesystem, and logs what each phase took. It
// runs only when RECON_CODEC_BENCH_MIB is set:
//
//   - RECON_CODEC_BENCH_MIB: MiB of key and value bytes, both sides.
//   - RECON_CODEC_BENCH_DIR: the data root; a temporary directory if unset.
//   - RECON_CODEC_BENCH_BUDGET_MIB: the memory budget, 128 by default.
//   - RECON_CODEC_BENCH_DATA: left/right profiles and the encoding, as
//     Payment/BankLine/protobuf, the default.
//   - RECON_CODEC_BENCH_CODECS: the codecs, none,s2,zstd by default.
//   - RECON_CODEC_BENCH_DROP_CACHES: 1 to empty the page cache before each
//     Join, so that it reads the disk; it needs root.
//
// The records are generated before the clock starts, ingested over 4
// streams at once, and dropped before the Join, so that the garbage
// collector's work on them doesn't count. To measure a slow disk, run the
// test binary in a cgroup that throttles the data root's device.
func TestCodecs_measure(t *testing.T) {
	mib, _ := strconv.Atoi(os.Getenv("RECON_CODEC_BENCH_MIB"))
	if mib <= 0 {
		t.Skip("set RECON_CODEC_BENCH_MIB to the MiB of records to measure")
	}
	root := cmp.Or(os.Getenv("RECON_CODEC_BENCH_DIR"), t.TempDir())
	budget, _ := strconv.Atoi(cmp.Or(os.Getenv("RECON_CODEC_BENCH_BUDGET_MIB"), "128"))
	data := strings.Split(cmp.Or(os.Getenv("RECON_CODEC_BENCH_DATA"), "Payment/BankLine/protobuf"), "/")
	enc, err := gen.ParseEncoding(data[2])
	if err != nil {
		t.Fatal(err)
	}
	cfg := gen.Config{
		Seed:      20,
		Left:      gen.Values{Schema: gen.Profile(data[0]), Encoding: enc},
		Right:     gen.Values{Schema: gen.Profile(data[1]), Encoding: enc},
		Mix:       gen.Typical,
		KeyFormat: gen.RFKeys,
	}
	cfg.Keys = cfg.KeysFor(int64(mib) << 20)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: %d MiB of keys and values, under a budget of %d MiB, in %s", strings.Join(data, "/"), mib, budget, root)

	var rows []string
	for _, name := range strings.Split(cmp.Or(os.Getenv("RECON_CODEC_BENCH_CODECS"), "none,s2,zstd"), ",") {
		codec, err := spill.ParseCodec(name)
		if err != nil {
			t.Fatal(err)
		}
		row := measureCodec(t, filepath.Join(root, fmt.Sprintf("codec-%s-%d", name, time.Now().UnixNano())), codec, int64(budget)<<20, cfg)
		rows = append(rows, row)
	}
	t.Logf("\n| Codec | Files | On disk / records | Ingest s | Spill stall s | Seal s | Join s | Join read MiB | Join preads | Ingest+Seal CPU s | Join CPU s |\n|---|---|---|---|---|---|---|---|---|---|---|\n%s", strings.Join(rows, "\n"))
}

// measureMessages generates cfg's records for streams streams, in
// messages of up to 1000 records of one side, and returns them and the
// bytes of their records.
func measureMessages(cfg gen.Config, streams int) ([][]message, int64) {
	msgs := make([][]message, streams)
	var raw int64
	for p := range streams {
		for r := range cfg.Records(p, streams) {
			side := record.Left
			if r.Side != harness.Left {
				side = record.Right
			}
			ms := msgs[p]
			if len(ms) == 0 || ms[len(ms)-1].side != side || len(ms[len(ms)-1].records) == 1000 {
				ms = append(ms, message{side: side})
			}
			m := &ms[len(ms)-1]
			m.records = append(m.records, &reconv1.Record{Key: r.Key, Value: r.Value})
			msgs[p] = ms
			raw += int64(record.Len(len(r.Key), len(r.Value)))
		}
		for i := range msgs[p] {
			msgs[p][i].size = int64(proto.Size(&reconv1.RecordBatch{Records: msgs[p][i].records}))
		}
	}
	return msgs, raw
}

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// measureCodec ingests, seals and joins cfg's records with codec in a new
// data directory, and returns the row of the table.
func measureCodec(t *testing.T, dir string, codec spill.Codec, limit int64, cfg gen.Config) string {
	t.Helper()
	msgs, raw := measureMessages(cfg, 4)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fsys := humus.OSHost().FS
	pool := mem.NewPool(limit)
	disk := spill.NewDisk(fsys, 1<<40)
	logs := &recorder{}
	m, err := NewManager(fsys, pool, disk, Config{
		DataDir:          dir,
		MaxRuns:          1,
		MaxBuckets:       4096,
		OutputBatchBytes: 1 << 20,
		JoinParallelism:  4,
		MaxMatchBytes:    3 << 20,
		SpillCodec:       codec,
		Logger:           slog.New(logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(t.Context())
	id, err := m.Create(4096)
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	start, cpu := time.Now(), cpuTime()
	var wg sync.WaitGroup
	errs := make([]error, len(msgs))
	for i, ms := range msgs {
		wg.Go(func() { errs[i] = ingestStreams(r, 1, ms) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ingest := time.Since(start)
	sealStart := time.Now()
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	seal := time.Since(sealStart)
	ingestCPU := cpuTime() - cpu
	sealed := logs.find("run sealed")[0]
	msgs = nil
	runtime.GC()

	if os.Getenv("RECON_CODEC_BENCH_DROP_CACHES") == "1" {
		syscall.Sync()
		if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0); err != nil {
			t.Fatalf("drop the page cache: %v", err)
		}
	}
	start, cpu = time.Now(), cpuTime()
	var results int64
	err = r.Join(t.Context(), join.NewFilter(nil), func(resp *reconv1.JoinResponse) error {
		results += int64(len(resp.GetResults()))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	joined, joinCPU := time.Since(start), cpuTime()-cpu
	j := logs.find("join finished")[0]

	fileBytes := sealed["recon.spill_file_bytes"].Int64()
	t.Logf("%v: %d files of %d MiB for %d MiB of records; ingest %.2fs, spills %.2fs, seal %.2fs, join %.2fs of %d results with %d workers and read buffers of %d KiB; %d MiB read in %d preads; disk peak %d MiB",
		codec, sealed["recon.run_files"].Int64(), fileBytes>>20, raw>>20, ingest.Seconds(), sealed["recon.spill_seconds"].Float64(), seal.Seconds(), joined.Seconds(),
		results, j["recon.join_workers"].Int64(), j["recon.join_read_buffer_bytes"].Int64()>>10, j["recon.join_bytes_read"].Int64()>>20, j["recon.join_reads"].Int64(), disk.Peak()>>20)
	return fmt.Sprintf("| %v | %d | %.2f | %.1f | %.1f | %.2f | %.1f | %d | %d | %.1f | %.1f |",
		codec, sealed["recon.run_files"].Int64(), float64(fileBytes)/float64(raw), ingest.Seconds(), sealed["recon.spill_seconds"].Float64(), seal.Seconds(),
		joined.Seconds(), j["recon.join_bytes_read"].Int64()>>20, j["recon.join_reads"].Int64(), ingestCPU.Seconds(), joinCPU.Seconds())
}
