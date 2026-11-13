//go:build e2e && measure

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"poc/e2e/gen"
	"poc/e2e/gen/stats"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

// sampleBytes is about how many spill-format bytes of a run's records a
// measurement compresses to report their ratios.
const sampleBytes = 64 << 20

// TestMeasure takes the measurements of PLAN.md §8.5 for POC-RESULTS.md:
// one run per scenario, ingested over 4 streams and joined, in a 512 MiB
// container without swap. It prints a RESULT line of JSON per scenario.
//
// Run it on its own, so that nothing else competes for the machine, with
// the data directories on a filesystem that doesn't compress them, such
// as a btrfs directory made with chattr +m:
//
//	RECON_E2E_IMAGE=recon-sidecar:dev RECON_E2E_DATA_ROOT=/var/tmp/recon-e2e-nocomp go test -tags e2e,measure -run TestMeasure -timeout 3h -v ./e2e/
//
// A scenario's name is its encoding, its profiles (left-right, or one for
// both sides), its size in key and value bytes, its budget and its
// GOMEMLIMIT, such as protobuf/Payment-BankLine/10GiB/budget=default/GOMEMLIMIT=483183820,
// so that -run can pick scenarios.
func TestMeasure(t *testing.T) {
	const (
		memory   = "512m"
		goMemMax = "483183820" // 90% of 512 MiB
	)
	type scenario struct {
		cfg        gen.Config // its Keys is picked for target, unless set
		size       string
		target     int64  // key and value bytes
		budget     string // RECON_MEMORY_LIMIT_BYTES, "" for the default of 70%
		goMemLimit string // GOMEMLIMIT, "" for none
	}
	var scenarios []scenario
	add := func(cfg gen.Config, size string, target int64, budgets, limits []string) {
		for _, budget := range budgets {
			for _, limit := range limits {
				scenarios = append(scenarios, scenario{cfg, size, target, budget, limit})
			}
		}
	}
	defaultOnly, limitOnly := []string{""}, []string{goMemMax}

	// The grid of the earlier results, so that the memory figures stay
	// comparable.
	for _, size := range []struct {
		name  string
		bytes int64
	}{{"1GiB", 1 << 30}, {"10GiB", 10 << 30}} {
		add(profiles(gen.Payment, gen.BankLine, gen.Protobuf), size.name, size.bytes, []string{"128MiB", ""}, []string{"", goMemMax})
	}
	for _, enc := range []gen.Encoding{gen.Avro, gen.JSON} {
		add(profiles(gen.Payment, gen.BankLine, enc), "10GiB", 10<<30, defaultOnly, limitOnly)
	}
	for _, p := range []*gen.Schema{gen.CardSettlement, gen.Envelope} {
		add(profiles(p, p, gen.Protobuf), "10GiB", 10<<30, defaultOnly, limitOnly)
	}
	// Small records, whose index entries and chunk slack weigh more.
	add(profiles(gen.Tiny, gen.Tiny, gen.Protobuf), "275MiB", 275<<20, []string{"128MiB"}, []string{"", goMemMax})
	// The old generator with the old settings, to compare with the earlier
	// results: about 10 GiB of 1000-byte values in the stress mix. In the
	// spill format they compress about 52 times with zstd level 1.
	add(gen.Config{Seed: 20, Keys: 3_500_000, ValueSize: 1000}, "10GiB", 10<<30, defaultOnly, limitOnly)

	for _, sc := range scenarios {
		name := fmt.Sprintf("%s/%s/budget=%s/GOMEMLIMIT=%s", dataName(sc.cfg), sc.size, or(sc.budget, "default"), or(sc.goMemLimit, "unset"))
		t.Run(name, func(t *testing.T) {
			opts := []harness.Option{harness.Memory(memory)}
			if sc.budget != "" {
				opts = append(opts, harness.Env("RECON_MEMORY_LIMIT_BYTES", sc.budget))
			}
			if sc.goMemLimit != "" {
				opts = append(opts, harness.Env("GOMEMLIMIT", sc.goMemLimit))
			}
			cfg := sc.cfg
			if cfg.Keys == 0 {
				cfg.Keys = cfg.KeysFor(sc.target)
			}
			measure(t, name, cfg, sc.target, opts...)
		})
	}
}

// profiles returns the Config of seed 20 with the left and right profiles'
// records marshaled as enc, in the Typical mix with RF keys. Its Keys is
// unset.
func profiles(left, right *gen.Schema, enc gen.Encoding) gen.Config {
	return gen.Config{
		Seed:      20,
		Left:      gen.Values{Schema: left, Encoding: enc},
		Right:     gen.Values{Schema: right, Encoding: enc},
		Mix:       gen.Typical,
		KeyFormat: gen.RFKeys,
	}
}

// dataName names cfg's data in a scenario's name: legacy and the value
// size, or the encoding and the profiles, one if both sides have it.
func dataName(cfg gen.Config) string {
	l, r := cfg.Left, cfg.Right
	switch {
	case l.Schema == nil:
		return fmt.Sprintf("legacy/%dB", cfg.ValueSize)
	case l.Schema == r.Schema && l.Encoding == r.Encoding:
		return l.Encoding.String() + "/" + l.Schema.Name
	case l.Encoding == r.Encoding:
		return l.Encoding.String() + "/" + l.Schema.Name + "-" + r.Schema.Name
	}
	return l.Encoding.String() + "-" + r.Encoding.String() + "/" + l.Schema.Name + "-" + r.Schema.Name
}

// sideOf returns the profile and encoding of a side's values, or legacy and
// no encoding.
func sideOf(v gen.Values) (profile, encoding string) {
	if v.Schema == nil {
		return "legacy", ""
	}
	return v.Schema.Name, v.Encoding.String()
}

func or(s, otherwise string) string {
	if s == "" {
		return otherwise
	}
	return s
}

// measurement is one scenario's RESULT.
type measurement struct {
	Scenario string

	// The data: each side's profile and encoding, or legacy, no encoding
	// and ValueSize.
	LeftProfile, LeftEncoding   string
	RightProfile, RightEncoding string
	ValueSize                   int // of legacy values, else 0
	Mix                         gen.Mix
	KeyFormat                   gen.KeyFormat
	Seed                        uint64
	Keys                        int
	TargetBytes                 int64 // the key and value bytes Keys was picked for

	Records        uint64  // of the run, both sides
	KeyBytes       int64   // of the records the Ingest streams were given
	ValueBytes     int64   // of the same records
	MeanValueBytes float64 // ValueBytes over those records
	RecordBytes    int64   // KeyBytes plus ValueBytes

	// A sample of the run's first records, about sampleBytes in the spill
	// format, taken before the container started: their ratios
	// compressed in 64 KiB blocks, and their value sizes.
	SampleSpillBytes int64
	SampleS2Ratio    float64
	SampleZstdRatio  float64 // zstd level 1
	SampleValueSizes stats.Sizes

	DataFS harness.Filesystem // of the container's data directory

	IngestSeconds  float64
	JoinSeconds    float64
	SpillFiles     int // after Seal
	SpillBytes     int64
	BudgetBytes    int64
	Parallelism    int64
	JoinFinished   map[string]any // the join finished record's attributes
	VmHWMBytes     int64          // the sidecar's peak resident set
	CgroupPeak     int64          // memory.peak of the container's cgroup, page cache included
	OOMKilled      bool
	Running        bool
	IngestError    string
	JoinError      string
	ContainerUsage int64 // the harness's sampled peak, page cache included
}

// sent counts the records an Ingest stream was given.
type sent struct {
	records      uint64
	keys, values int64
}

// counting yields the records of seq, counting them in n.
func counting(seq iter.Seq[harness.Record], n *sent) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		for r := range seq {
			n.records++
			n.keys += int64(len(r.Key))
			n.values += int64(len(r.Value))
			if !yield(r) {
				return
			}
		}
	}
}

func measure(t *testing.T, name string, cfg gen.Config, target int64, opts ...harness.Option) {
	m := measurement{Scenario: name, Mix: cfg.Mix, KeyFormat: cfg.KeyFormat, Seed: cfg.Seed, Keys: cfg.Keys, TargetBytes: target}
	m.LeftProfile, m.LeftEncoding = sideOf(cfg.Left)
	m.RightProfile, m.RightEncoding = sideOf(cfg.Right)
	if cfg.Left.Schema == nil {
		m.ValueSize = cfg.ValueSize
	}
	left, right := cfg.Totals()
	m.Records = left + right
	// Before the container starts, so that it doesn't compete with the run.
	sample := stats.Sample(cfg.Records(0, 1), sampleBytes)
	m.SampleSpillBytes, m.SampleS2Ratio, m.SampleZstdRatio, m.SampleValueSizes = sample.SpillBytes, sample.S2Ratio, sample.ZstdRatio, sample.Value

	c := harness.Start(t, opts...)
	var err error
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	limits, _ := c.Log("effective limits")
	m.BudgetBytes, _ = limits.Int("recon.memory_limit_bytes")
	m.Parallelism, _ = limits.Int("recon.join_parallelism")
	m.DataFS, err = harness.DescribeFilesystem(c.DataDir)
	if err != nil {
		t.Logf("describe the data directory's filesystem: %v", err)
	}
	if m.DataFS.Compresses {
		t.Logf("%s is on %s mounted %s, which may compress the spill files; set RECON_E2E_DATA_ROOT to a directory where it doesn't", c.DataDir, m.DataFS.Type, m.DataFS.Options)
	}

	// A sidecar that is OOM-killed is a result, not a failure of the test.
	defer func() {
		st := c.State()
		m.OOMKilled, m.Running = st.OOMKilled, st.Running
		if st.Running {
			m.VmHWMBytes = vmHWM(t, c)
			m.CgroupPeak = cgroupPeak(t, c)
		}
		m.ContainerUsage = c.PeakMemory()
		out, _ := json.Marshal(m)
		t.Logf("RESULT %s", out)
	}()

	id := create(ctx, t, c, 1024)
	start := time.Now()
	const streams = 4
	var counts [streams]sent
	errs := make(chan error, streams)
	for p := range streams {
		go func() { errs <- harness.Ingest(ctx, c.Client, id, counting(cfg.Records(p, streams), &counts[p])) }()
	}
	for range streams {
		if err := <-errs; err != nil && m.IngestError == "" {
			m.IngestError = err.Error()
		}
	}
	m.IngestSeconds = time.Since(start).Seconds()
	// Every stream has sent its error, so its counts are final.
	var records uint64
	for _, n := range counts {
		records += n.records
		m.KeyBytes += n.keys
		m.ValueBytes += n.values
	}
	m.RecordBytes = m.KeyBytes + m.ValueBytes
	if records > 0 {
		m.MeanValueBytes = float64(m.ValueBytes) / float64(records)
	}
	if m.IngestError != "" {
		return
	}
	if records != m.Records {
		t.Errorf("the Ingest streams were given %d records, want the run's %d", records, m.Records)
	}
	seal(ctx, t, c, id)
	m.SpillFiles, m.SpillBytes = spillFiles(t, c, id)

	start = time.Now()
	v := gen.NewVerifier(cfg)
	err = harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, v.Add)
	m.JoinSeconds = time.Since(start).Seconds()
	if err == nil {
		err = v.Finish()
	}
	if err != nil {
		m.JoinError = err.Error()
	}
	if rec, ok := lastJoinFinished(c, id); ok {
		m.JoinFinished = rec.Attributes
	}
}

// inspect returns a field of the container's inspect output.
func inspect(t *testing.T, c *harness.Container, format string) string {
	engine := or(os.Getenv("RECON_E2E_ENGINE"), "podman")
	out, err := exec.Command(engine, "inspect", "--format", format, c.ID).Output()
	if err != nil {
		t.Logf("inspect %s: %v", format, err)
		return ""
	}
	return strings.TrimSpace(string(out))
}

// vmHWM returns the peak resident set of the container's process, from
// the host's /proc, or -1.
func vmHWM(t *testing.T, c *harness.Container) int64 {
	f, err := os.Open(filepath.Join("/proc", inspect(t, c, "{{.State.Pid}}"), "status"))
	if err != nil {
		t.Logf("VmHWM: %v", err)
		return -1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "VmHWM:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err == nil {
				return kb << 10
			}
		}
	}
	return -1
}

// cgroupPeak returns the container cgroup's memory.peak, or -1.
func cgroupPeak(t *testing.T, c *harness.Container) int64 {
	path := inspect(t, c, "{{.State.CgroupPath}}")
	b, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", path, "memory.peak"))
	if err != nil {
		t.Logf("memory.peak: %v", err)
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return -1
	}
	return n
}
