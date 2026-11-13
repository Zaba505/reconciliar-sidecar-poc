//go:build e2e && measure

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"poc/e2e/gen"
	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

// TestMeasure takes the measurements of PLAN.md §8.5 for POC-RESULTS.md:
// one run per scenario, ingested over 4 streams and joined, in a 512 MiB
// container without swap. It prints a RESULT line of JSON per scenario. Run it on its
// own, so that nothing else competes for the machine:
//
//	RECON_E2E_IMAGE=recon-sidecar:dev go test -tags e2e,measure -run TestMeasure -timeout 2h -v ./e2e/
func TestMeasure(t *testing.T) {
	const (
		memory   = "512m"
		goMemMax = "483183820" // 90% of 512 MiB
	)
	type scenario struct {
		data       string
		keys       int
		valueSize  int
		budget     string // RECON_MEMORY_LIMIT_BYTES, "" for the default of 70%
		goMemLimit string // GOMEMLIMIT, "" for none
	}
	var scenarios []scenario
	for _, d := range []struct {
		name string
		keys int
	}{
		{"1GiB", 350_000},
		{"10GiB", 3_500_000},
	} {
		for _, budget := range []string{"128MiB", ""} {
			for _, limit := range []string{"", goMemMax} {
				scenarios = append(scenarios, scenario{d.name, d.keys, 1000, budget, limit})
			}
		}
	}
	// Small records, whose index entries and chunk slack weigh more.
	for _, limit := range []string{"", goMemMax} {
		scenarios = append(scenarios, scenario{"275MiB-small", 2_000_000, 32, "128MiB", limit})
	}
	for _, sc := range scenarios {
		name := fmt.Sprintf("%s/budget=%s/GOMEMLIMIT=%s", sc.data, or(sc.budget, "default"), or(sc.goMemLimit, "unset"))
		t.Run(name, func(t *testing.T) {
			opts := []harness.Option{harness.Memory(memory)}
			if sc.budget != "" {
				opts = append(opts, harness.Env("RECON_MEMORY_LIMIT_BYTES", sc.budget))
			}
			if sc.goMemLimit != "" {
				opts = append(opts, harness.Env("GOMEMLIMIT", sc.goMemLimit))
			}
			measure(t, name, gen.Config{Seed: 20, Keys: sc.keys, ValueSize: sc.valueSize}, opts...)
		})
	}
}

func or(s, otherwise string) string {
	if s == "" {
		return otherwise
	}
	return s
}

// measurement is one scenario's RESULT.
type measurement struct {
	Scenario       string
	Records        uint64
	RecordBytes    int64 // keys and values
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

func measure(t *testing.T, name string, cfg gen.Config, opts ...harness.Option) {
	c := harness.Start(t, opts...)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	limits, _ := c.Log("effective limits")
	m := measurement{Scenario: name}
	m.BudgetBytes, _ = limits.Int("recon.memory_limit_bytes")
	m.Parallelism, _ = limits.Int("recon.join_parallelism")
	left, right := cfg.Totals()
	m.Records = left + right
	m.RecordBytes = int64(m.Records) * int64(len(cfg.Key(0))+cfg.ValueSize)

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
	errs := make(chan error, 4)
	for p := range 4 {
		go func() { errs <- harness.Ingest(ctx, c.Client, id, cfg.Records(p, 4)) }()
	}
	for range 4 {
		if err := <-errs; err != nil && m.IngestError == "" {
			m.IngestError = err.Error()
		}
	}
	m.IngestSeconds = time.Since(start).Seconds()
	if m.IngestError != "" {
		return
	}
	seal(ctx, t, c, id)
	m.SpillFiles, m.SpillBytes = spillFiles(t, c, id)

	start = time.Now()
	v := gen.NewVerifier(cfg)
	err := harness.Join(ctx, c.Client, &reconv1.JoinRequest{RunId: id}, v.Add)
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
