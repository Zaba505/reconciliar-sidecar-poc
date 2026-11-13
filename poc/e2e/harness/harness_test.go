package harness

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseMemUsage(t *testing.T) {
	cases := map[string]int64{
		"729.1kB / 268.4MB\n": 729100,
		"712KiB / 256MiB":     712 << 10,
		"1.5GiB / 4GiB":       3 << 29,
		"0B / 0B":             0,
	}
	for in, want := range cases {
		if got, err := parseMemUsage(in); err != nil || got != want {
			t.Errorf("parseMemUsage(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "-- / --", "12 parsecs / 1GB"} {
		if got, err := parseMemUsage(in); err == nil {
			t.Errorf("parseMemUsage(%q) = %d, want an error", in, got)
		}
	}
}

func TestReservePorts(t *testing.T) {
	lo, hi := ephemeralRange()
	got := make([]int, 64)
	if err := reservePorts(got); err != nil {
		t.Fatal(err)
	}
	defer releasePorts(got...)
	seen := make(map[int]bool)
	for _, p := range got {
		if p < minHostPort || p > maxHostPort || (p >= lo && p <= hi) {
			t.Errorf("port %d is outside [%d, %d] or inside the ephemeral range [%d, %d]", p, minHostPort, maxHostPort, lo, hi)
		}
		if seen[p] {
			t.Errorf("port %d reserved twice", p)
		}
		seen[p] = true
	}
	releasePorts(got[0])
	ports.mu.Lock()
	defer ports.mu.Unlock()
	if ports.held[got[0]] || !ports.held[got[1]] {
		t.Errorf("after releasing %d: held[%d] = %t, held[%d] = %t; want false, true", got[0], got[0], ports.held[got[0]], got[1], ports.held[got[1]])
	}
}

func TestDataRoot(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for env, want := range map[string]string{
		"":             defaultDataRoot,
		"/mnt/x":       "/mnt/x",
		"data":         filepath.Join(wd, "data"), // a relative source would name a volume
		"./data/runs/": filepath.Join(wd, "data/runs"),
	} {
		t.Setenv("RECON_E2E_DATA_ROOT", env)
		if got, err := dataRoot(); err != nil || got != want {
			t.Errorf("RECON_E2E_DATA_ROOT=%q: dataRoot() = %q, %v; want %q", env, got, err, want)
		}
	}
}
