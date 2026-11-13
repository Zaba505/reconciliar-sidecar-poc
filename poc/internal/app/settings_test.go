package app

import (
	"os"
	"reflect"
	"runtime"
	"slices"
	"testing"
)

func TestReadSettings_defaults(t *testing.T) {
	e := newTestEnv(t)
	cfg, err := readSettings(t.Context(), e.setup(t, e.with(map[string]string{"RECON_DISK_LIMIT_BYTES": ""})), e.cgroup)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.GRPC.Addr != "127.0.0.1:7443" {
		t.Errorf("GRPC.Addr = %q, want 127.0.0.1:7443", cfg.GRPC.Addr)
	}
	if cfg.GRPC.TLS == nil || len(cfg.GRPC.TLS.Certificates) != 1 {
		t.Errorf("GRPC.TLS = %v, want one certificate", cfg.GRPC.TLS)
	}
	if cfg.HealthAddr != ":8081" {
		t.Errorf("HealthAddr = %q, want :8081", cfg.HealthAddr)
	}
	if want := int64(testCgroupLimit) * 7 / 10; cfg.MemoryLimit != want {
		t.Errorf("MemoryLimit = %d, want 70%% of the cgroup limit, %d", cfg.MemoryLimit, want)
	}
	if cfg.DiskLimit != 0 {
		t.Errorf("DiskLimit = %d, want 0, for Init to default after the wipe", cfg.DiskLimit)
	}
	if want := min(runtime.GOMAXPROCS(0), 4); cfg.JoinParallelism != want {
		t.Errorf("JoinParallelism = %d, want %d", cfg.JoinParallelism, want)
	}
	checks := []struct {
		name      string
		got, want int64
	}{
		{"MaxBuckets", int64(cfg.MaxBuckets), 4096},
		{"MaxRuns", int64(cfg.MaxRuns), 8},
		{"MaxRecvMessageBytes", int64(cfg.MaxRecvMessageBytes), 16 << 20},
		{"OutputBatchBytes", cfg.OutputBatchBytes, 1 << 20},
		{"MaxMatchBytes", cfg.MaxMatchBytes, 3 << 20},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if cfg.LogKeys {
		t.Error("LogKeys = true, want false")
	}
}

func TestReadSettings_explicit_values(t *testing.T) {
	e := newTestEnv(t)
	cfg, err := readSettings(t.Context(), e.setup(t, e.with(map[string]string{
		"RECON_LISTEN":                 ":7443",
		"RECON_HEALTH_ADDR":            "127.0.0.1:9000",
		"RECON_MAX_BUCKETS":            "64",
		"RECON_MEMORY_LIMIT_BYTES":     "128MiB",
		"RECON_DISK_LIMIT_BYTES":       "2GiB",
		"RECON_JOIN_PARALLELISM":       "3",
		"RECON_MAX_RUNS":               "2",
		"RECON_MAX_RECV_MESSAGE_BYTES": "4194304",
		"RECON_OUTPUT_BATCH_BYTES":     "64KiB",
		"RECON_MAX_MATCH_BYTES":        "5MiB",
		"RECON_LOG_KEYS":               "true",
	})), e.cgroup)
	if err != nil {
		t.Fatal(err)
	}
	want := Settings{
		HealthAddr:          "127.0.0.1:9000",
		DataDir:             e.env["RECON_DATA_DIR"],
		MaxBuckets:          64,
		MemoryLimit:         128 << 20,
		DiskLimit:           2 << 30,
		JoinParallelism:     3,
		MaxRuns:             2,
		MaxRecvMessageBytes: 4 << 20,
		OutputBatchBytes:    64 << 10,
		MaxMatchBytes:       5 << 20,
		LogKeys:             true,
	}
	if cfg.GRPC.Addr != ":7443" {
		t.Errorf("GRPC.Addr = %q, want :7443", cfg.GRPC.Addr)
	}
	cfg.GRPC = want.GRPC
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("settings = %+v, want %+v", cfg, want)
	}
}

func TestReadSettings_memory_limit_without_a_cgroup_limit(t *testing.T) {
	for _, file := range []string{"cgroup/nolimit.max", "cgroup/missing.max", "cgroup/garbage.max", "cgroup/negative.max"} {
		t.Run(file, func(t *testing.T) {
			e := newTestEnv(t)
			cgroup := e.host.Path(file)

			_, err := readSettings(t.Context(), e.setup(t, e.env), cgroup)
			if got := failedNames(err); !slices.Equal(got, []string{"RECON_MEMORY_LIMIT_BYTES"}) {
				t.Errorf("unset: failed variables = %q, want RECON_MEMORY_LIMIT_BYTES: %v", got, err)
			}

			cfg, err := readSettings(t.Context(), e.setup(t, e.with(map[string]string{"RECON_MEMORY_LIMIT_BYTES": "8GiB"})), cgroup)
			if err != nil {
				t.Fatalf("set: %v", err)
			}
			if cfg.MemoryLimit != 8<<30 {
				t.Errorf("MemoryLimit = %d, want 8GiB", cfg.MemoryLimit)
			}
		})
	}
}

func TestReadSettings_rejects_a_bad_value(t *testing.T) {
	cases := []struct {
		name    string
		changes map[string]string // relative paths are made absolute on the host
		wantVar string
	}{
		{"listen not host:port", map[string]string{"RECON_LISTEN": "7443"}, "RECON_LISTEN"},
		{"cert file unset", map[string]string{"RECON_TLS_CERT_FILE": ""}, "RECON_TLS_CERT_FILE"},
		{"cert file missing", map[string]string{"RECON_TLS_CERT_FILE": "tls/missing.pem"}, "RECON_TLS_CERT_FILE"},
		{"key file unset", map[string]string{"RECON_TLS_KEY_FILE": ""}, "RECON_TLS_KEY_FILE"},
		{"key file missing", map[string]string{"RECON_TLS_KEY_FILE": "tls/missing.pem"}, "RECON_TLS_KEY_FILE"},
		{"key of another certificate", map[string]string{"RECON_TLS_KEY_FILE": "tls/other-key.pem"}, "RECON_TLS_KEY_FILE"},
		{"cert not PEM", map[string]string{"RECON_TLS_CERT_FILE": "tls/not-a-cert.pem"}, "RECON_TLS_KEY_FILE"},
		{"health addr not host:port", map[string]string{"RECON_HEALTH_ADDR": "8081"}, "RECON_HEALTH_ADDR"},
		{"data dir missing", map[string]string{"RECON_DATA_DIR": "nope"}, "RECON_DATA_DIR"},
		{"data dir is a file", map[string]string{"RECON_DATA_DIR": "tls/server.pem"}, "RECON_DATA_DIR"},
		{"max buckets 0", map[string]string{"RECON_MAX_BUCKETS": "0"}, "RECON_MAX_BUCKETS"},
		{"max buckets not a number", map[string]string{"RECON_MAX_BUCKETS": "many"}, "RECON_MAX_BUCKETS"},
		{"max buckets above uint32", map[string]string{"RECON_MAX_BUCKETS": "4294967296"}, "RECON_MAX_BUCKETS"},
		{"memory limit 0", map[string]string{"RECON_MEMORY_LIMIT_BYTES": "0"}, "RECON_MEMORY_LIMIT_BYTES"},
		{"memory limit malformed", map[string]string{"RECON_MEMORY_LIMIT_BYTES": "1GB"}, "RECON_MEMORY_LIMIT_BYTES"},
		{"memory limit above the cgroup's", map[string]string{"RECON_MEMORY_LIMIT_BYTES": "1025MiB"}, "RECON_MEMORY_LIMIT_BYTES"},
		{"disk limit 0", map[string]string{"RECON_DISK_LIMIT_BYTES": "0"}, "RECON_DISK_LIMIT_BYTES"},
		{"join parallelism 0", map[string]string{"RECON_JOIN_PARALLELISM": "0"}, "RECON_JOIN_PARALLELISM"},
		{"max runs 0", map[string]string{"RECON_MAX_RUNS": "0"}, "RECON_MAX_RUNS"},
		{"max recv 0", map[string]string{"RECON_MAX_RECV_MESSAGE_BYTES": "0"}, "RECON_MAX_RECV_MESSAGE_BYTES"},
		{"output batch 0", map[string]string{"RECON_OUTPUT_BATCH_BYTES": "0B"}, "RECON_OUTPUT_BATCH_BYTES"},
		{"max match negative", map[string]string{"RECON_MAX_MATCH_BYTES": "-1"}, "RECON_MAX_MATCH_BYTES"},
		{"log keys not a bool", map[string]string{"RECON_LOG_KEYS": "maybe"}, "RECON_LOG_KEYS"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newTestEnv(t)
			changes := map[string]string{}
			for k, v := range c.changes {
				if v != "" && (k == "RECON_TLS_CERT_FILE" || k == "RECON_TLS_KEY_FILE" || k == "RECON_DATA_DIR") {
					v = e.host.Path(v)
				}
				changes[k] = v
			}
			_, err := readSettings(t.Context(), e.setup(t, e.with(changes)), e.cgroup)
			if got := failedNames(err); !slices.Equal(got, []string{c.wantVar}) {
				t.Errorf("failed variables = %q, want [%s]: %v", got, c.wantVar, err)
			}
		})
	}
}

func TestReadSettings_reports_every_bad_value(t *testing.T) {
	e := newTestEnv(t)
	_, err := readSettings(t.Context(), e.setup(t, e.with(map[string]string{
		"RECON_MAX_BUCKETS": "0",
		"RECON_MAX_RUNS":    "x",
		"RECON_LISTEN":      "",
	})), e.cgroup)
	got := failedNames(err)
	slices.Sort(got)
	if want := []string{"RECON_MAX_BUCKETS", "RECON_MAX_RUNS"}; !slices.Equal(got, want) {
		t.Errorf("failed variables = %q, want %q: %v", got, want, err)
	}
}

func TestReadSettings_leaves_no_probe_file(t *testing.T) {
	e := newTestEnv(t)
	if _, err := readSettings(t.Context(), e.setup(t, e.env), e.cgroup); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(e.env["RECON_DATA_DIR"])
	if err != nil {
		t.Fatal(err)
	}
	for _, de := range entries {
		if de.Name() != ".keep" {
			t.Errorf("data dir holds %q after the writability check", de.Name())
		}
	}
}

func TestDefaultDiskLimit(t *testing.T) {
	e := newTestEnv(t)
	n, err := defaultDiskLimit(t.Context(), e.host.Path("data"))
	if err != nil || n <= 0 {
		t.Errorf("defaultDiskLimit = %d, %v; want 90%% of the free space, which is positive", n, err)
	}

	_, err = defaultDiskLimit(t.Context(), e.host.Path("missing"))
	if got := failedNames(err); !slices.Equal(got, []string{"RECON_DISK_LIMIT_BYTES"}) {
		t.Errorf("missing dir: failed variables = %q, want [RECON_DISK_LIMIT_BYTES]: %v", got, err)
	}
}
