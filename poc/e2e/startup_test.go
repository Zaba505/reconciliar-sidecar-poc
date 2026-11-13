//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
)

func TestHealth(t *testing.T) {
	t.Parallel()
	c := harness.Start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	resp, err := c.Health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("plaintext health check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health = %v, want SERVING", resp.GetStatus())
	}

	plain, err := grpc.NewClient("passthrough:///"+c.APIAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	_, err = reconv1.NewReconcilerClient(plain).DeleteRun(ctx, &reconv1.DeleteRunRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("plaintext API call: %v, want Unavailable", err)
	}

	c.Conn.Connect()
	for st := c.Conn.GetState(); st != connectivity.Ready; st = c.Conn.GetState() {
		if !c.Conn.WaitForStateChange(ctx, st) {
			t.Fatalf("TLS connection to the API never turned ready; last state %v", st)
		}
	}
	_, err = c.Client.DeleteRun(ctx, &reconv1.DeleteRunRequest{})
	if status.Code(err) == codes.Unavailable {
		t.Errorf("TLS API call: %v, want it to reach the server", err)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		opts    []harness.Option
		wantVar string
	}{
		{"missing TLS cert file", []harness.Option{harness.Env("RECON_TLS_CERT_FILE", harness.ContainerTLS+"/missing.pem")}, "RECON_TLS_CERT_FILE"},
		{"missing TLS key file", []harness.Option{harness.Unset("RECON_TLS_KEY_FILE")}, "RECON_TLS_KEY_FILE"},
		{"memory limit above --memory", []harness.Option{harness.Memory("256m"), harness.Env("RECON_MEMORY_LIMIT_BYTES", "512MiB")}, "RECON_MEMORY_LIMIT_BYTES"},
		{"unwritable data dir", []harness.Option{harness.ReadOnlyDataDir()}, "RECON_DATA_DIR"},
		{"max buckets 0", []harness.Option{harness.Env("RECON_MAX_BUCKETS", "0")}, "RECON_MAX_BUCKETS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := harness.Start(t, append(tc.opts, harness.NoWait())...)
			st := c.Wait(60 * time.Second)
			if st.ExitCode != 78 {
				t.Errorf("exit code = %d, want 78", st.ExitCode)
			}
			var named bool
			for _, r := range c.Logs() {
				if strings.Contains(r.Raw, "config "+tc.wantVar+":") {
					named = true
				}
			}
			if !named {
				t.Errorf("no log line names %s", tc.wantVar)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	c := harness.Start(t, harness.Memory("512m"), harness.CPUs("2"))
	rec, ok := c.Log("effective limits")
	if !ok {
		t.Fatal("no \"effective limits\" record in the logs")
	}
	want := map[string]int64{
		"recon.memory_limit_bytes": (512 << 20) * 7 / 10,
		"recon.join_parallelism":   2,
		"recon.max_buckets":        4096,
	}
	for k, v := range want {
		if got, ok := rec.Int(k); !ok || got != v {
			t.Errorf("%s = %d (present %t), want %d", k, got, ok, v)
		}
	}
	if got, ok := rec.Int("recon.disk_limit_bytes"); !ok || got <= 0 {
		t.Errorf("recon.disk_limit_bytes = %d (present %t), want a positive default", got, ok)
	}
}
