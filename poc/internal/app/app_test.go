package app

import (
	"context"
	"os"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/grpc/grpcserver/grpcservertest"
	"github.com/z5labs/humus/humustest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	reconv1 "poc/gen/recon/v1"
)

func initForTest(cgroup string) func(context.Context, *humus.Setup) ([]humus.Service, error) {
	return func(ctx context.Context, s *humus.Setup) ([]humus.Service, error) {
		return initWith(ctx, s, cgroup)
	}
}

func TestInit_serves_probes_and_the_API(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEnv(t)
		if err := os.WriteFile(e.host.Path("data/stale.rec"), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		tel := humustest.NewTelemetry()
		p, exit := humustest.Start(t, initForTest(e.cgroup),
			humustest.WithApp(humus.WithHost(e.host.Host()), humus.WithTelemetry(tel.Builder())),
			humustest.WithEnv(e.with(map[string]string{"RECON_LISTEN": ":7443"})),
		)
		synctest.Wait()

		health := healthpb.NewHealthClient(grpcservertest.NewClient(t, e.host, "localhost:8081", nil,
			grpc.WithTransportCredentials(insecure.NewCredentials())))
		resp, err := health.Check(t.Context(), &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Errorf("health = %v, want SERVING", resp.GetStatus())
		}

		entries, err := os.ReadDir(e.host.Path("data"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("data dir holds %d entries after startup, want it wiped", len(entries))
		}

		api := reconv1.NewReconcilerClient(grpcservertest.NewClient(t, e.host, "localhost:7443", e.ca.Pool()))
		if _, err := api.CreateRun(t.Context(), &reconv1.CreateRunRequest{BucketCount: 1}); err != nil {
			t.Errorf("CreateRun over TLS: %v", err)
		}

		p.Signal(syscall.SIGTERM)
		if st := <-exit; st.Code() != 0 {
			t.Fatalf("exit %d: %v", st.Code(), st.Cause())
		}

		var found bool
		for _, r := range tel.Logs() {
			if r.Body().AsString() != "effective limits" {
				continue
			}
			found = true
			if r.Severity() != log.SeverityInfo {
				t.Errorf("severity = %v, want INFO", r.Severity())
			}
			attrs := map[string]int64{}
			r.WalkAttributes(func(kv attribute.KeyValue) bool {
				attrs[string(kv.Key)] = kv.Value.AsInt64()
				return true
			})
			want := map[string]int64{
				"recon.memory_limit_bytes": testCgroupLimit * 7 / 10,
				"recon.disk_limit_bytes":   1 << 30,
				"recon.max_buckets":        4096,
			}
			for k, v := range want {
				if attrs[k] != v {
					t.Errorf("%s = %d, want %d", k, attrs[k], v)
				}
			}
			if attrs["recon.join_parallelism"] < 1 {
				t.Errorf("recon.join_parallelism = %d, want at least 1", attrs["recon.join_parallelism"])
			}
		}
		if !found {
			t.Error("no \"effective limits\" record was logged")
		}
	})
}

func TestInit_a_bad_setting_exits_78(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEnv(t)
		_, exit := humustest.Start(t, initForTest(e.cgroup),
			humustest.WithApp(humus.WithHost(e.host.Host()), humus.WithTelemetry(humustest.NewTelemetry().Builder())),
			humustest.WithEnv(e.with(map[string]string{"RECON_MAX_BUCKETS": "0"})),
		)
		st := <-exit
		if st.Code() != humus.ExitConfig {
			t.Fatalf("exit %d, want %d: %v", st.Code(), humus.ExitConfig, st.Cause())
		}
	})
}
