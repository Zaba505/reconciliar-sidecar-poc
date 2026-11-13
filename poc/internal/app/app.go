// Package app wires the sidecar together on humus: its settings, telemetry,
// API server and health probes.
package app

import (
	"context"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/grpc/grpchealth"
	"github.com/z5labs/humus/grpc/grpcserver"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"google.golang.org/grpc"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/api"
	"poc/internal/mem"
	"poc/internal/run"
	"poc/internal/spill"
)

// loggerName is the instrumentation scope of the sidecar's own log records.
const loggerName = "poc/internal/app"

// Init reads the settings, wipes the data directory and returns the API
// server and the probes Service. A bad setting fails with a *config.Error
// naming its variable, so the process exits humus.ExitConfig.
func Init(ctx context.Context, s *humus.Setup) ([]humus.Service, error) {
	return initWith(ctx, s, cgroupMemoryMax)
}

// initWith is Init with the cgroup's memory.max at cgroupFile, so tests on
// a humustest Host can provide one.
func initWith(ctx context.Context, s *humus.Setup, cgroupFile string) ([]humus.Service, error) {
	cfg, err := readSettings(ctx, s, cgroupFile)
	if err != nil {
		return nil, err
	}
	if err := spill.Wipe(s.Host.FS, cfg.DataDir); err != nil {
		return nil, err
	}
	if cfg.DiskLimit == 0 {
		if cfg.DiskLimit, err = defaultDiskLimit(ctx, cfg.DataDir); err != nil {
			return nil, err
		}
	}

	log := otelslog.NewLogger(loggerName, otelslog.WithLoggerProvider(s.Telemetry.LoggerProvider))
	pool := mem.NewPool(cfg.MemoryLimit)
	runs := run.NewManager(s.Host.FS, pool, spill.NewDisk(s.Host.FS, cfg.DiskLimit), run.Config{
		DataDir:          cfg.DataDir,
		MaxRuns:          cfg.MaxRuns,
		MaxBuckets:       cfg.MaxBuckets,
		OutputBatchBytes: cfg.OutputBatchBytes,
		JoinParallelism:  cfg.JoinParallelism,
		MaxMatchBytes:    cfg.MaxMatchBytes,
		Logger:           log,
	})
	s.OnStop(runs.Close)

	svc, err := grpcserver.NewService(s, cfg.GRPC, grpc.MaxRecvMsgSize(cfg.MaxRecvMessageBytes))
	if err != nil {
		return nil, err
	}
	reconv1.RegisterReconcilerServer(svc, api.New(runs))

	probes, err := grpchealth.NewService(s, grpchealth.Settings{Addr: cfg.HealthAddr})
	if err != nil {
		return nil, err
	}

	log.InfoContext(ctx, "effective limits",
		"recon.memory_limit_bytes", cfg.MemoryLimit,
		"recon.disk_limit_bytes", cfg.DiskLimit,
		"recon.join_parallelism", cfg.JoinParallelism,
		"recon.max_buckets", cfg.MaxBuckets,
		"recon.max_runs", cfg.MaxRuns,
		"recon.max_recv_message_bytes", cfg.MaxRecvMessageBytes,
		"recon.output_batch_bytes", cfg.OutputBatchBytes,
		"recon.max_match_bytes", cfg.MaxMatchBytes,
	)
	return []humus.Service{svc, probes}, nil
}
