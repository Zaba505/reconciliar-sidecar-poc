package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"

	"github.com/z5labs/bedrock/config"
	"github.com/z5labs/humus"
	"github.com/z5labs/humus/grpc/grpchealth"
	"github.com/z5labs/humus/grpc/grpcserver"
	"golang.org/x/sys/unix"
)

// cgroupMemoryMax is the cgroup v2 file holding the container's memory limit.
const cgroupMemoryMax = "/sys/fs/cgroup/memory.max"

// Settings is the sidecar's configuration, read from the RECON_ variables.
type Settings struct {
	// GRPC is the API server's address (RECON_LISTEN) and TLS config
	// (RECON_TLS_CERT_FILE, RECON_TLS_KEY_FILE).
	GRPC grpcserver.Settings

	HealthAddr          string // RECON_HEALTH_ADDR
	DataDir             string // RECON_DATA_DIR
	MaxBuckets          uint32 // RECON_MAX_BUCKETS
	MemoryLimit         int64  // RECON_MEMORY_LIMIT_BYTES
	DiskLimit           int64  // RECON_DISK_LIMIT_BYTES; 0 until Init sets the default
	JoinParallelism     int    // RECON_JOIN_PARALLELISM
	MaxRuns             int    // RECON_MAX_RUNS
	MaxRecvMessageBytes int    // RECON_MAX_RECV_MESSAGE_BYTES
	OutputBatchBytes    int64  // RECON_OUTPUT_BATCH_BYTES
	MaxMatchBytes       int64  // RECON_MAX_MATCH_BYTES
	LogKeys             bool   // RECON_LOG_KEYS
}

// readSettings reads Settings from s's environment. Every bad value is
// reported in one error, each a *config.Error naming its variable, so Init
// fails with humus.ExitConfig. cgroupFile is the cgroup's memory.max, read
// through s.Host.FS for the default memory limit.
func readSettings(ctx context.Context, s *humus.Setup, cgroupFile string) (Settings, error) {
	env := func(name string) config.Value[string] { return config.Env(s.Process.Environment, name) }
	bytesVar := func(name string) config.Value[int64] { return env(name).Parse(parseBytes) }
	intVar := func(name string) config.Value[int] { return env(name).Parse(strconv.Atoi) }
	fsys := s.Host.FS

	cgroupLimit, hasCgroupLimit := readCgroupLimit(fsys, cgroupFile)

	return config.Struct(func(r *config.Resolver) Settings {
		return Settings{
			GRPC: grpcserver.Settings{
				Addr: r.Get(env("RECON_LISTEN").Default("127.0.0.1:7443").Validate(grpcserver.CheckAddr)),
				TLS: r.Get(env("RECON_TLS_CERT_FILE").Parse(readFile(fsys)).FlatMap(func(cert []byte) config.Value[*tls.Config] {
					return env("RECON_TLS_KEY_FILE").Parse(readFile(fsys)).Parse(keyPair(cert))
				}).Validate(grpcserver.CheckTLS)),
			},
			HealthAddr: r.Get(env("RECON_HEALTH_ADDR").Default(":8081").Validate(grpchealth.CheckAddr)),
			DataDir:    r.Get(env("RECON_DATA_DIR").Default("/var/lib/recon").Validate(writableDir(fsys))),
			MaxBuckets: r.Get(env("RECON_MAX_BUCKETS").Parse(parseUint32).Default(4096).Validate(atLeastOne)),
			MemoryLimit: r.Get(bytesVar("RECON_MEMORY_LIMIT_BYTES").
				Or(config.Func("RECON_MEMORY_LIMIT_BYTES", func(context.Context) (int64, error) {
					if !hasCgroupLimit {
						return 0, errors.New("required: the container has no cgroup memory limit to default from")
					}
					return cgroupLimit * 7 / 10, nil
				})).
				Validate(atLeastOne).
				Validate(func(n int64) error {
					if hasCgroupLimit && n > cgroupLimit {
						return fmt.Errorf("%d exceeds the cgroup memory limit of %d", n, cgroupLimit)
					}
					return nil
				})),
			DiskLimit:           r.Get(bytesVar("RECON_DISK_LIMIT_BYTES").Validate(atLeastOne).Default(0)),
			JoinParallelism:     r.Get(intVar("RECON_JOIN_PARALLELISM").Default(min(runtime.GOMAXPROCS(0), 4)).Validate(atLeastOne)),
			MaxRuns:             r.Get(intVar("RECON_MAX_RUNS").Default(8).Validate(atLeastOne)),
			MaxRecvMessageBytes: r.Get(bytesVar("RECON_MAX_RECV_MESSAGE_BYTES").Parse(toInt).Default(16 << 20).Validate(atLeastOne)),
			OutputBatchBytes:    r.Get(bytesVar("RECON_OUTPUT_BATCH_BYTES").Default(1 << 20).Validate(atLeastOne)),
			MaxMatchBytes:       r.Get(bytesVar("RECON_MAX_MATCH_BYTES").Default(3 << 20).Validate(atLeastOne)),
			LogKeys:             r.Get(env("RECON_LOG_KEYS").Parse(strconv.ParseBool).Default(false)),
		}
	}).Read(ctx)
}

// readCgroupLimit returns the memory limit in the cgroup's memory.max, and
// false when the cgroup sets none ("max") or the file can't be read, as on
// a host without cgroup v2. RECON_MEMORY_LIMIT_BYTES is then required.
func readCgroupLimit(fsys humus.FS, name string) (int64, bool) {
	b, err := readFile(fsys)(name)
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// readFile returns a parser that reads the whole file a variable names.
func readFile(fsys humus.FS) func(name string) ([]byte, error) {
	return func(name string) ([]byte, error) {
		f, err := fsys.Open(name)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		return io.ReadAll(f)
	}
}

// keyPair returns a parser that builds a server TLS config from cert and
// the PEM key it is given.
func keyPair(cert []byte) func(key []byte) (*tls.Config, error) {
	return func(key []byte) (*tls.Config, error) {
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}, nil
	}
}

// writableDir returns a check that dir is a directory the sidecar can
// create files in, by creating and removing a probe file.
func writableDir(fsys humus.FS) func(dir string) error {
	return func(dir string) error {
		f, err := fsys.CreateTemp(dir, ".write-probe-*")
		if err != nil {
			return fmt.Errorf("not writable: %w", err)
		}
		name := f.Name()
		if err := f.Close(); err != nil {
			return fmt.Errorf("not writable: %w", err)
		}
		if err := fsys.Remove(name); err != nil {
			return fmt.Errorf("not writable: %w", err)
		}
		return nil
	}
}

// defaultDiskLimit reads RECON_DISK_LIMIT_BYTES's default: 90% of the space
// available to unprivileged users on the filesystem holding dir. Init calls
// it after the wipe, so the spill files a restarted sidecar left behind don't
// shrink it. humus.FS has no Statfs, so it reads the real filesystem.
func defaultDiskLimit(ctx context.Context, dir string) (int64, error) {
	return config.Func("RECON_DISK_LIMIT_BYTES", func(context.Context) (int64, error) {
		var st unix.Statfs_t
		if err := unix.Statfs(dir, &st); err != nil {
			return 0, fmt.Errorf("no default: %w", err)
		}
		return int64(float64(st.Bavail) * float64(st.Bsize) * 0.9), nil
	}).Validate(atLeastOne).Read(ctx)
}

func parseUint32(s string) (uint32, error) {
	n, err := strconv.ParseUint(s, 10, 32)
	return uint32(n), err
}

func toInt(n int64) (int, error) {
	if int64(int(n)) != n {
		return 0, fmt.Errorf("%d is too large", n)
	}
	return int(n), nil
}

func atLeastOne[T int | int64 | uint32](n T) error {
	if n < 1 {
		return fmt.Errorf("%d is below the minimum of 1", n)
	}
	return nil
}
