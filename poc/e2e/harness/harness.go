// Package harness runs the recon-sidecar image in podman or docker for the
// e2e tests, through the engine's CLI.
//
// A test package calls Main from its TestMain, and each test starts its own
// container with Start:
//
//	func TestMain(m *testing.M) { os.Exit(harness.Main(m)) }
//
//	func TestSomething(t *testing.T) {
//		c := harness.Start(t, harness.Memory("512m"))
//		_, err := c.Client.CreateRun(ctx, req)
//		...
//	}
package harness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	reconv1 "poc/gen/recon/v1"
)

// The ports and data directory of the image.
const (
	apiPort          = "7443"
	healthPort       = "8081"
	containerDataDir = "/var/lib/recon"
	hostIP           = "127.0.0.1"
	readyTimeout     = 60 * time.Second
	samplePeriod     = 500 * time.Millisecond
	defaultMemory    = "1g"
	startAttempts    = 5
	// dataRoot holds the containers' data directories. It must not be a
	// tmpfs, whose pages are charged to the container's memory cgroup
	// (§6.3), and /tmp often is.
	dataRoot = "/var/tmp"
)

// env is what Main sets up for every test in the package.
var env struct {
	engine   string
	image    string
	rootless bool
	certs    *Certs
	data     string // the parent of every data directory
}

// Main sets up the harness, runs the tests and cleans up, returning the
// exit code for os.Exit. It reads the engine from RECON_E2E_ENGINE (podman
// or docker by default) and the image from RECON_E2E_IMAGE
// (recon-sidecar:dev by default), and generates the TLS certificates every
// container is given.
func Main(m *testing.M) int {
	env.engine = os.Getenv("RECON_E2E_ENGINE")
	if env.engine == "" {
		for _, name := range []string{"podman", "docker"} {
			if p, err := exec.LookPath(name); err == nil {
				env.engine = p
				break
			}
		}
	}
	if env.engine == "" {
		fmt.Fprintln(os.Stderr, "harness: neither podman nor docker found; set RECON_E2E_ENGINE")
		return 1
	}
	env.image = os.Getenv("RECON_E2E_IMAGE")
	if env.image == "" {
		env.image = "recon-sidecar:dev"
	}
	if out, err := engine(context.Background(), "image", "inspect", env.image); err != nil {
		fmt.Fprintf(os.Stderr, "harness: image %s not found (run make image): %v\n%s", env.image, err, out)
		return 1
	}
	out, _ := engine(context.Background(), "info", "--format", "{{.Host.Security.Rootless}}")
	env.rootless = strings.TrimSpace(string(out)) == "true"

	dir, err := os.MkdirTemp("", "recon-e2e-tls-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if env.certs, err = WriteCerts(dir); err != nil {
		fmt.Fprintln(os.Stderr, "harness:", err)
		return 1
	}
	if env.data, err = os.MkdirTemp(dataRoot, "recon-e2e-data-"); err != nil {
		fmt.Fprintln(os.Stderr, "harness:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(env.data) }()
	var st unix.Statfs_t
	if err := unix.Statfs(env.data, &st); err != nil || st.Type == unix.TMPFS_MAGIC {
		fmt.Fprintf(os.Stderr, "harness: %s is a tmpfs or can't be checked (%v); data directories must be on a disk\n", dataRoot, err)
		return 1
	}
	return m.Run()
}

// engine runs the container engine with args and returns its combined
// output.
func engine(ctx context.Context, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, env.engine, args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", env.engine, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return out, nil
}

// Option configures a container started by Start.
type Option func(*config)

type config struct {
	env          map[string]string
	memory       string
	cpus         string
	readOnlyData bool
	noWait       bool
}

// Env sets an environment variable in the container.
func Env(name, value string) Option {
	return func(c *config) { c.env[name] = value }
}

// Unset removes one of the harness's default variables, such as
// RECON_TLS_KEY_FILE.
func Unset(name string) Option {
	return func(c *config) { delete(c.env, name) }
}

// Memory sets the container's --memory limit, such as "512m". The default
// is 1g. The container gets no swap, as in a Kubernetes pod: the engines
// otherwise allow as much swap again, and a container that should have
// been OOM-killed swaps instead.
func Memory(limit string) Option {
	return func(c *config) { c.memory = limit }
}

// CPUs sets the container's --cpus limit.
func CPUs(n string) Option {
	return func(c *config) { c.cpus = n }
}

// ReadOnlyDataDir mounts the data directory read-only.
func ReadOnlyDataDir() Option {
	return func(c *config) { c.readOnlyData = true }
}

// NoWait returns from Start as soon as the container is created, without
// waiting for it to turn ready or dialling it, for a container expected to
// exit.
func NoWait() Option {
	return func(c *config) { c.noWait = true }
}

// Container is a running sidecar container. Its methods are not safe for
// concurrent use, except that the memory sampler runs alongside them.
type Container struct {
	t       testing.TB
	ID      string
	DataDir string // bind-mounted on the container's data directory

	APIAddr    string // 127.0.0.1:port of the TLS API
	HealthAddr string // 127.0.0.1:port of the plaintext health service

	Conn   *grpc.ClientConn // TLS connection to the API
	Client reconv1.ReconcilerClient
	Health healthpb.HealthClient

	healthConn *grpc.ClientConn

	mu      sync.Mutex
	peakMem int64
}

// Start runs the image with opts and waits until its health service
// answers SERVING, then dials the API over TLS. The container is removed
// when the test ends, and its logs are dumped if the test failed.
func Start(t testing.TB, opts ...Option) *Container {
	t.Helper()
	if env.certs == nil {
		t.Fatal("harness: Start called without harness.Main in TestMain")
	}
	cfg := &config{
		env: map[string]string{
			"RECON_TLS_CERT_FILE": ContainerTLS + "/" + CertFile,
			"RECON_TLS_KEY_FILE":  ContainerTLS + "/" + KeyFile,
		},
		memory: defaultMemory,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	dataDir, err := os.MkdirTemp(env.data, "data-")
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	// Registered first, so that it runs after the container is removed.
	t.Cleanup(func() {
		if err := os.RemoveAll(dataDir); err != nil {
			t.Logf("harness: %v", err)
		}
	})
	c := &Container{t: t, DataDir: dataDir}
	dataMount := c.DataDir + ":" + containerDataDir + ":Z"
	if cfg.readOnlyData {
		dataMount = c.DataDir + ":" + containerDataDir + ":ro,Z"
	}
	args := []string{
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--memory", cfg.memory,
		"--memory-swap", cfg.memory,
		"-v", env.certs.Dir + ":" + ContainerTLS + ":ro,z",
		"-v", dataMount,
	}
	if env.rootless {
		args = append(args, "--userns=keep-id")
	}
	if cfg.cpus != "" {
		args = append(args, "--cpus", cfg.cpus)
	}
	for _, k := range slices.Sorted(maps.Keys(cfg.env)) {
		args = append(args, "-e", k+"="+cfg.env[k])
	}
	args = append(args, env.image)

	var hostPorts [2]int
	c.ID, hostPorts = run(t, args)
	c.APIAddr = net.JoinHostPort(hostIP, strconv.Itoa(hostPorts[0]))
	c.HealthAddr = net.JoinHostPort(hostIP, strconv.Itoa(hostPorts[1]))

	// Cleanups run last in, first out: the logs are dumped and the clients
	// closed before the container is removed. The host ports stay held until
	// then, so a Restart can bind them again.
	t.Cleanup(func() {
		if _, err := engine(context.Background(), "rm", "-f", c.ID); err != nil {
			t.Logf("harness: %v", err)
		}
		releasePorts(hostPorts[:]...)
	})
	stopSampler := c.sample()
	t.Cleanup(func() {
		stopSampler()
		c.closeClients()
		t.Logf("harness: container %.12s peak memory %d MiB (from %s stats; includes page cache)", c.ID, c.PeakMemory()>>20, env.engine)
		if t.Failed() {
			t.Logf("harness: container %.12s logs:\n%s", c.ID, c.rawLogs())
		}
	})

	if !cfg.noWait {
		c.connect()
	}
	return c
}

// run runs a container with args, publishing the API and health ports on
// host ports from reservePorts, and returns its ID and those host ports,
// which the caller releases once the container is removed. If another
// process binds a reserved port before the engine does, run tries again
// with new ports. A failed run can leave the container created, so it is
// removed by name.
func run(t testing.TB, args []string) (string, [2]int) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		var hostPorts [2]int
		if err := reservePorts(hostPorts[:]); err != nil {
			t.Fatalf("harness: %v", err)
		}
		name := "recon-e2e-" + strings.ToLower(rand.Text()[:12])
		runArgs := append([]string{"run", "-d", "--name", name,
			"-p", fmt.Sprintf("%s:%d:%s", hostIP, hostPorts[0], apiPort),
			"-p", fmt.Sprintf("%s:%d:%s", hostIP, hostPorts[1], healthPort),
		}, args...)
		out, err := engine(t.Context(), runArgs...)
		if err == nil {
			return strings.TrimSpace(lastLine(out)), hostPorts
		}
		_, _ = engine(context.Background(), "rm", "-f", name)
		releasePorts(hostPorts[:]...)
		if attempt < startAttempts && portTaken(out) {
			t.Logf("harness: start container (attempt %d): %v", attempt, err)
			continue
		}
		t.Fatalf("harness: start container: %v", err)
	}
}

// portTaken reports whether a failed run's output says a host port was
// already bound: podman's "Address already in use" or docker's "port is
// already allocated".
func portTaken(out []byte) bool {
	return bytes.Contains(out, []byte("already in use")) || bytes.Contains(out, []byte("already allocated"))
}

func lastLine(out []byte) string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return lines[len(lines)-1]
}

// connect waits for readiness and dials the API.
func (c *Container) connect() {
	c.t.Helper()
	var err error
	c.healthConn, err = grpc.NewClient("passthrough:///"+c.HealthAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("harness: dial health: %v", err)
	}
	c.Health = healthpb.NewHealthClient(c.healthConn)
	c.waitReady()

	c.Conn, err = grpc.NewClient("passthrough:///"+c.APIAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: env.certs.Pool, ServerName: "localhost"})))
	if err != nil {
		c.t.Fatalf("harness: dial API: %v", err)
	}
	c.Client = reconv1.NewReconcilerClient(c.Conn)
}

func (c *Container) closeClients() {
	for _, conn := range []*grpc.ClientConn{c.Conn, c.healthConn} {
		if conn != nil {
			_ = conn.Close()
		}
	}
	c.Conn, c.healthConn = nil, nil
}

// waitReady polls grpc.health.v1.Health/Check, as a startup probe does,
// until it answers SERVING.
func (c *Container) waitReady() {
	c.t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		ctx, cancel := context.WithTimeout(c.t.Context(), time.Second)
		resp, err := c.Health.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return
		}
		if st := c.State(); !st.Running {
			c.t.Fatalf("harness: container exited with %d before turning ready:\n%s", st.ExitCode, c.rawLogs())
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("harness: container not ready after %s (last: %v, %v):\n%s", readyTimeout, resp.GetStatus(), err, c.rawLogs())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// State is a container's state, from the engine's inspect.
type State struct {
	Running   bool
	OOMKilled bool
	ExitCode  int
}

// State returns the container's current state.
func (c *Container) State() State {
	c.t.Helper()
	out, err := engine(context.Background(), "inspect", "--format", "{{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}", c.ID)
	if err != nil {
		c.t.Fatalf("harness: %v", err)
	}
	var st State
	if _, err := fmt.Sscan(lastLine(out), &st.Running, &st.OOMKilled, &st.ExitCode); err != nil {
		c.t.Fatalf("harness: parse inspect output %q: %v", out, err)
	}
	return st
}

// Wait waits up to timeout for the container to exit and returns its
// state.
func (c *Container) Wait(timeout time.Duration) State {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), timeout)
	defer cancel()
	if _, err := engine(ctx, "wait", c.ID); err != nil {
		c.t.Fatalf("harness: container did not exit within %s: %v", timeout, err)
	}
	return c.State()
}

// Stop sends SIGTERM, waits up to timeout for the container to exit, after
// which the engine sends SIGKILL, and returns its exit code.
func (c *Container) Stop(timeout time.Duration) int {
	c.t.Helper()
	secs := strconv.Itoa(int(timeout.Round(time.Second) / time.Second))
	if _, err := engine(context.Background(), "stop", "-t", secs, c.ID); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
	return c.State().ExitCode
}

// Kill sends SIGKILL and waits for the container to exit.
func (c *Container) Kill() {
	c.t.Helper()
	if _, err := engine(context.Background(), "kill", c.ID); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
	if _, err := engine(context.Background(), "wait", c.ID); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
}

// Restart starts the stopped container again, waits for it to turn ready
// and dials it again. The engine binds the same host ports as before, which
// the harness has kept for this container. Conn, Client and Health are
// replaced.
func (c *Container) Restart() {
	c.t.Helper()
	c.closeClients()
	if _, err := engine(c.t.Context(), "start", c.ID); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
	c.connect()
}

// LogRecord is one line of the container's output. Lines that aren't JSON
// have only Raw set.
type LogRecord struct {
	Raw        string
	Severity   string
	Body       string
	Scope      string
	Attributes map[string]any
}

// Int returns the integer attribute key.
func (r LogRecord) Int(key string) (int64, bool) {
	n, ok := r.Attributes[key].(json.Number)
	if !ok {
		return 0, false
	}
	v, err := n.Int64()
	return v, err == nil
}

// Logs returns the container's output, stdout and stderr, one record per
// line, parsed from humus's JSON log format.
func (c *Container) Logs() []LogRecord {
	c.t.Helper()
	var recs []LogRecord
	sc := bufio.NewScanner(strings.NewReader(c.rawLogs()))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		rec := LogRecord{Raw: line}
		var m struct {
			Severity   string         `json:"severity_text"`
			Body       any            `json:"body"`
			Scope      string         `json:"scope"`
			Attributes map[string]any `json:"attributes"`
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&m); err == nil {
			rec.Severity, rec.Scope, rec.Attributes = m.Severity, m.Scope, m.Attributes
			rec.Body, _ = m.Body.(string)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		c.t.Fatalf("harness: read logs: %v", err)
	}
	return recs
}

// Log returns the first record whose body is body.
func (c *Container) Log(body string) (LogRecord, bool) {
	c.t.Helper()
	for _, r := range c.Logs() {
		if r.Body == body {
			return r, true
		}
	}
	return LogRecord{}, false
}

func (c *Container) rawLogs() string {
	out, err := engine(context.Background(), "logs", c.ID)
	if err != nil {
		return fmt.Sprintf("(no logs: %v)", err)
	}
	return string(out)
}

// PeakMemory returns the highest memory use the sampler has seen, in bytes.
// It includes reclaimable page cache, so it is for information only.
func (c *Container) PeakMemory() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peakMem
}

// sample polls the engine's stats in the background until the returned
// function is called.
func (c *Container) sample() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(samplePeriod)
		defer tick.Stop()
		for {
			out, err := exec.CommandContext(ctx, env.engine, "stats", "--no-stream", "--format", "{{.MemUsage}}", c.ID).Output()
			if err == nil {
				if n, err := parseMemUsage(string(out)); err == nil {
					c.mu.Lock()
					c.peakMem = max(c.peakMem, n)
					c.mu.Unlock()
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// memUnits are the units of podman's (SI) and docker's (IEC) stats.
var memUnits = map[string]float64{
	"B": 1, "kB": 1e3, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
	"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
}

// parseMemUsage parses the usage in a MemUsage such as "729.1kB / 268.4MB".
func parseMemUsage(s string) (int64, error) {
	usage := strings.TrimSpace(strings.SplitN(strings.TrimSpace(s), "/", 2)[0])
	i := strings.IndexFunc(usage, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	if i <= 0 {
		return 0, fmt.Errorf("parse mem usage %q", s)
	}
	n, err := strconv.ParseFloat(usage[:i], 64)
	if err != nil {
		return 0, err
	}
	unit, ok := memUnits[strings.TrimSpace(usage[i:])]
	if !ok {
		return 0, errors.New("unknown unit in " + strconv.Quote(s))
	}
	return int64(n * unit), nil
}
