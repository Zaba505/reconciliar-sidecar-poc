// Package run manages runs: their IDs, states and limits, their buffers,
// and the operations of each state (§5.2, §6.2).
//
// A run is OPEN until it is sealed (SEALED) or an ingest stream that had
// records accepted fails (FAILED). The state is guarded by a per-run
// mutex. Deleting a run cancels the contexts of its open streams with
// ErrAborted, so that they end.
//
// The Manager makes the pool spill the buffers of every run, to files at
// runs/<run_id>/<side>/<bucket>/<seq>.rec in the data directory, with
// "empty" for the bucket of the empty-key partition. A Join writes the
// sub-partitions of oversized buckets under runs/<run_id>/join-<n>/ and
// removes them before it ends.
package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"runtime/metrics"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/z5labs/humus"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/spill"
)

// Error is a failure with a reason of the API's (§6.4).
type Error struct {
	Reason reconv1.ErrorReason
	Detail string
}

func (e *Error) Error() string { return e.Detail }

func errorf(reason reconv1.ErrorReason, format string, args ...any) *Error {
	return &Error{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// ErrAborted ends the open streams of a run that was deleted.
var ErrAborted = errors.New("the run was deleted")

// ParseID returns the run ID id holds, or an INVALID_REQUEST Error unless
// it is a UUIDv7 in canonical lowercase form.
func ParseID(id *reconv1.RunId) (string, error) {
	s := id.GetValue()
	u, err := uuid.Parse(s)
	if err != nil || u.Version() != 7 || u.Variant() != uuid.RFC4122 || u.String() != s {
		return "", errorf(reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST, "run_id is not a UUIDv7 in canonical lowercase form")
	}
	return s, nil
}

// Config is the Manager's share of the settings.
type Config struct {
	DataDir          string
	MaxRuns          int
	MaxBuckets       uint32
	OutputBatchBytes int64
	JoinParallelism  int
	MaxMatchBytes    int64        // 0 means no limit
	Logger           *slog.Logger // nil discards
}

// Manager holds every run. It is safe for concurrent use.
type Manager struct {
	fsys humus.FS
	pool *mem.Pool
	disk *spill.Disk
	cfg  Config
	log  *slog.Logger

	mu   sync.Mutex
	runs map[string]*Run
}

// NewManager returns a Manager whose runs reserve memory from pool and
// keep their files in cfg.DataDir/runs on fsys, spilled through disk. It
// makes pool spill the runs' buffers.
func NewManager(fsys humus.FS, pool *mem.Pool, disk *spill.Disk, cfg Config) *Manager {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := &Manager{fsys: fsys, pool: pool, disk: disk, cfg: cfg, log: log, runs: map[string]*Run{}}
	pool.SetSource(m.spillable)
	return m
}

// spillable returns every buffer of every run not being deleted.
func (m *Manager) spillable() []mem.Spillable {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []mem.Spillable
	for _, r := range m.runs {
		if r.isDeleted() {
			continue
		}
		for i := range r.targets {
			out = append(out, &r.targets[i])
		}
	}
	return out
}

// Create creates an OPEN run with buckets buckets per side and returns its
// ID.
func (m *Manager) Create(buckets uint32) (string, error) {
	if buckets < 1 || buckets > m.cfg.MaxBuckets {
		return "", errorf(reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST, "bucket_count %d is outside 1 to %d", buckets, m.cfg.MaxBuckets)
	}
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	id := u.String()
	r := &Run{
		m:       m,
		id:      id,
		dir:     filepath.Join(m.cfg.DataDir, "runs", id),
		parts:   buffer.NewPartitions(buckets),
		cancels: map[int]context.CancelCauseFunc{},
	}
	for s := range 2 {
		side := buffer.Side(s)
		for i := range buckets {
			r.targets = append(r.targets, target{r, side, strconv.FormatUint(uint64(i), 10), r.parts.Bucket(side, i)})
		}
		r.targets = append(r.targets, target{r, side, "empty", r.parts.Empty(side)})
	}

	m.mu.Lock()
	if len(m.runs) >= m.cfg.MaxRuns {
		m.mu.Unlock()
		return "", errorf(reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS, "%d runs exist, the most RECON_MAX_RUNS allows", len(m.runs))
	}
	m.runs[id] = r
	m.mu.Unlock()

	if err := m.fsys.MkdirAll(r.dir, 0o700); err != nil {
		_ = m.Delete(id)
		return "", fmt.Errorf("create run directory: %w", err)
	}
	return id, nil
}

// Get returns the run with id, which ParseID returned, or a RUN_NOT_FOUND
// Error.
func (m *Manager) Get(id string) (*Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return nil, errNotFound()
	}
	return r, nil
}

func errNotFound() *Error {
	return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND, "no such run")
}

// Delete deletes the run with id, if there is one: it aborts the run's
// streams, frees its memory and removes its files. A Delete of a run that
// is already being deleted returns once that deletion is done.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	r := m.runs[id]
	m.mu.Unlock()
	if r == nil {
		return nil
	}
	// The run stays listed until its files are gone, so that a concurrent
	// Delete finds it and waits.
	err := r.delete()
	m.mu.Lock()
	if m.runs[id] == r {
		delete(m.runs, id)
	}
	m.mu.Unlock()
	return err
}

// Close deletes every run. It is the App's OnStop function.
func (m *Manager) Close(context.Context) error {
	m.mu.Lock()
	ids := slices.Collect(maps.Keys(m.runs))
	m.mu.Unlock()
	var errs []error
	for _, id := range ids {
		errs = append(errs, m.Delete(id))
	}
	return errors.Join(errs...)
}

// State is a run's state.
type State int

const (
	Open State = iota
	Sealed
	Failed
)

// sideTotals counts one side's records.
type sideTotals struct {
	records, empty uint64
}

// Run is one run. It is safe for concurrent use.
type Run struct {
	m       *Manager
	id      string
	dir     string
	parts   *buffer.Partitions
	targets []target // one per buffer

	// files is held for reading while a buffer is spilled, and for writing
	// while delete removes the run's directory, so that no spill creates
	// files in it again.
	files     sync.RWMutex
	diskBytes atomic.Int64 // of the run's spill files

	deleteOnce sync.Once
	deleteErr  error

	mu         sync.Mutex
	state      State
	deleted    bool
	cancels    map[int]context.CancelCauseFunc // of the contexts from WithContext
	nextCancel int
	streams    int // open ingest streams
	committed  uint32
	totals     [2]sideTotals
	sealed     *reconv1.SealRunResponse
	joining    bool
}

// WithContext returns a context derived from parent that is also
// cancelled, with cause ErrAborted, when the run is deleted. The run's
// deletion cancels it before returning.
func (r *Run) WithContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleted {
		cancel(ErrAborted)
		return ctx, func() { cancel(nil) }
	}
	key := r.nextCancel
	r.nextCancel++
	r.cancels[key] = cancel
	return ctx, func() {
		r.mu.Lock()
		delete(r.cancels, key)
		r.mu.Unlock()
		cancel(nil)
	}
}

// stateErr returns the Error for an operation that needs the run to be in
// want, or nil if it is. r.mu must be held.
func (r *Run) stateErr(want State) error {
	switch {
	case r.deleted:
		return errNotFound()
	case r.state == want:
		return nil
	case r.state == Failed:
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_FAILED, "the run failed; delete it and rerun")
	case r.state == Open:
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE, "the run is not sealed")
	default:
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE, "the run is sealed")
	}
}

// delete deletes the run, once. Concurrent calls return when it is done.
func (r *Run) delete() error {
	r.deleteOnce.Do(func() { r.deleteErr = r.remove() })
	return r.deleteErr
}

func (r *Run) remove() error {
	r.mu.Lock()
	r.deleted = true
	for _, cancel := range r.cancels {
		cancel(ErrAborted)
	}
	joining := r.joining
	r.mu.Unlock()

	r.files.Lock()
	defer r.files.Unlock()
	// A Join still reads the buffers until it sees the abort, so it frees
	// them when it ends. Otherwise the pool would credit memory the Join
	// still holds.
	if !joining {
		r.parts.Free(r.m.pool)
	}
	if err := r.m.fsys.RemoveAll(r.dir); err != nil {
		return fmt.Errorf("remove run directory: %w", err)
	}
	r.m.disk.Release(r.diskBytes.Swap(0))
	return nil
}

func (r *Run) isDeleted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deleted
}

// target is a buffer of a run, as the pool spills it.
type target struct {
	r      *Run
	side   buffer.Side
	bucket string
	b      *buffer.Buffer
}

func (t *target) SpillSize() int64 { return t.b.SpillSize() }

func (t *target) Spill() error {
	r := t.r
	r.files.RLock()
	defer r.files.RUnlock()
	if r.isDeleted() {
		return nil
	}
	return t.b.Spill(r.m.pool, func(seq int, records int64, chunks [][]byte) (spill.File, error) {
		path := filepath.Join(r.dir, t.side.String(), t.bucket, strconv.Itoa(seq)+".rec")
		f, err := r.m.disk.Write(path, records, chunks)
		if err == nil {
			r.diskBytes.Add(f.Bytes)
		}
		return f, err
	})
}

// scratch is where the run's Joins write sub-partitions.
type scratch struct{ r *Run }

// Write writes a file under the run's directory, unless the run was
// deleted: like a spill, it must not create the directory again.
func (s scratch) Write(path string, records int64, chunks [][]byte) (spill.File, error) {
	r := s.r
	r.files.RLock()
	defer r.files.RUnlock()
	if r.isDeleted() {
		return spill.File{}, ErrAborted
	}
	return r.m.disk.Write(filepath.Join(r.dir, path), records, chunks)
}

// Remove removes a directory under the run's directory. If it fails, the
// run's deletion gives back the bytes once it removes the files.
func (s scratch) Remove(dir string, bytes int64) error {
	r := s.r
	r.files.RLock()
	defer r.files.RUnlock()
	if err := r.m.fsys.RemoveAll(filepath.Join(r.dir, dir)); err != nil {
		r.diskBytes.Add(bytes)
		return err
	}
	r.m.disk.Release(bytes)
	return nil
}

// Ingest is one open ingest stream of a run.
type Ingest struct {
	r        *Run
	totals   [2]sideTotals
	accepted bool // a record was appended
	failRun  bool // the disk limit was hit
	done     bool
}

// BeginIngest opens an ingest stream on an OPEN run. The caller must call
// Commit or Abandon.
func (r *Run) BeginIngest() (*Ingest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.stateErr(Open); err != nil {
		return nil, err
	}
	r.streams++
	return &Ingest{r: r}, nil
}

// Append appends one message's records to side. size is the decoded
// message's size, which is reserved while its records are copied.
func (in *Ingest) Append(side buffer.Side, records []*reconv1.Record, size int64) error {
	r := in.r
	r.mu.Lock()
	err := r.stateErr(Open)
	deleted := r.deleted
	r.mu.Unlock()
	if deleted {
		return ErrAborted
	}
	if err != nil {
		return err
	}

	pool := r.m.pool
	if err := pool.Reserve(size); err != nil {
		in.failOnDisk(err)
		return err
	}
	defer pool.Release(size)
	for _, rec := range records {
		if err := r.parts.Append(pool, side, rec.GetKey(), rec.GetValue()); err != nil {
			if errors.Is(err, buffer.ErrFreed) {
				return ErrAborted
			}
			in.failOnDisk(err)
			return err
		}
		in.accepted = true
		in.totals[side].records++
		if len(rec.GetKey()) == 0 {
			in.totals[side].empty++
		}
	}
	return nil
}

// failOnDisk makes Abandon fail the run if err is the disk limit's.
func (in *Ingest) failOnDisk(err error) {
	if errors.Is(err, spill.ErrDiskExhausted) {
		in.failRun = true
	}
}

// Commit adds the stream's counts to the run's totals and closes it.
func (in *Ingest) Commit() error {
	r := in.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleted {
		return ErrAborted
	}
	if err := r.stateErr(Open); err != nil {
		return err
	}
	for s := range r.totals {
		r.totals[s].records += in.totals[s].records
		r.totals[s].empty += in.totals[s].empty
	}
	r.committed++
	r.streams--
	in.done = true
	return nil
}

// Abandon closes a stream that didn't commit. If it had records accepted,
// or hit the disk limit, the run fails (§8.3, §10). Abandon after Commit
// does nothing.
func (in *Ingest) Abandon() {
	if in.done {
		return
	}
	in.done = true
	r := in.r
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streams--
	if (in.accepted || in.failRun) && r.state == Open {
		r.state = Failed
	}
}

// Seal seals an OPEN run and returns its totals. Sealing a SEALED run
// returns the same totals.
func (r *Run) Seal() (*reconv1.SealRunResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Sealed && !r.deleted {
		return r.sealed, nil
	}
	if err := r.stateErr(Open); err != nil {
		return nil, err
	}
	if r.streams > 0 {
		return nil, errorf(reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS, "%d ingest streams are open", r.streams)
	}
	r.state = Sealed
	r.sealed = &reconv1.SealRunResponse{
		Left:             &reconv1.SideTotals{Records: r.totals[buffer.Left].records, EmptyKeyRecords: r.totals[buffer.Left].empty},
		Right:            &reconv1.SideTotals{Records: r.totals[buffer.Right].records, EmptyKeyRecords: r.totals[buffer.Right].empty},
		CommittedIngests: r.committed,
	}
	return r.sealed, nil
}

// Join streams the full outer join of a SEALED run to send. Only one Join
// runs at a time. Deleting the run ends it with ErrAborted.
func (r *Run) Join(ctx context.Context, filter join.Filter, send func(*reconv1.JoinResponse) error) error {
	r.mu.Lock()
	if err := r.stateErr(Sealed); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.joining {
		r.mu.Unlock()
		return errorf(reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS, "another Join of the run is in progress")
	}
	r.joining = true
	totals := [2]uint64{r.totals[buffer.Left].records, r.totals[buffer.Right].records}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.joining = false
		deleted := r.deleted
		r.mu.Unlock()
		if deleted {
			r.parts.Free(r.m.pool)
		}
	}()

	ctx, cancel := r.WithContext(ctx)
	defer cancel()
	stats, err := join.Run(ctx, r.m.pool, r.m.disk, r.parts, join.Options{
		Filter:           filter,
		OutputBatchBytes: r.m.cfg.OutputBatchBytes,
		Totals:           totals,
		Parallelism:      r.m.cfg.JoinParallelism,
		MaxMatchBytes:    r.m.cfg.MaxMatchBytes,
		Scratch:          scratch{r},
		Log:              r.m.log.With("recon.run_id", r.id),
	}, send)
	if err != nil {
		return err
	}
	var factor float64
	if stats.RecordBytes > 0 {
		factor = float64(stats.WorkingSet) / float64(stats.RecordBytes)
	}
	r.m.log.InfoContext(ctx, "join finished",
		"recon.run_id", r.id,
		"recon.join_units", stats.Units,
		"recon.pool_peak_bytes", r.m.pool.Peak(),
		"recon.go_peak_bytes", goPeak(),
		"recon.disk_peak_bytes", r.m.disk.Peak(),
		"recon.expansion_factor", factor,
		"recon.largest_unit_record_bytes", stats.Largest.RecordBytes,
		"recon.largest_unit_working_set_bytes", stats.Largest.WorkingSet,
	)
	return nil
}

// goPeak returns the memory the Go runtime has mapped: the most it has
// held since the process started, since it returns the pages it frees to
// the OS but never unmaps them.
func goPeak() int64 {
	s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}
