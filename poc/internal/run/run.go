// Package run manages runs: their IDs, states and limits, their buffers,
// and the operations of each state (§5.2, §6.2).
//
// A run is OPEN until it is sealed (SEALED) or an ingest stream that had
// records accepted fails (FAILED). The state is guarded by a per-run
// mutex. Deleting a run cancels the contexts of its open streams, and of
// a Join in progress, with ErrAborted, so that they end.
//
// Each run is one Spillable of the pool: a spill writes everything the run
// holds in memory as one sorted run file, at runs/<run_id>/<seq>.run in the
// data directory, through the Manager's one aligned write buffer. Seal
// writes what a run that spilled still holds in memory as its last file,
// and merges files if there are too many for a Join to read at once; a
// run that never spilled stays in memory. A Join never writes, unless the
// pool needs the memory of a run it is about to read and spills it.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/z5labs/humus"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/record"
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
	MaxMatchBytes    int64        // 0 means a worker's share of the budget
	SpillCodec       spill.Codec  // of run files' blocks; None writes no blocks
	Logger           *slog.Logger // nil discards
}

// Manager holds every run. It is safe for concurrent use.
type Manager struct {
	fsys humus.FS
	pool *mem.Pool
	disk *spill.Disk
	cfg  Config
	log  *slog.Logger

	// wbuf is the write buffer of every spill, and enc its encoder,
	// reserved for the Manager's life. The pool spills one run at a time,
	// and Seal spills through it too, so wmu is rarely waited for. Nothing
	// reserves while holding it.
	wmu      sync.Mutex
	wbuf     []byte
	format   spill.Format
	enc      *spill.Encoder
	reserved int64

	mu   sync.Mutex
	runs map[string]*Run
}

// NewManager returns a Manager whose runs reserve memory from pool and
// keep their files in cfg.DataDir/runs on fsys, spilled through disk. It
// makes pool spill the runs' buffers. It fails if pool can't hold the
// write buffer and its encoder.
func NewManager(fsys humus.FS, pool *mem.Pool, disk *spill.Disk, cfg Config) (*Manager, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	size := buffer.BlockSize(pool.Limit()) &^ (spill.Align - 1)
	format := spill.FormatFor(cfg.SpillCodec, pool.Limit())
	reserved := int64(size) + spill.Align + format.EncoderBytes()
	if err := pool.Reserve(reserved); err != nil {
		return nil, fmt.Errorf("reserve the spill write buffer: %w", err)
	}
	m := &Manager{fsys: fsys, pool: pool, disk: disk, cfg: cfg, log: log, wbuf: spill.AlignedBuffer(size),
		format: format, enc: spill.NewEncoder(format), reserved: reserved, runs: map[string]*Run{}}
	pool.SetSource(m.spillable)
	return m, nil
}

// spillable returns every run not being deleted.
func (m *Manager) spillable() []mem.Spillable {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []mem.Spillable
	for _, r := range m.runs {
		if !r.isDeleted() {
			out = append(out, r)
		}
	}
	return out
}

// Create creates an OPEN run and returns its ID. buckets is the run's
// bucket_count, which is checked against the maximum and then ignored:
// records are sorted, not partitioned.
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
		buf:     buffer.New(m.pool),
		cancels: map[int]context.CancelCauseFunc{},
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

// Pool returns the Manager's memory pool.
func (m *Manager) Pool() *mem.Pool { return m.pool }

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

// Run is one run. It is safe for concurrent use. It is the Spillable the
// pool spills its records through.
type Run struct {
	m   *Manager
	id  string
	dir string
	buf *buffer.Buffer

	// files is held for reading while a file of the run is written, and for
	// writing while delete removes the run's directory, so that nothing
	// creates files in it again.
	files     sync.RWMutex
	diskBytes atomic.Int64 // of the run's files
	seq       atomic.Int64 // of the next file
	spills    atomic.Int64
	// The records the run's spills wrote, and the bytes their files took.
	spillRaw, spillBytes atomic.Int64

	deleteOnce sync.Once
	deleteErr  error

	sealMu sync.Mutex // held while Seal writes

	mu         sync.Mutex
	state      State
	deleted    bool
	cancels    map[int]context.CancelCauseFunc // of the contexts from WithContext
	nextCancel int
	streams    int  // open ingest streams
	sealing    bool // Seal is writing
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
	case r.state == want && !(want == Open && r.sealing):
		return nil
	case r.state == Failed:
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_FAILED, "the run failed; delete it and rerun")
	case r.state == Open && r.sealing && want == Open:
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE, "the run is being sealed")
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
	// A Join still reads the buffer until it sees the abort, so it frees it
	// when it ends. Otherwise the pool would credit memory the Join still
	// holds.
	if !joining {
		r.buf.Free()
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

// SpillSize returns the memory spilling the run would release now.
func (r *Run) SpillSize() int64 { return r.buf.SpillSize() }

// Spill writes the run's blocks as one new run file, unless the run was
// deleted.
func (r *Run) Spill() error {
	r.files.RLock()
	defer r.files.RUnlock()
	if r.isDeleted() {
		return nil
	}
	return r.buf.Spill(r.write)
}

// path returns the path of the run's next file.
func (r *Run) path() string {
	return filepath.Join(r.dir, fmt.Sprintf("%06d.run", r.seq.Add(1)-1))
}

// write writes a spill of bytes bytes, the records of m, through the
// Manager's write buffer. It reserves the most the file may take on disk,
// and keeps what it takes. r.files is held for reading.
func (r *Run) write(bytes int64, m *merge.Merger) (*spill.File, error) {
	disk := r.m.disk
	most := r.m.format.MaxBytes(bytes)
	if err := disk.Reserve(most); err != nil {
		return nil, err
	}
	r.m.wmu.Lock()
	defer r.m.wmu.Unlock()
	w, err := spill.Create(r.m.fsys, r.path(), r.m.wbuf, r.m.enc)
	if err != nil {
		disk.Release(most)
		return nil, err
	}
	if err := spill.Copy(context.Background(), w, m); err != nil {
		w.Abort()
		disk.Release(most)
		return nil, err
	}
	f, err := w.Finish()
	if err != nil {
		disk.Release(most)
		return nil, err
	}
	disk.Release(most - f.Bytes)
	r.diskBytes.Add(f.Bytes)
	r.spills.Add(1)
	r.spillRaw.Add(f.Raw)
	r.spillBytes.Add(f.Bytes)
	return f, nil
}

// runFiles is where Compact writes the run's merged files. r.files is held
// for reading while it is used.
type runFiles struct{ r *Run }

func (f runFiles) Format() spill.Format { return f.r.m.format }

func (f runFiles) Create(bytes int64, buf []byte, enc *spill.Encoder) (*spill.Writer, error) {
	r := f.r
	if err := r.m.disk.Reserve(bytes); err != nil {
		return nil, err
	}
	w, err := spill.Create(r.m.fsys, r.path(), buf, enc)
	if err != nil {
		r.m.disk.Release(bytes)
		return nil, err
	}
	r.diskBytes.Add(bytes)
	return w, nil
}

func (f runFiles) Unreserve(bytes int64) {
	f.r.diskBytes.Add(-bytes)
	f.r.m.disk.Release(bytes)
}

// Remove removes files. If that fails, the run's deletion gives back their
// bytes once it removes them.
func (f runFiles) Remove(files []*spill.File) {
	r := f.r
	for _, file := range files {
		if err := r.m.fsys.Remove(file.Path); err == nil {
			r.diskBytes.Add(-file.Bytes)
			r.m.disk.Release(file.Bytes)
		}
	}
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
func (in *Ingest) Append(side record.Side, records []*reconv1.Record, size int64) error {
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
	n, err := r.buf.Append(side, len(records), func(i int) ([]byte, []byte) {
		return records[i].GetKey(), records[i].GetValue()
	})
	for _, rec := range records[:n] {
		in.totals[side].records++
		if len(rec.GetKey()) == 0 {
			in.totals[side].empty++
		}
	}
	in.accepted = in.accepted || n > 0
	switch {
	case errors.Is(err, buffer.ErrFreed):
		return ErrAborted
	case errors.Is(err, buffer.ErrSealed):
		return errorf(reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE, "the run is sealed")
	case err != nil:
		in.failOnDisk(err)
		return err
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
//
// A run that spilled writes the records it still holds in memory as its
// last file, and merges files if a Join couldn't read them all at once.
// If that fails, the run stays OPEN, with every record it had, and Seal
// may be called again.
func (r *Run) Seal() (*reconv1.SealRunResponse, error) {
	r.sealMu.Lock()
	defer r.sealMu.Unlock()
	r.mu.Lock()
	if r.state == Sealed && !r.deleted {
		defer r.mu.Unlock()
		return r.sealed, nil
	}
	if err := r.stateErr(Open); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if r.streams > 0 {
		defer r.mu.Unlock()
		return nil, errorf(reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS, "%d ingest streams are open", r.streams)
	}
	r.sealing = true
	r.mu.Unlock()

	start := time.Now()
	merged, err := r.seal()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealing = false
	if r.deleted {
		return nil, errNotFound()
	}
	if err != nil {
		r.buf.Unseal()
		return nil, err
	}
	files := r.buf.Files()
	r.m.log.Info("run sealed",
		"recon.run_id", r.id,
		"recon.run_files", len(files),
		"recon.spills", r.spills.Load(),
		"recon.merges", merged.Merges,
		"recon.merge_bytes", merged.Bytes,
		"recon.merges_skipped", merged.Skipped,
		"recon.spill_codec", r.m.format.Codec.String(),
		"recon.spill_raw_bytes", r.spillRaw.Load(),
		"recon.spill_file_bytes", r.spillBytes.Load(),
		"recon.seal_seconds", time.Since(start).Seconds(),
	)
	r.state = Sealed
	r.sealed = &reconv1.SealRunResponse{
		Left:             &reconv1.SideTotals{Records: r.totals[record.Left].records, EmptyKeyRecords: r.totals[record.Left].empty},
		Right:            &reconv1.SideTotals{Records: r.totals[record.Right].records, EmptyKeyRecords: r.totals[record.Right].empty},
		CommittedIngests: r.committed,
	}
	return r.sealed, nil
}

// seal does Seal's I/O: if the run spilled, it spills the rest and merges
// files. Deleting the run cancels it.
func (r *Run) seal() (join.MergeStats, error) {
	spilled, err := r.buf.Seal()
	if err != nil {
		return join.MergeStats{}, err
	}
	if !spilled {
		return join.MergeStats{}, nil
	}
	if err := r.Spill(); err != nil {
		return join.MergeStats{}, err
	}
	ctx, cancel := r.WithContext(context.Background())
	defer cancel()
	r.files.RLock()
	defer r.files.RUnlock()
	if r.isDeleted() {
		return join.MergeStats{}, ErrAborted
	}
	return join.Compact(ctx, r.m.pool, r.m.fsys, r.buf, runFiles{r}, r.joinOptions(join.Filter{}, [2]uint64{}))
}

// joinOptions returns the options of a Join of the run, or of a merge.
func (r *Run) joinOptions(filter join.Filter, totals [2]uint64) join.Options {
	return join.Options{
		Filter:           filter,
		OutputBatchBytes: r.m.cfg.OutputBatchBytes,
		Totals:           totals,
		Parallelism:      r.m.cfg.JoinParallelism,
		MaxMatchBytes:    r.m.cfg.MaxMatchBytes,
	}
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
	totals := [2]uint64{r.totals[record.Left].records, r.totals[record.Right].records}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.joining = false
		deleted := r.deleted
		r.mu.Unlock()
		if deleted {
			r.buf.Free()
		}
	}()

	ctx, cancel := r.WithContext(ctx)
	defer cancel()
	start := time.Now()
	stats, err := join.Run(ctx, r.m.pool, r.m.fsys, r.buf, r.joinOptions(filter, totals), send)
	if err != nil {
		return err
	}
	r.m.log.InfoContext(ctx, "join finished",
		"recon.run_id", r.id,
		"recon.join_files", stats.Files,
		"recon.join_blocks", stats.Blocks,
		"recon.join_workers", stats.Workers,
		"recon.join_tasks", stats.Tasks,
		"recon.join_read_buffer_bytes", stats.ReadBuffer,
		"recon.join_reads", stats.Reads,
		"recon.join_batches", stats.Batches,
		"recon.join_streamed_keys", stats.Streamed,
		"recon.join_seconds", time.Since(start).Seconds(),
		"recon.max_match_bytes", stats.MaxMatch,
		"recon.pool_peak_bytes", r.m.pool.Peak(),
		"recon.go_peak_bytes", goPeak(),
		"recon.disk_peak_bytes", r.m.disk.Peak(),
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
