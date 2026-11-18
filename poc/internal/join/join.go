// Package join streams the full outer join of a sealed run as
// Venn-labelled results (§8.4, §8.5).
//
// A run's records are in sorted run files, and, if it never spilled, in
// sorted blocks in memory. The join never loads anything: each of its
// workers merges a range of fences of every file and block with a loser
// tree, in (hash, key, side, ingest) order, through one read buffer per
// file, and emits results as the records stream by. A key's left records
// are held, in the worker's arena, until its first right record, or the
// next key, shows whether they are a Match or left_only results; a right
// record with no left ones held is a right_only result at once. Empty keys
// hash to 0, sort first, and are one-sided results.
//
// So a Join holds one key at a time. A key is limited only by its Match:
// a Match larger than the maximum match size fails the Join with a
// HotKeyError naming the key. A key on one side only streams at any size:
// once its records held reach the maximum, the worker scans to the end of
// the key without keeping it, and if the scan finds no right record, emits
// the held ones, goes back to where the scan began, and streams the rest.
//
// The fences are planned into tasks of about equal bytes, several per
// worker; a worker takes the next task once it is done with one. Each
// worker fills an output batch of its own and hands it to the goroutine
// that called Run, which sends every batch, one at a time, so a slow
// client holds up the workers and memory doesn't grow. The batches of a
// task depend on the data and the plan alone, not on which worker took the
// task; batches of different tasks interleave.
package join

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/z5labs/humus"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/record"
	"poc/internal/spill"
)

const (
	// maxReadBuf is the largest read buffer of a cursor.
	maxReadBuf = 256 << 10
	// goodReadBuf is the smallest read buffer a Join takes before it gives
	// up workers instead.
	goodReadBuf = 32 << 10
	// minReadBuf is the smallest read buffer a Join or merge works with.
	minReadBuf = spill.Align
	// mergeReadBuf is the read buffer Compact merges files down to: until
	// one worker could read them all with buffers of this size.
	mergeReadBuf = 16 << 10
	// readSharePct is the share of the budget read buffers may take.
	readSharePct = 50
	// tasksPerWorker is how many tasks the plan has per worker.
	tasksPerWorker = 8
	// sourceBytes is the memory a merge source takes besides its buffer.
	sourceBytes = 256
)

// ErrControlTotals is returned when the join accounted for a different
// number of records per side than the run sealed with (§9).
var ErrControlTotals = errors.New("control totals mismatch")

// dropRecord makes the next join leave one record out, so that tests can
// reach the ErrControlTotals path.
var dropRecord atomic.Bool

// errStopped ends the tasks after one that failed.
var errStopped = errors.New("join: an earlier task failed")

// Filter is the set of states a Join returns.
type Filter [4]bool

// NewFilter returns the Filter of states, all of them when states is
// empty. The states must be valid, non-UNSPECIFIED values.
func NewFilter(states []reconv1.JoinState) Filter {
	var f Filter
	if len(states) == 0 {
		return Filter{false, true, true, true}
	}
	for _, s := range states {
		f[s] = true
	}
	return f
}

// oneSided reports whether f returns the one-sided results of side.
func (f Filter) oneSided(side record.Side) bool {
	if side == record.Left {
		return f[reconv1.JoinState_JOIN_STATE_LEFT_ONLY]
	}
	return f[reconv1.JoinState_JOIN_STATE_RIGHT_ONLY]
}

// HotKeyError is the failure of a Join because one key's Match would be
// larger than the maximum match size (§8.4).
type HotKeyError struct {
	Key         []byte
	Left, Right uint64 // the key's records per side
	Size, Limit int64  // the Match's encoded size, and the most it may be
}

func (e *HotKeyError) Error() string {
	return fmt.Sprintf("a key's Match of %d left and %d right values is %d bytes, more than the maximum match size of %d bytes",
		e.Left, e.Right, e.Size, e.Limit)
}

func hotKey(key []byte, left, right uint64, size, limit int64) *HotKeyError {
	return &HotKeyError{Key: bytes.Clone(key), Left: left, Right: right, Size: size, Limit: limit}
}

// Options configure Run and Compact.
type Options struct {
	Filter Filter
	// OutputBatchBytes is the size at which a batch is sent.
	OutputBatchBytes int64
	// Totals is the number of records per side the run sealed with.
	Totals [2]uint64
	// Parallelism is the number of workers a Join wants.
	Parallelism int
	// MaxMatchBytes is the largest Match a Join may send; 0 means a
	// worker's share of the budget, a quarter of it.
	MaxMatchBytes int64
}

// MaxMatch returns the largest Match a Join may send under a budget of
// limit bytes.
func MaxMatch(limit int64, opts Options) int64 {
	if opts.MaxMatchBytes > 0 {
		return opts.MaxMatchBytes
	}
	return max(limit/int64(4*max(opts.Parallelism, 1)), opts.OutputBatchBytes)
}

// Stats describe a finished Run.
type Stats struct {
	Files, Blocks  int // merged by each worker
	Workers, Tasks int
	ReadBuffer     int   // bytes of one cursor's read buffer
	ReadBytes      int64 // reserved for read buffers
	Reads          int64 // preads
	Batches        int64
	Streamed       int64 // one-sided keys too large for a Match, streamed
	MaxMatch       int64
}

// task is fences lo to hi-1, merged by one worker.
type task struct{ lo, hi int }

// plan cuts the fences into at most n tasks of about equal bytes, given the
// bytes of each fence. It depends only on its arguments.
func plan(sizes []int64, n int) []task {
	var total int64
	for _, s := range sizes {
		total += s
	}
	if total == 0 || n < 1 {
		return nil
	}
	var tasks []task
	var lo int
	var sum, part int64 // bytes so far, and of the task being filled
	for i, s := range sizes {
		sum += s
		part += s
		// Cut after fence i once the tasks so far hold their share.
		if last := i == len(sizes)-1; last || part > 0 && sum*int64(n) >= total*int64(len(tasks)+1) {
			if part > 0 || len(tasks) == 0 {
				tasks = append(tasks, task{lo, i + 1})
			} else {
				tasks[len(tasks)-1].hi = i + 1 // empty fences at the end
			}
			lo, part = i+1, 0
		}
	}
	return tasks
}

// workerBytes is what a worker may take besides its read buffers and the
// batch reserved for it: room for its largest Match, its emitter's structs
// and slack. A Match is held once, in arena chunks, with a slice of 24
// bytes per value besides: twice the maximum match size covers values of
// 22 bytes and more, and a worker holds no more than that.
func workerBytes(maxMatch, batch int64) int64 { return 2*maxMatch + emitterBytes(batch) }

// maxWorkers returns how many of want workers a budget of limit bytes
// runs: no more than whose batches and largest Matches take three quarters
// of it, and at least one.
func maxWorkers(limit int64, want int, maxMatch, batch int64) int {
	for want > 1 && int64(want)*(batch+workerBytes(maxMatch, batch)) > limit*3/4 {
		want--
	}
	return want
}

// readBudget returns the bytes the read buffers of workers workers may
// take: the share of the budget set for them, less if the pool's memory is
// held by what it can't spill, or if the workers' largest Matches wouldn't
// fit next to them with an eighth of the budget to spare, but at least a
// quarter of the budget. The floor lets a Join run while other runs hold
// memory, at the risk of running out if every worker meets a Match of the
// largest size at once.
func readBudget(pool *mem.Pool, workers int, maxMatch, batch int64) int64 {
	limit := pool.Limit()
	free := pool.Free() - int64(workers)*workerBytes(maxMatch, batch) - limit/8
	return min(max(free, limit/4), limit*readSharePct/100)
}

// readBuffer returns the size of a read buffer when workers workers each
// read files files.
func readBuffer(pool *mem.Pool, workers, files int, maxMatch, batch int64) int {
	return int(min(readBudget(pool, workers, maxMatch, batch)/int64(workers*files), maxReadBuf)) &^ (spill.Align - 1)
}

// Run streams the full outer join of buf, which is sealed, and sends the
// results in batches of about opts.OutputBatchBytes.
//
// The workers' memory is reserved from pool before buf is pinned: if it
// doesn't fit next to the blocks of a run still in memory, the reservation
// spills them, and the Join reads the file instead. Fewer workers run than
// opts.Parallelism if their largest Matches together would take more than
// three quarters of the budget. Then each worker gets a read buffer per
// file, as large as the budget allows, and fewer workers run before the
// buffers get small. Run fails with the error of pool.Reserve when even
// one worker can't have read buffers of minReadBuf bytes.
//
// A batch is valid only until send returns: its memory is used for the
// next one. grpc-go's SendMsg encodes the message before it returns.
//
// It returns context.Cause(ctx) once ctx is done, the error of a failed
// send, a *HotKeyError, or ErrControlTotals. When tasks fail, the error is
// that of the first of them in the plan, so it doesn't depend on which
// worker got there first.
func Run(ctx context.Context, pool *mem.Pool, fsys humus.FS, buf *buffer.Buffer, opts Options, send func(*reconv1.JoinResponse) error) (Stats, error) {
	batch := opts.OutputBatchBytes
	maxMatch := MaxMatch(pool.Limit(), opts)
	want := maxWorkers(pool.Limit(), max(opts.Parallelism, 1), maxMatch, batch)
	var reserved int64
	defer func() { pool.Release(reserved) }()
	reserve := func(n int64) error {
		if err := pool.Reserve(n); err != nil {
			return err
		}
		reserved += n
		return nil
	}

	// The probe is what the workers will take besides their arenas: their
	// Matches, and read buffers of a good size for the files there are,
	// and one more if the run is spilled to make room.
	arenas := int64(want) * batch
	nfiles := int64(len(buf.Files()) + 1)
	room := min(int64(want)*(workerBytes(maxMatch, batch)+nfiles*goodReadBuf), pool.Limit()*3/4)
	if err := reserve(arenas + room); err != nil {
		// Only a probe: whatever it couldn't spill, the Join may still have
		// room for, reading the run where it is.
		room = 0
		if err := reserve(arenas); err != nil {
			return Stats{}, err
		}
	}
	snap := buf.Pin()
	defer buf.Unpin()
	pool.Release(room)
	reserved -= room

	files := snap.Files
	var sizes [record.Fences]int64
	for _, f := range files {
		for i := range sizes {
			sizes[i] += f.Fences[i+1] - f.Fences[i]
		}
	}
	snap.AddFenceBytes(&sizes)
	tasks := plan(sizes[:], want*tasksPerWorker)
	st := Stats{Files: len(files), Blocks: snap.Blocks(), Tasks: len(tasks), MaxMatch: maxMatch}
	workers := min(want, len(tasks))
	var opened []*os.File
	defer func() {
		for _, f := range opened {
			_ = f.Close()
		}
	}()
	if len(files) > 0 && workers > 0 {
		for workers > 1 && readBuffer(pool, workers, len(files), maxMatch, batch) < goodReadBuf {
			workers--
		}
		st.ReadBuffer = readBuffer(pool, workers, len(files), maxMatch, batch)
		if st.ReadBuffer < minReadBuf {
			return st, fmt.Errorf("%w: a Join of %d run files needs read buffers of %d bytes each, and may take %d bytes",
				mem.ErrExhausted, len(files), minReadBuf, readBudget(pool, 1, maxMatch, batch))
		}
		st.ReadBytes = int64(workers) * int64(len(files)) * int64(st.ReadBuffer)
		if err := reserve(st.ReadBytes); err != nil {
			return st, err
		}
		for _, f := range files {
			r, err := spill.Open(fsys, f)
			if err != nil {
				if ctx.Err() != nil { // the run was deleted, its files with it
					return st, context.Cause(ctx)
				}
				return st, err
			}
			opened = append(opened, r)
		}
	}
	// The arenas of the workers the plan left out.
	if extra := int64(want-workers) * batch; extra > 0 {
		pool.Release(extra)
		reserved -= extra
	}
	st.Workers = workers
	fan := len(files) + snap.Blocks()
	if err := reserve(int64(workers)*(merge.Bytes(fan)+int64(fan)*sourceBytes) + int64(len(tasks))*16); err != nil {
		return st, err
	}

	sends := make(chan sendRequest)
	sched := newSchedule(ctx, len(tasks))
	ws := make([]*worker, workers)
	var wg sync.WaitGroup
	drop := dropRecord.Swap(false)
	for i := range ws {
		w := &worker{
			pool:     pool,
			snap:     snap,
			files:    files,
			f:        opts.Filter,
			maxMatch: maxMatch,
			e:        newEmitter(pool, opts.Filter, batch, sends, batch),
			save:     make([]int64, fan),
		}
		w.drop.Store(drop && i == 0)
		reserved -= batch // the emitter's to release
		srcs := make([]merge.Source, fan)
		if len(files) > 0 {
			slab := make([]byte, len(files)*st.ReadBuffer)
			for r := range files {
				rb := slab[r*st.ReadBuffer : (r+1)*st.ReadBuffer : (r+1)*st.ReadBuffer]
				c := spill.NewCursor(opened[r], uint64(r), rb, w.grow)
				w.cursors = append(w.cursors, c)
				srcs[r] = c
			}
		}
		w.m = merge.New(srcs)
		ws[i] = w
		wg.Go(func() {
			for {
				t, tctx, ok := sched.take()
				if !ok {
					return
				}
				w.ctx, w.e.ctx = tctx, tctx
				err := w.run(tasks[t])
				if err != nil && tctx.Err() != nil {
					err = context.Cause(tctx)
				}
				sched.done(t, err)
			}
		})
	}
	go func() {
		wg.Wait()
		close(sends)
	}()
	// grpc-go doesn't allow concurrent sends on a stream. After a failed
	// send every batch fails the same way, and the workers stop. Once ctx
	// is done nothing more is sent, though a worker had a batch queued.
	var sendErr error
	for r := range sends {
		if sendErr == nil && ctx.Err() != nil {
			sendErr = context.Cause(ctx)
		}
		if sendErr == nil {
			sendErr = send(r.resp)
		}
		r.sent <- sendErr
	}

	var counts [2]uint64
	for _, w := range ws {
		w.e.release()
		pool.Release(w.extra)
		counts[record.Left] += w.counts[record.Left]
		counts[record.Right] += w.counts[record.Right]
		st.Batches += w.e.batches
		st.Streamed += w.streamed
		for _, c := range w.cursors {
			st.Reads += c.Reads()
		}
	}
	if sched.err != nil {
		return st, sched.err
	}
	if counts != opts.Totals {
		return st, fmt.Errorf("%w: joined %d left and %d right records, sealed with %d and %d",
			ErrControlTotals, counts[record.Left], counts[record.Right], opts.Totals[record.Left], opts.Totals[record.Right])
	}
	return st, nil
}

// schedule hands out tasks in order and keeps the error of the first one
// in that order that failed. Once a task fails, no task starts, and the
// tasks after it are cancelled; those before it run on, since one of them
// may fail too.
type schedule struct {
	ctx context.Context

	mu      sync.Mutex
	next    int
	failed  int   // the first failed task, or the number of tasks
	err     error // of task failed; read once every task is done
	cancels map[int]context.CancelCauseFunc
}

func newSchedule(ctx context.Context, tasks int) *schedule {
	return &schedule{ctx: ctx, failed: tasks, cancels: map[int]context.CancelCauseFunc{}}
}

// take returns the next task and its context, or false when no task is
// left to start.
func (s *schedule) take() (int, context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= s.failed || s.ctx.Err() != nil {
		if s.ctx.Err() != nil && s.err == nil && s.next < s.failed {
			s.failed, s.err = s.next, context.Cause(s.ctx)
		}
		return 0, nil, false
	}
	i := s.next
	s.next++
	ctx, cancel := context.WithCancelCause(s.ctx)
	s.cancels[i] = cancel
	return i, ctx, true
}

// done records the end of task i, which returned err.
func (s *schedule) done(i int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels[i](nil)
	delete(s.cancels, i)
	if err == nil || i > s.failed || errors.Is(err, errStopped) {
		return
	}
	s.failed, s.err = i, err
	for k, cancel := range s.cancels {
		if k > i {
			cancel(errStopped)
		}
	}
}

// worker merges tasks, one after another, into its emitter. Its methods run
// on its goroutine.
type worker struct {
	pool     *mem.Pool
	ctx      context.Context // of the task in progress
	snap     *buffer.Snapshot
	files    []*spill.File
	cursors  []*spill.Cursor
	m        *merge.Merger
	e        *emitter
	f        Filter
	maxMatch int64
	save     []int64 // the sources' positions, for a rewind
	extra    int64   // reserved for read buffers that grew
	drop     atomic.Bool

	// A key with more left records than a Match may hold, and no right
	// ones, is streamed: its records are left_only results one by one.
	streaming bool
	shash     uint64
	skey      []byte
	// unbound is set for a group that is a Match within the maximum size
	// whose values take more memory than a worker holds one in: it is held
	// anyway, as far as the pool allows.
	unbound bool

	counts   [2]uint64
	streamed int64
}

// grow returns a reserved buffer of n bytes, for a cursor whose record's
// header and key don't fit its read buffer.
func (w *worker) grow(n int) ([]byte, error) {
	if err := w.pool.Reserve(int64(n)); err != nil {
		return nil, err
	}
	w.extra += int64(n)
	return make([]byte, n), nil
}

// open points the worker's sources at the task's fences.
func (w *worker) open(t task) error {
	for i, c := range w.cursors {
		off, end := w.files[i].Segment(t.lo, t.hi)
		if err := c.Reset(off, end); err != nil {
			return err
		}
	}
	srcs := w.m.Sources()
	base := uint64(len(w.files)) << 32
	for b := range w.snap.Blocks() {
		srcs[len(w.files)+b] = w.snap.Source(b, t.lo, t.hi, base)
	}
	w.m.Init()
	return nil
}

// run joins one task: it merges the task's records and emits results as
// they go by. The batch is sent at the end of the task, so that the
// batches depend on the plan alone, not on which worker took which task.
func (w *worker) run(t task) error {
	if err := w.open(t); err != nil {
		return err
	}
	m, e := w.m, w.e
	for n := 1; ; n++ {
		src := m.Top()
		if src == nil {
			break
		}
		if n&4095 == 0 && w.ctx.Err() != nil {
			return context.Cause(w.ctx)
		}
		head := src.Head()
		h, _ := record.ParseHeader(head)
		key := head[h.Len : h.Len+h.KeyLen]
		if w.drop.Load() {
			w.drop.Store(false)
			if err := m.Pop(); err != nil {
				return err
			}
			continue
		}
		var err error
		switch {
		case len(key) == 0:
			// An empty key never matches: a one-sided result per record.
			if err = e.finish(w.maxMatch); err == nil {
				err = w.single(src, h, key)
			}
		case w.streaming && h.Hash == w.shash && bytes.Equal(key, w.skey):
			err = w.single(src, h, key)
		case e.matching(h.Hash, key):
			if e.hold && (e.gsize+bytesField(h.ValLen) > w.maxMatch || !w.unbound && e.held(h.ValLen) > 2*w.maxMatch) {
				if err := w.overflow(); err != nil {
					return err
				}
				continue // the merge is back at this record, or past the key
			}
			err = e.add(h.Side, h.ValLen, src)
		default:
			w.streaming, w.unbound = false, false
			if err = e.finish(w.maxMatch); err != nil {
				break
			}
			if h.Side == record.Right {
				// Left records sort first: this key has none.
				err = w.single(src, h, key)
				break
			}
			if err = e.start(h.Hash, key, w.f[reconv1.JoinState_JOIN_STATE_BOTH] || w.f[reconv1.JoinState_JOIN_STATE_LEFT_ONLY]); err == nil {
				err = e.add(h.Side, h.ValLen, src)
			}
		}
		if err != nil {
			return err
		}
		w.counts[h.Side]++
		if err := m.Pop(); err != nil {
			return err
		}
	}
	w.streaming = false
	if err := e.finish(w.maxMatch); err != nil {
		return err
	}
	return e.flush()
}

// single emits the head of src as a one-sided result, if the filter
// returns it.
func (w *worker) single(src merge.Source, h record.Header, key []byte) error {
	if !w.f.oneSided(h.Side) {
		return nil
	}
	e := w.e
	if err := e.start(h.Hash, key, true); err != nil {
		return err
	}
	if err := e.add(h.Side, h.ValLen, src); err != nil {
		return err
	}
	return e.finish(w.maxMatch)
}

// overflow handles a key whose values held no longer fit a Match, or the
// memory a worker holds one in, when the head of the merge is one more
// record of it. It scans the rest of the key without keeping it, and goes
// back to where the scan began. If the key has right records and its Match
// is larger than the maximum, the Join fails with a HotKeyError; if its
// Match is within the maximum, its many small values are held on, as far
// as the pool allows. If it has only left ones, they are left_only
// results, however many: the ones held are emitted, and the rest are
// streamed.
func (w *worker) overflow() error {
	m, e := w.m, w.e
	key := e.key() // in the arena, which the scan doesn't touch
	left, right, size := e.nl, e.nr, e.gsize
	for i, s := range m.Sources() {
		w.save[i] = s.Pos()
	}
	for n := 1; ; n++ {
		src := m.Top()
		if src == nil {
			break
		}
		head := src.Head()
		h, _ := record.ParseHeader(head)
		if h.Hash != e.ghash || !bytes.Equal(head[h.Len:h.Len+h.KeyLen], key) {
			break
		}
		if h.Side == record.Left {
			left++
		} else {
			right++
		}
		size += bytesField(h.ValLen)
		if n&4095 == 0 && w.ctx.Err() != nil {
			return context.Cause(w.ctx)
		}
		if err := m.Pop(); err != nil {
			return err
		}
	}
	rewind := func() error {
		for i, s := range m.Sources() {
			if err := s.Rewind(w.save[i]); err != nil {
				return err
			}
		}
		m.Init()
		return nil
	}
	switch {
	case right > 0 && size > w.maxMatch:
		return hotKey(key, left, right, size, w.maxMatch)
	case right > 0:
		w.unbound = true
		return rewind()
	}
	if cap(w.skey) < len(key) {
		if err := e.reserve(int64(len(key))); err != nil {
			return err
		}
		w.skey = make([]byte, 0, len(key))
	}
	w.skey = append(w.skey[:0], key...)
	w.shash, w.streaming = e.ghash, true
	w.streamed++
	if err := e.finish(w.maxMatch); err != nil {
		return err
	}
	return rewind()
}
