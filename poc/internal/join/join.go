// Package join joins a sealed run's buffers into Venn-labelled results
// (§8.4, §8.5).
//
// The buckets are planned into units: contiguous bucket ranges whose
// estimated working set, record bytes times ExpansionFactor, fills up to
// the unit budget, the memory limit divided by the join parallelism. Each
// unit is loaded from its buffers and spill files, every record gets an
// index entry of its hash and position, the entries are sorted by hash,
// then key bytes, then side, and one pass over them emits left_only,
// right_only and both results in that deterministic order, without a hash
// map.
//
// A bucket whose estimate alone exceeds the unit budget is oversized. A
// counting scan looks for a key whose records exceed the budget, or whose
// Match would exceed the maximum match size, and the Join fails with a
// HotKeyError naming it. Otherwise the bucket is re-partitioned into
// sub-buckets with the seed of the next depth, written to files under the
// run's join-<n> directory, and joined like the run's own buckets, down to
// MaxDepth, one sub-unit after another. The empty-key partitions come
// last, one more unit streamed as one-sided results in ingest order.
//
// Up to the join parallelism of units run at once, each on a worker that
// hands its batches to a single sender. Within a unit the batches are
// deterministic; batches of different units interleave.
package join

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/spill"
)

// ErrControlTotals is returned when the join accounted for a different
// number of records per side than the run sealed with (§9).
var ErrControlTotals = errors.New("control totals mismatch")

// dropRecord makes the next join leave one record out, so that tests can
// reach the ErrControlTotals path.
var dropRecord atomic.Bool

// ExpansionFactor is the planner's estimate of a unit's working set per
// byte of its records (§8.4).
const ExpansionFactor = 2

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
func (f Filter) oneSided(side buffer.Side) bool {
	if side == buffer.Left {
		return f[reconv1.JoinState_JOIN_STATE_LEFT_ONLY]
	}
	return f[reconv1.JoinState_JOIN_STATE_RIGHT_ONLY]
}

// Options configure Run.
type Options struct {
	Filter Filter
	// OutputBatchBytes is the size at which a batch is sent.
	OutputBatchBytes int64
	// Totals is the number of records per side the run sealed with.
	Totals [2]uint64
	// Parallelism divides the memory limit into the unit budget.
	Parallelism int
	// MaxMatchBytes is the largest Match a Join may send; 0 means no
	// limit.
	MaxMatchBytes int64
	// Scratch holds the files of sub-partitioned buckets.
	Scratch Scratch
	// Log receives the sub-partition events; nil discards them.
	Log *slog.Logger
}

// Scratch is where a Join writes the sub-partitions of oversized buckets:
// a directory of the run's, which the run's deletion removes.
type Scratch interface {
	// Write writes chunks, which hold records records, to a new file at
	// path, relative to the run's directory, like spill.Disk.Write.
	Write(path string, records int64, chunks [][]byte) (spill.File, error)
	// Remove removes dir, relative to the run's directory, with the files
	// in it, and gives back the bytes they took on disk.
	Remove(dir string, bytes int64) error
}

// Stats describe a finished Run.
type Stats struct {
	Units         int // join units, sub-buckets' included, not the empty-key unit
	Subpartitions int // oversized buckets that were re-partitioned

	// RecordBytes is the encoded bytes of the units' records, and
	// WorkingSet what holding them took: their chunks in memory, their
	// spill files read, and the index.
	RecordBytes, WorkingSet int64
	// Largest is the unit whose working set was largest.
	Largest struct{ RecordBytes, WorkingSet int64 }
}

func (s *Stats) add(o Stats) {
	s.Units += o.Units
	s.Subpartitions += o.Subpartitions
	s.RecordBytes += o.RecordBytes
	s.WorkingSet += o.WorkingSet
	if o.Largest.WorkingSet > s.Largest.WorkingSet {
		s.Largest = o.Largest
	}
}

// unit records the record bytes and working set of a unit.
func (s *Stats) unit(records, working int64) {
	s.Units++
	s.RecordBytes += records
	s.WorkingSet += working
	if working > s.Largest.WorkingSet {
		s.Largest.RecordBytes, s.Largest.WorkingSet = records, working
	}
}

// Unit is the buckets Lo to Hi-1, joined together.
type Unit struct {
	Lo, Hi uint32
}

// Plan groups buckets with the given record bytes into units of
// contiguous buckets, each filled greedily while its estimate stays within
// budget. A bucket whose estimate alone exceeds budget is a unit of its
// own. The plan depends only on its arguments.
func Plan(sizes []int64, budget int64) []Unit {
	var units []Unit
	var lo int
	var estimate int64
	for i, size := range sizes {
		if i > lo && estimate+size*ExpansionFactor > budget {
			units = append(units, Unit{uint32(lo), uint32(i)})
			lo, estimate = i, 0
		}
		estimate += size * ExpansionFactor
	}
	if len(sizes) > 0 {
		units = append(units, Unit{uint32(lo), uint32(len(sizes))})
	}
	return units
}

// entry indexes one record: its hash and where it is.
type entry struct {
	hash  uint64
	chunk uint32 // index into the unit's chunks
	off   uint32 // offset of the record in its chunk
}

const entrySize = int64(unsafe.Sizeof(entry{}))

// chunk is whole records of one side: a buffer chunk, or a spill file or
// part of one.
type chunk struct {
	data []byte
	side buffer.Side
	// streamed is set for part of a file read through a buffer that is
	// reused once the chunk has been handled.
	streamed bool
}

// Run joins parts, whose spill files are on disk, and sends the results in
// batches of about opts.OutputBatchBytes.
//
// Up to opts.Parallelism workers join the units of the plan, each taking
// the next unit in order once it is done with one, and the empty-key unit
// last. Each worker fills an output batch of its own and hands it to the
// goroutine that called Run, which sends every batch, one at a time, so a
// slow client holds up the workers. A worker waits until its batch is sent
// before it goes on, because the batch points into the unit's memory.
//
// Memory for the workers' output batches and for each unit's working set
// is reserved from pool, so Run fails with the error of pool.Reserve when
// the budget can't hold them. It returns context.Cause(ctx) once ctx is
// done, the error of a failed send, a *HotKeyError, or ErrControlTotals.
// When units fail, the error is that of the first of them in the plan, so
// it doesn't depend on which worker got there first.
func Run(ctx context.Context, pool *mem.Pool, disk *spill.Disk, parts *buffer.Partitions, opts Options, send func(*reconv1.JoinResponse) error) (Stats, error) {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	sh := &shared{
		pool:     pool,
		disk:     disk,
		scratch:  opts.Scratch,
		log:      log,
		budget:   pool.Limit() / int64(max(opts.Parallelism, 1)),
		maxMatch: opts.MaxMatchBytes,
		f:        opts.Filter,
	}
	sh.drop.Store(dropRecord.Swap(false))
	tasks := sh.tasks(parts)

	workers := min(max(opts.Parallelism, 1), len(tasks))
	batches := int64(workers) * opts.OutputBatchBytes
	if err := pool.Reserve(batches); err != nil {
		return Stats{}, err
	}
	defer pool.Release(batches)

	sends := make(chan sendRequest)
	sched := newSchedule(ctx, len(tasks))
	joiners := make([]*joiner, workers)
	var wg sync.WaitGroup
	for w := range joiners {
		j := &joiner{shared: sh, out: &batcher{sends: sends, sent: make(chan error, 1), limit: opts.OutputBatchBytes}}
		joiners[w] = j
		wg.Go(func() {
			for {
				i, ctx, ok := sched.take()
				if !ok {
					return
				}
				j.ctx, j.out.ctx = ctx, ctx
				sched.done(i, tasks[i](j))
			}
		})
	}
	go func() {
		wg.Wait()
		close(sends)
	}()
	// grpc-go doesn't allow concurrent sends on a stream. After a failed
	// send every batch fails the same way, and the workers stop.
	var sendErr error
	for r := range sends {
		if sendErr == nil {
			sendErr = send(r.resp)
		}
		r.sent <- sendErr
	}
	if sched.err != nil {
		return Stats{}, sched.err
	}

	var stats Stats
	var counts [2]uint64
	for _, j := range joiners {
		stats.add(j.stats)
		counts[buffer.Left] += j.counts[buffer.Left]
		counts[buffer.Right] += j.counts[buffer.Right]
	}
	if counts != opts.Totals {
		return Stats{}, fmt.Errorf("%w: joined %d left and %d right records, sealed with %d and %d",
			ErrControlTotals, counts[buffer.Left], counts[buffer.Right], opts.Totals[buffer.Left], opts.Totals[buffer.Right])
	}
	return stats, nil
}

// shared is the state of a Join that its workers share.
type shared struct {
	pool     *mem.Pool
	disk     *spill.Disk
	scratch  Scratch
	log      *slog.Logger
	budget   int64 // of a unit
	maxMatch int64
	f        Filter
	drop     atomic.Bool  // the test hook's
	subparts atomic.Int64 // join-<n> directories made
}

// joiner is a worker of a Join. Its methods run on the worker's goroutine.
type joiner struct {
	*shared
	ctx    context.Context // of the task in progress
	out    *batcher
	counts [2]uint64
	stats  Stats
}

// task is a unit of the plan for a worker to join.
type task func(*joiner) error

// tasks returns the units of the plan of parts' buckets, then the empty-key
// unit if the run has empty keys.
func (sh *shared) tasks(parts *buffer.Partitions) []task {
	sizes := bucketSizes(parts)
	var tasks []task
	for _, u := range Plan(sizes, sh.budget) {
		tasks = append(tasks, func(j *joiner) error { return j.joinPlanned(parts, sizes, u, 0) })
	}
	if parts.Empty(buffer.Left).Records()+parts.Empty(buffer.Right).Records() > 0 {
		tasks = append(tasks, func(j *joiner) error { return j.emitEmpty(parts) })
	}
	return tasks
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
	if s.next >= s.failed {
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
	if err == nil || i > s.failed {
		return
	}
	s.failed, s.err = i, err
	for k, cancel := range s.cancels {
		if k > i {
			cancel(err)
		}
	}
}

// bucketSizes returns the record bytes of each bucket of parts.
func bucketSizes(parts *buffer.Partitions) []int64 {
	sizes := make([]int64, parts.Buckets())
	for i := range sizes {
		sizes[i] = parts.Bucket(buffer.Left, uint32(i)).Bytes() + parts.Bucket(buffer.Right, uint32(i)).Bytes()
	}
	return sizes
}

// joinBuckets joins the buckets of parts, which are sub-buckets of depth
// depth, one unit after another.
func (j *joiner) joinBuckets(parts *buffer.Partitions, depth int) error {
	sizes := bucketSizes(parts)
	for _, u := range Plan(sizes, j.budget) {
		if err := j.joinPlanned(parts, sizes, u, depth); err != nil {
			return err
		}
	}
	return nil
}

// joinPlanned joins unit u of parts, whose buckets have sizes and are of
// depth depth: re-partitioned if it is an oversized bucket, else loaded.
func (j *joiner) joinPlanned(parts *buffer.Partitions, sizes []int64, u Unit, depth int) error {
	if u.Hi-u.Lo == 1 && sizes[u.Lo]*ExpansionFactor > j.budget && depth < MaxDepth {
		return j.oversized(parts, u.Lo, depth)
	}
	return j.load(bucketsOf(parts, u.Lo, u.Hi), j.joinUnit)
}

// bucketsOf returns the buffers of buckets lo to hi-1, left before right.
func bucketsOf(parts *buffer.Partitions, lo, hi uint32) []sided {
	var bufs []sided
	for s := range 2 {
		for i := lo; i < hi; i++ {
			bufs = append(bufs, sided{parts.Bucket(buffer.Side(s), i), buffer.Side(s)})
		}
	}
	return bufs
}

// sided is a buffer and its side.
type sided struct {
	b    *buffer.Buffer
	side buffer.Side
}

// pinned is a pinned buffer and the records it held when it was pinned.
type pinned struct {
	b     *buffer.Buffer
	side  buffer.Side
	files []spill.File
	mem   [][]byte
}

// pin pins bufs. It returns them with the bytes of their spill files and
// of their in-memory chunks, and their number of records.
func pin(bufs []sided) (pins []pinned, inFiles, inMemory, records int64) {
	for _, b := range bufs {
		files, mem := b.b.Pin()
		pins = append(pins, pinned{b.b, b.side, files, mem})
		for _, f := range files {
			inFiles += f.Bytes
		}
		for _, c := range mem {
			inMemory += int64(len(c))
		}
		records += b.b.Records()
	}
	return pins, inFiles, inMemory, records
}

func unpin(pins []pinned) {
	for _, p := range pins {
		p.b.Unpin()
	}
}

// load loads bufs, the left buffers before the right ones, and calls fn
// with their chunks in that order, each buffer's in ingest order. It pins
// the buffers and reserves what loading adds to the memory they hold, their
// spill files once read and an index entry per record, so that the
// reservation doesn't spill the buffers it is about to read. The results fn
// adds are sent before the memory is released.
func (j *joiner) load(bufs []sided, fn func(chunks []chunk, records int64) error) error {
	pins, inFiles, inMemory, records := pin(bufs)
	need := inFiles + records*entrySize
	if err := j.pool.Reserve(need); err != nil {
		// Pinned, the buffers hold their chunks' full capacity; spilled,
		// they need only their bytes back. So reserve as if every record
		// were in a file, which may spill the buffers themselves, and give
		// back the share of the chunks still in memory once pinned again.
		unpin(pins)
		upper := need + inMemory
		if err := j.pool.Reserve(upper); err != nil {
			return err
		}
		pins, inFiles, inMemory, _ = pin(bufs)
		need = inFiles + records*entrySize
		j.pool.Release(upper - need)
	}
	defer func() { unpin(pins) }()
	defer j.pool.Release(need)

	working := need
	for _, p := range pins {
		for _, c := range p.mem {
			working += int64(cap(c))
		}
	}
	j.stats.unit(inFiles+inMemory, working)

	var chunks []chunk
	for _, p := range pins {
		for _, f := range p.files {
			if err := j.ctx.Err(); err != nil {
				return context.Cause(j.ctx)
			}
			data, err := j.disk.Read(f)
			if err != nil {
				if j.ctx.Err() != nil { // the run was deleted, its files with it
					return context.Cause(j.ctx)
				}
				return err
			}
			chunks = append(chunks, chunk{data: data, side: p.side})
		}
		for _, c := range p.mem {
			chunks = append(chunks, chunk{data: c, side: p.side})
		}
	}
	if err := fn(chunks, records); err != nil {
		return err
	}
	return j.out.flush()
}

// joinUnit indexes, sorts and emits the records of one unit.
func (j *joiner) joinUnit(data []chunk, records int64) error {
	// Left chunks come before right ones, so for one key the entries of a
	// lower chunk index are left's, and within a side they are in ingest
	// order. A file larger than an offset can address is split in parts.
	var chunks []chunk
	entries := make([]entry, 0, records)
	for _, d := range data {
		start := 0
		chunks = append(chunks, d)
		for off := 0; off < len(d.data); {
			if off-start > math.MaxUint32 {
				start = off
				chunks = append(chunks, chunk{data: d.data[start:], side: d.side})
			}
			hash, _, _, next := buffer.Decode(d.data, off)
			entries = append(entries, entry{hash: hash, chunk: uint32(len(chunks) - 1), off: uint32(off - start)})
			off = next
		}
	}
	key := func(e entry) []byte {
		_, k, _, _ := buffer.Decode(chunks[e.chunk].data, int(e.off))
		return k
	}
	value := func(e entry) []byte {
		_, _, v, _ := buffer.Decode(chunks[e.chunk].data, int(e.off))
		return v
	}
	slices.SortFunc(entries, func(a, b entry) int {
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		if c := bytes.Compare(key(a), key(b)); c != 0 {
			return c
		}
		return cmp.Or(cmp.Compare(a.chunk, b.chunk), cmp.Compare(a.off, b.off))
	})

	f := j.f
	for i, groups := 0, 0; i < len(entries); groups++ {
		// A unit whose results are all filtered out sends nothing, so ctx
		// is checked here too, not only by flush.
		if groups%4096 == 0 && j.ctx.Err() != nil {
			return context.Cause(j.ctx)
		}
		k := key(entries[i])
		end := i + 1
		for end < len(entries) && entries[end].hash == entries[i].hash && bytes.Equal(key(entries[end]), k) {
			end++
		}
		group := entries[i:end]
		if j.drop.Load() && j.drop.CompareAndSwap(true, false) {
			group = group[1:]
		}
		nl := 0
		for nl < len(group) && chunks[group[nl].chunk].side == buffer.Left {
			nl++
		}
		j.counts[buffer.Left] += uint64(nl)
		j.counts[buffer.Right] += uint64(len(group) - nl)
		switch {
		case nl > 0 && nl < len(group):
			if j.maxMatch > 0 {
				size := matchKeySize(len(k))
				for _, e := range group {
					size += matchValueSize(len(value(e)))
				}
				if size > j.maxMatch {
					return hotMatch(k, uint64(nl), uint64(len(group)-nl), size, j.maxMatch)
				}
			}
			if f[reconv1.JoinState_JOIN_STATE_BOTH] {
				m := &reconv1.Match{Key: k, Left: make([][]byte, 0, nl), Right: make([][]byte, 0, len(group)-nl)}
				for _, e := range group[:nl] {
					m.Left = append(m.Left, value(e))
				}
				for _, e := range group[nl:] {
					m.Right = append(m.Right, value(e))
				}
				if err := j.out.add(&reconv1.JoinResult{Result: &reconv1.JoinResult_Both{Both: m}}); err != nil {
					return err
				}
			}
		default:
			for _, e := range group {
				if err := j.out.oneSided(f, chunks[e.chunk].side, k, value(e)); err != nil {
					return err
				}
			}
		}
		i = end
	}
	return nil
}

// emitEmpty streams the records of parts' empty-key partitions, left then
// right, as one-sided results in ingest order. Their files are read one
// buffer at a time, so the partitions needn't fit in memory, however many
// records they hold.
func (j *joiner) emitEmpty(parts *buffer.Partitions) error {
	bufs := []sided{{parts.Empty(buffer.Left), buffer.Left}, {parts.Empty(buffer.Right), buffer.Right}}
	read := j.readSize()
	pins, reserved, err := j.pinFor(bufs, 0, read)
	if err != nil {
		return err
	}
	defer unpin(pins)
	defer j.pool.Release(reserved)
	err = j.each(pins, read, func(c chunk) error {
		for off := 0; off < len(c.data); {
			_, k, v, next := buffer.Decode(c.data, off)
			off = next
			j.counts[c.side]++
			if !c.streamed || !j.f.oneSided(c.side) {
				if err := j.out.oneSided(j.f, c.side, k, v); err != nil {
					return err
				}
				continue
			}
			// The batch outlives the read buffer, so it holds clones, which
			// the output batch's reservation covers up to its limit. A
			// record larger than that would be sent in a batch of its own
			// anyway: it is sent straight from the read buffer, which holds
			// it in reserved memory, before the buffer is reused.
			alone := int64(len(k)+len(v)) > j.out.limit
			if !alone {
				k, v = bytes.Clone(k), bytes.Clone(v)
			}
			if err := j.out.oneSided(j.f, c.side, k, v); err != nil {
				return err
			}
			if alone {
				if err := j.out.flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return j.out.flush()
}

// batcher collects a worker's results into JoinResponses of about limit
// bytes, which it hands to the sender.
type batcher struct {
	ctx   context.Context // of the worker's task
	sends chan<- sendRequest
	sent  chan error // the sender's reply
	limit int64
	batch []*reconv1.JoinResult
	size  int64
}

// sendRequest asks the sender to send resp, and to reply on sent with the
// send's error.
type sendRequest struct {
	resp *reconv1.JoinResponse
	sent chan<- error
}

func (b *batcher) oneSided(f Filter, side buffer.Side, key, value []byte) error {
	if !f.oneSided(side) {
		return nil
	}
	rec := &reconv1.Record{Key: key, Value: value}
	if side == buffer.Left {
		return b.add(&reconv1.JoinResult{Result: &reconv1.JoinResult_LeftOnly{LeftOnly: rec}})
	}
	return b.add(&reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{RightOnly: rec}})
}

// add adds r to the batch, first sending the batch if r would take it past
// the limit.
func (b *batcher) add(r *reconv1.JoinResult) error {
	n := proto.Size(r)
	size := int64(protowire.SizeTag(1) + protowire.SizeBytes(n))
	if len(b.batch) > 0 && b.size+size > b.limit {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.batch = append(b.batch, r)
	b.size += size
	return nil
}

// flush sends the batch, if it holds any results, once ctx is checked, and
// returns once it is sent.
func (b *batcher) flush() error {
	if err := b.ctx.Err(); err != nil {
		return context.Cause(b.ctx)
	}
	if len(b.batch) == 0 {
		return nil
	}
	resp := &reconv1.JoinResponse{Results: b.batch}
	b.batch, b.size = nil, 0
	select {
	case b.sends <- sendRequest{resp, b.sent}:
	case <-b.ctx.Done():
		return context.Cause(b.ctx)
	}
	return <-b.sent
}
