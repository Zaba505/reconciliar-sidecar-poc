package join

import (
	"context"
	"errors"
	"unsafe"

	"google.golang.org/protobuf/encoding/protowire"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/record"
)

// sendRequest asks the sender to send resp, and to reply on sent with the
// send's error.
type sendRequest struct {
	resp *reconv1.JoinResponse
	sent chan<- error
}

const (
	// maxChunk is the largest arena chunk: a sixteenth of a 1 MiB batch.
	// A chunk is half a smaller batch, down to minChunk. Bytes larger than
	// a quarter of a chunk get a chunk of their own, so a chunk wastes at
	// most a quarter.
	maxChunk = 64 << 10
	minChunk = 4 << 10
	// maxSlab is the most structs a slab allocates at once: a 1 MiB batch
	// of 200-byte results takes ten such chunks. A smaller batch takes
	// smaller chunks, down to minSlab.
	maxSlab = 512
	minSlab = 16
)

// chunkSizeOf returns the arena chunk size of batches of limit bytes.
func chunkSizeOf(limit int64) int { return int(min(max(limit/2, minChunk), maxChunk)) }

// slabSizeOf returns the slab chunk of batches of limit bytes.
func slabSizeOf(limit int64) int { return int(min(max(limit/256, minSlab), maxSlab)) }

// emitterBytes estimates what an emitter of batches of limit bytes takes
// besides its arena's batch and its Match: a chunk's slack, the structs
// of a batch, which a batch is sent before they reach the limit, and their
// slices.
func emitterBytes(limit int64) int64 { return int64(chunkSizeOf(limit)) + 2*limit }

// chunk is a piece of an arena.
type chunk struct {
	buf []byte // all of it
	n   int    // in use
}

// slab hands out structs from chunks that are reused after each send.
type slab[T any] struct {
	chunks [][]T
	c, i   int
}

func (s *slab[T]) get(e *emitter) (*T, error) {
	if s.c == len(s.chunks) {
		if err := e.reserve(int64(e.slabSize) * int64(unsafe.Sizeof(*new(T)))); err != nil {
			return nil, err
		}
		s.chunks = append(s.chunks, make([]T, e.slabSize))
	}
	p := &s.chunks[s.c][s.i]
	if s.i++; s.i == len(s.chunks[s.c]) {
		s.c, s.i = s.c+1, 0
	}
	return p, nil
}

func (s *slab[T]) reset() { s.c, s.i = 0, 0 }

// emitter turns the records of a worker's merge into results, batches them
// into JoinResponses of about limit bytes, and hands those to the sender.
//
// Keys and values are copied from the merge's sources into the arena: a
// list of chunks holding the batch's results and then the group in
// progress, the key whose left records are held until its first right
// record, or the next key, shows what they are. Nothing in the arena moves,
// so a group needs its size once. Once a batch is sent, the chunks before
// the group's are used again, and the protobuf structs too.
//
// Every byte the emitter holds is reserved from the pool: the arena, the
// structs, and the slices of results and values.
type emitter struct {
	pool  *mem.Pool
	ctx   context.Context
	f     Filter
	limit int64
	sends chan<- sendRequest
	sent  chan error

	keep     int64 // reserved for the emitter's life
	reserved int64 // from the pool
	credit   int64 // of reserved, not in use

	chunkSize int      // of the arena's chunks
	slabSize  int      // structs per slab chunk
	chunks    []*chunk // the batch's, then the group's from gfirst
	spare     []*chunk // emptied chunks of chunkSize

	results []*reconv1.JoinResult
	wire    int64 // the batch's encoded size
	structs int64 // the memory of the batch's structs
	batches int64

	jr      slab[reconv1.JoinResult]
	both    slab[reconv1.JoinResult_Both]
	match   slab[reconv1.Match]
	lo      slab[reconv1.JoinResult_LeftOnly]
	ro      slab[reconv1.JoinResult_RightOnly]
	rec     slab[reconv1.Record]
	pending int64 // reserved for the batch's Matches' value slices

	// The group in progress: one key, its left values, then its right ones.
	active bool
	hold   bool // its values are kept: a result may hold them
	ghash  uint64
	gkey   []byte
	gvals  [][]byte
	gfirst int // the chunk its key is in
	nl, nr uint64
	gsize  int64 // the encoded size of its Match
	garena int64 // the arena bytes it took, the tails of chunks it left included
}

// newEmitter returns an emitter that starts with prepaid bytes, which the
// caller reserved and the emitter releases.
func newEmitter(pool *mem.Pool, f Filter, limit int64, sends chan<- sendRequest, prepaid int64) *emitter {
	return &emitter{
		pool: pool, f: f, limit: limit, sends: sends, sent: make(chan error, 1),
		keep: prepaid, reserved: prepaid, credit: prepaid,
		chunkSize: chunkSizeOf(limit), slabSize: slabSizeOf(limit),
	}
}

// reserve takes n bytes of the emitter's reservation, reserving more from
// the pool if it must.
func (e *emitter) reserve(n int64) error {
	if e.credit < n {
		if err := e.pool.Reserve(n - e.credit); err != nil {
			return err
		}
		e.reserved += n - e.credit
		e.credit = n
	}
	e.credit -= n
	return nil
}

// free gives back n bytes taken with reserve, releasing to the pool what
// the emitter holds beyond keep.
func (e *emitter) free(n int64) {
	e.credit += n
	if extra := min(e.credit, e.reserved-e.keep); extra > 0 {
		e.pool.Release(extra)
		e.reserved -= extra
		e.credit -= extra
	}
}

// release gives the emitter's memory back at the end of a Join.
func (e *emitter) release() {
	e.pool.Release(e.reserved)
	e.reserved, e.credit = 0, 0
	e.chunks, e.spare = nil, nil
}

// alloc returns n bytes of the arena. If the pool can't cover a chunk and
// the batch holds results, it sends the batch, which frees chunks, and
// tries again.
func (e *emitter) alloc(n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	size := e.chunkSize
	if k := len(e.chunks); k > 0 {
		c := e.chunks[k-1]
		if len(c.buf) == size && len(c.buf)-c.n >= n && n <= size/4 {
			p := c.buf[c.n : c.n+n : c.n+n]
			c.n += n
			e.garena += int64(n)
			return p, nil
		}
		if e.active && len(c.buf) == size && n <= size/4 {
			e.garena += int64(len(c.buf) - c.n) // the tail left behind
		}
	}
	e.garena += int64(n)
	var c *chunk
	switch {
	case n > size/4:
		if err := e.retry(func() error { return e.reserve(int64(n)) }); err != nil {
			return nil, err
		}
		c = &chunk{buf: make([]byte, n)}
	case len(e.spare) > 0:
		c = e.spare[len(e.spare)-1]
		e.spare = e.spare[:len(e.spare)-1]
	default:
		if err := e.retry(func() error { return e.reserve(int64(size)) }); err != nil {
			return nil, err
		}
		c = &chunk{buf: make([]byte, size)}
	}
	e.chunks = append(e.chunks, c)
	c.n = n
	return c.buf[:n:n], nil
}

// retry runs fn, and if it fails for want of memory while the batch holds
// results, sends the batch and runs fn again.
func (e *emitter) retry(fn func() error) error {
	err := fn()
	if err == nil || !errors.Is(err, mem.ErrExhausted) || len(e.results) == 0 {
		return err
	}
	if err := e.flush(); err != nil {
		return err
	}
	return fn()
}

// lastChunk returns the index of the arena's last chunk.
func (e *emitter) lastChunk() int { return len(e.chunks) - 1 }

// start begins the group of key, holding its values if hold.
func (e *emitter) start(hash uint64, key []byte, hold bool) error {
	k, err := e.alloc(len(key))
	if err != nil {
		return err
	}
	copy(k, key)
	e.active, e.hold, e.ghash, e.gkey = true, hold, hash, k
	e.gfirst = max(e.lastChunk(), 0)
	e.gvals, e.nl, e.nr = e.gvals[:0], 0, 0
	e.gsize = bytesField(len(key))
	e.garena = int64(len(key))
	return nil
}

// held returns the memory the group would hold with one more value of n
// bytes: its arena bytes, the value and a chunk's tail it may leave, and
// its slices, three per value for the copy when the slice grows.
func (e *emitter) held(n int) int64 {
	return e.garena + int64(n) + int64(e.chunkSize/4) + 3*sliceBytes*int64(len(e.gvals)+1)
}

// key returns the key of the group in progress.
func (e *emitter) key() []byte { return e.gkey }

// matching reports whether the group in progress is that of a record of
// hash and key.
func (e *emitter) matching(hash uint64, key []byte) bool {
	return e.active && e.ghash == hash && string(e.gkey) == string(key)
}

// add adds the value of src's head, n bytes, to the group, reading it from
// src if the group holds its values.
func (e *emitter) add(side record.Side, n int, src merge.Source) error {
	if side == record.Left {
		e.nl++
	} else {
		e.nr++
		if e.nl > 0 && e.hold && !e.f[reconv1.JoinState_JOIN_STATE_BOTH] {
			// A Match, which the filter leaves out: nothing of it is sent.
			e.hold = false
			e.gvals = e.gvals[:0]
		}
	}
	e.gsize += bytesField(n)
	if !e.hold {
		return nil
	}
	v, err := e.alloc(n)
	if err != nil {
		return err
	}
	if err := src.ReadValue(v); err != nil {
		return err
	}
	if len(e.gvals) == cap(e.gvals) {
		size := max(16, 2*cap(e.gvals))
		if err := e.reserve(int64(size) * sliceBytes); err != nil {
			return err
		}
		old := cap(e.gvals)
		e.gvals = append(make([][]byte, 0, size), e.gvals...)
		e.free(int64(old) * sliceBytes)
	}
	e.gvals = append(e.gvals, v)
	return nil
}

// sliceBytes is the size of a value's slice in a group.
const sliceBytes = int64(unsafe.Sizeof([]byte(nil)))

// finish emits the group: a Match if it has both sides, else a one-sided
// result per held value. maxMatch is the most a Match may be; a larger one
// is a HotKeyError. A batch the group fills is sent once the group is
// done, so that a batch holds at most its limit, or one result larger
// than that, and nothing of the group is kept for it.
func (e *emitter) finish(maxMatch int64) error {
	if !e.active {
		return nil
	}
	// The group stays active while it is emitted, so that a batch sent
	// meanwhile keeps its chunks.
	err := e.emitGroup(maxMatch)
	e.active = false
	if err == nil && e.full() {
		err = e.flush()
	}
	return err
}

func (e *emitter) emitGroup(maxMatch int64) error {
	if e.nl > 0 && e.nr > 0 {
		if e.gsize > maxMatch {
			return hotKey(e.gkey, e.nl, e.nr, e.gsize, maxMatch)
		}
		if !e.hold {
			return nil
		}
		return e.emitMatch()
	}
	side := record.Left
	if e.nr > 0 {
		side = record.Right
	}
	if !e.hold || !e.f.oneSided(side) {
		// Held in case it was a Match, which the filter wants, and it
		// isn't.
		return nil
	}
	for i := range e.gvals {
		if err := e.emitRecord(side, i); err != nil {
			return err
		}
	}
	return nil
}

func sizeVarint(n int64) int64 { return int64(protowire.SizeVarint(uint64(n))) }

// bytesField is the encoded size of a bytes field or a message of n bytes
// whose tag takes one byte, as every field of the API's results does.
func bytesField(n int) int64 { return 1 + sizeVarint(int64(n)) + int64(n) }

// optField is the size of a bytes field that is left out when empty.
func optField(n int) int64 {
	if n == 0 {
		return 0
	}
	return bytesField(n)
}

// resultBytes is the memory a result's structs take: the larger of a
// Match's and a one-sided result's, and its slot in the batch.
var resultBytes = int64(unsafe.Sizeof(reconv1.JoinResult{})) + 8 + max(
	int64(unsafe.Sizeof(reconv1.JoinResult_Both{})+unsafe.Sizeof(reconv1.Match{})),
	int64(unsafe.Sizeof(reconv1.JoinResult_LeftOnly{})+unsafe.Sizeof(reconv1.Record{})))

// full reports whether the batch has reached its limit, in encoded bytes or
// in the memory of its structs, which for small results is several times
// their encoding.
func (e *emitter) full() bool { return e.wire >= e.limit || e.structs >= e.limit }

// room sends the batch if a result of size bytes would take it past the
// limit, in encoded bytes or in the memory of its structs.
func (e *emitter) room(size int64) error {
	if len(e.results) > 0 && (e.wire+size > e.limit || e.structs+resultBytes > e.limit) {
		return e.flush()
	}
	return nil
}

// push adds a result of size bytes to the batch.
func (e *emitter) push(r *reconv1.JoinResult, size int64) error {
	if len(e.results) == cap(e.results) {
		n := max(256, 2*cap(e.results))
		if err := e.reserve(int64(n) * 8); err != nil {
			return err
		}
		e.results = append(make([]*reconv1.JoinResult, 0, n), e.results...)
	}
	e.results = append(e.results, r)
	e.wire += size
	e.structs += resultBytes
	return nil
}

// build builds a result with fn and adds it to the batch. If that fails for
// want of memory, it sends the batch, which frees the structs, and builds
// it again.
func (e *emitter) build(size int64, fn func() (*reconv1.JoinResult, error)) error {
	if err := e.room(size); err != nil {
		return err
	}
	return e.retry(func() error {
		r, err := fn()
		if err == nil {
			err = e.push(r, size)
		}
		return err
	})
}

// emitMatch emits the group as a Match. The Match takes the group's slice
// of values, whose memory is given back once the batch is sent.
func (e *emitter) emitMatch() error {
	size := bytesField(int(bytesField(int(e.gsize))))
	if err := e.room(size); err != nil {
		return err
	}
	return e.retry(func() error {
		r, err := e.jr.get(e)
		if err != nil {
			return err
		}
		b, err := e.both.get(e)
		if err != nil {
			return err
		}
		m, err := e.match.get(e)
		if err != nil {
			return err
		}
		nl := int(e.nl)
		m.Key, m.Left, m.Right = e.gkey, e.gvals[:nl:nl], e.gvals[nl:]
		b.Both = m
		r.Result = b
		if err := e.push(r, size); err != nil {
			return err
		}
		e.pending += int64(cap(e.gvals)) * sliceBytes
		e.gvals = nil
		return nil
	})
}

// emitRecord emits value i of the group as a one-sided result of side.
func (e *emitter) emitRecord(side record.Side, i int) error {
	key, value := e.gkey, e.gvals[i]
	size := bytesField(int(bytesField(int(optField(len(key)) + optField(len(value))))))
	return e.build(size, func() (*reconv1.JoinResult, error) {
		r, err := e.jr.get(e)
		if err != nil {
			return nil, err
		}
		rec, err := e.rec.get(e)
		if err != nil {
			return nil, err
		}
		rec.Key, rec.Value = key, value
		if side == record.Left {
			w, err := e.lo.get(e)
			if err != nil {
				return nil, err
			}
			w.LeftOnly = rec
			r.Result = w
		} else {
			w, err := e.ro.get(e)
			if err != nil {
				return nil, err
			}
			w.RightOnly = rec
			r.Result = w
		}
		return r, nil
	})
}

// flush sends the batch, if it holds results, once ctx is checked, and
// waits until it is sent: its memory is then free to be used again, all
// but the chunks of the group in progress.
func (e *emitter) flush() error {
	if err := e.ctx.Err(); err != nil {
		return context.Cause(e.ctx)
	}
	if len(e.results) == 0 {
		return nil
	}
	resp := &reconv1.JoinResponse{Results: e.results}
	select {
	case e.sends <- sendRequest{resp, e.sent}:
	case <-e.ctx.Done():
		return context.Cause(e.ctx)
	}
	if err := <-e.sent; err != nil {
		return err
	}
	e.batches++
	e.wire, e.structs = 0, 0
	clear(e.results)
	e.results = e.results[:0]
	e.jr.reset()
	e.both.reset()
	e.match.reset()
	e.lo.reset()
	e.ro.reset()
	e.rec.reset()
	e.free(e.pending)
	e.pending = 0

	done := e.chunks
	if e.active {
		done = e.chunks[:e.gfirst]
	}
	for _, c := range done {
		e.recycle(c)
	}
	e.chunks = append(e.chunks[:0], e.chunks[len(done):]...)
	e.gfirst = 0
	return nil
}

// recycle keeps a chunk the batch no longer uses for the next results, or
// frees it: a chunk of its own, or one more than a batch needs.
func (e *emitter) recycle(c *chunk) {
	if size := e.chunkSize; len(c.buf) == size && int64(len(e.spare)+1)*int64(size) <= e.limit+int64(size) {
		c.n = 0
		e.spare = append(e.spare, c)
		return
	}
	e.free(int64(len(c.buf)))
}
