// Package mem accounts for the sidecar's memory against one global budget
// (§8.2). Every ingest block, decoded message, join read buffer, arena and
// output batch reserves its size from the Pool before it is allocated and
// releases it once it is freed.
//
// The Pool also keeps idle buffers: blocks a spill emptied, kept for the
// next records instead of being left to the garbage collector. They stay
// reserved while they are idle, so the budget covers them, and every run
// sharing the pool can take them. A reservation that doesn't fit drops
// idle buffers first.
//
// When a reservation still doesn't fit, the reserving goroutine spills
// runs' buffers to disk, largest first, until usage, less the idle
// buffers, is below a low-water mark, and then retries the reservation
// once.
package mem

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrExhausted is returned by Reserve when the budget can't cover a
// reservation, even after spilling.
var ErrExhausted = errors.New("memory exhausted")

// lowWaterPercent is the share of the limit that spilling brings usage
// under, so that the next reservations don't each spill again.
const lowWaterPercent = 80

// Spillable is memory the pool may write to disk to free it: a run's
// buffer.
type Spillable interface {
	// SpillSize returns the bytes spilling would release now, or 0 when it
	// can't be spilled, for example because a join is reading it.
	SpillSize() int64
	// Spill writes the records to disk and releases their memory.
	Spill() error
}

// Pool is the global memory budget. It is safe for concurrent use.
type Pool struct {
	limit  int64
	source func() []Spillable

	mu    sync.Mutex // guards the accounting; never held during I/O
	used  int64
	peak  int64
	idle  []idleBuf
	idled int64 // reserved by idle

	// spillMu serializes spilling, so that goroutines that fail to reserve
	// together don't each spill down to the low-water mark.
	spillMu sync.Mutex
}

// idleBuf is an idle buffer and the bytes reserved for it.
type idleBuf struct {
	buf      []byte
	reserved int64
}

// NewPool returns a Pool with a budget of limit bytes.
func NewPool(limit int64) *Pool {
	return &Pool{limit: limit}
}

// SetSource makes the pool spill the buffers source returns when a
// reservation fails. It must be called before the pool is used
// concurrently.
func (p *Pool) SetSource(source func() []Spillable) {
	p.source = source
}

// Limit returns the budget.
func (p *Pool) Limit() int64 { return p.limit }

// Used returns the bytes currently reserved, idle buffers' included.
func (p *Pool) Used() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used
}

// Idle returns the bytes reserved for idle buffers.
func (p *Pool) Idle() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idled
}

// PutIdle keeps buf, for which reserved bytes are reserved, for TakeIdle.
// The reservation passes to the pool, which drops the buffer when a
// reservation needs the room.
func (p *Pool) PutIdle(buf []byte, reserved int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.idle = append(p.idle, idleBuf{buf, reserved})
	p.idled += reserved
}

// TakeIdle returns an idle buffer of exactly size bytes and the bytes
// reserved for it, whose reservation passes to the caller, or nil.
func (p *Pool) TakeIdle(size int) ([]byte, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.idle) - 1; i >= 0; i-- {
		if b := p.idle[i]; len(b.buf) == size {
			p.idle = slices.Delete(p.idle, i, i+1)
			p.idled -= b.reserved
			return b.buf, b.reserved
		}
	}
	return nil, 0
}

// DropIdle drops every idle buffer and releases its reservation.
func (p *Pool) DropIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropIdle(p.limit)
}

// dropIdle drops idle buffers until n more bytes fit, or none is left.
// p.mu must be held.
func (p *Pool) dropIdle(n int64) {
	for len(p.idle) > 0 && p.used+n > p.limit {
		b := p.idle[len(p.idle)-1]
		p.idle[len(p.idle)-1] = idleBuf{}
		p.idle = p.idle[:len(p.idle)-1]
		p.idled -= b.reserved
		p.used -= b.reserved
	}
}

// Peak returns the most bytes that were ever reserved at once.
func (p *Pool) Peak() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// Reserve reserves n bytes. If they don't fit, it drops idle buffers, and
// then spills the largest spillable buffers until usage is below the
// low-water mark, or low enough for n to fit, and retries once. It fails with an error wrapping
// ErrExhausted when n still doesn't fit, or with the error of a spill that
// failed.
func (p *Pool) Reserve(n int64) error {
	if n < 0 {
		panic(fmt.Sprintf("mem: Reserve(%d)", n))
	}
	if p.tryReserve(n) {
		return nil
	}
	if n > p.limit || p.source == nil {
		return p.exhausted(n)
	}
	p.spillMu.Lock()
	defer p.spillMu.Unlock()
	if p.tryReserve(n) { // another goroutine's spill made room
		return nil
	}
	err := p.spill(min(p.limit*lowWaterPercent/100, p.limit-n))
	if p.tryReserve(n) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.exhausted(n)
}

// TryReserve reserves n bytes if they fit once idle buffers are dropped,
// without spilling. It is for a Spillable, which can't Reserve while the
// pool spills it.
func (p *Pool) TryReserve(n int64) bool {
	if n < 0 {
		panic(fmt.Sprintf("mem: TryReserve(%d)", n))
	}
	return p.tryReserve(n)
}

func (p *Pool) tryReserve(n int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropIdle(n)
	if p.used+n > p.limit {
		return false
	}
	p.used += n
	p.peak = max(p.peak, p.used)
	return true
}

func (p *Pool) exhausted(n int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Errorf("%w: reserving %d bytes with %d of %d in use", ErrExhausted, n, p.used, p.limit)
}

// spill spills the largest spillable buffers until usage, less the idle
// buffers, is at most target, or nothing is left to spill. It stops at the
// first failed spill.
func (p *Pool) spill(target int64) error {
	type candidate struct {
		s    Spillable
		size int64
	}
	var cands []candidate
	for _, s := range p.source() {
		if size := s.SpillSize(); size > 0 {
			cands = append(cands, candidate{s, size})
		}
	}
	slices.SortStableFunc(cands, func(a, b candidate) int { return cmp.Compare(b.size, a.size) })
	for _, c := range cands {
		if p.inUse() <= target {
			return nil
		}
		if err := c.s.Spill(); err != nil {
			return err
		}
	}
	return nil
}

// Reclaimable returns the bytes a reservation could make room for without
// failing: the idle buffers', and what spilling would release now.
func (p *Pool) Reclaimable() int64 {
	n := p.Idle()
	if p.source != nil {
		for _, s := range p.source() {
			n += s.SpillSize()
		}
	}
	return n
}

// Free returns the bytes a reservation could have now: those not reserved,
// and those it could reclaim.
func (p *Pool) Free() int64 {
	return p.limit - p.Used() + p.Reclaimable()
}

// inUse returns the bytes reserved other than for idle buffers.
func (p *Pool) inUse() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used - p.idled
}

// Release returns n bytes reserved earlier.
func (p *Pool) Release(n int64) {
	if n < 0 {
		panic(fmt.Sprintf("mem: Release(%d)", n))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > p.used-p.idled {
		panic(fmt.Sprintf("mem: Release(%d) with only %d reserved", n, p.used-p.idled))
	}
	p.used -= n
}
