// Package mem accounts for the sidecar's memory against one global budget
// (§8.2). Every buffer chunk, decoded message, join working set and output
// batch reserves its size from the Pool before it is allocated and releases
// it once it is freed.
//
// When a reservation doesn't fit, the reserving goroutine spills ingest
// buffers to disk, largest first, until usage is below a low-water mark,
// and then retries the reservation once.
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

// Spillable is memory the pool may write to disk to free it: an ingest
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

	mu   sync.Mutex // guards the accounting; never held during I/O
	used int64
	peak int64

	// spillMu serializes spilling, so that goroutines that fail to reserve
	// together don't each spill down to the low-water mark.
	spillMu sync.Mutex
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

// Used returns the bytes currently reserved.
func (p *Pool) Used() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.used
}

// Peak returns the most bytes that were ever reserved at once.
func (p *Pool) Peak() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// Reserve reserves n bytes. If they don't fit, it spills the largest
// spillable buffers until usage is below the low-water mark, or low enough
// for n to fit, and retries once. It fails with an error wrapping
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

func (p *Pool) tryReserve(n int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
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

// spill spills the largest spillable buffers until usage is at most
// target, or nothing is left to spill. It stops at the first failed spill.
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
		if p.Used() <= target {
			return nil
		}
		if err := c.s.Spill(); err != nil {
			return err
		}
	}
	return nil
}

// Release returns n bytes reserved earlier.
func (p *Pool) Release(n int64) {
	if n < 0 {
		panic(fmt.Sprintf("mem: Release(%d)", n))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > p.used {
		panic(fmt.Sprintf("mem: Release(%d) with only %d reserved", n, p.used))
	}
	p.used -= n
}
