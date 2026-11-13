package gen

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/zeebo/xxh3"

	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

// maxWorkers caps a Verifier's workers: 8 regenerate values faster than a
// Join sends them.
const maxWorkers = 8

// roundPerWorker is how many results a round holds per worker, enough
// that starting a round costs little.
const roundPerWorker = 512

// Verifier checks a Join's results against a Config as they stream by. It
// keeps 4 bits per key: one for its Match, and one for each one-sided
// record.
//
// Regenerating values is slower than a Join sends them, so Add only parses
// a result's key and queues the result for one of W workers. The workers
// check the queued results in rounds: while they check one round, Add
// queues the next. Key k belongs to worker (k/16) mod W, so no two workers
// share a word of the bitmap. A round's goroutines exit when it is
// checked, so a Verifier that is dropped without Finish, after an error
// say, leaves nothing running.
//
// A Verifier is not safe for concurrent use: call Add and Finish from one
// goroutine.
type Verifier struct {
	c       Config
	seen    []uint64 // 4 bits per key, 16 keys per word
	workers []*worker
	queued  [][]result     // the next round, per worker
	n       int            // results queued
	round   sync.WaitGroup // the round being checked
	failure atomic.Pointer[error]
	done    bool // Finish was called
}

// result is a JoinResult taken apart: key k's Match, or one of its
// records on side.
type result struct {
	k     int
	side  harness.Side // the record's side, 0 for a Match
	value []byte
	match *reconv1.Match
}

// worker checks the results of its keys with its own generator, and
// counts the records it saw.
type worker struct {
	v      *Verifier
	g      *ValueGen
	buf    []byte   // a regenerated value
	batch  []result // the round being checked
	counts [2]uint64
}

// NewVerifier returns a Verifier of c's run with a worker per CPU, up to
// 8. It panics if c is not valid.
func NewVerifier(c Config) *Verifier {
	return newVerifier(c, min(runtime.GOMAXPROCS(0), maxWorkers))
}

// newVerifier returns a Verifier of c's run with the given number of
// workers.
func newVerifier(c Config, workers int) *Verifier {
	v := &Verifier{c: c, seen: make([]uint64, (c.Keys+15)/16), queued: make([][]result, workers)}
	for range workers {
		v.workers = append(v.workers, &worker{v: v, g: c.ValueGen()})
	}
	return v
}

// Add checks one result: it checks the key now and queues the rest. It
// returns the first error found so far in any result, so that a Join can
// stop at it, and Finish returns that error too. r must not change until
// Finish returns, since a worker may still be reading it.
func (v *Verifier) Add(r *reconv1.JoinResult) error {
	if err := v.err(); err != nil {
		return err
	}
	if v.done {
		return errors.New("gen: Verifier.Add after Finish")
	}
	var (
		res result
		key []byte
	)
	switch x := r.GetResult().(type) {
	case *reconv1.JoinResult_LeftOnly:
		res.side, key, res.value = harness.Left, x.LeftOnly.GetKey(), x.LeftOnly.GetValue()
	case *reconv1.JoinResult_RightOnly:
		res.side, key, res.value = harness.Right, x.RightOnly.GetKey(), x.RightOnly.GetValue()
	case *reconv1.JoinResult_Both:
		res.match, key = x.Both, x.Both.GetKey()
	default:
		return v.fail(errors.New("a result with no state"))
	}
	k, ok := v.c.ParseKey(key)
	if !ok {
		return v.fail(fmt.Errorf("unexpected key %q", key))
	}
	res.k = k
	i := k / 16 % len(v.workers)
	v.queued[i] = append(v.queued[i], res)
	if v.n++; v.n == roundPerWorker*len(v.workers) {
		v.start()
	}
	return v.err()
}

// start waits for the round being checked, then starts checking the
// results queued since.
func (v *Verifier) start() {
	v.round.Wait()
	for i, w := range v.workers {
		clear(w.batch) // let the checked results be collected
		w.batch, v.queued[i] = v.queued[i], w.batch[:0]
		if len(w.batch) > 0 {
			v.round.Go(w.check)
		}
	}
	v.n = 0
}

// fail records err unless an error was recorded already, and returns the
// first one.
func (v *Verifier) fail(err error) error {
	v.failure.CompareAndSwap(nil, &err)
	return v.err()
}

// err returns the first error found, or nil.
func (v *Verifier) err() error {
	if p := v.failure.Load(); p != nil {
		return *p
	}
	return nil
}

// Finish waits for the queued results to be checked, then checks that
// every key's results were all seen and that they hold each side's total
// of records. If any result was wrong, it returns the first error found
// instead. It is safe to call after an error, and more than once.
func (v *Verifier) Finish() error {
	if !v.done {
		v.done = true
		if v.err() == nil {
			v.start()
		}
		v.round.Wait()
	}
	if err := v.err(); err != nil {
		return err
	}
	var (
		missing       int
		first         = -1
		want, counted [2]uint64
	)
	for k := range v.c.Keys {
		l, r := v.c.Counts(k)
		want[0] += uint64(l)
		want[1] += uint64(r)
		var bits uint64
		switch {
		case l > 0 && r > 0:
			bits = 1
		default:
			bits = (uint64(1)<<max(l, r) - 1) << 1
		}
		if got := v.seen[k/16] >> (k % 16 * 4) & 0xf; got != bits {
			missing++
			if first < 0 {
				first = k
			}
		}
	}
	if missing > 0 {
		return fmt.Errorf("%d keys are missing results, the first key %d", missing, first)
	}
	for _, w := range v.workers {
		counted[0] += w.counts[0]
		counted[1] += w.counts[1]
	}
	if counted != want {
		return fmt.Errorf("counted %d left and %d right records, want %d and %d", counted[0], counted[1], want[0], want[1])
	}
	return nil
}

// check checks the worker's round, and stops at the first error any
// worker finds.
func (w *worker) check() {
	for _, r := range w.batch {
		if w.v.failure.Load() != nil {
			return
		}
		if err := w.result(r); err != nil {
			w.v.fail(err)
			return
		}
	}
}

// result checks one result against its key's counts and values.
func (w *worker) result(r result) error {
	k := r.k
	l, rt := w.v.c.Counts(k)
	if r.match != nil {
		if l == 0 || rt == 0 {
			return fmt.Errorf("key %d: a Match, but it has %d left and %d right records", k, l, rt)
		}
		if err := w.values(k, harness.Left, r.match.GetLeft(), l); err != nil {
			return err
		}
		if err := w.values(k, harness.Right, r.match.GetRight(), rt); err != nil {
			return err
		}
		w.counts[0] += uint64(l)
		w.counts[1] += uint64(rt)
		return w.mark(k, 0)
	}
	n, other := l, rt
	if r.side == harness.Right {
		n, other = rt, l
	}
	if n == 0 || other > 0 {
		return fmt.Errorf("key %d: a %s-only result, but it has %d left and %d right records", k, sideName(r.side), l, rt)
	}
	j, err := w.value(k, r.side, r.value, n)
	if err != nil {
		return err
	}
	w.counts[r.side-1]++
	return w.mark(k, 1+j)
}

// mark sets bit b of key k, failing if it was set already. Only k's
// worker touches its word.
func (w *worker) mark(k, b int) error {
	seen := w.v.seen
	bit := uint64(1) << (k%16*4 + b)
	if seen[k/16]&bit != 0 {
		return fmt.Errorf("key %d: result %d seen twice", k, b)
	}
	seen[k/16] |= bit
	return nil
}

// values checks that vs are key k's n values on side, in any order.
func (w *worker) values(k int, side harness.Side, vs [][]byte, n int) error {
	if len(vs) != n {
		return fmt.Errorf("key %d: %d %s values, want %d", k, len(vs), sideName(side), n)
	}
	var seen [4]bool
	for _, val := range vs {
		j, err := w.value(k, side, val, n)
		if err != nil {
			return err
		}
		if seen[j] {
			return fmt.Errorf("key %d: %s value %d appears twice", k, sideName(side), j)
		}
		seen[j] = true
	}
	return nil
}

// value checks that val is one of key k's n values on side and returns its
// index j. Locate reads j from val, and val must equal the value
// regenerated from (side, k, j) byte for byte.
func (w *worker) value(k int, side harness.Side, val []byte, n int) (int, error) {
	if vk, j, ok := w.v.c.Locate(side, val); ok && vk == k && j < n {
		w.buf = w.g.Append(w.buf[:0], side, k, j)
		if bytes.Equal(val, w.buf) {
			return j, nil
		}
	}
	return 0, fmt.Errorf("key %d: a %s value that isn't one of its %d: %s", k, sideName(side), n, w.mismatch(k, side, val))
}

// mismatch says why val is not one of key k's values on side.
func (w *worker) mismatch(k int, side harness.Side, val []byte) string {
	c := &w.v.c
	vk, j, ok := c.Locate(side, val)
	if !ok {
		if ko, oj, found := c.Locate(other(side), val); found {
			return fmt.Sprintf("it is %s value %d of key %d", sideName(other(side)), oj, ko)
		}
		return fmt.Sprintf("its %d bytes name no %s record of the run", len(val), sideName(side))
	}
	if vk != k {
		return fmt.Sprintf("it is value %d of key %d", j, vk)
	}
	w.buf = w.g.Append(w.buf[:0], side, k, j)
	want := w.buf
	at := 0
	for at < min(len(val), len(want)) && val[at] == want[at] {
		at++
	}
	if at == len(val) && at == len(want) {
		// val is intact, but j is not below the key's count.
		return fmt.Sprintf("it is value %d", j)
	}
	return fmt.Sprintf("it differs from value %d at byte %d (%d bytes, want %d)", j, at, len(val), len(want))
}

func sideName(s harness.Side) string {
	if s == harness.Left {
		return "left"
	}
	return "right"
}

// other returns the side that isn't s.
func other(s harness.Side) harness.Side { return harness.Left + harness.Right - s }

// Fingerprint is an order-independent summary of a Join's results: the sum
// and XOR of the XXH3 of each canonical result, and their count.
type Fingerprint struct {
	Sum, Xor, Count uint64
}

// Add adds one result.
func (f *Fingerprint) Add(r *reconv1.JoinResult) {
	h := xxh3.Hash(oracle.Canonical(r))
	f.Sum += h
	f.Xor ^= h
	f.Count++
}
