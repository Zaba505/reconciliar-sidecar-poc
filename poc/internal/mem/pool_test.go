package mem

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPool_accounts_reservations(t *testing.T) {
	p := NewPool(100)
	if err := p.Reserve(60); err != nil {
		t.Fatal(err)
	}
	if err := p.Reserve(40); err != nil {
		t.Fatal(err)
	}
	if err := p.Reserve(1); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Reserve past the limit: %v, want ErrExhausted", err)
	}
	if got := p.Used(); got != 100 {
		t.Errorf("Used = %d, want 100: a failed Reserve must reserve nothing", got)
	}
	p.Release(60)
	if err := p.Reserve(50); err != nil {
		t.Fatalf("Reserve after Release: %v", err)
	}
	p.Release(90)
	if got := p.Used(); got != 0 {
		t.Errorf("Used = %d, want 0", got)
	}
}

func TestPool_never_exceeds_its_limit_under_concurrency(t *testing.T) {
	const limit = 1000
	p := NewPool(limit)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				if err := p.Reserve(7); err != nil {
					continue
				}
				if u := p.Used(); u > limit {
					t.Errorf("Used = %d, above the limit", u)
				}
				p.Release(7)
			}
		})
	}
	wg.Wait()
	if got := p.Used(); got != 0 {
		t.Errorf("Used = %d after every release, want 0", got)
	}
}

func TestPool_Release_of_more_than_reserved_panics(t *testing.T) {
	p := NewPool(10)
	if err := p.Reserve(5); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Error("Release(6) with 5 reserved didn't panic")
		}
	}()
	p.Release(6)
}

// fake is a Spillable buffer holding size bytes of a pool.
type fake struct {
	p       *Pool
	name    string
	size    int64
	pinned  bool
	err     error
	spilled *[]string
}

func (f *fake) SpillSize() int64 {
	if f.pinned {
		return 0
	}
	return f.size
}

func (f *fake) Spill() error {
	if f.err != nil {
		return f.err
	}
	*f.spilled = append(*f.spilled, f.name)
	f.p.Release(f.size)
	f.size = 0
	return nil
}

// fakes reserves a fake of each size from p and makes p spill them.
func fakes(t *testing.T, p *Pool, sizes ...int64) ([]*fake, *[]string) {
	t.Helper()
	spilled := new([]string)
	var fs []*fake
	for i, n := range sizes {
		if err := p.Reserve(n); err != nil {
			t.Fatal(err)
		}
		fs = append(fs, &fake{p: p, name: string(rune('a' + i)), size: n, spilled: spilled})
	}
	p.SetSource(func() []Spillable {
		var out []Spillable
		for _, f := range fs {
			out = append(out, f)
		}
		return out
	})
	return fs, spilled
}

func TestPool_spills_the_largest_down_to_the_low_water_mark(t *testing.T) {
	p := NewPool(1000)
	fs, spilled := fakes(t, p, 100, 300, 50, 250, 200, 90)
	fs[1].pinned = true // the largest, but a join is reading it
	if err := p.Reserve(20); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// 990 in use: spilling 250 brings it to 740, under 800.
	if want := []string{"d"}; !slices.Equal(*spilled, want) {
		t.Errorf("spilled %v, want %v", *spilled, want)
	}
	if got := p.Used(); got != 760 {
		t.Errorf("Used = %d, want 760", got)
	}
}

func TestPool_spills_until_a_large_reservation_fits(t *testing.T) {
	p := NewPool(1000)
	_, spilled := fakes(t, p, 100, 300, 50, 250, 200)
	if err := p.Reserve(600); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// 900 in use: it must come down to 400, past the low-water mark.
	if want := []string{"b", "d"}; !slices.Equal(*spilled, want) {
		t.Errorf("spilled %v, want %v", *spilled, want)
	}
}

func TestPool_fails_when_spilling_cant_make_room(t *testing.T) {
	p := NewPool(1000)
	fs, spilled := fakes(t, p, 500, 300)
	fs[0].pinned = true
	if err := p.Reserve(600); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Reserve: %v, want ErrExhausted", err)
	}
	if want := []string{"b"}; !slices.Equal(*spilled, want) {
		t.Errorf("spilled %v, want %v", *spilled, want)
	}
	if got := p.Used(); got != 500 {
		t.Errorf("Used = %d, want 500", got)
	}
}

func TestPool_a_reservation_above_the_limit_spills_nothing(t *testing.T) {
	p := NewPool(1000)
	_, spilled := fakes(t, p, 500)
	if err := p.Reserve(1001); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Reserve: %v, want ErrExhausted", err)
	}
	if len(*spilled) != 0 {
		t.Errorf("spilled %v for a reservation that can never fit", *spilled)
	}
}

func TestPool_a_failed_spill_fails_the_reservation_only_if_it_still_doesnt_fit(t *testing.T) {
	disk := errors.New("disk full")
	p := NewPool(1000)
	fs, spilled := fakes(t, p, 300, 200, 400)
	fs[2].err = disk
	if err := p.Reserve(200); !errors.Is(err, disk) {
		t.Fatalf("Reserve: %v, want the spill's error", err)
	}
	if len(*spilled) != 0 {
		t.Errorf("spilled %v after the largest failed, want the spill to stop", *spilled)
	}

	p = NewPool(1000)
	fs, spilled = fakes(t, p, 400, 300, 200)
	fs[1].err = disk
	if err := p.Reserve(200); err != nil {
		t.Fatalf("Reserve: %v; the first spill made room", err)
	}
	if want := []string{"a"}; !slices.Equal(*spilled, want) {
		t.Errorf("spilled %v, want %v", *spilled, want)
	}
}

func TestPool_records_its_peak(t *testing.T) {
	p := NewPool(100)
	for _, n := range []int64{30, 50} {
		if err := p.Reserve(n); err != nil {
			t.Fatal(err)
		}
	}
	p.Release(70)
	if err := p.Reserve(10); err != nil {
		t.Fatal(err)
	}
	if p.Peak() != 80 {
		t.Errorf("Peak = %d, want 80", p.Peak())
	}
}

// spillable is a buffer for the concurrency test: it holds 10 bytes
// whenever it is full.
type spillable struct {
	p     *Pool
	mu    sync.Mutex
	full  bool
	count *atomic.Int64
}

func (s *spillable) SpillSize() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.full {
		return 10
	}
	return 0
}

func (s *spillable) Spill() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.full {
		s.full = false
		s.p.Release(10)
		s.count.Add(1)
	}
	return nil
}

func TestPool_concurrent_reservers_spill_and_stay_within_the_limit(t *testing.T) {
	p := NewPool(1000)
	var count atomic.Int64
	bufs := make([]*spillable, 200)
	for i := range bufs {
		bufs[i] = &spillable{p: p, count: &count}
	}
	p.SetSource(func() []Spillable {
		out := make([]Spillable, len(bufs))
		for i, b := range bufs {
			out[i] = b
		}
		return out
	})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 500 {
				b := bufs[(g*500+i)%len(bufs)]
				if err := p.Reserve(10); err != nil {
					t.Error(err)
					return
				}
				b.mu.Lock()
				if b.full {
					p.Release(10)
				} else {
					b.full = true
				}
				b.mu.Unlock()
				if u := p.Used(); u > p.Limit() {
					t.Errorf("Used = %d, above the limit", u)
				}
			}
		})
	}
	wg.Wait()
	if count.Load() == 0 {
		t.Error("nothing was spilled")
	}
	if p.Peak() > p.Limit() {
		t.Errorf("Peak = %d, above the limit", p.Peak())
	}
}
