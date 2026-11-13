package join

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/oracle"
)

// manyKeys returns records of keys in many buckets, with a Match of each
// key and some one-sided records.
func manyKeys(keys, size int) []oracle.Record {
	var records []oracle.Record
	for i := range keys {
		key := fmt.Appendf(nil, "key-%d", i)
		for side := range 2 {
			if i%5 == side+1 { // one-sided
				continue
			}
			records = append(records, oracle.Record{Side: oracle.Side(side), Key: key, Value: bytes.Repeat([]byte{byte(i)}, size)})
		}
	}
	return records
}

// unitRecords returns the records of each unit of p's plan under budget.
func unitRecords(p *buffer.Partitions, budget int64) []int64 {
	var out []int64
	for _, u := range Plan(bucketSizes(p), budget) {
		var n int64
		for _, b := range bucketsOf(p, u.Lo, u.Hi) {
			n += b.b.Records()
		}
		out = append(out, n)
	}
	return out
}

func TestRun_parallel_workers_hand_their_batches_to_one_sender(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const parallelism = 4
		const batch = 256
		data := mem.NewPool(1 << 30)
		records := manyKeys(2000, 50)
		p, totals := load(t, data, 64, records)
		var count int64
		for _, b := range buffers(p) {
			count += b.Records()
		}
		// The buffers are in data, so the join's pool holds only what Run
		// reserves: an output batch per worker, and the index of each unit.
		pool := mem.NewPool(parallelism*batch + encoded(records) + count*entrySize)
		units := unitRecords(p, pool.Limit()/parallelism)
		if len(units) <= parallelism {
			t.Fatalf("%d units, want more than %d", len(units), parallelism)
		}

		release := make(chan struct{})
		var inFlight, sends atomic.Int64
		var maxInFlight int64
		var got []*reconv1.JoinResult
		done := make(chan error)
		go func() {
			_, err := Run(context.Background(), pool, nil, p, Options{Filter: NewFilter(nil), OutputBatchBytes: batch, Totals: totals, Parallelism: parallelism}, func(r *reconv1.JoinResponse) error {
				maxInFlight = max(maxInFlight, inFlight.Add(1))
				defer inFlight.Add(-1)
				if sends.Add(1) == 1 {
					<-release
				}
				got = append(got, proto.Clone(r).(*reconv1.JoinResponse).GetResults()...)
				return nil
			})
			done <- err
		}()

		// With the first batch held up, every worker has loaded a unit, the
		// first four of the plan, and waits to hand over a batch.
		synctest.Wait()
		want := int64(parallelism * batch)
		for _, n := range units[:parallelism] {
			want += n * entrySize
		}
		if used := pool.Used(); used != want {
			t.Errorf("with the sender held up, %d bytes reserved, want %d: a batch per worker and the first %d units' indexes", used, want, parallelism)
		}
		if sends.Load() != 1 {
			t.Errorf("%d sends started while the first was held up, want 1", sends.Load())
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if maxInFlight != 1 {
			t.Errorf("%d sends at once, want 1", maxInFlight)
		}
		if d := oracle.Diff(oracle.Join(records), got); d != "" {
			t.Error(d)
		}
		if pool.Used() != 0 {
			t.Errorf("%d bytes reserved after the join", pool.Used())
		}
	})
}

func TestRun_reports_the_first_failing_unit_of_the_plan(t *testing.T) {
	// Every key with a Match is over the maximum match size, so every unit
	// fails. The first unit's first such key in the join's order, of hash
	// then key, is reported, whichever worker fails first.
	const parallelism = 4
	data := mem.NewPool(1 << 30)
	records := manyKeys(2000, 50)
	p, totals := load(t, data, 64, records)
	pool := mem.NewPool(128 << 10)
	units := Plan(bucketSizes(p), pool.Limit()/parallelism)
	if len(units) < 2*parallelism {
		t.Fatalf("%d units, want at least %d", len(units), 2*parallelism)
	}
	var want []byte
	for _, r := range records {
		hash := buffer.Hash(r.Key)
		if b := uint32(hash % 64); b < units[0].Lo || b >= units[0].Hi || r.Side != oracle.Right {
			continue // every key with a right record has a Match
		}
		if h := buffer.Hash(want); want == nil || hash < h || hash == h && bytes.Compare(r.Key, want) < 0 {
			want = r.Key
		}
	}

	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals, Parallelism: parallelism, MaxMatchBytes: 100}
	for range 50 {
		_, err := Run(t.Context(), pool, nil, p, opts, func(*reconv1.JoinResponse) error { return nil })
		hk, ok := errors.AsType[*HotKeyError](err)
		if !ok {
			t.Fatalf("Run: %v, want a HotKeyError", err)
		}
		if !bytes.Equal(hk.Key, want) {
			t.Fatalf("HOT_KEY for key %q, want %q, the first of the plan's first unit", hk.Key, want)
		}
		if pool.Used() != 0 {
			t.Fatalf("%d bytes reserved after the failed join", pool.Used())
		}
	}
}

func TestRun_measures_the_working_set_of_its_units(t *testing.T) {
	data := mem.NewPool(1 << 30)
	sp := newSpiller(t)
	records := manyKeys(2000, 50)
	p, totals := load(t, data, 64, records)
	for i, b := range buffers(p) {
		if i%3 == 0 {
			if err := sp.spill(data, b); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A unit holds its chunks at their capacity, its files once read, and
	// an index entry per record.
	held := func(bufs []sided) (records, working int64) {
		for _, b := range bufs {
			records += b.b.Bytes()
			working += b.b.Records() * entrySize
			for _, f := range b.b.Files() {
				working += f.Bytes
			}
			for _, c := range b.b.Chunks() {
				working += int64(cap(c))
			}
		}
		return records, working
	}
	pool := mem.NewPool(256 << 10)
	var want Stats
	for _, u := range Plan(bucketSizes(p), pool.Limit()/2) {
		want.unit(held(bucketsOf(p, u.Lo, u.Hi)))
	}
	if want.Units < 4 {
		t.Fatalf("%d units, want several", want.Units)
	}
	opts := Options{Filter: NewFilter(nil), OutputBatchBytes: 1024, Totals: totals, Parallelism: 2}
	got, err := Run(t.Context(), pool, sp.disk, p, opts, func(*reconv1.JoinResponse) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("Stats = %+v, want %+v", got, want)
	}
	if bytes := encoded(records); got.RecordBytes != bytes {
		t.Errorf("units of %d record bytes, the records take %d", got.RecordBytes, bytes)
	}
}
