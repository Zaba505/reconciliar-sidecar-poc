package join

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/humustest"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/oracle"
	"poc/internal/record"
	"poc/internal/spill"
)

// testCodec is the codec env spills with: RECON_TEST_SPILL_CODEC's, so
// that the package's tests can run on files of blocks, or none.
var testCodec = func() spill.Codec {
	c, err := spill.ParseCodec(cmp.Or(os.Getenv("RECON_TEST_SPILL_CODEC"), "none"))
	if err != nil {
		panic(err)
	}
	return c
}()

// env is a run's buffer, spilled to real files, below run.Manager.
type env struct {
	fsys  humus.FS
	dir   string
	pool  *mem.Pool
	buf   *buffer.Buffer
	wbuf  []byte
	enc   *spill.Encoder
	seq   atomic.Int64
	disk  atomic.Int64 // bytes of files
	recs  []oracle.Record
	spill atomic.Bool // the pool may spill the buffer
}

var hostOnce struct {
	sync.Mutex
	n int
}

func newEnv(t testing.TB, limit int64) *env {
	t.Helper()
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	return newEnvOn(h.FS(), h.Path("data"), limit)
}

func newEnvOn(fsys humus.FS, dir string, limit int64) *env {
	return newEnvAs(fsys, dir, limit, testCodec)
}

// newEnvAs is newEnvOn, spilling with codec.
func newEnvAs(fsys humus.FS, dir string, limit int64, codec spill.Codec) *env {
	pool := mem.NewPool(limit)
	e := &env{fsys: fsys, dir: dir, pool: pool, buf: buffer.New(pool), wbuf: spill.AlignedBuffer(buffer.BlockSize(limit) &^ (spill.Align - 1)),
		enc: spill.NewEncoder(spill.FormatFor(codec, limit))}
	e.spill.Store(true)
	pool.SetSource(func() []mem.Spillable { return []mem.Spillable{(*envSpill)(e)} })
	return e
}

type envSpill env

func (s *envSpill) SpillSize() int64 {
	if !s.spill.Load() {
		return 0
	}
	return s.buf.SpillSize()
}
func (s *envSpill) Spill() error { return s.buf.Spill((*env)(s).write) }

func (e *env) write(size int64, m *merge.Merger) (*spill.File, error) {
	w, err := spill.Create(e.fsys, filepath.Join(e.dir, fmt.Sprintf("%06d.run", e.seq.Add(1))), e.wbuf, e.enc)
	if err != nil {
		return nil, err
	}
	if err := spill.Copy(context.Background(), w, m); err != nil {
		w.Abort()
		return nil, err
	}
	f, err := w.Finish()
	if err == nil {
		if f.Raw != size {
			return nil, fmt.Errorf("a spill of %d bytes wrote %d", size, f.Raw)
		}
		e.disk.Add(f.Bytes)
	}
	return f, err
}

// format is the format the env spills in.
func (e *env) format() spill.Format {
	if e.enc == nil {
		return spill.Format{}
	}
	return e.enc.Format()
}

// ingest appends records, in messages of up to per records of one side.
func (e *env) ingest(t testing.TB, recs []oracle.Record, per int) {
	t.Helper()
	for i := 0; i < len(recs); {
		j := i + 1
		for j < len(recs) && j-i < per && recs[j].Side == recs[i].Side {
			j++
		}
		msg := recs[i:j]
		if _, err := e.buf.Append(record.Side(msg[0].Side), len(msg), func(k int) ([]byte, []byte) { return msg[k].Key, msg[k].Value }); err != nil {
			t.Fatalf("Append: %v", err)
		}
		i = j
	}
	e.recs = append(e.recs, recs...)
}

func (e *env) spillNow(t testing.TB) {
	t.Helper()
	if err := e.buf.Spill(e.write); err != nil {
		t.Fatal(err)
	}
}

// seal seals the buffer, spilling what is in memory if it spilled before,
// as run.Seal does.
func (e *env) seal(t testing.TB) {
	t.Helper()
	spilled, err := e.buf.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if spilled {
		e.spillNow(t)
	}
}

func (e *env) totals() [2]uint64 {
	var n [2]uint64
	for _, r := range e.recs {
		n[r.Side]++
	}
	return n
}

func (e *env) options(parallelism int, batch int64) Options {
	return Options{Filter: NewFilter(nil), OutputBatchBytes: batch, Totals: e.totals(), Parallelism: parallelism}
}

// join runs a Join and returns its batches, cloned: a batch is valid only
// until send returns.
func (e *env) join(ctx context.Context, opts Options) ([]*reconv1.JoinResponse, Stats, error) {
	var mu sync.Mutex
	var out []*reconv1.JoinResponse
	st, err := Run(ctx, e.pool, e.fsys, e.buf, opts, func(r *reconv1.JoinResponse) error {
		mu.Lock()
		defer mu.Unlock()
		out = append(out, proto.Clone(r).(*reconv1.JoinResponse))
		return nil
	})
	return out, st, err
}

func results(batches []*reconv1.JoinResponse) []*reconv1.JoinResult {
	var out []*reconv1.JoinResult
	for _, b := range batches {
		out = append(out, b.GetResults()...)
	}
	return out
}

// randomRecords returns n records over keys keys and empty keys, with
// values of up to valueSize bytes, in runs of one side.
func randomRecords(rng *rand.Rand, n, keys, valueSize int) []oracle.Record {
	out := make([]oracle.Record, n)
	side := oracle.Left
	for i := range out {
		if rng.IntN(30) == 0 {
			side = 1 - side
		}
		var key []byte
		if k := rng.IntN(keys + 1); k > 0 {
			key = fmt.Appendf(nil, "key-%d", k)
		}
		v := make([]byte, rng.IntN(valueSize+1))
		for j := range v {
			v[j] = byte(rng.Uint32())
		}
		out[i] = oracle.Record{Side: side, Key: key, Value: v}
	}
	return out
}

func TestRun_joins_like_the_oracle_from_memory_files_or_both(t *testing.T) {
	for _, tt := range []struct {
		name   string
		spills []int // the record counts after which the buffer spills
		seal   bool  // spill the rest at Seal
	}{
		{"memory", nil, false},
		{"one file", nil, true},
		{"files", []int{1000, 4000, 4001}, true},
		{"files and blocks", []int{2500, 5000}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, 64<<20)
			recs := randomRecords(rand.New(rand.NewPCG(1, 2)), 8000, 1500, 300)
			at := 0
			for _, n := range append(tt.spills, len(recs)) {
				e.ingest(t, recs[at:n], 40)
				at = n
				if n < len(recs) {
					e.spillNow(t)
				}
			}
			if _, err := e.buf.Seal(); err != nil {
				t.Fatal(err)
			}
			if tt.seal {
				e.spillNow(t)
			}
			batches, st, err := e.join(t.Context(), e.options(3, 4096))
			if err != nil {
				t.Fatal(err)
			}
			// One stream: each Match's values are in ingest order.
			if d := oracle.DiffOrdered(oracle.Join(recs), results(batches)); d != "" {
				t.Fatal(d)
			}
			t.Logf("%d files, %d blocks, %d workers, %d tasks, %d batches, %d reads", st.Files, st.Blocks, st.Workers, st.Tasks, st.Batches, st.Reads)
			if e.pool.Used() != e.pool.Idle()+spill.FilesMem(e.buf.Files())+e.blockBytes() {
				t.Errorf("the Join left %d reserved", e.pool.Used()-e.pool.Idle())
			}
		})
	}
}

// blockBytes is what the buffer holds in memory.
func (e *env) blockBytes() int64 {
	e.spill.Store(true)
	return e.buf.SpillSize()
}

func TestRun_property_matches_the_oracle_in_value_order(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		n++
		dir := h.Path(fmt.Sprintf("data/%d", n))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			rt.Fatal(err)
		}
		limit := rapid.Int64Range(256<<10, 8<<20).Draw(rt, "budget")
		e := newEnvOn(h.FS(), dir, limit)
		keys := rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 10), 1, 100).Draw(rt, "keys")
		count := rapid.IntRange(0, 2000).Draw(rt, "records")
		recs := make([]oracle.Record, count)
		side := oracle.Side(rapid.IntRange(0, 1).Draw(rt, "first side"))
		for i := range recs {
			if rapid.IntRange(0, 10).Draw(rt, "switch") == 0 {
				side = 1 - side
			}
			v := make([]byte, rapid.IntRange(0, 200).Draw(rt, "value size"))
			for j := range v {
				v[j] = byte(i * (j + 1))
			}
			recs[i] = oracle.Record{Side: side, Key: rapid.SampledFrom(keys).Draw(rt, "key"), Value: v}
		}
		spills := rapid.SliceOfN(rapid.IntRange(0, count), 0, 5).Draw(rt, "spills")
		slices.Sort(spills)
		at := 0
		for _, s := range spills {
			e.ingest(t, recs[at:s], 30)
			at = s
			e.spillNow(t)
		}
		e.ingest(t, recs[at:], 30)
		if rapid.Bool().Draw(rt, "spill at seal") {
			e.seal(t)
		} else if _, err := e.buf.Seal(); err != nil {
			rt.Fatal(err)
		}
		opts := e.options(rapid.IntRange(1, 4).Draw(rt, "parallelism"), rapid.Int64Range(64, 8192).Draw(rt, "batch"))
		want := oracle.Join(recs)
		hot := firstHotKey(want, MaxMatch(limit, opts))
		batches, _, err := e.join(context.Background(), opts)
		if hot != nil {
			if hk, ok := errors.AsType[*HotKeyError](err); !ok || !bytes.Equal(hk.Key, hot.GetKey()) || hk.Left != uint64(len(hot.GetLeft())) || hk.Right != uint64(len(hot.GetRight())) {
				rt.Fatalf("Join: %v, want HOT_KEY for %q with %d and %d records", err, hot.GetKey(), len(hot.GetLeft()), len(hot.GetRight()))
			}
		} else {
			if err != nil {
				rt.Fatal(err)
			}
			if d := oracle.DiffOrdered(want, results(batches)); d != "" {
				rt.Fatal(d)
			}
		}
		e.buf.Free()
		if e.pool.Used() != e.pool.Idle() {
			rt.Fatalf("%d reserved after Free", e.pool.Used()-e.pool.Idle())
		}
	})
}

// firstHotKey returns the Match of results larger than maxMatch that comes
// first in the plan: the lowest hash, then key. A Match's size is that of
// its encoding, since a non-empty key and each value are fields.
func firstHotKey(results []*reconv1.JoinResult, maxMatch int64) *reconv1.Match {
	var first *reconv1.Match
	for _, r := range results {
		m := r.GetBoth()
		if m == nil || int64(proto.Size(m)) <= maxMatch {
			continue
		}
		if first == nil || cmp.Or(cmp.Compare(record.Hash(m.GetKey()), record.Hash(first.GetKey())), bytes.Compare(m.GetKey(), first.GetKey())) < 0 {
			first = m
		}
	}
	return first
}

func TestRun_states_filter_still_counts(t *testing.T) {
	e := newEnv(t, 64<<20)
	recs := randomRecords(rand.New(rand.NewPCG(3, 4)), 3000, 500, 50)
	e.ingest(t, recs[:1500], 20)
	e.spillNow(t)
	e.ingest(t, recs[1500:], 20)
	e.seal(t)
	all := oracle.Join(recs)
	states := []reconv1.JoinState{reconv1.JoinState_JOIN_STATE_LEFT_ONLY, reconv1.JoinState_JOIN_STATE_RIGHT_ONLY, reconv1.JoinState_JOIN_STATE_BOTH}
	for mask := 1; mask < 8; mask++ {
		var want []reconv1.JoinState
		for i, s := range states {
			if mask&(1<<i) != 0 {
				want = append(want, s)
			}
		}
		opts := e.options(2, 1024)
		opts.Filter = NewFilter(want)
		batches, _, err := e.join(t.Context(), opts)
		if err != nil {
			t.Fatalf("states %v: %v", want, err)
		}
		if d := oracle.DiffOrdered(oracle.Filter(all, want...), results(batches)); d != "" {
			t.Errorf("states %v: %s", want, d)
		}
	}
}

func TestRun_batches_are_bounded(t *testing.T) {
	e := newEnv(t, 64<<20)
	recs := randomRecords(rand.New(rand.NewPCG(5, 6)), 5000, 300, 400)
	recs = append(recs, oracle.Record{Side: oracle.Left, Key: []byte("large"), Value: make([]byte, 10<<10)})
	e.ingest(t, recs, 50)
	e.seal(t)
	const limit = 2048
	batches, _, err := e.join(t.Context(), e.options(2, limit))
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range batches {
		if n := proto.Size(b); n > limit && len(b.GetResults()) > 1 {
			t.Errorf("batch %d has %d results in %d bytes, over the %d-byte limit", i, len(b.GetResults()), n, limit)
		}
		if len(b.GetResults()) == 0 {
			t.Errorf("batch %d is empty", i)
		}
	}
}

func TestRun_property_spilling_changes_no_batch(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		recs := randomRecords(rand.New(rand.NewPCG(rapid.Uint64().Draw(rt, "seed"), 0)), rapid.IntRange(0, 3000).Draw(rt, "records"), 200, 100)
		batch := rapid.Int64Range(128, 4096).Draw(rt, "batch")
		var first []*reconv1.JoinResponse
		for _, spills := range [][]int{nil, {len(recs) / 2}, {len(recs) / 5, len(recs) / 3, len(recs) / 2}} {
			n++
			dir := h.Path(fmt.Sprintf("data/%d", n))
			if err := os.MkdirAll(dir, 0o700); err != nil {
				rt.Fatal(err)
			}
			e := newEnvOn(h.FS(), dir, 32<<20)
			at := 0
			for _, s := range spills {
				e.ingest(t, recs[at:s], 25)
				at = s
				e.spillNow(t)
			}
			e.ingest(t, recs[at:], 25)
			e.seal(t)
			// One worker: the batches of every task in plan order.
			batches, _, err := e.join(context.Background(), e.options(1, batch))
			if err != nil {
				rt.Fatal(err)
			}
			if first == nil {
				first = batches
				continue
			}
			if len(batches) != len(first) {
				rt.Fatalf("%d batches after %d spills, %d without", len(batches), len(spills), len(first))
			}
			for i := range batches {
				if !proto.Equal(batches[i], first[i]) {
					rt.Fatalf("batch %d differs after %d spills", i, len(spills))
				}
			}
		}
	})
}

func TestRun_a_dropped_record_fails_the_control_totals(t *testing.T) {
	e := newEnv(t, 1<<20)
	e.ingest(t, []oracle.Record{{Key: []byte("a")}, {Side: oracle.Right, Key: []byte("a")}}, 10)
	e.seal(t)
	dropRecord.Store(true)
	if _, _, err := e.join(t.Context(), e.options(1, 1024)); !errors.Is(err, ErrControlTotals) {
		t.Fatalf("Join with a dropped record: %v, want ErrControlTotals", err)
	}
	if _, _, err := e.join(t.Context(), e.options(1, 1024)); err != nil {
		t.Errorf("the next Join: %v", err)
	}
}

func TestRun_stops_when_its_context_ends(t *testing.T) {
	e := newEnv(t, 16<<20)
	e.ingest(t, randomRecords(rand.New(rand.NewPCG(7, 8)), 20000, 5000, 100), 100)
	e.spillNow(t)
	e.seal(t)
	cause := errors.New("the run was deleted")
	ctx, cancel := context.WithCancelCause(t.Context())
	sent := 0
	_, err := Run(ctx, e.pool, e.fsys, e.buf, e.options(4, 1024), func(*reconv1.JoinResponse) error {
		if sent++; sent == 3 {
			cancel(cause)
		}
		return nil
	})
	if !errors.Is(err, cause) {
		t.Fatalf("Join cancelled during a send: %v, want its cause", err)
	}
	if sent > 3+4 {
		t.Errorf("%d batches sent after the cancel, want at most one per worker", sent-3)
	}
	if e.pool.Used()-e.pool.Idle() != spill.FilesMem(e.buf.Files()) {
		t.Errorf("%d reserved after the cancelled Join", e.pool.Used()-e.pool.Idle())
	}

	// A Join of a run whose results are all filtered out stops too.
	ctx, cancel = context.WithCancelCause(t.Context())
	cancel(cause)
	opts := e.options(2, 1024)
	opts.Filter = NewFilter([]reconv1.JoinState{reconv1.JoinState_JOIN_STATE_BOTH})
	if _, _, err := e.join(ctx, opts); !errors.Is(err, cause) {
		t.Errorf("a cancelled Join that sends nothing: %v, want its cause", err)
	}
}

func TestRun_a_missing_run_file_fails_the_Join(t *testing.T) {
	e := newEnv(t, 1<<20)
	e.ingest(t, []oracle.Record{{Key: []byte("a")}}, 10)
	e.seal(t)
	e.spillNow(t)
	for _, f := range e.buf.Files() {
		if err := os.Remove(f.Path); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := e.join(t.Context(), e.options(1, 1024)); err == nil {
		t.Error("a Join of a run whose file is gone succeeded")
	}
}

func TestPlan_property_covers_every_fence_once_in_balanced_tasks(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sizes := rapid.SliceOfN(rapid.Int64Range(0, 1000), record.Fences, record.Fences).Draw(t, "sizes")
		n := rapid.IntRange(1, 40).Draw(t, "tasks")
		tasks := plan(sizes, n)
		var total, largest int64
		for _, s := range sizes {
			total += s
			largest = max(largest, s)
		}
		if total == 0 {
			if len(tasks) != 0 {
				t.Fatalf("%d tasks of no bytes", len(tasks))
			}
			return
		}
		if len(tasks) > n || tasks[0].lo != 0 || tasks[len(tasks)-1].hi != record.Fences {
			t.Fatalf("plan %v of %d tasks", tasks, n)
		}
		for i, tk := range tasks {
			if tk.lo >= tk.hi || i > 0 && tk.lo != tasks[i-1].hi {
				t.Fatalf("plan %v doesn't cover the fences once, in order", tasks)
			}
			var b int64
			for _, s := range sizes[tk.lo:tk.hi] {
				b += s
			}
			if b > total/int64(n)+2*largest {
				t.Fatalf("task %v has %d bytes, more than a share of %d and two fences", tk, b, total/int64(n))
			}
		}
	})
}

// hotRecords returns left then right records of key, with values of size
// bytes, each value numbered.
func hotRecords(key []byte, left, right, size int) []oracle.Record {
	var out []oracle.Record
	for i := range left + right {
		side := oracle.Left
		if i >= left {
			side = oracle.Right
		}
		v := make([]byte, size)
		copy(v, fmt.Appendf(nil, "%08d", i))
		out = append(out, oracle.Record{Side: side, Key: key, Value: v})
	}
	return out
}

func TestRun_HOT_KEY_for_a_Match_above_the_maximum(t *testing.T) {
	for _, filter := range [][]reconv1.JoinState{nil, {reconv1.JoinState_JOIN_STATE_LEFT_ONLY}} {
		t.Run(fmt.Sprint(filter), func(t *testing.T) {
			e := newEnv(t, 8<<20)
			key := []byte("hot")
			others := randomRecords(rand.New(rand.NewPCG(9, 9)), 3000, 500, 100)
			e.ingest(t, others[:1500], 50)
			e.ingest(t, hotRecords(key, 300, 200, 1000), 50)
			e.spillNow(t)
			e.ingest(t, others[1500:], 50)
			e.seal(t)
			opts := e.options(3, 4096)
			opts.Filter = NewFilter(filter)
			opts.MaxMatchBytes = 256 << 10
			var first *HotKeyError
			for range 2 {
				_, _, err := e.join(t.Context(), opts)
				hk, ok := errors.AsType[*HotKeyError](err)
				if !ok {
					t.Fatalf("Join: %v, want a HotKeyError", err)
				}
				size := proto.Size(&reconv1.Match{Key: key, Left: make([][]byte, 300), Right: make([][]byte, 200)}) + 500*1000 + 500*1 // each value's length takes one more byte
				if !bytes.Equal(hk.Key, key) || hk.Left != 300 || hk.Right != 200 || hk.Limit != 256<<10 || hk.Size < int64(size)-1000 || hk.Size > int64(size)+1000 {
					t.Errorf("HotKeyError %+v, want the key's 300 and 200 records in a Match of about %d bytes", hk, size)
				}
				if first != nil && hk.Error() != first.Error() {
					t.Errorf("the second Join failed with %v, the first with %v", hk, first)
				}
				first = hk
			}
		})
	}
}

func TestRun_reports_the_first_hot_key_of_the_plan(t *testing.T) {
	e := newEnv(t, 16<<20)
	var keys [][]byte
	for i := 0; len(keys) < 6; i++ {
		k := fmt.Appendf(nil, "hot-%d", i)
		// Keys far apart in the hash, so that they fall in different tasks.
		if f := record.Fence(record.Hash(k)); f%40 == 0 && !slices.ContainsFunc(keys, func(o []byte) bool { return record.Fence(record.Hash(o)) == f }) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		e.ingest(t, hotRecords(k, 100, 100, 200), 50)
	}
	e.ingest(t, randomRecords(rand.New(rand.NewPCG(10, 10)), 20000, 5000, 100), 100)
	e.seal(t)
	first := slices.MinFunc(keys, func(a, b []byte) int { return cmp.Compare(record.Hash(a), record.Hash(b)) })
	opts := e.options(4, 4096)
	opts.MaxMatchBytes = 16 << 10
	for range 10 {
		_, _, err := e.join(t.Context(), opts)
		if hk, ok := errors.AsType[*HotKeyError](err); !ok || !bytes.Equal(hk.Key, first) {
			t.Fatalf("Join: %v, want the HotKeyError of %q, the first in the plan", err, first)
		}
	}
}

func TestRun_streams_a_one_sided_key_at_any_size(t *testing.T) {
	for _, side := range []oracle.Side{oracle.Left, oracle.Right} {
		t.Run(fmt.Sprint(side), func(t *testing.T) {
			// 20 MiB of one key under a 4 MiB budget and a 256 KiB maximum
			// Match: it is never held, however it is spread over files.
			e := newEnv(t, 4<<20)
			key := []byte("one-sided")
			n := 20_000
			left, right := n, 0
			if side == oracle.Right {
				left, right = 0, n
			}
			hot := hotRecords(key, left, right, 1000)
			others := randomRecords(rand.New(rand.NewPCG(11, 11)), 4000, 1000, 100)
			e.ingest(t, others[:2000], 50)
			e.ingest(t, hot, 50) // the pool spills as they come
			e.ingest(t, others[2000:], 50)
			e.seal(t)
			if len(e.buf.Files()) < 3 {
				t.Fatalf("%d files, want the key spread over several", len(e.buf.Files()))
			}
			opts := e.options(2, 64<<10)
			opts.MaxMatchBytes = 256 << 10
			for range 2 {
				batches, st, err := e.join(t.Context(), opts)
				if err != nil {
					t.Fatal(err)
				}
				if d := oracle.DiffOrdered(oracle.Join(e.recs), results(batches)); d != "" {
					t.Fatal(d)
				}
				// The key's results keep their ingest order.
				var vals [][]byte
				for _, r := range results(batches) {
					if rec := cmp.Or(r.GetLeftOnly(), r.GetRightOnly()); rec != nil && bytes.Equal(rec.GetKey(), key) {
						vals = append(vals, rec.GetValue())
					}
				}
				if len(vals) != n || !slices.IsSortedFunc(vals, bytes.Compare) {
					t.Errorf("%d results of the key, sorted %v; want %d in ingest order", len(vals), slices.IsSortedFunc(vals, bytes.Compare), n)
				}
				if want := int64(1 - side); st.Streamed != want {
					t.Errorf("%d keys streamed, want %d: a left key streams once it outgrows a Match, a right one never needs to", st.Streamed, want)
				}
				if e.pool.Peak() > e.pool.Limit() {
					t.Errorf("pool peak %d above its limit", e.pool.Peak())
				}
			}
		})
	}
}

// TestRun_large_values_are_read_when_emitted joins values of 1 to 4 MiB
// from files, which no read buffer holds: a cursor keeps the key, and the
// value is read into the arena when it is emitted.
func TestRun_large_values_are_read_when_emitted(t *testing.T) {
	for _, limit := range []int64{128 << 20, 24 << 20} {
		t.Run(fmt.Sprint(limit>>20, "MiB"), func(t *testing.T) {
			e := newEnv(t, limit)
			rng := rand.New(rand.NewPCG(12, 12))
			var recs []oracle.Record
			for i := range 40 {
				v := make([]byte, 1<<20+rng.IntN(3<<20))
				for j := 0; j < len(v); j += 4096 {
					v[j] = byte(i)
				}
				side := oracle.Side(i % 2)
				recs = append(recs, oracle.Record{Side: side, Key: fmt.Appendf(nil, "k%d", i%25), Value: v})
			}
			recs = append(recs, randomRecords(rng, 5000, 1000, 100)...)
			for i := 0; i < len(recs); i += 500 {
				e.ingest(t, recs[i:min(i+500, len(recs))], 1)
				e.spillNow(t)
			}
			e.seal(t)
			opts := e.options(4, 1<<20)
			opts.MaxMatchBytes = 12 << 20
			var mu sync.Mutex
			var got []*reconv1.JoinResult
			st, err := Run(t.Context(), e.pool, e.fsys, e.buf, opts, func(r *reconv1.JoinResponse) error {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, proto.Clone(r).(*reconv1.JoinResponse).GetResults()...)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if d := oracle.DiffOrdered(oracle.Join(recs), got); d != "" {
				t.Fatal(d)
			}
			t.Logf("%d files, %d workers, read buffers of %d bytes, pool peak %d MiB", st.Files, st.Workers, st.ReadBuffer, e.pool.Peak()>>20)
			if st.ReadBuffer >= 1<<20 {
				t.Errorf("read buffers of %d bytes hold the values whole", st.ReadBuffer)
			}
		})
	}
}

// TestRun_a_Match_needs_its_size_once builds a Match of 4.5 MiB under an
// 8 MiB budget, which a contiguous arena that grows by copying couldn't,
// and fails a Match over the maximum with HOT_KEY, not memory exhausted.
func TestRun_a_Match_needs_its_size_once(t *testing.T) {
	for _, tt := range []struct {
		left, right int
		hot         bool
	}{{4400, 100, false}, {5500, 100, true}} {
		t.Run(fmt.Sprint(tt.left), func(t *testing.T) {
			e := newEnv(t, 8<<20)
			key := []byte("big")
			e.ingest(t, hotRecords(key, tt.left, tt.right, 1000), 100)
			e.ingest(t, randomRecords(rand.New(rand.NewPCG(13, 13)), 2000, 500, 100), 100)
			e.seal(t)
			opts := e.options(1, 1<<20)
			opts.MaxMatchBytes = 5 << 20
			batches, _, err := e.join(t.Context(), opts)
			if tt.hot {
				if hk, ok := errors.AsType[*HotKeyError](err); !ok || !bytes.Equal(hk.Key, key) {
					t.Fatalf("Join of a Match over the maximum: %v, want HOT_KEY", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Join of a %d-value Match under an 8 MiB budget: %v", tt.left+tt.right, err)
			}
			if d := oracle.DiffOrdered(oracle.Join(e.recs), results(batches)); d != "" {
				t.Fatal(d)
			}
			if e.pool.Peak() > e.pool.Limit() {
				t.Errorf("pool peak %d above its limit", e.pool.Peak())
			}
		})
	}
}

// TestRun_gives_up_workers_before_read_buffers_get_small joins many files
// under a small budget.
// TestRun_runs_the_workers_a_tiny_budget_holds asks for 64 workers with
// output batches of 1 MiB under a 4 MiB budget: one runs.
func TestRun_runs_the_workers_a_tiny_budget_holds(t *testing.T) {
	e := newEnv(t, 4<<20)
	recs := randomRecords(rand.New(rand.NewPCG(16, 16)), 50_000, 10_000, 300)
	e.ingest(t, recs, 100)
	e.seal(t)
	opts := e.options(64, 1<<20)
	opts.MaxMatchBytes = 3 << 20
	batches, st, err := e.join(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.DiffOrdered(oracle.Join(recs), results(batches)); d != "" {
		t.Fatal(d)
	}
	if st.Workers != 1 {
		t.Errorf("%d workers under a 4 MiB budget with 1 MiB batches, want 1", st.Workers)
	}
}

// TestRun_batches_of_small_results_fit_their_structs joins 400,000 small
// records under an 8 MiB budget with batches of 1 MiB, which, of results of
// 40 bytes, would take four times that in structs, next to a Match of
// 20,000 values.
func TestRun_batches_of_small_results_fit_their_structs(t *testing.T) {
	e := newEnv(t, 8<<20)
	recs := randomRecords(rand.New(rand.NewPCG(17, 17)), 400_000, 130_000, 64)
	for i := range recs {
		if i%20 == 0 {
			recs[i].Key = []byte("hot") // a Match of 20,000 values, within the maximum
		}
	}
	e.ingest(t, recs, 300)
	e.seal(t)
	opts := e.options(4, 1<<20)
	opts.MaxMatchBytes = 3 << 20
	batches, _, err := e.join(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.DiffOrdered(oracle.Join(recs), results(batches)); d != "" {
		t.Fatal(d)
	}
	if e.pool.Peak() > e.pool.Limit() {
		t.Errorf("pool peak %d above its limit", e.pool.Peak())
	}
	for i, b := range batches {
		if n := int64(len(b.GetResults())); n > 1 && (n-1)*resultBytes > opts.OutputBatchBytes {
			t.Fatalf("batch %d holds %d results, whose structs take %d bytes, over the %d-byte limit", i, n, n*resultBytes, opts.OutputBatchBytes)
		}
	}
}

func TestRun_gives_up_workers_before_read_buffers_get_small(t *testing.T) {
	e := newEnv(t, 2<<20)
	recs := randomRecords(rand.New(rand.NewPCG(14, 14)), 200_000, 50_000, 200)
	e.ingest(t, recs, 100)
	e.seal(t)
	files := len(e.buf.Files())
	batches, st, err := e.join(t.Context(), e.options(4, 16<<10))
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.DiffOrdered(oracle.Join(recs), results(batches)); d != "" {
		t.Fatal(d)
	}
	t.Logf("%d files, %d workers, read buffers of %d bytes", files, st.Workers, st.ReadBuffer)
	if st.Workers == 4 || st.ReadBuffer < minReadBuf {
		t.Errorf("%d workers with read buffers of %d bytes for %d files: want fewer workers, with buffers of at least %d", st.Workers, st.ReadBuffer, files, minReadBuf)
	}
}

// files writes runs for Compact in the env's directory.
type envFiles struct{ e *env }

func (f envFiles) Format() spill.Format { return f.e.format() }

func (f envFiles) Create(size int64, buf []byte, enc *spill.Encoder) (*spill.Writer, error) {
	f.e.disk.Add(size)
	return spill.Create(f.e.fsys, filepath.Join(f.e.dir, fmt.Sprintf("%06d.run", f.e.seq.Add(1))), buf, enc)
}

func (f envFiles) Unreserve(size int64) { f.e.disk.Add(-size) }

func (f envFiles) Remove(files []*spill.File) {
	for _, file := range files {
		if os.Remove(file.Path) == nil {
			f.e.disk.Add(-file.Bytes)
		}
	}
}

func TestCompact_merges_files_until_one_worker_reads_them(t *testing.T) {
	e := newEnv(t, 256<<10)
	recs := randomRecords(rand.New(rand.NewPCG(15, 15)), 50000, 10000, 200)
	e.ingest(t, recs, 100)
	e.seal(t)
	before := len(e.buf.Files())
	opts := e.options(2, 16<<10)
	st, err := Compact(t.Context(), e.pool, e.fsys, e.buf, envFiles{e}, opts)
	if err != nil {
		t.Fatal(err)
	}
	after := len(e.buf.Files())
	most := int(readBudget(e.pool, 1, MaxMatch(e.pool.Limit(), opts), opts.OutputBatchBytes) / int64(mergeBuffer(e.buf.Files())))
	t.Logf("%d files merged to %d in %d merges of %d bytes", before, after, st.Merges, st.Bytes)
	if st.Merges == 0 || after > max(most, 2) || after >= before {
		t.Errorf("%d files merged to %d in %d merges; want at most %d", before, after, st.Merges, most)
	}
	var disk int64
	for _, f := range e.buf.Files() {
		disk += f.Bytes
	}
	if e.disk.Load() != disk {
		t.Errorf("%d bytes of files reserved after the merges, which take %d", e.disk.Load(), disk)
	}
	if entries, _ := os.ReadDir(e.dir); len(entries) != after+1 { // and .keep
		t.Errorf("%d entries in the run's directory, want its %d files", len(entries)-1, after)
	}
	if e.pool.Used()-e.pool.Idle() != spill.FilesMem(e.buf.Files()) {
		t.Errorf("%d reserved after the merges, want the %d files' tables", e.pool.Used()-e.pool.Idle(), after)
	}
	batches, _, err := e.join(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.DiffOrdered(oracle.Join(recs), results(batches)); d != "" {
		t.Fatal(d)
	}
}
