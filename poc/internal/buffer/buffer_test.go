package buffer

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"poc/internal/mem"
	"poc/internal/spill"
)

func TestEncoding_round_trips(t *testing.T) {
	cases := []struct{ key, value []byte }{
		{nil, nil},
		{[]byte("k"), nil},
		{nil, []byte("v")},
		{[]byte("key"), []byte("value")},
		{bytes.Repeat([]byte{0}, 200), bytes.Repeat([]byte{0xff}, 70000)},
	}
	var b []byte
	for i, c := range cases {
		b = AppendRecord(b, uint64(i)*0x0101010101010101, c.key, c.value)
	}
	off := 0
	for i, c := range cases {
		start := off
		hash, key, value, next := Decode(b, off)
		if hash != uint64(i)*0x0101010101010101 || !bytes.Equal(key, c.key) || !bytes.Equal(value, c.value) {
			t.Errorf("record %d decoded as (%x, %q, %d bytes)", i, hash, key, len(value))
		}
		if next-start != EncodedLen(c.key, c.value) {
			t.Errorf("record %d: %d bytes, EncodedLen says %d", i, next-start, EncodedLen(c.key, c.value))
		}
		if cap(key) != len(key) || cap(value) != len(value) {
			t.Errorf("record %d: key or value isn't capped, so appending would overwrite the chunk", i)
		}
		off = next
	}
	if off != len(b) {
		t.Errorf("decoded %d of %d bytes", off, len(b))
	}
}

func records(b *Buffer) (keys, values []string) {
	for _, c := range b.Chunks() {
		for off := 0; off < len(c); {
			var k, v []byte
			_, k, v, off = Decode(c, off)
			keys, values = append(keys, string(k)), append(values, string(v))
		}
	}
	return keys, values
}

func TestBuffer_chunks_grow_and_are_reserved(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	var b Buffer
	value := bytes.Repeat([]byte("x"), 1000)
	for i := range 5000 {
		if err := b.Append(pool, uint64(i), fmt.Appendf(nil, "k%d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	chunks := b.Chunks()
	var reserved int64
	for i, c := range chunks {
		reserved += int64(cap(c))
		want := min(minChunk<<i, maxChunk)
		if cap(c) != want {
			t.Errorf("chunk %d has capacity %d, want %d", i, cap(c), want)
		}
	}
	if got := pool.Used(); got != reserved {
		t.Errorf("pool has %d reserved, chunks hold %d", got, reserved)
	}
	if b.Records() != 5000 {
		t.Errorf("Records = %d, want 5000", b.Records())
	}
	keys, values := records(&b)
	for i := range keys {
		if keys[i] != fmt.Sprintf("k%d", i) || values[i] != string(value) {
			t.Fatalf("record %d is %q, out of ingest order", i, keys[i])
		}
	}
	b.Free(pool)
	if pool.Used() != 0 {
		t.Errorf("pool has %d reserved after Free", pool.Used())
	}
	if err := b.Append(pool, 0, []byte("k"), nil); !errors.Is(err, ErrFreed) {
		t.Errorf("Append after Free: %v, want ErrFreed", err)
	}
}

func TestBuffer_a_record_larger_than_a_chunk_gets_its_own(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	var b Buffer
	big := bytes.Repeat([]byte("v"), 3*maxChunk)
	if err := b.Append(pool, 1, []byte("k"), big); err != nil {
		t.Fatal(err)
	}
	if c := b.Chunks(); len(c) != 1 || cap(c[0]) != EncodedLen([]byte("k"), big) {
		t.Errorf("got %d chunks, want one of exactly the record's size", len(c))
	}
}

func TestBuffer_fails_when_the_pool_is_exhausted(t *testing.T) {
	pool := mem.NewPool(minChunk + minChunk/2)
	var b Buffer
	value := bytes.Repeat([]byte("v"), 1000)
	var err error
	n := 0
	for ; err == nil; n++ {
		err = b.Append(pool, 0, []byte("k"), value)
	}
	if !errors.Is(err, mem.ErrExhausted) {
		t.Fatalf("Append: %v, want ErrExhausted", err)
	}
	if b.Records() != int64(n-1) || pool.Used() != minChunk {
		t.Errorf("after the failure: %d records, %d reserved; want %d and %d", b.Records(), pool.Used(), n-1, minChunk)
	}
}

func TestBuffer_concurrent_appends_keep_every_record(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	var b Buffer
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				if err := b.Append(pool, 0, fmt.Appendf(nil, "%d-%d", g, i), []byte("value")); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	keys, _ := records(&b)
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	if len(keys) != 16000 || len(seen) != 16000 {
		t.Errorf("got %d records, %d distinct, want 16000", len(keys), len(seen))
	}
	var reserved int64
	for _, c := range b.Chunks() {
		reserved += int64(cap(c))
	}
	if pool.Used() != reserved {
		t.Errorf("pool has %d reserved, chunks hold %d", pool.Used(), reserved)
	}
}

func TestPartitions_route_by_hash_and_empty_keys_apart(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	p := NewPartitions(7)
	for i := range 100 {
		if err := p.Append(pool, Right, fmt.Appendf(nil, "key-%d", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Append(pool, Left, nil, []byte("no key")); err != nil {
		t.Fatal(err)
	}
	var total int64
	for i := range p.Buckets() {
		if p.Bucket(Left, i).Records() != 0 {
			t.Errorf("left bucket %d holds records", i)
		}
		for _, c := range p.Bucket(Right, i).Chunks() {
			for off := 0; off < len(c); {
				hash, key, _, next := Decode(c, off)
				if hash != Hash(key) || hash%7 != uint64(i) {
					t.Errorf("key %q with hash %x is in bucket %d", key, hash, i)
				}
				total++
				off = next
			}
		}
	}
	if total != 100 {
		t.Errorf("buckets hold %d records, want 100", total)
	}
	if p.Empty(Left).Records() != 1 || p.Empty(Right).Records() != 0 {
		t.Errorf("empty-key partitions hold %d and %d, want 1 and 0", p.Empty(Left).Records(), p.Empty(Right).Records())
	}
	p.Free(pool)
	if pool.Used() != 0 {
		t.Errorf("pool has %d reserved after Free", pool.Used())
	}
}

// memWrite is a spill write that keeps files in memory.
type memWrite struct {
	files map[string][]byte
	err   error
}

func (w *memWrite) write(seq int, records int64, chunks [][]byte) (spill.File, error) {
	if w.err != nil {
		return spill.File{}, w.err
	}
	name := fmt.Sprint(seq)
	if _, ok := w.files[name]; ok {
		return spill.File{}, fmt.Errorf("file %s exists", name)
	}
	data := slices.Concat(chunks...)
	w.files[name] = data
	return spill.File{Path: name, Records: records, Bytes: int64(len(data))}, nil
}

// pinned returns the keys of b's records, from its files and chunks, in
// order.
func (w *memWrite) pinned(b *Buffer) []string {
	files, chunks := b.Pin()
	defer b.Unpin()
	var all [][]byte
	for _, f := range files {
		all = append(all, w.files[f.Path])
	}
	var keys []string
	for _, c := range append(all, chunks...) {
		for off := 0; off < len(c); {
			var k []byte
			_, k, _, off = Decode(c, off)
			keys = append(keys, string(k))
		}
	}
	return keys
}

func TestBuffer_Spill_moves_its_chunks_to_a_file(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	w := &memWrite{files: map[string][]byte{}}
	var b Buffer
	var want []string
	add := func(n int) {
		for range n {
			k := fmt.Sprintf("k%d", len(want))
			if err := b.Append(pool, 0, []byte(k), make([]byte, 100)); err != nil {
				t.Fatal(err)
			}
			want = append(want, k)
		}
	}
	add(300)
	size := b.SpillSize()
	if size == 0 || size != pool.Used() {
		t.Fatalf("SpillSize = %d with %d reserved", size, pool.Used())
	}
	if err := b.Spill(pool, w.write); err != nil {
		t.Fatal(err)
	}
	if pool.Used() != 0 || len(b.Chunks()) != 0 || b.SpillSize() != 0 {
		t.Errorf("after Spill: %d reserved, %d chunks", pool.Used(), len(b.Chunks()))
	}
	if f := b.Files(); len(f) != 1 || f[0].Records != 300 || f[0].Bytes != b.Bytes() {
		t.Errorf("Files = %+v, want one of 300 records and %d bytes", f, b.Bytes())
	}
	add(5)
	if c := b.Chunks(); len(c) != 1 || cap(c[0]) != minChunk {
		t.Errorf("after a spill the buffer has %d chunks, want one of %d bytes", len(c), minChunk)
	}
	if err := b.Spill(pool, w.write); err != nil {
		t.Fatal(err)
	}
	add(7)
	if b.Records() != 312 || len(b.Files()) != 2 {
		t.Errorf("Records = %d, %d files; want 312 and 2", b.Records(), len(b.Files()))
	}
	if got := w.pinned(&b); !slices.Equal(got, want) {
		t.Errorf("records out of ingest order across files and chunks")
	}
}

func TestBuffer_a_failed_Spill_keeps_the_records_in_memory(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	w := &memWrite{files: map[string][]byte{}}
	var b Buffer
	for i := range 100 {
		if err := b.Append(pool, 0, fmt.Appendf(nil, "k%d", i), make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
	}
	used := pool.Used()
	w.err = errors.New("disk full")
	if err := b.Spill(pool, w.write); !errors.Is(err, w.err) {
		t.Fatalf("Spill: %v, want the write's error", err)
	}
	if pool.Used() != used || len(b.Files()) != 0 || b.SpillSize() != used {
		t.Errorf("after the failed Spill: %d reserved, %d files", pool.Used(), len(b.Files()))
	}
	w.err = nil
	if err := b.Spill(pool, w.write); err != nil {
		t.Fatalf("Spill after the failure: %v", err)
	}
	if got := w.pinned(&b); len(got) != 100 || got[0] != "k0" || got[99] != "k99" {
		t.Errorf("got %d records after the retried spill", len(got))
	}
}

func TestBuffer_a_pinned_buffer_isnt_spilled(t *testing.T) {
	pool := mem.NewPool(1 << 30)
	w := &memWrite{files: map[string][]byte{}}
	var b Buffer
	if err := b.Append(pool, 0, []byte("k"), nil); err != nil {
		t.Fatal(err)
	}
	b.Pin()
	if b.SpillSize() != 0 {
		t.Errorf("SpillSize of a pinned buffer = %d, want 0", b.SpillSize())
	}
	if err := b.Spill(pool, w.write); err != nil || len(w.files) != 0 {
		t.Errorf("Spill of a pinned buffer: %v, %d files", err, len(w.files))
	}
	b.Unpin()
	if err := b.Spill(pool, w.write); err != nil || len(w.files) != 1 {
		t.Errorf("Spill once unpinned: %v, %d files", err, len(w.files))
	}
}

func TestBuffer_Pin_and_Free_wait_for_a_spill_in_progress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := mem.NewPool(1 << 30)
		w := &memWrite{files: map[string][]byte{}}
		var b Buffer
		if err := b.Append(pool, 0, []byte("a"), nil); err != nil {
			t.Fatal(err)
		}
		writing, release := make(chan struct{}), make(chan struct{})
		go func() {
			err := b.Spill(pool, func(seq int, records int64, chunks [][]byte) (spill.File, error) {
				close(writing)
				<-release
				return w.write(seq, records, chunks)
			})
			if err != nil {
				t.Error(err)
			}
		}()
		<-writing
		// Records arrive into new chunks while the spill writes.
		if err := b.Append(pool, 0, []byte("b"), nil); err != nil {
			t.Fatal(err)
		}
		if b.SpillSize() != 0 {
			t.Errorf("SpillSize while spilling = %d, want 0", b.SpillSize())
		}
		var pinned, freed bool
		go func() {
			files, chunks := b.Pin()
			if len(files) != 1 || len(chunks) != 1 {
				t.Errorf("Pin = %d files, %d chunks; want the finished spill's file and the new chunk", len(files), len(chunks))
			}
			pinned = true
			b.Unpin()
			b.Free(pool)
			freed = true
		}()
		synctest.Wait()
		if pinned {
			t.Fatal("Pin returned while the spill was writing")
		}
		close(release)
		synctest.Wait()
		if !freed || pool.Used() != 0 {
			t.Errorf("after the spill: freed %v, %d reserved", freed, pool.Used())
		}
	})
}

func TestBuffer_chunks_are_at_most_a_sixteenth_of_a_small_budget(t *testing.T) {
	pool := mem.NewPool(256 << 10)
	w := &memWrite{files: map[string][]byte{}}
	var b Buffer
	for i := range 2000 {
		if err := b.Append(pool, 0, fmt.Appendf(nil, "k%d", i), make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
		if pool.Used() > 128<<10 {
			if err := b.Spill(pool, w.write); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range b.Chunks() {
		if cap(c) > 16<<10 {
			t.Errorf("a chunk of %d bytes, above a sixteenth of the 256 KiB budget", cap(c))
		}
	}
}

func TestRecordLen_needs_the_whole_header(t *testing.T) {
	rec := AppendRecord(nil, 7, bytes.Repeat([]byte{'k'}, 200), bytes.Repeat([]byte{'v'}, 300))
	header := 8 + 2 + 2
	for n := range len(rec) + 1 {
		l, ok := RecordLen(rec[:n])
		if ok != (n >= header) || ok && l != len(rec) {
			t.Errorf("RecordLen of %d bytes = %d, %v; want %d, %v", n, l, ok, len(rec), n >= header)
		}
	}
	corrupt := append(make([]byte, 8), bytes.Repeat([]byte{0xff}, MaxHeader)...)
	if _, ok := RecordLen(corrupt); ok {
		t.Error("RecordLen of an overflowing length succeeded")
	}
}

func TestAddFile_adds_to_the_totals(t *testing.T) {
	var b Buffer
	b.AddFile(spill.File{Path: "0.rec", Records: 3, Bytes: 100})
	b.AddFile(spill.File{Path: "1.rec", Records: 2, Bytes: 50})
	if b.Records() != 5 || b.Bytes() != 150 || len(b.Files()) != 2 {
		t.Errorf("%d records, %d bytes, %d files after two AddFiles", b.Records(), b.Bytes(), len(b.Files()))
	}
}
