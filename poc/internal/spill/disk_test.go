package spill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/humustest"
	"pgregory.net/rapid"

	"poc/internal/merge"
	"poc/internal/record"
)

func newHost(t *testing.T) (humus.FS, string) {
	t.Helper()
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	return h.FS(), h.Path("data")
}

func TestDisk_reserves_up_to_its_limit(t *testing.T) {
	fsys, _ := newHost(t)
	d := NewDisk(fsys, 100)
	if err := d.Reserve(60); err != nil {
		t.Fatal(err)
	}
	if err := d.Reserve(41); !errors.Is(err, ErrDiskExhausted) {
		t.Errorf("Reserve past the limit: %v, want ErrDiskExhausted", err)
	}
	if err := d.Reserve(40); err != nil {
		t.Errorf("Reserve up to the limit: %v", err)
	}
	d.Release(100)
	if d.Used() != 0 || d.Peak() != 100 {
		t.Errorf("Used %d, Peak %d; want 0 and 100", d.Used(), d.Peak())
	}
	defer func() {
		if recover() == nil {
			t.Error("Release of more than was reserved didn't panic")
		}
	}()
	d.Release(1)
}

func TestAlignedBuffer_is_aligned(t *testing.T) {
	for _, n := range []int{Align, 3 * Align, 1 << 20, Align + 1} {
		b := AlignedBuffer(n)
		if len(b)%Align != 0 || len(b) == 0 || cap(b) != len(b) {
			t.Errorf("AlignedBuffer(%d) has len %d, cap %d", n, len(b), cap(b))
		}
	}
}

// sorted returns n records in run order, with values of up to maxValue
// bytes and some of exactly big bytes.
func sorted(t *rapid.T, n, maxValue, big int) [][]byte {
	var recs [][]byte
	for i := range n {
		key := rapid.SliceOfN(rapid.Byte(), 0, 20).Draw(t, "key")
		vl := rapid.IntRange(0, maxValue).Draw(t, "value size")
		if big > 0 && rapid.IntRange(0, 9).Draw(t, "big") == 0 {
			vl = big
		}
		value := make([]byte, vl)
		for j := range value {
			value[j] = byte(i + j)
		}
		side := record.Side(rapid.IntRange(0, 1).Draw(t, "side"))
		recs = append(recs, record.Append(nil, record.Hash(key), side, key, value))
	}
	slices.SortStableFunc(recs, record.Compare)
	return recs
}

// write writes recs to a new run file through a buffer of bufSize bytes.
func write(t testing.TB, fsys humus.FS, path string, bufSize int, recs [][]byte) *File {
	t.Helper()
	w, err := Create(fsys, path, AlignedBuffer(bufSize))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		h, _ := record.ParseHeader(r)
		w.Add(h.Hash, r)
	}
	f, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// readAll reads segment lo to hi of f through a cursor with a read buffer
// of bufSize bytes, and returns its records, values read as the join reads
// them.
func readAll(t testing.TB, fsys humus.FS, f *File, lo, hi, bufSize int) [][]byte {
	t.Helper()
	r, err := Open(fsys, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := NewCursor(r, 0, make([]byte, bufSize), func(n int) ([]byte, error) { return make([]byte, n), nil })
	off, end := f.Segment(lo, hi)
	if err := c.Reset(off, end); err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for {
		if _, ok := c.Peek(); !ok {
			return out
		}
		head := c.Head()
		h, _ := record.ParseHeader(head)
		if len(head) != h.Size() && len(head) != h.Len+h.KeyLen {
			t.Fatalf("a head of %d bytes, of a record of %d with a header and key of %d", len(head), h.Size(), h.Len+h.KeyLen)
		}
		if len(head) < h.Size() && h.Size() <= bufSize {
			t.Fatalf("a record of %d bytes, which fits the %d-byte buffer, wasn't read whole", h.Size(), bufSize)
		}
		v := make([]byte, h.ValLen)
		if err := c.ReadValue(v); err != nil {
			t.Fatal(err)
		}
		out = append(out, record.Append(nil, h.Hash, h.Side, head[h.Len:h.Len+h.KeyLen], v))
		if err := c.Next(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunFile_property_round_trips_by_fence(t *testing.T) {
	fsys, dir := newHost(t)
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		n++
		recs := sorted(rt, rapid.IntRange(0, 300).Draw(rt, "records"), 300, 20<<10)
		bufSize := rapid.SampledFrom([]int{Align, 2 * Align, 64 << 10}).Draw(rt, "write buffer")
		f := write(t, fsys, filepath.Join(dir, fmt.Sprintf("%d", n), "0.run"), bufSize, recs)
		var size int64
		for _, r := range recs {
			size += int64(len(r))
		}
		if f.Bytes != size || f.Records != int64(len(recs)) || f.Fences[0] != 0 || f.Fences[record.Fences] != size {
			rt.Fatalf("File has %d bytes, %d records, fences from %d to %d; want %d bytes, %d records", f.Bytes, f.Records, f.Fences[0], f.Fences[record.Fences], size, len(recs))
		}
		if st, err := os.Stat(f.Path); err != nil || st.Size() != size {
			rt.Fatalf("the file is %v bytes (%v), want %d: a direct write's padding must be cut", st.Size(), err, size)
		}
		readBuf := rapid.SampledFrom([]int{Align, 16 << 10, 256 << 10}).Draw(rt, "read buffer")
		lo := rapid.IntRange(0, record.Fences).Draw(rt, "lo")
		hi := rapid.IntRange(lo, record.Fences).Draw(rt, "hi")
		var want [][]byte
		for _, r := range recs {
			h, _ := record.ParseHeader(r)
			if fence := record.Fence(h.Hash); fence >= lo && fence < hi {
				want = append(want, r)
			}
		}
		got := readAll(t, fsys, f, lo, hi, readBuf)
		if len(got) != len(want) {
			rt.Fatalf("read %d records of fences %d to %d, want %d", len(got), lo, hi, len(want))
		}
		for i := range got {
			if !bytes.Equal(got[i], want[i]) {
				rt.Fatalf("record %d differs", i)
			}
		}
	})
}

func TestCursor_rewinds_past_a_large_value(t *testing.T) {
	fsys, dir := newHost(t)
	small := record.Append(nil, 1, record.Left, []byte("a"), []byte("x"))
	large := record.Append(nil, 2, record.Left, []byte("b"), bytes.Repeat([]byte("y"), 3*Align))
	last := record.Append(nil, 3, record.Right, []byte("c"), []byte("z"))
	f := write(t, fsys, filepath.Join(dir, "0.run"), Align, [][]byte{small, large, last})
	r, err := Open(fsys, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := NewCursor(r, 7, make([]byte, Align), nil)
	if err := c.Reset(0, f.Bytes); err != nil {
		t.Fatal(err)
	}
	if err := c.Next(); err != nil {
		t.Fatal(err)
	}
	pos := c.Pos()
	if h, _ := c.Peek(); h != 2 || len(c.Head()) != 8+1+2+1 { // hash, lengths, key
		t.Fatalf("head of hash %d and %d bytes, want the large record's header and key", h, len(c.Head()))
	}
	if err := c.Next(); err != nil {
		t.Fatal(err)
	}
	if h, _ := c.Peek(); h != 3 || !bytes.Equal(c.Head(), last) {
		t.Fatalf("after the large record: hash %d", h)
	}
	if err := c.Rewind(pos); err != nil {
		t.Fatal(err)
	}
	v := make([]byte, 3*Align)
	if err := c.ReadValue(v); err != nil || !bytes.Equal(v, large[len(large)-len(v):]) {
		t.Fatalf("the large value after a rewind: %v", err)
	}
	if c.Ord() != 7 || c.Reads() == 0 {
		t.Errorf("Ord %d, %d reads", c.Ord(), c.Reads())
	}
}

func TestCursor_grows_for_a_key_larger_than_its_buffer(t *testing.T) {
	fsys, dir := newHost(t)
	key := bytes.Repeat([]byte("k"), 2*Align)
	rec := record.Append(nil, record.Hash(key), record.Right, key, bytes.Repeat([]byte("v"), 3*Align))
	f := write(t, fsys, filepath.Join(dir, "0.run"), Align, [][]byte{rec})
	var grown int
	r, err := Open(fsys, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := NewCursor(r, 0, make([]byte, Align), func(n int) ([]byte, error) {
		grown = n
		return make([]byte, n), nil
	})
	if err := c.Reset(0, f.Bytes); err != nil {
		t.Fatal(err)
	}
	if _, k := record.SideKey(c.Head()); !bytes.Equal(k, key) || grown < len(key) || grown >= len(rec) {
		t.Errorf("grew to %d bytes for a key of %d in a record of %d; want the header and key only", grown, len(key), len(rec))
	}
}

func TestCursor_a_truncated_file_fails(t *testing.T) {
	fsys, dir := newHost(t)
	rec := record.Append(nil, 5, record.Left, []byte("key"), []byte("value"))
	f := write(t, fsys, filepath.Join(dir, "0.run"), Align, [][]byte{rec, rec})
	r, err := Open(fsys, f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := NewCursor(r, 0, make([]byte, Align), nil)
	if err := c.Reset(0, f.Bytes-1); err == nil {
		if err = c.Next(); err == nil {
			t.Error("a segment that ends inside a record read without an error")
		}
	}
}

func TestCopy_copies_large_values_through_the_cursor(t *testing.T) {
	fsys, dir := newHost(t)
	rapid.Check(t, func(rt *rapid.T) {
		a := sorted(rt, rapid.IntRange(0, 50).Draw(rt, "a"), 200, 3*Align)
		b := sorted(rt, rapid.IntRange(0, 50).Draw(rt, "b"), 200, 3*Align)
		fa := write(t, fsys, filepath.Join(dir, "a.run"), Align, a)
		fb := write(t, fsys, filepath.Join(dir, "b.run"), Align, b)
		defer os.Remove(fa.Path)
		defer os.Remove(fb.Path)
		var srcs []merge.Source
		for i, f := range []*File{fa, fb} {
			r, err := Open(fsys, f)
			if err != nil {
				rt.Fatal(err)
			}
			defer r.Close()
			c := NewCursor(r, uint64(i), make([]byte, Align), nil)
			if err := c.Reset(0, f.Bytes); err != nil {
				rt.Fatal(err)
			}
			srcs = append(srcs, c)
		}
		m := merge.New(srcs)
		m.Init()
		w, err := Create(fsys, filepath.Join(dir, "merged.run"), AlignedBuffer(Align))
		if err != nil {
			rt.Fatal(err)
		}
		if err := Copy(context.Background(), w, m); err != nil {
			rt.Fatal(err)
		}
		f, err := w.Finish()
		if err != nil {
			rt.Fatal(err)
		}
		defer os.Remove(f.Path)
		want := append(slices.Clone(a), b...)
		slices.SortStableFunc(want, record.Compare) // a's before b's when equal: their ords
		got, err := os.ReadFile(f.Path)
		if err != nil {
			rt.Fatal(err)
		}
		if !bytes.Equal(got, bytes.Join(want, nil)) {
			rt.Fatalf("the merged file differs from the sorted records")
		}
		if f.Records != int64(len(want)) {
			rt.Fatalf("merged %d records, want %d", f.Records, len(want))
		}
	})
}

func TestWriter_a_failed_Create_leaves_no_file(t *testing.T) {
	fsys, dir := newHost(t)
	path := filepath.Join(dir, "0.run")
	f := write(t, fsys, path, Align, nil)
	if f.Bytes != 0 || f.Records != 0 {
		t.Errorf("an empty run file has %d bytes, %d records", f.Bytes, f.Records)
	}
	if _, err := Create(fsys, path, AlignedBuffer(Align)); err == nil {
		t.Error("Create of a file that exists succeeded")
	}
	w, err := Create(fsys, filepath.Join(dir, "1.run"), AlignedBuffer(Align))
	if err != nil {
		t.Fatal(err)
	}
	w.Add(0, record.Append(nil, 0, record.Left, nil, nil))
	w.Abort()
	if _, err := os.Stat(filepath.Join(dir, "1.run")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("an aborted run file is still there: %v", err)
	}
}

// TestWriter_writes_directly_where_it_can writes a run file in a temporary
// directory, and in RECON_TEST_RUN_DIR if it is set, so that direct I/O
// can be checked on the filesystem the sidecar will spill to. A file is
// exactly its records' size either way.
func TestWriter_writes_directly_where_it_can(t *testing.T) {
	fsys := humus.OSHost().FS
	dirs := []string{t.TempDir()}
	if dir := os.Getenv("RECON_TEST_RUN_DIR"); dir != "" {
		dirs = append(dirs, dir)
	}
	for _, dir := range dirs {
		w, err := Create(fsys, filepath.Join(dir, fmt.Sprintf("direct-%d.run", os.Getpid())), AlignedBuffer(Align))
		if err != nil {
			t.Fatal(err)
		}
		var size int64
		for i := range 1000 {
			rec := record.Append(nil, uint64(i)<<40, record.Left, []byte("k"), make([]byte, 37))
			w.Add(uint64(i)<<40, rec)
			size += int64(len(rec))
		}
		f, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(f.Path)
		if err != nil || st.Size() != size {
			t.Errorf("%s: the file is %d bytes (%v), want %d", dir, st.Size(), err, size)
		}
		t.Logf("%s: direct I/O %v, %d writes", dir, w.Direct(), w.Writes())
		_ = os.Remove(f.Path)
	}
}
