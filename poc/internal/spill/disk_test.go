package spill

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
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
// bytes and some of exactly big bytes. Some values are noise, which
// doesn't compress, so that files of blocks hold raw blocks too.
func sorted(t *rapid.T, n, maxValue, big int) [][]byte {
	var recs [][]byte
	noise := rand.New(rand.NewPCG(rapid.Uint64().Draw(t, "seed"), 0))
	noisy := rapid.IntRange(0, 2).Draw(t, "noisy") // none, some or all
	for i := range n {
		key := rapid.SliceOfN(rapid.Byte(), 0, 20).Draw(t, "key")
		vl := rapid.IntRange(0, maxValue).Draw(t, "value size")
		if big > 0 && rapid.IntRange(0, 9).Draw(t, "big") == 0 {
			vl = big
		}
		value := make([]byte, vl)
		random := noisy == 2 || noisy == 1 && noise.IntN(2) == 0
		for j := range value {
			if random {
				value[j] = byte(noise.Uint32())
			} else {
				value[j] = byte(i + j)
			}
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
	return writeAs(t, fsys, path, bufSize, Format{}, recs)
}

// writeAs is write in format.
func writeAs(t testing.TB, fsys humus.FS, path string, bufSize int, format Format, recs [][]byte) *File {
	t.Helper()
	w, err := Create(fsys, path, AlignedBuffer(bufSize), NewEncoder(format))
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
	d := NewDecoder([]*File{f})
	defer d.Close()
	c := NewFileCursor(r, f, 0, make([]byte, bufSize), func(n int) ([]byte, error) { return make([]byte, n), nil }, d)
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
		if fits := h.Size() <= bufSize; f.Format.Blocked() {
			if large := h.Size() > f.Format.Block; large != (len(head) < h.Size()) {
				t.Fatalf("a record of %d bytes in blocks of %d has a head of %d", h.Size(), f.Format.Block, len(head))
			}
		} else if len(head) < h.Size() && fits {
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
		format := drawFormat(rt)
		f := writeAs(t, fsys, filepath.Join(dir, fmt.Sprintf("%d", n), "0.run"), bufSize, format, recs)
		var size int64
		for _, r := range recs {
			size += int64(len(r))
		}
		fileBytes := size
		if format.Blocked() {
			fileBytes = f.Bytes
			if f.Bytes > format.MaxBytes(size) {
				rt.Fatalf("a file of %d bytes of records is %d bytes, more than MaxBytes's %d", size, f.Bytes, format.MaxBytes(size))
			}
		}
		if f.Bytes != fileBytes || f.Raw != size || f.Records != int64(len(recs)) || f.Fences[0] != 0 || f.Fences[record.Fences] != fileBytes || f.Format != format {
			rt.Fatalf("File has %d bytes, %d raw, %d records, fences from %d to %d; want %d bytes, %d records", f.Bytes, f.Raw, f.Records, f.Fences[0], f.Fences[record.Fences], size, len(recs))
		}
		var raws [record.Fences]int64
		for _, r := range recs {
			raws[record.Fence(binary.LittleEndian.Uint64(r))] += int64(len(r))
		}
		for i, raw := range raws {
			if f.RawSize(i) != raw || !format.Blocked() && f.Fences[i+1]-f.Fences[i] != raw {
				rt.Fatalf("fence %d holds %d bytes of records, %d of the file; want %d", i, f.RawSize(i), f.Fences[i+1]-f.Fences[i], raw)
			}
		}
		if st, err := os.Stat(f.Path); err != nil || st.Size() != fileBytes {
			rt.Fatalf("the file is %v bytes (%v), want %d: a direct write's padding must be cut", st.Size(), err, fileBytes)
		}
		readBuf := drawReadBuffer(rt, format)
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

// drawFormat draws a format: no blocks, or blocks of a codec.
func drawFormat(t *rapid.T) Format {
	codec := rapid.SampledFrom([]Codec{None, S2, Zstd}).Draw(t, "codec")
	if codec == None {
		return Format{}
	}
	return Format{Codec: codec, Block: rapid.SampledFrom([]int{Align, 8 << 10, 32 << 10}).Draw(t, "block")}
}

// drawReadBuffer draws a read buffer for a file of format: the smallest,
// one that reads ahead, or the largest a Join takes.
func drawReadBuffer(t *rapid.T, format Format) int {
	if !format.Blocked() {
		return rapid.SampledFrom([]int{Align, 16 << 10, 256 << 10}).Draw(t, "read buffer")
	}
	b := format.MinBuffer()
	return rapid.SampledFrom([]int{b, b + 2*Align, 2*b + blockHeader, 3 * b, 256 << 10}).Draw(t, "read buffer")
}

func TestFormatFor_scales_blocks_with_the_budget(t *testing.T) {
	for _, c := range []struct {
		limit int64
		block int
	}{{128 << 10, 4 << 10}, {16 << 20, 16 << 10}, {128 << 20, 32 << 10}, {8 << 30, 32 << 10}} {
		if f := FormatFor(S2, c.limit); f.Block != c.block || f.Codec != S2 {
			t.Errorf("FormatFor(s2, %d) = %+v, want blocks of %d", c.limit, f, c.block)
		}
	}
	if f := FormatFor(None, 128<<20); f.Blocked() || f.MinBuffer() != Align {
		t.Errorf("FormatFor(none) = %+v", f)
	}
	if f := FormatFor(Zstd, Zstd.MinLimit()-1); f.Blocked() {
		t.Errorf("FormatFor(zstd) under its least budget = %+v, want no blocks", f)
	}
	if f := FormatFor(Zstd, Zstd.MinLimit()); f.Codec != Zstd || f.Block != 8<<10 {
		t.Errorf("FormatFor(zstd) at its least budget = %+v", f)
	}
	for _, s := range []string{"none", "s2", "zstd"} {
		if c, err := ParseCodec(s); err != nil || c.String() != s {
			t.Errorf("ParseCodec(%q) = %v, %v", s, c, err)
		}
	}
	if _, err := ParseCodec("lz4"); err == nil {
		t.Error("ParseCodec(lz4) succeeded")
	}
}

// TestCursor_property_rewinds_in_files_of_blocks moves cursors of two
// files, which share a Decoder, by Next and by Rewind to positions taken
// at random, and checks every head against the records.
func TestCursor_property_rewinds_in_files_of_blocks(t *testing.T) {
	fsys, dir := newHost(t)
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		n++
		format := drawFormat(rt)
		if !format.Blocked() {
			format = Format{Codec: S2, Block: Align}
		}
		var recs [2][][]byte
		var files []*File
		for i := range recs {
			recs[i] = sorted(rt, rapid.IntRange(1, 200).Draw(rt, "records"), 300, 3*format.Block/2)
			files = append(files, writeAs(t, fsys, filepath.Join(dir, fmt.Sprintf("r%d-%d.run", n, i)), Align, format, recs[i]))
		}
		d := NewDecoder(files)
		defer d.Close()
		bufSize := drawReadBuffer(rt, format)
		var cs [2]*Cursor
		for i, f := range files {
			r, err := Open(fsys, f)
			if err != nil {
				rt.Fatal(err)
			}
			defer r.Close()
			cs[i] = NewFileCursor(r, f, uint64(i), make([]byte, bufSize), func(n int) ([]byte, error) { return make([]byte, n), nil }, d)
			if err := cs[i].Reset(0, f.Bytes); err != nil {
				rt.Fatal(err)
			}
		}
		var at [2]int                     // the index of each cursor's head
		saved := [2]map[int64]int{{}, {}} // positions taken, and their heads
		check := func(i int) {
			c := cs[i]
			if at[i] == len(recs[i]) {
				if _, ok := c.Peek(); ok {
					rt.Fatalf("cursor %d has a head past its last record", i)
				}
				return
			}
			want := recs[i][at[i]]
			h, _ := record.ParseHeader(want)
			if head := c.Head(); !bytes.Equal(head, want[:len(head)]) || len(head) != h.Size() && len(head) != h.Len+h.KeyLen {
				rt.Fatalf("cursor %d: head %d differs", i, at[i])
			}
			if rapid.IntRange(0, 3).Draw(rt, "read value") == 0 {
				v := make([]byte, h.ValLen)
				if err := c.ReadValue(v); err != nil || !bytes.Equal(v, want[h.Len+h.KeyLen:]) {
					rt.Fatalf("cursor %d: value %d differs (%v)", i, at[i], err)
				}
			}
		}
		check(0)
		check(1)
		for range rapid.IntRange(0, 400).Draw(rt, "steps") {
			i := rapid.IntRange(0, 1).Draw(rt, "cursor")
			switch op := rapid.IntRange(0, 9).Draw(rt, "op"); {
			case op < 7:
				if at[i] < len(recs[i]) {
					if err := cs[i].Next(); err != nil {
						rt.Fatal(err)
					}
					at[i]++
				}
			case op < 9:
				saved[i][cs[i].Pos()] = at[i]
			default:
				for pos, k := range saved[i] {
					if err := cs[i].Rewind(pos); err != nil {
						rt.Fatal(err)
					}
					at[i] = k
					break
				}
			}
			check(i)
		}
	})
}

// TestCursor_property_a_corrupt_file_of_blocks_fails_without_a_panic
// flips a byte of a file of blocks, or cuts its segment short, and reads it:
// the read may fail, or read records whose bytes the flip changed, since
// blocks carry no checksum, but it never panics or reads past the file. A
// cut segment reads a prefix of the records: all of them up to the block
// the cut is in, if it isn't at the start of one.
func TestCursor_property_a_corrupt_file_of_blocks_fails_without_a_panic(t *testing.T) {
	fsys, dir := newHost(t)
	n := 0
	rapid.Check(t, func(rt *rapid.T) {
		n++
		format := drawFormat(rt)
		if !format.Blocked() {
			format = Format{Codec: Zstd, Block: 8 << 10}
		}
		recs := sorted(rt, rapid.IntRange(1, 200).Draw(rt, "records"), 300, 3*format.Block/2)
		f := writeAs(t, fsys, filepath.Join(dir, fmt.Sprintf("c%d.run", n)), Align, format, recs)
		data, err := os.ReadFile(f.Path)
		if err != nil {
			rt.Fatal(err)
		}
		end := f.Bytes
		if rapid.Bool().Draw(rt, "flip") {
			i := rapid.IntRange(0, len(data)-1).Draw(rt, "byte")
			data[i] ^= byte(rapid.IntRange(1, 255).Draw(rt, "bits"))
			if err := os.WriteFile(f.Path, data, 0o600); err != nil {
				rt.Fatal(err)
			}
		} else {
			end = rapid.Int64Range(0, f.Bytes-1).Draw(rt, "end")
		}
		r, err := Open(fsys, f)
		if err != nil {
			rt.Fatal(err)
		}
		defer r.Close()
		d := NewDecoder([]*File{f})
		defer d.Close()
		c := NewFileCursor(r, f, 0, make([]byte, drawReadBuffer(rt, format)), func(n int) ([]byte, error) {
			if int64(n) > f.Bytes {
				rt.Fatalf("grew to %d bytes for a file of %d", n, f.Bytes)
			}
			return make([]byte, n), nil
		}, d)
		err = c.Reset(0, end)
		for i := 0; err == nil; i++ {
			if _, ok := c.Peek(); !ok {
				break
			}
			if i >= len(recs) {
				rt.Fatalf("read %d records of a file of %d", i+1, len(recs))
			}
			head := c.Head()
			h, _ := record.ParseHeader(head)
			v := make([]byte, h.ValLen)
			if c.ReadValue(v) != nil {
				break
			}
			if got := record.Append(nil, h.Hash, h.Side, head[h.Len:h.Len+h.KeyLen], v); end < f.Bytes && !bytes.Equal(got, recs[i]) {
				rt.Fatalf("record %d of a segment cut to %d of %d bytes differs", i, end, f.Bytes)
			}
			err = c.Next()
		}
	})
}

// TestCursor_reads_each_byte_once reads files through and checks the
// bytes their preads took: each byte once, as the Join's plan assumes.
// Without read ahead, a file of blocks takes at most one pread per block,
// which reads the next block's header too; the first takes the most a block
// can be, since its size isn't known yet, and the next blocks, if they are
// small, come from it.
func TestCursor_reads_each_byte_once(t *testing.T) {
	fsys, dir := newHost(t)
	var recs [][]byte
	for i := range 20000 {
		key := fmt.Appendf(nil, "key-%d", i)
		recs = append(recs, record.Append(nil, record.Hash(key), record.Side(i&1), key, fmt.Appendf(nil, "value %d of a run", i*i)))
	}
	slices.SortFunc(recs, record.Compare)
	for _, c := range []struct {
		format  Format
		bufSize int
	}{
		{Format{}, 16 << 10},
		{Format{Codec: S2, Block: 8 << 10}, 8 << 10},
		{Format{Codec: S2, Block: 8 << 10}, 64 << 10},
		{Format{Codec: Zstd, Block: 32 << 10}, 32 << 10},
		{Format{Codec: Zstd, Block: 32 << 10}, 128 << 10},
	} {
		f := writeAs(t, fsys, filepath.Join(dir, fmt.Sprintf("%v-%d-%d.run", c.format.Codec, c.format.Block, c.bufSize)), 64<<10, c.format, recs)
		r, err := Open(fsys, f)
		if err != nil {
			t.Fatal(err)
		}
		d := NewDecoder([]*File{f})
		cur := NewFileCursor(r, f, 0, make([]byte, c.bufSize), nil, d)
		n := 0
		for err = cur.Reset(0, f.Bytes); err == nil && cur.Head() != nil; err = cur.Next() {
			n++
		}
		if err != nil || n != len(recs) {
			t.Fatalf("%+v: read %d records (%v), want %d", c, n, err, len(recs))
		}
		lo, hi := f.Bytes, f.Bytes
		if c.format.Blocked() && c.bufSize < 2*c.format.Block+blockHeader {
			blocks := countBlocks(t, f)
			if cur.Reads() > blocks {
				t.Errorf("%+v: %d preads of %d blocks", c, cur.Reads(), blocks)
			}
			hi = f.Bytes + blockHeader*blocks + int64(c.format.Block)
		}
		t.Logf("%v, blocks of %d, buffer of %d: %d bytes of records in %d, read in %d preads of %d bytes", c.format.Codec, c.format.Block, c.bufSize, f.Raw, f.Bytes, cur.Reads(), cur.BytesRead())
		if got := cur.BytesRead(); got < lo || got > hi {
			t.Errorf("%+v: read %d bytes of a file of %d, want %d to %d", c, got, f.Bytes, lo, hi)
		}
		_ = r.Close()
		d.Close()
	}
}

// TestFile_keeps_wide_raw_sizes_for_a_fence_of_4_GiB keeps the bytes of
// records before each fence in full when a fence's records take 4 GiB or
// more, as a hot key's may. Writing them would take too long, so the
// Writer is made to count them.
func TestFile_keeps_wide_raw_sizes_for_a_fence_of_4_GiB(t *testing.T) {
	fsys, dir := newHost(t)
	for _, extra := range []int64{0, 5 << 30} {
		w, err := Create(fsys, filepath.Join(dir, fmt.Sprintf("wide-%d.run", extra)), AlignedBuffer(Align), NewEncoder(Format{Codec: S2, Block: Align}))
		if err != nil {
			t.Fatal(err)
		}
		rec := record.Append(nil, 7<<56, record.Left, []byte("hot"), []byte("value"))
		w.Add(7<<56, rec)
		w.file.Raw += extra
		f, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		mem := FileBytes + 1024
		if extra > 0 {
			mem = MaxFileBytes
		}
		if got := f.RawSize(7); got != int64(len(rec))+extra || f.RawSize(6) != 0 || f.RawSize(8) != 0 || f.Mem() != mem {
			t.Errorf("fence 7 of %d extra bytes holds %d bytes, its neighbours %d and %d; the File takes %d, want %d", extra, got, f.RawSize(6), f.RawSize(8), f.Mem(), mem)
		}
	}
}

// countBlocks returns the number of blocks in f.
func countBlocks(t testing.TB, f *File) int64 {
	t.Helper()
	data, err := os.ReadFile(f.Path)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for at := 0; at < len(data); n++ {
		_, _, stored := parseBlockHeader(data[at:])
		at += blockHeader + int(stored)
	}
	return n
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
		fa := writeAs(t, fsys, filepath.Join(dir, "a.run"), Align, drawFormat(rt), a)
		fb := writeAs(t, fsys, filepath.Join(dir, "b.run"), Align, drawFormat(rt), b)
		defer os.Remove(fa.Path)
		defer os.Remove(fb.Path)
		d := NewDecoder([]*File{fa, fb})
		defer d.Close()
		var srcs []merge.Source
		for i, f := range []*File{fa, fb} {
			r, err := Open(fsys, f)
			if err != nil {
				rt.Fatal(err)
			}
			defer r.Close()
			c := NewFileCursor(r, f, uint64(i), make([]byte, f.Format.MinBuffer()), nil, d)
			if err := c.Reset(0, f.Bytes); err != nil {
				rt.Fatal(err)
			}
			srcs = append(srcs, c)
		}
		m := merge.New(srcs)
		m.Init()
		out := drawFormat(rt)
		w, err := Create(fsys, filepath.Join(dir, "merged.run"), AlignedBuffer(Align), NewEncoder(out))
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
		if !out.Blocked() {
			got, err := os.ReadFile(f.Path)
			if err != nil {
				rt.Fatal(err)
			}
			if !bytes.Equal(got, bytes.Join(want, nil)) {
				rt.Fatalf("the merged file differs from the sorted records")
			}
		}
		if got := readAll(t, fsys, f, 0, record.Fences, 256<<10); !slices.EqualFunc(got, want, bytes.Equal) {
			rt.Fatalf("the merged file's records differ from the sorted records")
		}
		if f.Records != int64(len(want)) || f.Raw != fa.Raw+fb.Raw {
			rt.Fatalf("merged %d records of %d bytes, want %d of %d", f.Records, f.Raw, len(want), fa.Raw+fb.Raw)
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
	if _, err := Create(fsys, path, AlignedBuffer(Align), nil); err == nil {
		t.Error("Create of a file that exists succeeded")
	}
	w, err := Create(fsys, filepath.Join(dir, "1.run"), AlignedBuffer(Align), NewEncoder(Format{Codec: S2, Block: Align}))
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
	for _, c := range []struct {
		dir    string
		format Format
	}{{dirs[0], Format{}}, {dirs[0], Format{Codec: S2, Block: 8 << 10}}, {dirs[len(dirs)-1], Format{}}} {
		dir := c.dir
		w, err := Create(fsys, filepath.Join(dir, fmt.Sprintf("direct-%d.run", os.Getpid())), AlignedBuffer(Align), NewEncoder(c.format))
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
		if c.format.Blocked() {
			size = f.Bytes
		}
		st, err := os.Stat(f.Path)
		if err != nil || st.Size() != size || f.Raw != f.Records*int64(len(record.Append(nil, 0, record.Left, []byte("k"), make([]byte, 37)))) {
			t.Errorf("%s: the file is %d bytes (%v), want %d", dir, st.Size(), err, size)
		}
		t.Logf("%s, %v: direct I/O %v, %d writes, %d bytes of %d", dir, c.format.Codec, w.Direct(), w.Writes(), f.Bytes, f.Raw)
		_ = os.Remove(f.Path)
	}
}
