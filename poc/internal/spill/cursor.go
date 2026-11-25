package spill

import (
	"fmt"
	"os"

	"poc/internal/record"
)

// Cursor reads a segment of a run file through one buffer, with pread. It
// is a merge.Source.
//
// A record larger than the buffer is not read whole: the cursor holds its
// header and key, and its value is read when it is needed, by ReadValue or
// CopyValue, into memory its reader reserved for it. A record whose header
// and key alone are larger than the buffer makes the cursor take a larger
// buffer from grow.
//
// A cursor of a file of blocks decodes one block at a time into the first
// Block bytes of its buffer, and reads the file ahead into the rest: as
// many blocks as fit, in one pread. If the rest can't hold a block, it
// reads each block into its Decoder's scratch buffer instead. A large
// record's block is never read whole: the cursor holds its header and key,
// as above.
type Cursor struct {
	f    *os.File
	ord  uint64
	buf  []byte // all of it is the buffer
	r, w int    // buf[r:w] is read and not yet consumed
	off  int64  // the file offset of buf[w]
	end  int64  // of the segment
	cur  []byte // the head: a whole record, or its header and key
	hdr  record.Header
	lazy bool  // the head's value is not in cur
	skip int64 // the file offset after a lazy head's value
	// grow returns a buffer of at least n bytes, reserved, for a header and
	// key larger than the buffer.
	grow  func(n int) ([]byte, error)
	reads int64
	read  int64   // bytes
	b     *blocks // of a file of blocks
}

// blocks is the state of a cursor of a file of blocks. Its head is in dec,
// at r, and r is an offset in the block being decoded.
type blocks struct {
	format Format
	d      *Decoder
	dec    []byte // the decoded block, or a large record's header and key
	area   []byte // the file read ahead; nil if it can't hold a block
	aw     int    // area[:aw] holds the file's bytes aoff-aw to aoff
	aoff   int64
	at     int64 // the offset of the block being decoded
	n      int   // its records' bytes in dec; 0 for a large record
	next   int64 // the offset of the block after it
	valid  bool  // dec holds the block at at
	// Without read ahead, each read of a block takes the next block's
	// header too, so that the next read is exactly that block.
	peek   [blockHeader]byte
	peekAt int64 // of the block whose header peek is; -1 for none
}

// NewCursor returns a Cursor of f, a file without blocks, whose records
// are ordered by ord among those of other files, reading through buf.
func NewCursor(f *os.File, ord uint64, buf []byte, grow func(n int) ([]byte, error)) *Cursor {
	return &Cursor{f: f, ord: ord, buf: buf, grow: grow}
}

// NewFileCursor returns a Cursor of file, opened as f, whose records are
// ordered by ord among those of other files, reading through buf. A file
// of blocks needs a buffer of at least file.Format.MinBuffer() bytes, and
// a Decoder of its format, which the cursor shares with the other cursors
// of one goroutine.
func NewFileCursor(f *os.File, file *File, ord uint64, buf []byte, grow func(n int) ([]byte, error), d *Decoder) *Cursor {
	c := NewCursor(f, ord, buf, grow)
	if !file.Format.Blocked() {
		return c
	}
	block := file.Format.Block
	if len(buf) < file.Format.MinBuffer() || d == nil || len(d.scratch) < 2*blockHeader+block {
		panic("spill: a cursor of a file of blocks without room to decode them")
	}
	c.b = &blocks{format: file.Format, d: d, dec: buf[:block], peekAt: -1}
	if area := buf[block:]; len(area) >= blockHeader+block {
		c.b.area = area
	}
	return c
}

// Reads returns the number of preads so far.
func (c *Cursor) Reads() int64 { return c.reads }

// BytesRead returns the bytes the preads so far read.
func (c *Cursor) BytesRead() int64 { return c.read }

// Reset makes the cursor read the bytes off to end of its file. In a file
// of blocks, they are whole blocks.
func (c *Cursor) Reset(off, end int64) error {
	c.off, c.end, c.r, c.w = off, end, 0, 0
	if c.b != nil {
		c.b.aw, c.b.aoff, c.b.peekAt = 0, off, -1
		return c.loadBlock(off)
	}
	return c.load()
}

func (c *Cursor) Head() []byte { return c.cur }

func (c *Cursor) Peek() (uint64, bool) {
	if c.cur == nil {
		return 0, false
	}
	return c.hdr.Hash, true
}

func (c *Cursor) Ord() uint64 { return c.ord }

// Pos returns the position of the head. In a file of blocks, it is the
// offset of the head's block, shifted, and the head's offset in the block.
func (c *Cursor) Pos() int64 {
	if c.b != nil {
		if c.cur == nil {
			return c.end << posShift
		}
		return c.b.at<<posShift | int64(c.r)
	}
	return c.off - int64(c.w-c.r)
}

func (c *Cursor) Next() error {
	if c.b != nil {
		return c.nextInBlock()
	}
	if c.lazy {
		c.off, c.r, c.w = c.skip, 0, 0
		return c.load()
	}
	c.r += len(c.cur)
	// The common case: the next record is whole in the buffer.
	if avail := c.buf[c.r:c.w]; len(avail) >= 10 && avail[8] < 0x80 && avail[9] < 0x80 {
		if n := 10 + int(avail[8]>>1) + int(avail[9]); n <= len(avail) {
			c.hdr, _ = record.ParseHeader(avail)
			c.cur = avail[:n:n]
			return nil
		}
	}
	return c.load()
}

func (c *Cursor) Rewind(pos int64) error {
	if c.b != nil {
		return c.rewindBlock(pos)
	}
	if start := c.off - int64(c.w); !c.lazy && pos >= start && pos <= c.off {
		c.r = int(pos - start)
	} else {
		c.off, c.r, c.w = pos, 0, 0
	}
	return c.load()
}

// ReadValue reads the head's value into dst.
func (c *Cursor) ReadValue(dst []byte) error {
	if len(dst) != c.hdr.ValLen {
		panic("spill: ReadValue into a buffer of the wrong size")
	}
	if !c.lazy {
		copy(dst, c.cur[c.hdr.Len+c.hdr.KeyLen:])
		return nil
	}
	c.reads++
	c.read += int64(len(dst))
	if _, err := c.f.ReadAt(dst, c.skip-int64(len(dst))); err != nil {
		return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
	}
	return nil
}

// CopyValue writes the value of a head that doesn't hold it to w, through
// the cursor's buffer. The head is no longer valid afterwards; Next moves
// past it.
func (c *Cursor) CopyValue(w *Writer) error {
	at := c.skip - int64(c.hdr.ValLen)
	for at < c.skip {
		n := int(min(int64(len(c.buf)), c.skip-at))
		c.reads++
		c.read += int64(n)
		if _, err := c.f.ReadAt(c.buf[:n], at); err != nil {
			return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
		}
		w.Write(c.buf[:n])
		at += int64(n)
	}
	c.cur = nil
	if c.b != nil {
		// The copy went through the whole buffer.
		c.b.aw, c.b.valid = 0, false
	}
	return nil
}

// load makes cur the record at buf[r:], reading more of the segment if the
// buffer doesn't hold what the cursor keeps of it.
func (c *Cursor) load() error {
	c.lazy = false
	for {
		avail := c.buf[c.r:c.w]
		h, ok := record.ParseHeader(avail)
		need := h.Size()
		lazy := ok && need > len(c.buf)
		if lazy {
			need = h.Len + h.KeyLen // the value stays on disk
		}
		if ok && need <= len(avail) {
			c.hdr, c.cur, c.lazy = h, avail[:need:need], lazy
			if lazy {
				c.skip = c.off - int64(len(avail)) + int64(h.Size())
			}
			return nil
		}
		if c.off >= c.end {
			if len(avail) == 0 {
				c.cur = nil
				return nil
			}
			return fmt.Errorf("run file %s ends inside a record", c.f.Name())
		}
		if ok && need > len(c.buf) {
			nb, err := c.grow(need)
			if err != nil {
				return err
			}
			copy(nb, avail)
			c.buf = nb
		} else if !ok && len(avail) >= record.MaxHeader {
			return fmt.Errorf("run file %s holds a corrupt record", c.f.Name())
		} else if c.r > 0 {
			copy(c.buf, avail)
		}
		c.w, c.r = len(avail), 0
		m := int(min(int64(len(c.buf)-c.w), c.end-c.off))
		// End the read on a page of the file, so that the next one starts
		// on one and no page is read twice, unless that leaves too little
		// for what the cursor needs.
		if tail := int((c.off + int64(m)) & (Align - 1)); tail < m && c.off+int64(m) < c.end && (!ok || c.w+m-tail >= need) {
			m -= tail
		}
		c.reads++
		c.read += int64(m)
		if _, err := c.f.ReadAt(c.buf[c.w:c.w+m], c.off); err != nil {
			return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
		}
		c.off += int64(m)
		c.w += m
	}
}

// readAt reads len(p) bytes of the file at off.
func (c *Cursor) readAt(p []byte, off int64) error {
	c.reads++
	c.read += int64(len(p))
	if _, err := c.f.ReadAt(p, off); err != nil {
		return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
	}
	return nil
}

func (c *Cursor) corrupt(at int64, what string) error {
	return fmt.Errorf("run file %s holds %s at %d", c.f.Name(), what, at)
}

// nextInBlock is Next in a file of blocks.
func (c *Cursor) nextInBlock() error {
	b := c.b
	if c.lazy {
		return c.loadBlock(c.skip)
	}
	c.r += len(c.cur)
	if c.r < b.n {
		return c.parse()
	}
	return c.loadBlock(b.next)
}

// rewindBlock is Rewind in a file of blocks. A position in the block being
// decoded costs nothing; one in another block decodes it again, from the
// file read ahead if it is still there.
func (c *Cursor) rewindBlock(pos int64) error {
	b := c.b
	at, r := pos>>posShift, int(pos&(1<<posShift-1))
	if !b.valid || at != b.at {
		if err := c.loadBlock(at); err != nil {
			return err
		}
	}
	switch {
	case c.cur == nil || c.lazy && r == 0:
		return nil
	case c.lazy || r >= b.n:
		return fmt.Errorf("spill: Rewind to %d in the block at %d, which isn't a head's position", r, at)
	}
	c.r = r
	return c.parse()
}

// parse makes cur the record at dec[r:], which the block holds whole.
func (c *Cursor) parse() error {
	avail := c.b.dec[c.r:c.b.n]
	h, ok := record.ParseHeader(avail)
	if n := h.Size(); ok && n <= len(avail) {
		c.hdr, c.cur = h, avail[:n:n]
		return nil
	}
	return c.corrupt(c.b.at, "a block that ends inside a record")
}

// loadBlock decodes the block at at, and makes its first record the head.
func (c *Cursor) loadBlock(at int64) error {
	b := c.b
	b.at, b.valid, c.lazy, c.cur, c.r = at, false, false, nil, 0
	if at >= c.end {
		return nil
	}
	// The block's size, from the header the last read took, or, failing
	// that, the largest a block of records takes.
	want := 2*blockHeader + b.format.Block
	var p []byte
	if b.peekAt == at {
		p = b.peek[:]
	} else {
		var err error
		if p, err = c.fetch(at, blockHeader, want); err != nil {
			return err
		}
	}
	b.peekAt = -1
	kind, raw, stored := parseBlockHeader(p)
	end := at + blockHeader + stored
	if end > c.end || end < at {
		return c.corrupt(at, "a block that runs past its segment")
	}
	switch kind {
	case blockRaw, blockCompressed:
		if raw == 0 || raw > int64(b.format.Block) || stored > raw || kind == blockRaw && stored != raw {
			return c.corrupt(at, "a block of a bad size")
		}
		p, err := c.fetch(at, blockHeader+int(stored), 2*blockHeader+int(stored))
		if err != nil {
			return err
		}
		dst := b.dec[:raw]
		if kind == blockRaw {
			copy(dst, p[blockHeader:])
		} else if err := b.d.decode(b.format.Codec, dst, p[blockHeader:]); err != nil {
			return c.corrupt(at, "a block that doesn't decode")
		}
		b.n, b.next, b.valid = int(raw), end, true
		if d := b.d; b.area == nil && d.owner == c && end >= d.off && end+blockHeader <= d.off+int64(d.n) {
			i := int(end - d.off)
			copy(b.peek[:], d.scratch[i:i+blockHeader])
			b.peekAt = end
		}
		return c.parse()
	case blockLarge:
		// The record's header, and its key, which may be larger than the
		// buffer; its value stays in the file.
		p, err := c.fetch(at, blockHeader+int(min(stored, record.MaxHeader)), blockHeader+256)
		if err != nil {
			return err
		}
		h, ok := record.ParseHeader(p[blockHeader:])
		if !ok || int64(h.Size()) != stored {
			return c.corrupt(at, "a large record of a bad size")
		}
		need := h.Len + h.KeyLen
		if need > len(b.dec) {
			nb, err := c.grow(need)
			if err != nil {
				return err
			}
			b.dec = nb
		}
		if blockHeader+need <= c.fetchable() {
			p, err := c.fetch(at, blockHeader+need, blockHeader+need)
			if err != nil {
				return err
			}
			copy(b.dec, p[blockHeader:])
		} else if err := c.readAt(b.dec[:need], at+blockHeader); err != nil {
			return err
		}
		c.hdr, c.cur, c.lazy, c.skip = h, b.dec[:need:need], true, end
		b.n, b.next, b.valid = 0, end, true
		return nil
	}
	return c.corrupt(at, "a block of an unknown kind")
}

// fetchable returns the most bytes fetch returns at once.
func (c *Cursor) fetchable() int {
	if c.b.area != nil {
		return len(c.b.area)
	}
	return len(c.b.d.scratch)
}

// fetch returns n bytes of the file at off, from the file read ahead or,
// if the cursor can't read ahead, from the Decoder's scratch buffer, which
// a read fills with want bytes, if the segment has them. They are valid
// until the next fetch of any cursor of the Decoder.
func (c *Cursor) fetch(off int64, n, want int) ([]byte, error) {
	b, d := c.b, c.b.d
	if off+int64(n) > c.end {
		return nil, c.corrupt(off, "a block that runs past its segment")
	}
	if b.area == nil {
		if d.owner == c && off >= d.off && off+int64(n) <= d.off+int64(d.n) {
			i := int(off - d.off)
			return d.scratch[i : i+n], nil
		}
		m := int(min(int64(min(max(n, want), len(d.scratch))), c.end-off))
		if err := c.readAt(d.scratch[:m], off); err != nil {
			d.owner = nil
			return nil, err
		}
		d.owner, d.off, d.n = c, off, m
		return d.scratch[:n], nil
	}
	start := b.aoff - int64(b.aw)
	if off >= start && off+int64(n) <= b.aoff {
		i := int(off - start)
		return b.area[i : i+n], nil
	}
	// Read ahead from off, keeping what the area holds of it.
	if off >= start && off < b.aoff {
		b.aw = copy(b.area, b.area[off-start:b.aw])
	} else {
		b.aw, b.aoff = 0, off
	}
	m := int(min(int64(len(b.area)-b.aw), c.end-b.aoff))
	// End the read on a page of the file, as load does.
	if tail := int((b.aoff + int64(m)) & (Align - 1)); tail < m && b.aoff+int64(m) < c.end && b.aw+m-tail >= n {
		m -= tail
	}
	if err := c.readAt(b.area[b.aw:b.aw+m], b.aoff); err != nil {
		b.aw = 0
		return nil, err
	}
	b.aoff += int64(m)
	b.aw += m
	return b.area[:n], nil
}
