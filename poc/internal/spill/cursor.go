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
}

// NewCursor returns a Cursor of f, whose records are ordered by ord among
// those of other files, reading through buf.
func NewCursor(f *os.File, ord uint64, buf []byte, grow func(n int) ([]byte, error)) *Cursor {
	return &Cursor{f: f, ord: ord, buf: buf, grow: grow}
}

// Reads returns the number of preads so far.
func (c *Cursor) Reads() int64 { return c.reads }

// Reset makes the cursor read the bytes off to end of its file.
func (c *Cursor) Reset(off, end int64) error {
	c.off, c.end, c.r, c.w = off, end, 0, 0
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

func (c *Cursor) Pos() int64 { return c.off - int64(c.w-c.r) }

func (c *Cursor) Next() error {
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
		if _, err := c.f.ReadAt(c.buf[:n], at); err != nil {
			return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
		}
		w.Write(c.buf[:n])
		at += int64(n)
	}
	c.cur = nil
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
		if _, err := c.f.ReadAt(c.buf[c.w:c.w+m], c.off); err != nil {
			return fmt.Errorf("read run file %s: %w", c.f.Name(), err)
		}
		c.off += int64(m)
		c.w += m
	}
}
