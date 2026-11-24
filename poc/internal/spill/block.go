package spill

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// A file of blocks holds its records in blocks, each a header and a body:
//
//	header u64 (little endian) | body
//
// The header's low two bits are its kind:
//
//   - blockRaw: whole records, raw bytes long, stored as they are.
//   - blockCompressed: whole records, raw bytes long, compressed to stored
//     bytes with the file's codec.
//   - blockLarge: one record larger than the file's block size, stored as
//     it is, so that its value can be read with one pread, or skipped.
//
// A raw or compressed block's header holds raw in bits 2 to 32 and stored
// in bits 33 to 63; a large one's holds the record's size in bits 2 to 63.
//
// A block holds at most the file's block size of records, and no record
// spans two blocks. A fence starts a new block, so a file's fences are
// offsets of blocks, and a segment of fences is whole blocks.
const (
	blockRaw = iota
	blockCompressed
	blockLarge

	// blockHeader is the size of a block's header.
	blockHeader = 8
	// MaxBlock is the largest block size.
	MaxBlock = 1 << 20
	// posShift is where a cursor's position keeps the offset of its block;
	// the bits below it are the offset of the head in the block.
	posShift = 21
	// maxBlockedFile is the size of a file of blocks a cursor's positions
	// hold: 4 TiB.
	maxBlockedFile = 1 << (63 - posShift)
	// minSaving is the share of its records a block's compression must save
	// for the block to be stored compressed: one in minSaving bytes.
	// Otherwise its decompression would cost more than the bytes it saves.
	minSaving = 16
)

// Codec is how a run file's blocks are compressed.
type Codec uint8

const (
	// None writes records back to back, without blocks.
	None Codec = iota
	// S2 compresses blocks with S2, a faster Snappy.
	S2
	// Zstd compresses blocks with zstd at its fastest level.
	Zstd
)

func (c Codec) String() string {
	switch c {
	case None:
		return "none"
	case S2:
		return "s2"
	case Zstd:
		return "zstd"
	}
	return fmt.Sprintf("Codec(%d)", uint8(c))
}

// MinLimit returns the smallest budget c spills under. zstd's encoder
// holds about 300 KiB whatever the block size, so it needs a budget large
// enough that this is a small part of it.
func (c Codec) MinLimit() int64 {
	if c == Zstd {
		return 8 << 20
	}
	return 0
}

// ParseCodec parses the name of a codec: none, s2 or zstd.
func ParseCodec(s string) (Codec, error) {
	for _, c := range []Codec{None, S2, Zstd} {
		if s == c.String() {
			return c, nil
		}
	}
	return None, fmt.Errorf("unknown codec %q: want none, s2 or zstd", s)
}

// Format is the layout of a run file.
type Format struct {
	Codec Codec
	// Block is the most bytes of records a block holds; 0 with None.
	Block int
}

// Blocked reports whether a file of f holds blocks.
func (f Format) Blocked() bool { return f.Codec != None }

// FormatFor returns the Format of codec under a budget of limit bytes:
// blocks of a 1024th of the budget, between 4 KiB and 32 KiB. Under a
// budget below codec.MinLimit(), it is the format without blocks.
//
// A cursor decodes a block into its read buffer, so a Join's read buffers
// can't be smaller than a block. A 1024th of the budget keeps that floor
// where it is for small budgets, which a Join's buffers shrink to as files
// pile up. Larger blocks compress better: s2 saves about 30% of the
// realistic records at 16 KiB and 33% at 32 KiB, zstd 39% and 43%.
func FormatFor(codec Codec, limit int64) Format {
	if codec == None || limit < codec.MinLimit() {
		return Format{}
	}
	return Format{Codec: codec, Block: int(min(max(limit/1024, 4<<10), 32<<10)) &^ (Align - 1)}
}

func (f Format) check() {
	if f.Blocked() && (f.Block < 1 || f.Block > MaxBlock || f.Codec > Zstd) {
		panic(fmt.Sprintf("spill: bad format %v with blocks of %d", f.Codec, f.Block))
	}
}

// MaxBytes returns the most bytes a file of f takes holding raw bytes of
// records: a block's body is never larger than its records, and every
// block adds a header. A block that is cut before it is full is followed
// by a fence, by a large record, or by a record that would overflow it, so
// there are at most 4·raw/Block+Fences+1 of them.
func (f Format) MaxBytes(raw int64) int64 {
	if !f.Blocked() {
		return raw
	}
	return raw + blockHeader*(4*raw/int64(f.Block)+257)
}

// MinBuffer returns the smallest read buffer a cursor of a file of f
// takes: one that holds a block.
func (f Format) MinBuffer() int {
	if !f.Blocked() {
		return Align
	}
	return max(f.Block, Align)
}

// MinBuffer returns the smallest read buffer a cursor of every one of
// files takes.
func MinBuffer(files []*File) int {
	n := Align
	for _, f := range files {
		n = max(n, f.Format.MinBuffer())
	}
	return n
}

// zstdEncoderBytes and zstdDecoderBytes return the memory a zstd encoder
// and decoder of blocks of n bytes hold besides the buffers this package
// gives them. Measured with klauspost/compress 1.20 at its fastest level
// and with lower memory: the encoder holds 304 to 359 KiB for blocks of 4
// to 32 KiB, the decoder 22 to 57 KiB. These leave room to spare.
func zstdEncoderBytes(n int) int64 { return 320<<10 + 2*int64(n) }
func zstdDecoderBytes(n int) int64 { return 24<<10 + 2*int64(n) }

// Encoder holds what a Writer of a Format needs besides its write buffer:
// the block being filled, its compressed bytes, and the codec's state. One
// Writer uses it at a time, and it is reused from one to the next.
type Encoder struct {
	format Format
	raw    []byte // the block being filled
	out    []byte // its compressed bytes
	zstd   *zstd.Encoder
}

// NewEncoder returns an Encoder of f, or nil if f has no blocks.
func NewEncoder(f Format) *Encoder {
	f.check()
	if !f.Blocked() {
		return nil
	}
	e := &Encoder{format: f, raw: make([]byte, 0, f.Block)}
	switch f.Codec {
	case S2:
		e.out = make([]byte, s2.MaxEncodedLen(f.Block))
	case Zstd:
		z, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithLowerEncoderMem(true),
			zstd.WithWindowSize(windowFor(f.Block)),
			zstd.WithEncoderCRC(false))
		if err != nil {
			panic("spill: " + err.Error())
		}
		e.zstd = z
		e.out = make([]byte, 0, f.Block+f.Block/128+512)
	}
	return e
}

// EncoderBytes returns the memory an Encoder of f takes.
func (f Format) EncoderBytes() int64 {
	switch f.Codec {
	case S2:
		return int64(f.Block + s2.MaxEncodedLen(f.Block))
	case Zstd:
		return int64(2*f.Block+f.Block/128+512) + zstdEncoderBytes(f.Block)
	}
	return 0
}

// Format returns the format the Encoder writes.
func (e *Encoder) Format() Format { return e.format }

// compress returns the compressed bytes of raw, valid until the next call.
func (e *Encoder) compress(raw []byte) []byte {
	switch e.format.Codec {
	case S2:
		return s2.Encode(e.out[:cap(e.out)], raw)
	case Zstd:
		e.out = e.zstd.EncodeAll(raw, e.out[:0])
		return e.out
	}
	return raw
}

// windowFor returns the zstd window of blocks of n bytes: a power of two
// at least as large, and at least zstd's smallest.
func windowFor(n int) int {
	w := zstd.MinWindowSize
	for w < n {
		w <<= 1
	}
	return w
}

// Decoder decodes the blocks of a join worker's or a merge's cursors, one
// cursor at a time. It holds a scratch buffer, which a cursor whose read
// buffer is too small to read ahead reads each block into, with the next
// block's header.
type Decoder struct {
	scratch []byte
	owner   *Cursor // whose bytes scratch holds
	off     int64   // of scratch[0] in the owner's file
	n       int     // bytes scratch holds
	zstd    *zstd.Decoder
}

// NewDecoder returns a Decoder of files' blocks, or nil if none of them
// holds blocks.
func NewDecoder(files []*File) *Decoder {
	block, codecs := decoderNeeds(files)
	if block == 0 {
		return nil
	}
	d := &Decoder{scratch: make([]byte, 2*blockHeader+block)}
	if codecs[Zstd] {
		z, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(uint64(windowFor(block))),
			zstd.WithDecoderMaxMemory(uint64(block)))
		if err != nil {
			panic("spill: " + err.Error())
		}
		d.zstd = z
	}
	return d
}

// DecoderBytes returns the memory a Decoder of files takes.
func DecoderBytes(files []*File) int64 {
	block, codecs := decoderNeeds(files)
	if block == 0 {
		return 0
	}
	n := int64(2*blockHeader + block)
	if codecs[Zstd] {
		n += zstdDecoderBytes(block)
	}
	return n
}

func decoderNeeds(files []*File) (block int, codecs [Zstd + 1]bool) {
	for _, f := range files {
		if f.Format.Blocked() {
			block = max(block, f.Format.Block)
			codecs[f.Format.Codec] = true
		}
	}
	return block, codecs
}

// Close releases the codec's resources.
func (d *Decoder) Close() {
	if d != nil && d.zstd != nil {
		d.zstd.Close()
	}
}

var errCorruptBlock = errors.New("a corrupt block")

// decode decodes src, a block compressed with codec, into dst, which is
// exactly as long as its records.
func (d *Decoder) decode(codec Codec, dst, src []byte) error {
	switch codec {
	case S2:
		if n, err := s2.DecodedLen(src); err != nil || n != len(dst) {
			return errCorruptBlock
		}
		if _, err := s2.Decode(dst, src); err != nil {
			return errCorruptBlock
		}
		return nil
	case Zstd:
		out, err := d.zstd.DecodeAll(src, dst[:0])
		if err != nil || len(out) != len(dst) || len(out) > 0 && &out[0] != &dst[0] {
			return errCorruptBlock
		}
		return nil
	}
	return errCorruptBlock
}

func putBlockHeader(p []byte, kind, raw, stored int) {
	binary.LittleEndian.PutUint64(p, uint64(kind)|uint64(raw)<<2|uint64(stored)<<33)
}

func putLargeHeader(p []byte, size int) {
	binary.LittleEndian.PutUint64(p, blockLarge|uint64(size)<<2)
}

// parseBlockHeader returns the kind of the block whose header is p, and
// its raw and stored sizes; a large block's are both the record's size.
func parseBlockHeader(p []byte) (kind int, raw, stored int64) {
	h := binary.LittleEndian.Uint64(p)
	kind = int(h & 3)
	if kind == blockLarge {
		return kind, int64(h >> 2), int64(h >> 2)
	}
	return kind, int64(h >> 2 & (1<<31 - 1)), int64(h >> 33)
}
