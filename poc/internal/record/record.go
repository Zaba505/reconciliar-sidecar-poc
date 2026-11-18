// Package record is the encoding of a run's records, in memory and in run
// files alike (§8.1):
//
//	hash u64 (little endian) | uvarint(key len << 1 | side) | uvarint(value len) | key | value
//
// Records sort by (hash, key, side), then by ingest order, which is not
// part of the encoding. A non-empty key's hash is XXH3 with Seed; an empty
// key's is 0, so that empty keys sort first. The hash's top byte is the
// record's fence: the 256 fences are equal ranges of the hash, so a sorted
// run is a sequence of fences, and the join cuts its work at them.
package record

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math/bits"
	"slices"

	"github.com/zeebo/xxh3"
)

// Side is the side of a run a record belongs to.
type Side uint8

const (
	Left Side = iota
	Right
)

func (s Side) String() string {
	if s == Left {
		return "left"
	}
	return "right"
}

// Seed is the XXH3 seed keys are hashed with. Runs don't survive a restart,
// so it may change between versions.
const Seed uint64 = 0x5265636f6e53640a

// Hash returns the hash of key: 0 for an empty key.
func Hash(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}
	return xxh3.HashSeed(key, Seed)
}

// Fences is the number of fences: equal ranges of the hash, in order.
const Fences = 256

// Fence returns the fence of hash.
func Fence(hash uint64) int { return int(hash >> 56) }

// MaxHeader is the most bytes a record's hash and lengths take.
const MaxHeader = 8 + 2*binary.MaxVarintLen64

func uvarintLen(x uint64) int { return (bits.Len64(x|1) + 6) / 7 }

// Len returns the size of the encoding of a record with a key of kl bytes
// and a value of vl bytes.
func Len(kl, vl int) int {
	return 8 + uvarintLen(uint64(kl)<<1) + uvarintLen(uint64(vl)) + kl + vl
}

// Put encodes a record into p, which must be Len(len(key), len(value))
// bytes long.
func Put(p []byte, hash uint64, side Side, key, value []byte) {
	binary.LittleEndian.PutUint64(p, hash)
	i := 8
	kl, vl := len(key), len(value)
	if kl < 64 && vl < 128 {
		p[8], p[9] = byte(kl<<1)|byte(side), byte(vl)
		i = 10
	} else {
		i += binary.PutUvarint(p[i:], uint64(kl)<<1|uint64(side))
		i += binary.PutUvarint(p[i:], uint64(vl))
	}
	i += copy(p[i:], key)
	copy(p[i:], value)
}

// Append appends the encoding of a record to dst.
func Append(dst []byte, hash uint64, side Side, key, value []byte) []byte {
	n := Len(len(key), len(value))
	dst = slices.Grow(dst, n)[:len(dst)+n]
	Put(dst[len(dst)-n:], hash, side, key, value)
	return dst
}

// Header is the part of a record before its key.
type Header struct {
	Hash   uint64
	Side   Side
	Len    int // of the hash and the lengths
	KeyLen int
	ValLen int
}

// Size returns the size of the whole record.
func (h Header) Size() int { return h.Len + h.KeyLen + h.ValLen }

// ParseHeader parses the header at the start of b. It returns false if b
// doesn't hold all of it; with MaxHeader bytes or more, false means it is
// corrupt.
func ParseHeader(b []byte) (Header, bool) {
	if len(b) >= 10 && b[8] < 0x80 && b[9] < 0x80 {
		return Header{Hash: binary.LittleEndian.Uint64(b), Side: Side(b[8] & 1), Len: 10, KeyLen: int(b[8] >> 1), ValLen: int(b[9])}, true
	}
	if len(b) < 9 {
		return Header{}, false
	}
	kl, n := binary.Uvarint(b[8:])
	if n <= 0 {
		return Header{}, false
	}
	vl, m := binary.Uvarint(b[8+n:])
	if m <= 0 || kl>>1 > 1<<40 || vl > 1<<40 {
		return Header{}, false
	}
	return Header{Hash: binary.LittleEndian.Uint64(b), Side: Side(kl & 1), Len: 8 + n + m, KeyLen: int(kl >> 1), ValLen: int(vl)}, true
}

// Size returns the size of the record encoded at the start of b, or false
// if b doesn't hold its header.
func Size(b []byte) (int, bool) {
	if len(b) >= 10 && b[8] < 0x80 && b[9] < 0x80 {
		return 10 + int(b[8]>>1) + int(b[9]), true
	}
	h, ok := ParseHeader(b)
	return h.Size(), ok
}

// SideKey returns the side and key of the record that starts at b, which
// holds at least its header and key. The key aliases b.
func SideKey(b []byte) (Side, []byte) {
	if b[8] < 0x80 && b[9] < 0x80 {
		return Side(b[8] & 1), b[10 : 10+int(b[8]>>1)]
	}
	h, _ := ParseHeader(b)
	return h.Side, b[h.Len : h.Len+h.KeyLen]
}

// Decode returns the parts of rec, which holds exactly one whole record.
// The key and value alias rec, and are capped so that appending to them
// copies.
func Decode(rec []byte) (hash uint64, side Side, key, value []byte) {
	h, _ := ParseHeader(rec)
	k := h.Len + h.KeyLen
	v := k + h.ValLen
	return h.Hash, h.Side, rec[h.Len:k:k], rec[k:v:v]
}

// Compare orders the records that start at a and b, which hold at least
// their headers and keys, by (hash, key, side).
func Compare(a, b []byte) int {
	if c := cmp.Compare(binary.LittleEndian.Uint64(a), binary.LittleEndian.Uint64(b)); c != 0 {
		return c
	}
	as, ak := SideKey(a)
	bs, bk := SideKey(b)
	if c := bytes.Compare(ak, bk); c != 0 {
		return c
	}
	return cmp.Compare(as, bs)
}
