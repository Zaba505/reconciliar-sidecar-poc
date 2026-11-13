package gen

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"poc/e2e/harness"
)

// KeyFormat is how key indexes become key bytes.
type KeyFormat int

const (
	// DecimalKeys are "key-" and 12 decimal digits of k, like
	// "key-000000001234".
	DecimalKeys KeyFormat = iota
	// RFKeys are "RF" and 16 uppercase hex digits of a bijective, seeded
	// scramble of k, like "RF3A9F00C1D2E4B7A6": a reference a client might
	// really join on, in no particular order.
	RFKeys
)

var keyFormatNames = []string{"decimal", "rf"}

// String returns "decimal" or "rf".
func (f KeyFormat) String() string {
	if f < 0 || int(f) >= len(keyFormatNames) {
		return fmt.Sprintf("KeyFormat(%d)", int(f))
	}
	return keyFormatNames[f]
}

// ParseKeyFormat parses a key format's name, ignoring case.
func ParseKeyFormat(s string) (KeyFormat, error) {
	i := slices.Index(keyFormatNames, strings.ToLower(s))
	if i < 0 {
		return 0, fmt.Errorf("unknown key format %q, want decimal or rf", s)
	}
	return KeyFormat(i), nil
}

// MarshalText returns f's name.
func (f KeyFormat) MarshalText() ([]byte, error) { return []byte(f.String()), nil }

// UnmarshalText parses a key format's name.
func (f *KeyFormat) UnmarshalText(text []byte) error {
	v, err := ParseKeyFormat(string(text))
	*f = v
	return err
}

const (
	decimalPrefix = "key-"
	decimalDigits = 12
	maxDecimalKey = 1_000_000_000_000
	rfPrefix      = "RF"
	hexUpper      = "0123456789ABCDEF"
	hexLower      = "0123456789abcdef"
)

// keySalt is what RF keys XOR k with before scrambling it.
func keySalt(seed uint64) uint64 { return mix(seed ^ 0x5bd1e995) }

// append appends key k in format f.
func (f KeyFormat) append(dst []byte, seed uint64, k int) []byte {
	if f == RFKeys {
		id := mix(uint64(k) ^ keySalt(seed))
		dst = append(dst, rfPrefix...)
		for i := range 16 {
			dst = append(dst, hexUpper[id>>(60-4*i)&15])
		}
		return dst
	}
	if k < 0 || k >= maxDecimalKey {
		return fmt.Appendf(dst, "key-%012d", k)
	}
	dst = append(dst, decimalPrefix...)
	var d [decimalDigits]byte
	for i := range d {
		d[len(d)-1-i] = byte('0' + k%10)
		k /= 10
	}
	return append(dst, d[:]...)
}

// Key returns key k's bytes, in the Config's KeyFormat.
func (c Config) Key(k int) []byte { return c.AppendKey(nil, k) }

// AppendKey appends key k's bytes to dst.
func (c Config) AppendKey(dst []byte, k int) []byte { return c.KeyFormat.append(dst, c.Seed, k) }

// ParseKey returns the index of key, which must be in the Config's
// KeyFormat and name one of its Keys.
func (c Config) ParseKey(key []byte) (k int, ok bool) {
	var x uint64
	switch c.KeyFormat {
	case DecimalKeys:
		s, found := bytes.CutPrefix(key, []byte(decimalPrefix))
		if !found || len(s) != decimalDigits {
			return 0, false
		}
		for _, d := range s {
			if d < '0' || d > '9' {
				return 0, false
			}
			x = x*10 + uint64(d-'0')
		}
	case RFKeys:
		s, found := bytes.CutPrefix(key, []byte(rfPrefix))
		if !found || len(s) != 16 {
			return 0, false
		}
		var id uint64
		for _, d := range s {
			i := strings.IndexByte(hexUpper, d)
			if i < 0 {
				return 0, false
			}
			id = id<<4 | uint64(i)
		}
		x = unmix(id) ^ keySalt(c.Seed)
	default:
		return 0, false
	}
	if x >= uint64(c.Keys) {
		return 0, false
	}
	return int(x), true
}

// The identity is a bijective scramble of (side, k, j) packed into 64
// bits: the side in bits 62-63, j in bits 60-61 and k in bits 0-59.
const (
	maxKeys   = 1 << 60
	keyMask   = maxKeys - 1
	maxRecord = 3 // records per key and side
)

func identity(seed uint64, side harness.Side, k, j int) uint64 {
	return mix(uint64(side)<<62|uint64(j&3)<<60|uint64(k)&keyMask) ^ seed
}

// Identity returns the identity field of key k's j-th record on side: a
// bijective, seeded scramble of (side, k, j). k must be below 2^60 and j
// at most 3.
func (c Config) Identity(side harness.Side, k, j int) uint64 {
	return identity(c.Seed, side, k, j)
}

// ParseIdentity inverts Identity. For a number Identity didn't return,
// side may be neither Left nor Right and k may be out of range.
func (c Config) ParseIdentity(id uint64) (side harness.Side, k, j int) {
	x := unmix(id ^ c.Seed)
	return harness.Side(x >> 62), int(x & keyMask), int(x >> 60 & 3)
}

// Locate returns the key and record index that val, a value stored on
// side, says it came from. It reads a schema value's identity where its
// side's encoding puts it: in protobuf the 8 little-endian bytes after the
// leading 0x09 tag, in Avro the first 8 bytes, and in JSON the 16 hex
// digits after {"id":". It reads a legacy value's header: the side at
// [0], k little-endian at [1:9] and j at [9]. ok is false if val is too
// short or malformed, names the other side, or k is not one of the
// Config's Keys. Locate doesn't check the rest of val.
func (c Config) Locate(side harness.Side, val []byte) (k, j int, ok bool) {
	var v Values
	switch side {
	case harness.Left:
		v = c.Left
	case harness.Right:
		v = c.Right
	default:
		return 0, 0, false
	}
	if v.Schema == nil {
		if len(val) < legacyHeader || val[0] != byte(side) {
			return 0, 0, false
		}
		x := binary.LittleEndian.Uint64(val[1:])
		j := int(val[9])
		if x >= uint64(c.Keys) || j > maxRecord {
			return 0, 0, false
		}
		return int(x), j, true
	}
	id, found := locateID(v.Encoding, val)
	if !found {
		return 0, 0, false
	}
	s, k, j := c.ParseIdentity(id)
	if s != side || k >= c.Keys {
		return 0, 0, false
	}
	return k, j, true
}

// jsonIDPrefix is how every JSON value starts.
const jsonIDPrefix = `{"id":"`

// locateID reads the identity of a value in enc.
func locateID(enc Encoding, val []byte) (uint64, bool) {
	switch enc {
	case Protobuf:
		if len(val) < 9 || val[0] != 0x09 {
			return 0, false
		}
		return binary.LittleEndian.Uint64(val[1:]), true
	case Avro:
		if len(val) < 8 {
			return 0, false
		}
		return binary.LittleEndian.Uint64(val), true
	case JSON:
		s, found := bytes.CutPrefix(val, []byte(jsonIDPrefix))
		if !found || len(s) < 17 || s[16] != '"' {
			return 0, false
		}
		var id uint64
		for _, d := range s[:16] {
			i := strings.IndexByte(hexLower, d)
			if i < 0 {
				return 0, false
			}
			id = id<<4 | uint64(i)
		}
		return id, true
	}
	return 0, false
}
