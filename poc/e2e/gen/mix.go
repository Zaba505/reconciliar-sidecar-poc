package gen

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/zeebo/xxh3"
)

// Mix is how many records each side of a key has.
type Mix int

const (
	// Stress gives each side 0 to 3 records, uniformly: 6% of keys are
	// 1:1, 37.5% one-sided and 50% N:M. It is the default.
	Stress Mix = iota
	// Typical makes 93% of keys 1:1, 3% left-only, 3% right-only, 0.5%
	// 1:N and 0.5% N:1, with N 2 or 3.
	Typical
)

var mixNames = []string{"stress", "typical"}

// String returns "stress" or "typical".
func (m Mix) String() string {
	if m < 0 || int(m) >= len(mixNames) {
		return fmt.Sprintf("Mix(%d)", int(m))
	}
	return mixNames[m]
}

// ParseMix parses a mix's name, ignoring case.
func ParseMix(s string) (Mix, error) {
	i := slices.Index(mixNames, strings.ToLower(s))
	if i < 0 {
		return 0, fmt.Errorf("unknown mix %q, want stress or typical", s)
	}
	return Mix(i), nil
}

// MarshalText returns m's name.
func (m Mix) MarshalText() ([]byte, error) { return []byte(m.String()), nil }

// UnmarshalText parses a mix's name.
func (m *Mix) UnmarshalText(text []byte) error {
	v, err := ParseMix(string(text))
	*m = v
	return err
}

// Counts returns how many left and right records key k has, 0 to 3 each,
// by the Config's Mix. It is a pure function of the Seed and k.
func (c Config) Counts(k int) (left, right int) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(k))
	h := xxh3.HashSeed(b[:], c.Seed)
	if c.Mix == Typical {
		return typical(h)
	}
	return int(h & 3), int(h >> 2 & 3)
}

// typical draws a Typical key's counts from its hash: the low bit picks N,
// and bits 4 and up pick the kind of key in thousandths.
func typical(h uint64) (left, right int) {
	n := 2 + int(h&1)
	switch u := (h >> 4) % 1000; {
	case u < 930:
		return 1, 1
	case u < 960:
		return 1, 0
	case u < 990:
		return 0, 1
	case u < 995:
		return 1, n
	default:
		return n, 1
	}
}
