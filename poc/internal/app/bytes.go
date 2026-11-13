package app

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// byteUnits are the suffixes parseBytes accepts, longest first so that
// "KiB" is tried before "B".
var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
	{"B", 1},
}

// parseBytes parses a byte count: a non-negative decimal integer with an
// optional IEC suffix, such as "1048576", "16MiB" or "2GiB".
func parseBytes(s string) (int64, error) {
	digits, scale := s, int64(1)
	for _, u := range byteUnits {
		if d, ok := strings.CutSuffix(s, u.suffix); ok {
			digits, scale = d, u.scale
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a byte count such as 1048576 or 16MiB", s)
	}
	if n > math.MaxInt64/scale {
		return 0, fmt.Errorf("%q is too large", s)
	}
	return n * scale, nil
}
