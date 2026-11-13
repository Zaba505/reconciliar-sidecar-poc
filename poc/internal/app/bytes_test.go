package app

import "testing"

func TestParseBytes(t *testing.T) {
	valid := map[string]int64{
		"0":       0,
		"1048576": 1 << 20,
		"512B":    512,
		"4KiB":    4 << 10,
		"16MiB":   16 << 20,
		"2GiB":    2 << 30,
		"3TiB":    3 << 40,
	}
	for in, want := range valid {
		got, err := parseBytes(in)
		if err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "-1", "1.5GiB", "1GB", "1 MiB", "16mib", "9223372036854775807KiB"} {
		if got, err := parseBytes(in); err == nil {
			t.Errorf("parseBytes(%q) = %d, want an error", in, got)
		}
	}
}
