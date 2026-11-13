package gen

import (
	"encoding/binary"
	"testing"

	"github.com/zeebo/xxh3"

	"poc/e2e/harness"
)

// legacyDigest hashes everything a legacy Config generates: two orders of
// its records, its totals, every key's counts, and some keys and values.
func legacyDigest(c Config) map[string]uint64 {
	records := func(part, parts int) uint64 {
		h := xxh3.New()
		for r := range c.Records(part, parts) {
			var b []byte
			b = append(b, byte(r.Side))
			b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Key)))
			b = append(b, r.Key...)
			b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Value)))
			b = append(b, r.Value...)
			h.Write(b)
		}
		return h.Sum64()
	}
	out := map[string]uint64{
		"records 0/1": records(0, 1),
		"records 1/3": records(1, 3),
	}
	l, r := c.Totals()
	out["totals"] = xxh3.Hash(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, l), r))

	h := xxh3.New()
	for k := range c.Keys {
		l, r := c.Counts(k)
		h.Write([]byte{byte(l), byte(r)})
	}
	out["counts"] = h.Sum64()

	h.Reset()
	for _, k := range []int{0, 1, 17, c.Keys / 2, c.Keys - 1} {
		h.Write(c.Key(k))
		for _, side := range []harness.Side{harness.Left, harness.Right} {
			for j := range 3 {
				h.Write(c.Value(side, k, j))
			}
		}
	}
	out["keys and values"] = h.Sum64()
	return out
}

// TestLegacy_output_is_unchanged freezes what a Config with no schema
// generates, so that the e2e tests and published results stay comparable.
func TestLegacy_output_is_unchanged(t *testing.T) {
	tests := []struct {
		c    Config
		want map[string]uint64
	}{
		{Config{Seed: 7, Keys: 50_000, ValueSize: 100}, map[string]uint64{
			"records 0/1":     0x235f151ffa248eab,
			"records 1/3":     0xb5b313a952c27cbe,
			"totals":          0x8cabdb073c7cf26a,
			"counts":          0x530a9aaaa317e194,
			"keys and values": 0x544ed3d67cf53d2c,
		}},
		{Config{Seed: 20, Keys: 3_000, ValueSize: 1000}, map[string]uint64{
			"records 0/1":     0x8ecddaff0ce65109,
			"records 1/3":     0x4672b365d8b5abc5,
			"totals":          0x03f399ffe2ce1efa,
			"counts":          0x67c8c8fa811e4f13,
			"keys and values": 0x16925c487fad1ca9,
		}},
	}
	for _, tt := range tests {
		got := legacyDigest(tt.c)
		for name, h := range got {
			if tt.want[name] != h {
				t.Errorf("%+v: %s = %#x, want %#x", tt.c, name, h, tt.want[name])
			}
		}
	}
}
