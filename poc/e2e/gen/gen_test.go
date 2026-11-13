package gen

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"poc/e2e/harness"
)

func TestRecords_partition_the_run(t *testing.T) {
	c := Config{Seed: 1, Keys: 5000, ValueSize: 20}
	var whole, parts []string
	for r := range c.Records(0, 1) {
		whole = append(whole, string(r.Key)+string(r.Value))
	}
	var left, right uint64
	for p := range 3 {
		for r := range c.Records(p, 3) {
			parts = append(parts, string(r.Key)+string(r.Value))
			if r.Side == harness.Left {
				left++
			} else {
				right++
			}
		}
	}
	slices.Sort(whole)
	slices.Sort(parts)
	if !slices.Equal(whole, parts) {
		t.Error("the parts don't hold the run's records")
	}
	if l, r := c.Totals(); l != left || r != right || l == 0 || r == 0 {
		t.Errorf("Totals = %d, %d; the records have %d, %d", l, r, left, right)
	}
}

// TestRecords_keep_yielded_slices_while_parts_run_at_once iterates every
// part at once, one of them twice, and holds every record yielded, as
// Ingest holds a batch's records until it sends them. At the end, every
// record must still be the one it was generated as.
func TestRecords_keep_yielded_slices_while_parts_run_at_once(t *testing.T) {
	for _, c := range []Config{
		{Seed: 8, Keys: 6000, Left: Values{Schema: Payment, Encoding: JSON}, Right: Values{Schema: BankLine, Encoding: Avro}, Mix: Typical, KeyFormat: RFKeys},
		// Most of these don't fit the rest of a slab.
		{Seed: 8, Keys: 40, ValueSize: 300_000},
	} {
		const parts = 3
		iterations := []int{0, 1, 2, 1}
		got := make([][]harness.Record, len(iterations))
		var wg sync.WaitGroup
		for i, p := range iterations {
			wg.Go(func() {
				for r := range c.Records(p, parts) {
					got[i] = append(got[i], r)
				}
			})
		}
		wg.Wait()
		g := c.ValueGen()
		var total int
		for i, rs := range got {
			for _, r := range rs {
				k, j, ok := c.Locate(r.Side, r.Value)
				if !ok || k%parts != iterations[i] || !slices.Equal(r.Key, c.Key(k)) || !slices.Equal(r.Value, g.Append(nil, r.Side, k, j)) {
					t.Fatalf("%s: a record of part %d changed after it was yielded", name(c), iterations[i])
				}
				if cap(r.Key) != len(r.Key) || cap(r.Value) != len(r.Value) {
					t.Fatalf("%s: a yielded slice has spare capacity", name(c))
				}
			}
			if i < parts {
				total += len(rs)
			}
		}
		if l, r := c.Totals(); uint64(total) != l+r {
			t.Errorf("%s: the parts yielded %d records, want %d", name(c), total, l+r)
		}
		if len(got[1]) != len(got[3]) {
			t.Errorf("%s: part 1 yielded %d records, then %d", name(c), len(got[1]), len(got[3]))
		}
	}
}

// TestCounts_depend_on_the_seed_mix_and_key_only compares two Configs that
// differ in all else, and checks the counts' bounds.
func TestCounts_depend_on_the_seed_mix_and_key_only(t *testing.T) {
	for _, mix := range []Mix{Stress, Typical} {
		a := Config{Seed: 9, Keys: 10, ValueSize: 20, Mix: mix}
		b := Config{Seed: 9, Keys: 1 << 40, Mix: mix, KeyFormat: RFKeys, Left: Values{Schema: Payment}, Right: Values{Schema: Tiny, Encoding: JSON}}
		for k := range 200_000 {
			l, r := a.Counts(k)
			if bl, br := b.Counts(k); l != bl || r != br {
				t.Fatalf("%v: key %d has %d:%d records in one Config, %d:%d in the other", mix, k, l, r, bl, br)
			}
			if l < 0 || l > maxRecord || r < 0 || r > maxRecord {
				t.Fatalf("%v: key %d has %d:%d records", mix, k, l, r)
			}
			if mix == Typical && (l+r == 0 || l > 1 && r != 1 || r > 1 && l != 1) {
				t.Fatalf("typical: key %d has %d:%d records", k, l, r)
			}
		}
	}
}

// TestParseKey_takes_exactly_the_runs_keys checks the edges of the range
// of keys in both formats.
func TestParseKey_takes_exactly_the_runs_keys(t *testing.T) {
	for _, f := range []KeyFormat{DecimalKeys, RFKeys} {
		for _, keys := range []int{1, 17, maxDecimalKey} {
			c := Config{Seed: 10, Keys: keys, KeyFormat: f}
			for _, k := range []int{0, keys - 1} {
				if got, ok := c.ParseKey(c.Key(k)); !ok || got != k {
					t.Errorf("%v, %d keys: ParseKey(%q) = %d, %t", f, keys, c.Key(k), got, ok)
				}
			}
			if got, ok := c.ParseKey(c.Key(keys)); ok {
				t.Errorf("%v, %d keys: ParseKey(%q) = %d, want a rejection", f, keys, c.Key(keys), got)
			}
		}
	}
	if got, ok := (Config{Keys: maxDecimalKey}).ParseKey([]byte("key-999999999999")); !ok || got != maxDecimalKey-1 {
		t.Errorf("the last decimal key parses as %d, %t", got, ok)
	}
}

func TestFingerprint_ignores_order_only(t *testing.T) {
	c := Config{Seed: 3, Keys: 2000, ValueSize: 12}
	results := expected(c)
	var a, b, d Fingerprint
	for _, r := range results {
		a.Add(r)
	}
	for _, r := range slices.Backward(results) {
		b.Add(r)
	}
	if a != b {
		t.Errorf("fingerprints differ by order: %+v, %+v", a, b)
	}
	for _, r := range results[1:] {
		d.Add(r)
	}
	if a == d || a.Count != uint64(len(results)) {
		t.Errorf("fingerprint %+v doesn't change without a result (%+v)", a, d)
	}
}

func TestKeysFor_sizes_legacy_runs_to_a_key(t *testing.T) {
	const target = 64 << 20
	for _, c := range []Config{
		{Seed: 3, ValueSize: 1000},
		{Seed: 3, ValueSize: 100, Mix: Typical, KeyFormat: RFKeys},
		{Seed: 3, ValueSize: 4}, // values are at least their 10-byte header
	} {
		size := func(keys int) int64 {
			c.Keys = keys
			l, r := c.Totals()
			return int64(l+r) * int64(len(c.Key(0))+max(c.ValueSize, legacyHeader))
		}
		n := c.KeysFor(target)
		if size(n) < target || size(n-1) >= target {
			t.Errorf("%+v: KeysFor = %d keys, which make %d bytes, and %d bytes without the last; want the fewest keys that make %d", c, n, size(n), size(n-1), target)
		}
	}
}

func TestKeysFor_sizes_schema_runs_within_2_percent(t *testing.T) {
	tests := []struct {
		c      Config
		target int64
	}{
		{Config{Seed: 20, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine}, Mix: Typical, KeyFormat: RFKeys}, 64 << 20},
		{Config{Seed: 21, Left: Values{Schema: Payment, Encoding: JSON}, Right: Values{Schema: CardSettlement, Encoding: Avro}}, 16 << 20},
		{Config{Seed: 22, Left: Values{Schema: Envelope, Encoding: Avro}, Right: Values{Schema: Tiny}, Mix: Typical}, 16 << 20},
	}
	for _, tt := range tests {
		c := tt.c
		c.Keys = c.KeysFor(tt.target)
		const parts = 4
		var total atomic.Int64
		var wg sync.WaitGroup
		for p := range parts {
			wg.Go(func() {
				var n int64
				for r := range c.Records(p, parts) {
					n += int64(len(r.Key) + len(r.Value))
				}
				total.Add(n)
			})
		}
		wg.Wait()
		ratio := float64(total.Load()) / float64(tt.target)
		t.Logf("%s: %d keys make %d bytes, %.4f of the target", name(c), c.Keys, total.Load(), ratio)
		if math.Abs(ratio-1) > 0.02 {
			t.Errorf("%s: %d keys make %d bytes, want %d ± 2%%", name(c), c.Keys, total.Load(), tt.target)
		}
	}
}

func TestKeysFor_extrapolates_past_its_count(t *testing.T) {
	const target = 4 << 30
	tests := []struct {
		c      Config
		perKey float64 // the mean bytes of a key's records
	}{
		{Config{Seed: 5, ValueSize: 10}, 3 * (16 + 10)},
		// 93% of keys 1:1, 6% one-sided, 1% 1:N or N:1 with N 2.5 on average.
		{Config{Seed: 5, ValueSize: 10, Mix: Typical, KeyFormat: RFKeys}, 1.955 * (18 + 10)},
	}
	for _, tt := range tests {
		n := tt.c.KeysFor(target)
		if n <= countSamples {
			t.Fatalf("%+v: KeysFor = %d keys, which it didn't need to extrapolate", tt.c, n)
		}
		if got := float64(n) * tt.perKey / target; math.Abs(got-1) > 0.002 {
			t.Errorf("%+v: KeysFor = %d keys, which make about %.4f of the target", tt.c, n, got)
		}
	}
	if n := (Config{}).KeysFor(1 << 62); n != maxDecimalKey {
		t.Errorf("KeysFor(2^62) = %d decimal keys, want the most there are, %d", n, maxDecimalKey)
	}
	for _, b := range []int64{0, -1} {
		if n := (Config{}).KeysFor(b); n != 0 {
			t.Errorf("KeysFor(%d) = %d", b, n)
		}
	}
}

// BenchmarkRecords measures how fast 4 streams, as TestMeasure has, can be
// fed key and value bytes.
func BenchmarkRecords(b *testing.B) {
	for _, c := range []Config{
		{Seed: 20, Keys: 200_000, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine}, Mix: Typical, KeyFormat: RFKeys},
		{Seed: 20, Keys: 60_000, ValueSize: 1000},
	} {
		const parts = 4
		var size int64
		for r := range c.Records(0, 1) {
			size += int64(len(r.Key) + len(r.Value))
		}
		b.Run(name(c), func(b *testing.B) {
			b.SetBytes(size)
			for b.Loop() {
				var wg sync.WaitGroup
				for p := range parts {
					wg.Go(func() {
						for range c.Records(p, parts) {
						}
					})
				}
				wg.Wait()
			}
		})
	}
}
