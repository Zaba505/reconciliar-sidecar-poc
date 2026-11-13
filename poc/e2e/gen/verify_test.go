package gen

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

// expected returns the results a correct Join of c's run gives, in the
// oracle's order.
func expected(c Config) []*reconv1.JoinResult {
	var records []oracle.Record
	for r := range c.Records(0, 1) {
		records = append(records, oracle.Record{Side: oracle.Side(r.Side - 1), Key: r.Key, Value: r.Value})
	}
	return oracle.Join(records)
}

// shuffled shuffles results, and the values of each Match, the way a Join
// may order them.
func shuffled(results []*reconv1.JoinResult, seed uint64) []*reconv1.JoinResult {
	r := rand.New(rand.NewPCG(seed, 1))
	r.Shuffle(len(results), func(i, j int) { results[i], results[j] = results[j], results[i] })
	for _, res := range results {
		if m := res.GetBoth(); m != nil {
			r.Shuffle(len(m.Left), func(i, j int) { m.Left[i], m.Left[j] = m.Left[j], m.Left[i] })
			r.Shuffle(len(m.Right), func(i, j int) { m.Right[i], m.Right[j] = m.Right[j], m.Right[i] })
		}
	}
	return results
}

// cloned deep-copies results, so that a test can change them.
func cloned(results []*reconv1.JoinResult) []*reconv1.JoinResult {
	out := make([]*reconv1.JoinResult, len(results))
	for i, r := range results {
		out[i] = proto.Clone(r).(*reconv1.JoinResult)
	}
	return out
}

// verify checks results with a Verifier of c that has workers workers,
// and returns the first error it found.
func verify(c Config, workers int, results []*reconv1.JoinResult) error {
	v := newVerifier(c, workers)
	for _, r := range results {
		if v.Add(r) != nil {
			break
		}
	}
	return v.Finish()
}

// name describes c for a subtest.
func name(c Config) string {
	if c.Left.Schema == nil {
		return fmt.Sprintf("legacy/%v/%v", c.Mix, c.KeyFormat)
	}
	return fmt.Sprintf("%s-%v+%s-%v/%v/%v", c.Left.Schema.Name, c.Left.Encoding, c.Right.Schema.Name, c.Right.Encoding, c.Mix, c.KeyFormat)
}

func TestVerifier_accepts_a_correct_Join(t *testing.T) {
	var configs []Config
	for _, mix := range []Mix{Stress, Typical} {
		for _, f := range []KeyFormat{DecimalKeys, RFKeys} {
			configs = append(configs, Config{Seed: 30, Keys: 3000, ValueSize: 40, Mix: mix, KeyFormat: f})
			for i, enc := range encodings {
				configs = append(configs, Config{
					Seed: 30, Keys: 3000, Mix: mix, KeyFormat: f,
					Left:  Values{Schema: Payment, Encoding: enc},
					Right: Values{Schema: BankLine, Encoding: encodings[(i+1)%len(encodings)]},
				})
			}
		}
	}
	for _, c := range configs {
		t.Run(name(c), func(t *testing.T) {
			t.Parallel()
			results := shuffled(expected(c), c.Seed)
			for _, w := range []int{1, 4} {
				if err := verify(c, w, results); err != nil {
					t.Errorf("%d workers: %v", w, err)
				}
			}
		})
	}
}

// picks are results of a Stress run that the mistakes change.
type picks struct {
	one   int // a left-only result of a key with one left record
	two   int // a left-only result of another key
	match int // a Match with at least two left values
}

func pick(t *testing.T, c Config, results []*reconv1.JoinResult) picks {
	p := picks{-1, -1, -1}
	for i, r := range results {
		switch {
		case r.GetBoth() != nil && len(r.GetBoth().GetLeft()) > 1 && p.match < 0:
			p.match = i
		case r.GetLeftOnly() != nil && p.one < 0:
			k, _ := c.ParseKey(r.GetLeftOnly().GetKey())
			if l, _ := c.Counts(k); l == 1 {
				p.one = i
			}
		case r.GetLeftOnly() != nil && p.two < 0 && p.one >= 0:
			p.two = i
		}
	}
	if p.one < 0 || p.two < 0 || p.match < 0 {
		t.Fatalf("the run lacks results to change: %+v", p)
	}
	return p
}

func TestVerifier_rejects_mistakes(t *testing.T) {
	leftOnly := func(rs []*reconv1.JoinResult, i int) *reconv1.Record { return rs[i].GetLeftOnly() }
	key := func(c Config, rs []*reconv1.JoinResult, i int) int {
		k, _ := c.ParseKey(leftOnly(rs, i).GetKey())
		return k
	}
	mistakes := []struct {
		name string
		want string // in the error
		make func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult
	}{
		{"a value of another key", "isn't one of its 1: it is value 0 of key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			leftOnly(rs, p.one).Value = leftOnly(rs, p.two).Value
			return rs
		}},
		{"a value of the other side", "isn't one of its 1: it is right value 0 of key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			leftOnly(rs, p.one).Value = c.Value(harness.Right, key(c, rs, p.one), 0)
			return rs
		}},
		{"a value it doesn't have", "isn't one of its 1: it is value 1", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			leftOnly(rs, p.one).Value = c.Value(harness.Left, key(c, rs, p.one), 1)
			return rs
		}},
		{"a changed byte", "it differs from value", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			v := rs[p.match].GetBoth().GetRight()[0]
			v[len(v)-1] ^= 1
			return rs
		}},
		{"a truncated value", "it differs from value 0 at byte", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			r := leftOnly(rs, p.one)
			r.Value = r.Value[:len(r.Value)-1]
			return rs
		}},
		{"a value cut to 4 bytes", "its 4 bytes name no left record of the run", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			r := leftOnly(rs, p.one)
			r.Value = r.Value[:4]
			return rs
		}},
		{"a duplicate result", "seen twice", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			return append(rs, proto.Clone(rs[p.one]).(*reconv1.JoinResult))
		}},
		{"a duplicate value in a Match", "appears twice", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			m := rs[p.match].GetBoth()
			m.Left[1] = m.Left[0]
			return rs
		}},
		{"a missing value in a Match", "left values, want", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			m := rs[p.match].GetBoth()
			m.Left = m.Left[1:]
			return rs
		}},
		{"a missing result", "keys are missing results", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			return slices.Delete(rs, p.one, p.one+1)
		}},
		{"a Match for a one-sided key", "a Match, but it has 1 left and 0 right records", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			r := leftOnly(rs, p.one)
			rs[p.one] = &reconv1.JoinResult{Result: &reconv1.JoinResult_Both{Both: &reconv1.Match{Key: r.Key, Left: [][]byte{r.Value}, Right: [][]byte{r.Value}}}}
			return rs
		}},
		{"a one-sided result for a matched key", "a left-only result, but it has", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			m := rs[p.match].GetBoth()
			rs[p.match] = &reconv1.JoinResult{Result: &reconv1.JoinResult_LeftOnly{LeftOnly: &reconv1.Record{Key: m.Key, Value: m.Left[0]}}}
			return rs
		}},
		{"the wrong side's state", "a right-only result, but it has 1 left and 0 right records", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			rs[p.one] = &reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{RightOnly: leftOnly(rs, p.one)}}
			return rs
		}},
		{"an unknown key", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			leftOnly(rs, p.one).Key = c.Key(c.Keys)
			return rs
		}},
		{"a malformed key", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			leftOnly(rs, p.one).Key = []byte("key-12")
			return rs
		}},
		{"a result with no state", "no state", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			return append(rs, &reconv1.JoinResult{})
		}},
		{"an empty Match", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			return append(rs, &reconv1.JoinResult{Result: &reconv1.JoinResult_Both{}})
		}},
		{"an empty right-only result", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			return append(rs, &reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{}})
		}},
		{"a correct value under the key in another format", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			other := c
			other.KeyFormat = RFKeys + DecimalKeys - c.KeyFormat
			leftOnly(rs, p.one).Key = other.Key(key(c, rs, p.one))
			return rs
		}},
		{"a correct value under its key in lower case", "unexpected key", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			r := leftOnly(rs, p.one)
			r.Key = bytes.ToLower(r.Key)
			if c.KeyFormat == DecimalKeys {
				r.Key = bytes.ToUpper(r.Key) // "KEY-..." for decimal keys
			}
			return rs
		}},
		{"a Match without its right values", "right values, want", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			rs[p.match].GetBoth().Right = nil
			return rs
		}},
		{"a Match with an extra value", "left values, want", func(c Config, rs []*reconv1.JoinResult, p picks) []*reconv1.JoinResult {
			m := rs[p.match].GetBoth()
			k, _ := c.ParseKey(m.Key)
			l, _ := c.Counts(k)
			if l < maxRecord {
				m.Left = append(m.Left, c.Value(harness.Left, k, l)) // the next value it would have had
			} else {
				m.Left = append(m.Left, m.Left[0])
			}
			return rs
		}},
	}
	for _, c := range []Config{
		{Seed: 31, Keys: 3000, ValueSize: 40},
		{Seed: 31, Keys: 3000, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine, Encoding: JSON}, KeyFormat: RFKeys},
		{Seed: 31, Keys: 3000, Left: Values{Schema: Tiny, Encoding: Avro}, Right: Values{Schema: Envelope}},
	} {
		t.Run(name(c), func(t *testing.T) {
			t.Parallel()
			results := expected(c)
			p := pick(t, c, results)
			for _, w := range []int{1, 4} {
				for _, m := range mistakes {
					err := verify(c, w, m.make(c, cloned(results), p))
					if err == nil || !strings.Contains(err.Error(), m.want) {
						t.Errorf("%d workers, %s: got %v, want an error with %q", w, m.name, err, m.want)
					} else if w == 1 {
						t.Logf("%s: %v", m.name, err)
					}
				}
			}
		})
	}
}

func TestVerifier_Add_stops_at_the_first_error(t *testing.T) {
	c := Config{Seed: 32, Keys: 12_000, ValueSize: 24}
	results := expected(c)
	for _, w := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d workers", w), func(t *testing.T) {
			rs := cloned(results)
			i := slices.IndexFunc(rs, func(r *reconv1.JoinResult) bool { return r.GetLeftOnly() != nil })
			rs[0], rs[i] = rs[i], rs[0]
			bad := rs[0].GetLeftOnly().Value
			bad[len(bad)-1] ^= 1

			// The bad result's round starts once a round is queued, and
			// is waited for when the next one is.
			v := newVerifier(c, w)
			at := -1
			for n, r := range rs {
				if v.Add(r) != nil {
					at = n
					break
				}
			}
			if round := roundPerWorker * w; at < 0 || at >= 2*round {
				t.Fatalf("Add returned the error at result %d of %d, want it within %d", at, len(rs), 2*round)
			}
			err := v.Add(rs[at+1])
			if err == nil || !strings.Contains(err.Error(), "differs") {
				t.Errorf("Add after the error = %v", err)
			}
			for range 2 {
				if got := v.Finish(); got != err {
					t.Errorf("Finish = %v, want %v", got, err)
				}
			}
		})
	}
	t.Run("after Finish", func(t *testing.T) {
		v := newVerifier(c, 2)
		for _, r := range results {
			if err := v.Add(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := v.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := v.Finish(); err != nil {
			t.Errorf("a second Finish = %v", err)
		}
		if err := v.Add(results[0]); err == nil {
			t.Error("Add after Finish succeeded")
		}
	})
}

// TestVerifier_leaves_nothing_running drops Verifiers without calling
// Finish. synctest.Test fails if a goroutine they started never exits.
func TestVerifier_leaves_nothing_running(t *testing.T) {
	c := Config{Seed: 33, Keys: 4000, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine}, KeyFormat: RFKeys}
	results := expected(c) // compiles the schemas outside the bubbles
	bad := cloned(results)
	for _, r := range bad[len(bad)/4:] {
		if lo := r.GetLeftOnly(); lo != nil {
			lo.Value[len(lo.Value)-1] ^= 1
			break
		}
	}
	for name, rs := range map[string][]*reconv1.JoinResult{
		"after an error": bad,
		"part way":       results[:len(results)/2],
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				v := newVerifier(c, 4)
				for _, r := range rs {
					if v.Add(r) != nil {
						return
					}
				}
			})
		})
	}
}

// TestVerifier_rejects_any_change_to_a_value changes each byte of a
// one-sided value and of a Match value of each side, cuts them to each
// shorter length, and adds a byte, in every encoding and in legacy values.
// The Verifier must reject every one, and for that value.
func TestVerifier_rejects_any_change_to_a_value(t *testing.T) {
	configs := []Config{{Seed: 34, Keys: 48, ValueSize: 40}}
	for _, enc := range encodings {
		configs = append(configs, Config{
			Seed: 34, Keys: 48, KeyFormat: RFKeys,
			Left:  Values{Schema: Payment, Encoding: enc},
			Right: Values{Schema: BankLine, Encoding: enc},
		})
	}
	for _, c := range configs {
		t.Run(name(c), func(t *testing.T) {
			t.Parallel()
			results := expected(c)
			// The values to change: the first one-sided value of each side,
			// and the first left and last right value of a Match.
			var targets []*[]byte
			var lo, ro, both bool
			for _, r := range results {
				switch {
				case r.GetLeftOnly() != nil && !lo:
					lo, targets = true, append(targets, &r.GetLeftOnly().Value)
				case r.GetRightOnly() != nil && !ro:
					ro, targets = true, append(targets, &r.GetRightOnly().Value)
				case r.GetBoth() != nil && !both:
					m := r.GetBoth()
					both, targets = true, append(targets, &m.Left[0], &m.Right[len(m.Right)-1])
				}
			}
			if len(targets) != 4 {
				t.Fatalf("the run has %d of the 4 kinds of value to change", len(targets))
			}
			if err := verify(c, 1, results); err != nil {
				t.Fatal(err)
			}
			for n, p := range targets {
				orig := *p
				check := func(what string, changed []byte) {
					t.Helper()
					*p = changed
					err := verify(c, 1, results)
					*p = orig
					if err == nil || !strings.Contains(err.Error(), "value") {
						t.Fatalf("value %d (%d bytes), %s: got %v, want an error about the value", n, len(orig), what, err)
					}
				}
				for i := range orig {
					changed := slices.Clone(orig)
					changed[i] ^= 1 << (i % 8)
					check(fmt.Sprintf("byte %d changed", i), changed)
				}
				for i := range orig {
					check(fmt.Sprintf("cut to %d bytes", i), orig[:i:i])
				}
				check("a byte added", append(slices.Clip(orig), orig[len(orig)-1]))
			}
		})
	}
}

// TestVerifier_shards_by_key_block checks a Typical run with numbers of
// workers that don't divide 16, so that the race detector would see two
// workers sharing a word of the bitmap, and that each worker checked the
// records of exactly the keys (k/16) mod W gives it.
func TestVerifier_shards_by_key_block(t *testing.T) {
	c := Config{Seed: 35, Keys: 20_000 + 5, ValueSize: 12, Mix: Typical}
	results := shuffled(expected(c), 2)
	for _, w := range []int{1, 2, 3, 5, 8, 13} {
		v := newVerifier(c, w)
		for _, r := range results {
			if err := v.Add(r); err != nil {
				t.Fatalf("%d workers: %v", w, err)
			}
		}
		if err := v.Finish(); err != nil {
			t.Fatalf("%d workers: %v", w, err)
		}
		want := make([][2]uint64, w)
		for k := range c.Keys {
			l, r := c.Counts(k)
			want[k/16%w][0] += uint64(l)
			want[k/16%w][1] += uint64(r)
		}
		for i, wk := range v.workers {
			if wk.counts != want[i] {
				t.Errorf("%d workers: worker %d counted %v records, want %v", w, i, wk.counts, want[i])
			}
		}
	}
}

// TestVerifier_of_no_keys accepts no results and rejects any.
func TestVerifier_of_no_keys(t *testing.T) {
	c := Config{Seed: 36, ValueSize: 12}
	if err := verify(c, 3, nil); err != nil {
		t.Errorf("no results: %v", err)
	}
	r := &reconv1.JoinResult{Result: &reconv1.JoinResult_LeftOnly{LeftOnly: &reconv1.Record{Key: c.Key(0), Value: c.Value(harness.Left, 0, 0)}}}
	if err := verify(c, 3, []*reconv1.JoinResult{r}); err == nil || !strings.Contains(err.Error(), "unexpected key") {
		t.Errorf("a result: got %v, want an unexpected key", err)
	}
}

// TestLocate_reads_exactly_16_hex_digits_of_a_JSON_id changes the JSON
// identity's digits: Locate must find none of the changed values.
func TestLocate_reads_exactly_16_hex_digits_of_a_JSON_id(t *testing.T) {
	v := Values{Schema: Tiny, Encoding: JSON}
	c := Config{Seed: 37, Keys: 1000, Left: v, Right: v}
	val := c.Value(harness.Left, 123, 1)
	if k, j, ok := c.Locate(harness.Left, val); !ok || k != 123 || j != 1 {
		t.Fatalf("Locate(%s) = %d, %d, %t", val, k, j, ok)
	}
	n := len(jsonIDPrefix)
	digits := val[n : n+16]
	for name, changed := range map[string][]byte{
		"15 digits":    slices.Concat(val[:n], digits[1:], val[n+16:]),
		"17 digits":    slices.Concat(val[:n], []byte("0"), digits, val[n+16:]),
		"upper case":   slices.Concat(val[:n], bytes.ToUpper(digits), val[n+16:]),
		"a space":      slices.Concat([]byte(`{ "id":"`), digits, val[n+16:]),
		"another name": slices.Concat([]byte(`{"ID":"`), digits, val[n+16:]),
		"no quote":     slices.Concat(val[:n], digits),
		"a non-digit":  slices.Concat(val[:n], digits[:15], []byte("g"), val[n+16:]),
	} {
		if bytes.Equal(changed, val) {
			t.Fatalf("%s: unchanged", name) // digits that were upper case already
		}
		if k, j, ok := c.Locate(harness.Left, changed); ok {
			t.Errorf("%s: Locate(%s) = %d, %d", name, changed, k, j)
		}
	}
}

func BenchmarkVerifier(b *testing.B) {
	for _, enc := range []Encoding{Protobuf, JSON} {
		c := Config{Seed: 20, Keys: 100_000, Left: Values{Schema: Payment, Encoding: enc}, Right: Values{Schema: BankLine, Encoding: enc}, Mix: Typical, KeyFormat: RFKeys}
		results := shuffled(expected(c), 1)
		var size int64
		for _, r := range results {
			if m := r.GetBoth(); m != nil {
				for _, v := range slices.Concat(m.Left, m.Right) {
					size += int64(len(v))
				}
			} else {
				size += int64(len(r.GetLeftOnly().GetValue()) + len(r.GetRightOnly().GetValue()))
			}
		}
		for _, w := range []int{1, len(NewVerifier(Config{}).workers)} {
			b.Run(fmt.Sprintf("%v/W=%d", enc, w), func(b *testing.B) {
				b.SetBytes(size)
				for b.Loop() {
					if err := verify(c, w, results); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
