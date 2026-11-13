package gen

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"

	"poc/e2e/harness"
)

// encodeWith returns what fn appends with an encoder for enc.
func encodeWith(enc Encoding, fn func(e *encoder)) []byte {
	e := &encoder{enc: enc}
	fn(e)
	return e.b
}

// fataler is what the checks need of a *testing.T and a *rapid.T.
type fataler interface {
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// marshalJSON returns what encoding/json writes for v.
func marshalJSON(t fataler, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal(%v): %v", v, err)
	}
	return b
}

func TestJSON_strings_match_encoding_json(t *testing.T) {
	var ascii []byte
	for c := range 0x80 {
		ascii = append(ascii, byte(c))
	}
	for _, s := range []string{
		"",
		"Pabanvin & Co",
		"<script>alert('x')</script>",
		`a "quoted" back\slash`,
		"\b\f\n\r\t\x00\x01\x1f\x7f",
		string(ascii),
		"line\u2028and\u2029paragraph",
		"caf\u00e9 \u65e5\u672c \U0001f600",
		"\xff\xfe invalid \xc3\x28 \xe2\x82 \xed\xa0\x80 \xf4\x90\x80\x80",
		"\xc3",
	} {
		got := encodeWith(JSON, func(e *encoder) { e.jstr([]byte(s)) })
		if want := marshalJSON(t, s); !bytes.Equal(got, want) {
			t.Errorf("%q:\n got %s\nwant %s", s, got, want)
		}
	}
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.SliceOf(rapid.Byte()).Draw(t, "s")
		got := encodeWith(JSON, func(e *encoder) { e.jstr(s) })
		if want := marshalJSON(t, string(s)); !bytes.Equal(got, want) {
			t.Fatalf("%q:\n got %s\nwant %s", s, got, want)
		}
	})
}

func TestJSON_doubles_match_encoding_json(t *testing.T) {
	check := func(t fataler, x float64) {
		got := encodeWith(JSON, func(e *encoder) { e.jsonDouble(x) })
		if want := marshalJSON(t, x); !bytes.Equal(got, want) {
			t.Fatalf("%v (%#x): got %s, want %s", x, math.Float64bits(x), got, want)
		}
	}
	for _, x := range []float64{
		0, math.Copysign(0, -1), 1, -1, 1.1, 0.1, 123456.789012,
		1e-6, 9.99999e-7, 1e-7, 1.5e-9, 1e-10, 1e-300, math.SmallestNonzeroFloat64,
		1e20, 999999999999999999999, 1e21, 1.5e21, 1e100, math.MaxFloat64,
		-1e-7, -1e21, float64(1 << 50), float64(1<<53 + 1),
	} {
		check(t, x)
	}
	rapid.Check(t, func(t *rapid.T) {
		x := math.Float64frombits(rapid.Uint64().Draw(t, "bits"))
		if math.IsNaN(x) || math.IsInf(x, 0) {
			t.Skip("encoding/json rejects it")
		}
		check(t, x)
	})
}

func TestJSON_timestamps_match_time_MarshalJSON(t *testing.T) {
	check := func(t fataler, ms int64) {
		got := encodeWith(JSON, func(e *encoder) { e.jsonTimestamp(ms) })
		if want := marshalJSON(t, time.UnixMilli(ms).UTC()); !bytes.Equal(got, want) {
			t.Fatalf("%d: got %s, want %s", ms, got, want)
		}
	}
	for _, ms := range []int64{0, 1, 10, 100, 120, 999, 1000, -1, -1001, base, base + 30*day - 1} {
		check(t, ms)
	}
	// time.Time marshals years 0 to 9999.
	lo := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	hi := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1
	rapid.Check(t, func(t *rapid.T) { check(t, rapid.Int64Range(lo, hi).Draw(t, "ms")) })
}

func TestJSON_dates_are_YYYY_MM_DD(t *testing.T) {
	for days, want := range map[int64]string{
		0:      `"1970-01-01"`,
		-1:     `"1969-12-31"`,
		20727:  `"2026-10-01"`,
		11016:  `"2000-02-29"`,
		-25567: `"1900-01-01"`,
	} {
		if got := encodeWith(JSON, func(e *encoder) { e.jsonDate(days) }); string(got) != want {
			t.Errorf("%d: got %s, want %s", days, got, want)
		}
	}
}

// decimalString formats minor hundredths as a JSON money amount, by
// math/big.
func decimalString(minor int64) string {
	n := big.NewInt(minor)
	s := new(big.Int).Abs(n).String()
	if len(s) < 3 {
		s = strings.Repeat("0", 3-len(s)) + s
	}
	s = s[:len(s)-2] + "." + s[len(s)-2:]
	if n.Sign() < 0 {
		s = "-" + s
	}
	return s
}

func TestJSON_money_matches_encoding_json(t *testing.T) {
	type money struct {
		CurrencyCode string `json:"currency_code"`
		Amount       string `json:"amount"`
	}
	check := func(t fataler, minor int64) {
		got := encodeWith(JSON, func(e *encoder) { e.jsonMoney([]byte("EUR"), minor) })
		if want := marshalJSON(t, money{"EUR", decimalString(minor)}); !bytes.Equal(got, want) {
			t.Fatalf("%d: got %s, want %s", minor, got, want)
		}
	}
	for _, minor := range []int64{0, 1, -1, 5, -5, 10, 99, -99, 100, -100, 101, 5958, -5958, math.MaxInt64, math.MinInt64} {
		check(t, minor)
	}
	rapid.Check(t, func(t *rapid.T) { check(t, rapid.Int64().Draw(t, "minor")) })
}

// twosComplement returns v as Java's BigInteger.toByteArray writes it, by
// math/big: big-endian two's complement in as few bytes as hold its sign.
func twosComplement(v int64) []byte {
	n := big.NewInt(v)
	if v < 0 {
		n.Not(n) // -v-1, whose bits are v's inverted
	}
	b := n.Bytes()
	if len(b) == 0 || b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	if v < 0 {
		for i := range b {
			b[i] = ^b[i]
		}
	}
	return b
}

func TestAvro_money_amounts_are_minimal_twos_complement(t *testing.T) {
	check := func(t fataler, minor int64) {
		got := encodeWith(Avro, func(e *encoder) { e.avroMoney([]byte("USD"), minor) })
		want := []byte{6, 'U', 'S', 'D'}
		amount := twosComplement(minor)
		want = binary.AppendVarint(want, int64(len(amount)))
		want = append(want, amount...)
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: got % x, want % x", minor, got, want)
		}
	}
	for minor, amount := range map[int64]string{
		0:             "00",
		1:             "01",
		-1:            "ff",
		127:           "7f",
		128:           "0080",
		-128:          "80",
		-129:          "ff7f",
		255:           "00ff",
		256:           "0100",
		32767:         "7fff",
		32768:         "008000",
		-32768:        "8000",
		-32769:        "ff7fff",
		math.MaxInt64: "7fffffffffffffff",
		math.MinInt64: "8000000000000000",
	} {
		// The table pins math/big's answer, so a mistake in
		// twosComplement can't hide one in the encoder.
		if got := hexString(twosComplement(minor)); got != amount {
			t.Errorf("%d: math/big gives %s, the table %s", minor, got, amount)
		}
		check(t, minor)
	}
	rapid.Check(t, func(t *rapid.T) { check(t, rapid.Int64().Draw(t, "minor")) })
}

func hexString(b []byte) string {
	var s []byte
	for _, c := range b {
		s = append(s, hexLower[c>>4], hexLower[c&15])
	}
	return string(s)
}

func TestProto_timestamps_match_timestamppb(t *testing.T) {
	check := func(t fataler, ms int64) {
		got := encodeWith(Protobuf, func(e *encoder) { e.protoTimestamp(5, 0, ms) })
		inner, err := proto.Marshal(timestamppb.New(time.UnixMilli(ms)))
		if err != nil {
			t.Fatal(err)
		}
		want := protowire.AppendTag(nil, 5, protowire.BytesType)
		want = protowire.AppendBytes(want, inner)
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: got % x, want % x", ms, got, want)
		}
	}
	for _, ms := range []int64{0, 1, 999, 1000, 1001, -1, -999, -1000, -1001, base, base + 123} {
		check(t, ms)
	}
	rapid.Check(t, func(t *rapid.T) { check(t, rapid.Int64Range(-1<<50, 1<<50).Draw(t, "ms")) })
}

func TestValueGens_run_at_once_from_a_cold_cache(t *testing.T) {
	for _, p := range Profiles() {
		t.Run(p.Name, func(t *testing.T) {
			// A copy has its own address, so it isn't compiled yet, and
			// the goroutines race to compile it.
			s := *p
			const workers, values = 8, 300
			var got [workers][]byte
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					var gens [3]*ValueGen
					for enc := range gens {
						v := Values{Schema: &s, Encoding: Encoding(enc)}
						gens[enc] = Config{Seed: 99, Keys: 1 << 30, Left: v, Right: v, KeyFormat: RFKeys}.ValueGen()
					}
					for i := range values {
						for _, g := range gens {
							got[w] = g.Append(got[w], harness.Side(1+i%2), i*7919, i%4)
						}
					}
				})
			}
			wg.Wait()
			// The original makes the same values one at a time.
			var want []byte
			var gens [3]*ValueGen
			for enc := range gens {
				v := Values{Schema: p, Encoding: Encoding(enc)}
				gens[enc] = Config{Seed: 99, Keys: 1 << 30, Left: v, Right: v, KeyFormat: RFKeys}.ValueGen()
			}
			for i := range values {
				for _, g := range gens {
					want = g.Append(want, harness.Side(1+i%2), i*7919, i%4)
				}
			}
			for w := range workers {
				if !bytes.Equal(got[w], want) {
					t.Errorf("worker %d made different values", w)
				}
			}
		})
	}
}

func TestFees_are_a_fraction_of_large_amounts_too(t *testing.T) {
	s := &Schema{Name: "S", Fields: []Field{
		{Name: "id", Type: TypeIdentity},
		{Name: "amount", Type: TypeMoney, Median: 1 << 50, Sigma: 4, Signed: true},
		{Name: "fee", Type: TypeMoney, Fee: true},
	}}
	v := Values{Schema: s, Encoding: JSON}
	g := Config{Seed: 3, Keys: 1000, Left: v, Right: v}.ValueGen()
	var rec struct {
		Amount struct{ Amount string } `json:"amount"`
		Fee    struct{ Amount string } `json:"fee"`
	}
	large := 0
	for k := range 1000 {
		if err := json.Unmarshal(g.Append(nil, 1, k, 0), &rec); err != nil {
			t.Fatal(err)
		}
		amount, _ := new(big.Rat).SetString(rec.Amount.Amount)
		fee, _ := new(big.Rat).SetString(rec.Fee.Amount)
		if amount.Sign() == 0 {
			continue
		}
		if amount.Cmp(big.NewRat(math.MaxInt64/300, 100)) > 0 {
			large++
		}
		// The fee is 0.1% to 3% of the amount, truncated toward 0 to a
		// cent.
		abs := new(big.Rat).Abs(amount)
		lo := new(big.Rat).Sub(new(big.Rat).Mul(abs, big.NewRat(1, 1000)), big.NewRat(1, 100))
		hi := new(big.Rat).Mul(abs, big.NewRat(3, 100))
		if fee.Sign()*amount.Sign() < 0 || new(big.Rat).Abs(fee).Cmp(lo) < 0 || new(big.Rat).Abs(fee).Cmp(hi) > 0 {
			t.Fatalf("k %d: a fee of %s on %s", k, rec.Fee.Amount, rec.Amount.Amount)
		}
	}
	if large == 0 {
		t.Fatal("no amount was large enough to overflow")
	}
}
