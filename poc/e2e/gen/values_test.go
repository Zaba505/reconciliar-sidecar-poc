package gen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"poc/e2e/harness"
)

var encodings = []Encoding{Protobuf, Avro, JSON}

// pair returns a Config with s on both sides in enc.
func pair(s *Schema, enc Encoding) Config {
	v := Values{Schema: s, Encoding: enc}
	return Config{Seed: 20, Keys: 1 << 20, Left: v, Right: v, Mix: Typical, KeyFormat: RFKeys}
}

// each runs fn for every profile and encoding.
func each(t *testing.T, fn func(t *testing.T, s *Schema, enc Encoding)) {
	for _, s := range Profiles() {
		for _, enc := range encodings {
			t.Run(s.Name+"/"+enc.String(), func(t *testing.T) { fn(t, s, enc) })
		}
	}
}

func TestValueGen_is_deterministic(t *testing.T) {
	each(t, func(t *testing.T, s *Schema, enc Encoding) {
		c := pair(s, enc)
		a, b := c.ValueGen(), c.ValueGen()
		var bufA []byte
		for i := range 2000 {
			side, k, j := harness.Side(1+i%2), i*7919%c.Keys, i%4
			bufA = a.Append(bufA[:0], side, k, j)
			if got := b.Append(nil, side, k, j); !bytes.Equal(bufA, got) {
				t.Fatalf("(%d, %d, %d): two ValueGens differ:\n%q\n%q", side, k, j, bufA, got)
			}
			if i%100 == 0 {
				if got := c.Value(side, k, j); !bytes.Equal(bufA, got) {
					t.Fatalf("(%d, %d, %d): Value differs from Append", side, k, j)
				}
			}
		}
	})
}

func TestRecords_hold_the_generated_values(t *testing.T) {
	for _, c := range []Config{
		{Seed: 1, Keys: 3000, ValueSize: 40},
		{Seed: 1, Keys: 3000, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine, Encoding: Avro}, KeyFormat: RFKeys},
		{Seed: 1, Keys: 3000, Left: Values{Schema: Envelope, Encoding: JSON}, Right: Values{Schema: Tiny}, Mix: Typical},
	} {
		g := c.ValueGen()
		seen := map[[3]int]bool{}
		for r := range c.Records(0, 1) {
			k, j, ok := c.Locate(r.Side, r.Value)
			if !ok {
				t.Fatalf("%v: can't locate a record of key %q", c.Left.Schema, r.Key)
			}
			if kk, ok := c.ParseKey(r.Key); !ok || kk != k {
				t.Fatalf("key %q parses as %d, %v; the value says %d", r.Key, kk, ok, k)
			}
			if want := g.Append(nil, r.Side, k, j); !bytes.Equal(r.Value, want) {
				t.Fatalf("record (%d, %d, %d) differs from Append", r.Side, k, j)
			}
			id := [3]int{int(r.Side), k, j}
			if seen[id] {
				t.Fatalf("record %v twice", id)
			}
			seen[id] = true
		}
		l, r := c.Totals()
		if uint64(len(seen)) != l+r {
			t.Errorf("%d records, Totals says %d", len(seen), l+r)
		}
	}
}

func TestAppend_allocates_nothing(t *testing.T) {
	cs := []Config{{Seed: 3, Keys: 1000, ValueSize: 100}}
	for _, s := range Profiles() {
		for _, enc := range encodings {
			cs = append(cs, pair(s, enc))
		}
	}
	for _, c := range cs {
		g := c.ValueGen()
		buf := make([]byte, 0, 1<<20)
		k := 0
		// Warm the scratch buffers up first.
		for range 1000 {
			g.Append(buf, harness.Left, k, 0)
			k++
		}
		if n := testing.AllocsPerRun(1000, func() {
			g.Append(buf[:0], harness.Side(1+k%2), k, k%4)
			k++
		}); n > 0 {
			t.Errorf("%v: %v allocations per Append", c.Left, n)
		}
	}
}

func TestIdentity_round_trips(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := Config{Seed: rapid.Uint64().Draw(t, "seed")}
		side := harness.Side(rapid.IntRange(1, 2).Draw(t, "side"))
		k := rapid.IntRange(0, maxKeys-1).Draw(t, "k")
		j := rapid.IntRange(0, 3).Draw(t, "j")
		gs, gk, gj := c.ParseIdentity(c.Identity(side, k, j))
		if gs != side || gk != k || gj != j {
			t.Fatalf("ParseIdentity(Identity(%d, %d, %d)) = %d, %d, %d", side, k, j, gs, gk, gj)
		}
	})
}

func TestKeys_round_trip(t *testing.T) {
	for _, f := range []KeyFormat{DecimalKeys, RFKeys} {
		t.Run(f.String(), func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				c := Config{Seed: rapid.Uint64().Draw(t, "seed"), KeyFormat: f, Keys: rapid.IntRange(1, maxDecimalKey).Draw(t, "keys")}
				k := rapid.IntRange(0, c.Keys-1).Draw(t, "k")
				key := c.Key(k)
				if got, ok := c.ParseKey(key); !ok || got != k {
					t.Fatalf("ParseKey(%q) = %d, %v, want %d", key, got, ok, k)
				}
			})
		})
	}
	dec := Config{Seed: 4, Keys: 1000}
	if got := string(dec.Key(42)); got != "key-000000000042" {
		t.Errorf("decimal key 42 = %q", got)
	}
	rf := Config{Seed: 4, Keys: 1000, KeyFormat: RFKeys}
	if got := rf.Key(42); len(got) != 18 || !bytes.HasPrefix(got, []byte("RF")) {
		t.Errorf("RF key 42 = %q", got)
	}
	if bytes.Equal(rf.Key(42), Config{Seed: 5, KeyFormat: RFKeys}.Key(42)) {
		t.Error("RF keys don't depend on the seed")
	}
}

func TestParseKey_rejects_malformed_keys(t *testing.T) {
	dec := Config{Seed: 4, Keys: 1000}
	rf := Config{Seed: 4, Keys: 1000, KeyFormat: RFKeys}
	outOfRange := rf.Key(1000)
	lower := bytes.ToLower(rf.Key(7))
	lower[0], lower[1] = 'R', 'F'
	tests := []struct {
		c   Config
		key string
	}{
		{dec, "key-000000001000"},
		{dec, "key-00000000001"},
		{dec, "key-0000000000001"},
		{dec, "key-+00000000001"},
		{dec, "key-00000000000a"},
		{dec, "kex-000000000001"},
		{dec, ""},
		{dec, string(rf.Key(7))},
		{rf, string(outOfRange)},
		{rf, string(lower)},
		{rf, "RF" + strings.Repeat("G", 16)},
		{rf, string(rf.Key(7)[:17])},
		{rf, "XX" + string(rf.Key(7)[2:])},
		{rf, "key-000000000007"},
	}
	for _, tt := range tests {
		if k, ok := tt.c.ParseKey([]byte(tt.key)); ok {
			t.Errorf("%v: ParseKey(%q) = %d, want a rejection", tt.c.KeyFormat, tt.key, k)
		}
	}
}

func TestLocate_finds_every_value(t *testing.T) {
	each(t, func(t *testing.T, s *Schema, enc Encoding) {
		c := pair(s, enc)
		c.Right = Values{Schema: Tiny, Encoding: enc}
		g := c.ValueGen()
		var buf []byte
		for i := range 3000 {
			side, k, j := harness.Side(1+i%2), i*104729%c.Keys, i%4
			buf = g.Append(buf[:0], side, k, j)
			if gk, gj, ok := c.Locate(side, buf); !ok || gk != k || gj != j {
				t.Fatalf("Locate(%d, value of %d, %d) = %d, %d, %v", side, k, j, gk, gj, ok)
			}
			if _, _, ok := c.Locate(3-side, buf); ok {
				t.Fatalf("a %d value located on side %d", side, 3-side)
			}
		}
		if _, _, ok := c.Locate(harness.Left, nil); ok {
			t.Error("located an empty value")
		}
		small := c
		small.Keys = 10
		buf = g.Append(buf[:0], harness.Left, 10, 0)
		if _, _, ok := small.Locate(harness.Left, buf); ok {
			t.Error("located a key out of range")
		}
	})
}

func TestLocate_reads_legacy_headers(t *testing.T) {
	c := Config{Seed: 9, Keys: 100, ValueSize: 30}
	for _, side := range []harness.Side{harness.Left, harness.Right} {
		if k, j, ok := c.Locate(side, c.Value(side, 42, 3)); !ok || k != 42 || j != 3 {
			t.Errorf("Locate(%d) = %d, %d, %v", side, k, j, ok)
		}
		if _, _, ok := c.Locate(side, c.Value(3-side, 42, 3)); ok {
			t.Errorf("located a side %d value on side %d", 3-side, side)
		}
	}
	if _, _, ok := c.Locate(harness.Left, c.Value(harness.Left, 42, 3)[:9]); ok {
		t.Error("located a short value")
	}
}

func TestJSON_values_are_valid(t *testing.T) {
	for _, s := range Profiles() {
		t.Run(s.Name, func(t *testing.T) {
			c := pair(s, JSON)
			g := c.ValueGen()
			var buf []byte
			for i := range 20000 {
				buf = g.Append(buf[:0], harness.Side(1+i%2), i, i%4)
				if !json.Valid(buf) {
					t.Fatalf("invalid JSON: %s", buf)
				}
			}
		})
	}
}

func TestSchema_round_trips_through_JSON(t *testing.T) {
	for _, s := range Profiles() {
		b, err := json.MarshalIndent(s, "", "  ")
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		var got Schema
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("%s: %v\n%s", s.Name, err, b)
		}
		if !reflect.DeepEqual(&got, s) {
			t.Errorf("%s doesn't round-trip:\n%s", s.Name, b)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
		if s == Payment && !bytes.Contains(b, []byte(`"type": "money"`)) {
			t.Errorf("types aren't written by name:\n%s", b)
		}
	}
	var s Schema
	err := json.Unmarshal([]byte(`{"name":"X","fields":[{"name":"id","type":"identity"},{"name":"m","type":"map"}]}`), &s)
	if err == nil || !strings.Contains(err.Error(), "map") {
		t.Errorf("a map type: %v", err)
	}
}

func TestValidate_rejects_bad_schemas(t *testing.T) {
	id := Field{Name: "id", Type: TypeIdentity}
	str := Field{Name: "s", Type: TypeString, Source: SourceKey}
	with := func(fs ...Field) *Schema { return &Schema{Name: "S", Fields: append([]Field{id}, fs...)} }
	tests := map[string]*Schema{
		"no schema":           nil,
		"bad schema name":     {Name: "1S", Fields: []Field{id}},
		"no fields":           {Name: "S"},
		"identity not first":  {Name: "S", Fields: []Field{str, id}},
		"identity not id":     {Name: "S", Fields: []Field{{Name: "key", Type: TypeIdentity}}},
		"optional identity":   {Name: "S", Fields: []Field{{Name: "id", Type: TypeIdentity, Optional: 0.1}}},
		"two identities":      with(Field{Name: "id2", Type: TypeIdentity}),
		"bad field name":      with(Field{Name: "a-b", Type: TypeBool}),
		"duplicate field":     with(str, str),
		"no type":             with(Field{Name: "x"}),
		"optional 1":          with(Field{Name: "x", Type: TypeBool, Optional: 1}),
		"string no source":    with(Field{Name: "x", Type: TypeString}),
		"source on int":       with(Field{Name: "x", Type: TypeInt32, Source: SourceKey, Max: 3}),
		"empty alphabet":      with(Field{Name: "x", Type: TypeString, Source: SourceToken, Len: Dist{Median: 3, Min: 1, Max: 5}}),
		"non-ASCII alphabet":  with(Field{Name: "x", Type: TypeString, Source: SourceToken, Alphabet: "aé", Len: Dist{Median: 3, Min: 1, Max: 5}}),
		"inverted length":     with(Field{Name: "x", Type: TypeBytes, Len: Dist{Median: 3, Min: 5, Max: 1}}),
		"dict without kind":   with(Field{Name: "x", Type: TypeString, Source: SourceDict, Card: 10}),
		"dict without card":   with(Field{Name: "x", Type: TypeString, Source: SourceDict, Dict: DictName}),
		"choice of nothing":   with(Field{Name: "x", Type: TypeString, Source: SourceChoice}),
		"control character":   with(Field{Name: "x", Type: TypeString, Source: SourceChoice, Symbols: []string{"a\nb"}}),
		"enum without name":   with(Field{Name: "x", Type: TypeEnum, Symbols: []string{"A"}}),
		"enum symbol dash":    with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"PMNT-ICDT"}}),
		"enum UNSPECIFIED":    with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"UNSPECIFIED"}}),
		"enum symbol twice":   with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"A", "A"}}),
		"weights mismatch":    with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"A", "B"}, Weights: []float64{1}}),
		"zero weight":         with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"A", "B"}, Weights: []float64{1, 0}}),
		"type name twice":     with(Field{Name: "x", Type: TypeEnum, TypeName: "E", Symbols: []string{"A"}}, Field{Name: "y", Type: TypeEnum, TypeName: "E", Symbols: []string{"A"}}),
		"reserved type name":  with(Field{Name: "x", Type: TypeEnum, TypeName: "Money", Symbols: []string{"A"}}),
		"schema's type name":  with(Field{Name: "x", Type: TypeRecord, TypeName: "S", Fields: []Field{str}}),
		"int32 out of range":  with(Field{Name: "x", Type: TypeInt32, Min: 0, Max: 1 << 40}),
		"inverted int range":  with(Field{Name: "x", Type: TypeInt64, Min: 5, Max: 1}),
		"ints and range":      with(Field{Name: "x", Type: TypeInt64, Ints: []int64{1}, Max: 4}),
		"zero median":         with(Field{Name: "x", Type: TypeSInt64}),
		"huge sigma":          with(Field{Name: "x", Type: TypeDouble, Median: 1, Sigma: 50}),
		"no window":           with(Field{Name: "x", Type: TypeTimestamp}),
		"negative lag":        with(Field{Name: "x", Type: TypeDate, Lag: -1}),
		"fee before amount":   with(Field{Name: "x", Type: TypeMoney, Fee: true}),
		"empty record":        with(Field{Name: "x", Type: TypeRecord, TypeName: "R"}),
		"identity in record":  with(Field{Name: "x", Type: TypeRecord, TypeName: "R", Fields: []Field{id}}),
		"bad nested field":    with(Field{Name: "x", Type: TypeRecord, TypeName: "R", Fields: []Field{{Name: "y"}}}),
		"bad repeat":          with(Field{Name: "x", Type: TypeBool, Repeat: &Dist{Median: 1, Min: 2, Max: 1}}),
		"text without length": with(Field{Name: "x", Type: TypeString, Source: SourceText}),
	}
	for name, s := range tests {
		if err := s.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}
	for _, s := range Profiles() {
		if err := s.Validate(); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	bad := map[string]Config{
		"one schema":     {Keys: 10, Left: Values{Schema: Tiny}},
		"bad schema":     {Keys: 10, Left: Values{Schema: &Schema{Name: "S"}}, Right: Values{Schema: Tiny}},
		"bad encoding":   {Keys: 10, Left: Values{Schema: Tiny, Encoding: 3}, Right: Values{Schema: Tiny}},
		"too many keys":  {Keys: maxDecimalKey + 1},
		"negative keys":  {Keys: -1},
		"unknown mix":    {Keys: 10, Mix: 2},
		"unknown format": {Keys: 10, KeyFormat: 2},
		"identity of 0": {Keys: 10, Left: Values{Schema: Tiny}, Right: Values{Schema: Tiny},
			Seed: mix(uint64(harness.Left)<<62 | 1<<60 | 7)},
	}
	for name, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}
	for _, c := range []Config{{Keys: 10, ValueSize: 5}, pair(Payment, JSON), {Keys: maxDecimalKey + 1, KeyFormat: RFKeys}} {
		if err := c.Validate(); err != nil {
			t.Errorf("%+v: %v", c, err)
		}
	}
}

func TestParse_names(t *testing.T) {
	for _, e := range encodings {
		if got, err := ParseEncoding(strings.ToUpper(e.String())); err != nil || got != e {
			t.Errorf("ParseEncoding(%v) = %v, %v", e, got, err)
		}
	}
	for _, m := range []Mix{Stress, Typical} {
		if got, err := ParseMix(m.String()); err != nil || got != m {
			t.Errorf("ParseMix(%v) = %v, %v", m, got, err)
		}
	}
	for _, f := range []KeyFormat{DecimalKeys, RFKeys} {
		if got, err := ParseKeyFormat(f.String()); err != nil || got != f {
			t.Errorf("ParseKeyFormat(%v) = %v, %v", f, got, err)
		}
	}
	if _, err := ParseEncoding("xml"); err == nil {
		t.Error("ParseEncoding accepted xml")
	}
	for _, name := range []string{"payment", "BANKLINE", "Tiny"} {
		if Profile(name) == nil {
			t.Errorf("no profile %q", name)
		}
	}
	if Profile("nope") != nil {
		t.Error("Profile(nope) isn't nil")
	}
}

// records collects a part's records as strings.
func records(c Config, part, parts int) []string {
	var out []string
	for r := range c.Records(part, parts) {
		out = append(out, fmt.Sprintf("%d %s %x", r.Side, r.Key, r.Value))
	}
	return out
}

func TestRecords_parts_can_run_at_once(t *testing.T) {
	c := Config{Seed: 6, Keys: 20_000, Left: Values{Schema: Payment}, Right: Values{Schema: BankLine, Encoding: JSON}, Mix: Typical, KeyFormat: RFKeys}
	const parts = 4
	var want [parts][]string
	for p := range parts {
		want[p] = records(c, p, parts)
	}
	var got [parts][]string
	var wg sync.WaitGroup
	for p := range parts {
		wg.Go(func() { got[p] = records(c, p, parts) })
	}
	wg.Wait()
	for p := range parts {
		if !slices.Equal(got[p], want[p]) {
			t.Errorf("part %d differs when the parts run at once", p)
		}
	}
}

func TestRecords_never_reuse_yielded_slices(t *testing.T) {
	for _, c := range []Config{
		{Seed: 7, Keys: 30_000, ValueSize: 100},
		{Seed: 7, Keys: 30_000, Left: Values{Schema: Envelope}, Right: Values{Schema: Payment, Encoding: Avro}, KeyFormat: RFKeys},
	} {
		type kept struct{ key, value, copy []byte }
		var all []kept
		for r := range c.Records(0, 1) {
			all = append(all, kept{r.Key, r.Value, slices.Concat(r.Key, r.Value)})
			if cap(r.Key) != len(r.Key) || cap(r.Value) != len(r.Value) {
				t.Fatal("a yielded slice has spare capacity, which an append would overwrite")
			}
		}
		for i, k := range all {
			if !bytes.Equal(slices.Concat(k.key, k.value), k.copy) {
				t.Fatalf("record %d changed after it was yielded", i)
			}
		}
	}
}

func TestTypical_mix_proportions(t *testing.T) {
	c := Config{Seed: 11, Keys: 1_000_000, Mix: Typical}
	got := map[string]int{}
	for k := range c.Keys {
		l, r := c.Counts(k)
		switch {
		case l > 3 || r > 3:
			t.Fatalf("key %d has %d left and %d right records", k, l, r)
		case l == 1 && r == 1:
			got["1:1"]++
		case l == 1 && r == 0:
			got["left only"]++
		case l == 0 && r == 1:
			got["right only"]++
		case l == 1 && r >= 2:
			got["1:N"]++
		case l >= 2 && r == 1:
			got["N:1"]++
		default:
			t.Fatalf("key %d has %d left and %d right records", k, l, r)
		}
	}
	want := map[string]float64{"1:1": 0.93, "left only": 0.03, "right only": 0.03, "1:N": 0.005, "N:1": 0.005}
	for kind, p := range want {
		// Five standard deviations of a binomial count.
		mean := p * float64(c.Keys)
		if d := math.Abs(float64(got[kind]) - mean); d > 5*math.Sqrt(mean*(1-p)) {
			t.Errorf("%s: %d keys, want about %.0f", kind, got[kind], mean)
		}
	}
	t.Logf("%v", got)
}

// meanSizes is the prototype's mean value size of each profile, in
// protobuf, Avro and JSON.
var meanSizes = map[string][3]float64{
	"Payment":        {261, 219, 613},
	"BankLine":       {191, 148, 405},
	"CardSettlement": {144, 104, 415},
	"Envelope":       {581, 578, 793},
	"Tiny":           {23, 14, 86},
}

func TestValue_sizes_match_the_prototype(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n = 20_000
	}
	each(t, func(t *testing.T, s *Schema, enc Encoding) {
		t.Parallel()
		c := pair(s, enc)
		g := c.ValueGen()
		var buf []byte
		var total int
		for i := range n {
			buf = g.Append(buf[:0], harness.Left, i*7+3, i%3)
			total += len(buf)
		}
		mean := float64(total) / float64(n)
		want := meanSizes[s.Name][enc]
		t.Logf("mean %.1f bytes, prototype %.0f", mean, want)
		if math.Abs(mean-want) > 0.1*want {
			t.Errorf("mean %.1f bytes, want %.0f ± 10%%", mean, want)
		}
	})
}

func TestEmitters_declare_the_encodings(t *testing.T) {
	for _, s := range Profiles() {
		p, a := ProtoFile(s, "poc/x"), AvroIDL(s, "gen.x")
		if p != ProtoFile(s, "poc/x") || a != AvroIDL(s, "gen.x") {
			t.Errorf("%s: the emitters aren't deterministic", s.Name)
		}
		if !strings.Contains(p, "  fixed64 id = 1;\n") || !strings.Contains(a, "  Id id;\n") || !strings.Contains(a, "fixed Id(8);") {
			t.Errorf("%s: the identity isn't declared first:\n%s\n%s", s.Name, p, a)
		}
		for _, bad := range []string{"map<", "timestamp-millis", "decimal(", "date)"} {
			if strings.Contains(p, bad) || strings.Contains(a, bad) {
				t.Errorf("%s: emitted %q", s.Name, bad)
			}
		}
	}
	a := AvroIDL(Payment, "gen.payment")
	for _, want := range []string{"array<KeyValue> metadata;", "union { null, double } fx_rate;", "long booked_at;", "int value_date;", "record Money {\n  string currency_code;\n  bytes amount;\n}"} {
		if !strings.Contains(a, want) {
			t.Errorf("Payment's IDL lacks %q", want)
		}
	}
	p := ProtoFile(Payment, "poc/x")
	for _, want := range []string{"optional double fx_rate = 8;", "google.protobuf.Timestamp booked_at = 9;", "repeated KeyValue metadata = 21;", "PAYMENT_TYPE_UNSPECIFIED = 0;", "CHANNEL_CARD_NETWORK = 6;"} {
		if !strings.Contains(p, want) {
			t.Errorf("Payment's .proto lacks %q", want)
		}
	}
}

func BenchmarkValueGen(b *testing.B) {
	for _, s := range Profiles() {
		for _, enc := range encodings {
			b.Run(s.Name+"/"+enc.String(), func(b *testing.B) {
				c := pair(s, enc)
				g := c.ValueGen()
				buf := make([]byte, 0, 1<<20)
				var total int
				i := 0
				for b.Loop() {
					buf = g.Append(buf[:0], harness.Side(1+i%2), i, i%3)
					total += len(buf)
					i++
				}
				b.SetBytes(int64(total / max(i, 1)))
			})
		}
	}
}
