package reference

import (
	"math"

	"poc/e2e/gen"
)

// list returns a list length distribution.
func list(median, sigma float64, lo, hi int) *gen.Dist {
	return &gen.Dist{Median: median, Sigma: sigma, Min: lo, Max: hi}
}

// kitchenSink is no profile. It has every type as a plain field, an
// optional one (but see below) and a list, so that the encoders' paths
// the profiles never take are checked against generated code too. Some
// fields are made to draw edge cases every time or often:
//   - zero values, which protobuf leaves out of plain fields but writes for
//     optional ones and list items: the empty string and bytes, 0, false,
//     a double that rounds to 0, a timestamp with no nanos, a money amount
//     of 0, and a record of nothing but zero values;
//   - negative int32 and int64 values, which protobuf writes in 10 bytes,
//     and the extremes of both;
//   - strings JSON escapes;
//   - doubles of 10^21 and more, which JSON writes with an exponent;
//   - money amounts below 1 and negative, and fees of nothing;
//   - a date with no timestamp before it;
//   - a list of more than 63 items, which needs a two-byte Avro count;
//   - records in lists and records in records, and a record JSON writes
//     as {}.
//
// No enum or record is optional: avroc-gen-go v0.3.0 declares no Go type
// for an enum or record that only a union refers to, so its code wouldn't
// compile. Their encoding is an optional field's, which other types check.
var kitchenSink = &gen.Schema{Name: "KitchenSink", Fields: []gen.Field{
	{Name: "id", Type: gen.TypeIdentity},

	{Name: "key", Type: gen.TypeString, Source: gen.SourceKey},
	{Name: "empty", Type: gen.TypeString, Source: gen.SourceToken, Alphabet: "x"},
	{Name: "maybe_empty", Type: gen.TypeString, Source: gen.SourceToken, Alphabet: `"\&<>' ~`, Len: gen.Dist{Median: 1, Sigma: 1, Max: 4}, Optional: 0.5},
	{Name: "words", Type: gen.TypeString, Source: gen.SourceText, Len: gen.Dist{Median: 3, Sigma: 0.8, Min: 1, Max: 20}, Repeat: list(1, 1, 0, 5)},
	{Name: "company", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictName, Card: 50, Skew: 1, Optional: 0.5},
	{Name: "choices", Type: gen.TypeString, Source: gen.SourceChoice, Symbols: []string{"", "a&b", "<x>", `q"\`}, Repeat: list(2, 1, 0, 6)},

	{Name: "blob", Type: gen.TypeBytes, Len: gen.Dist{Median: 2, Sigma: 1, Max: 8}},
	{Name: "maybe_blob", Type: gen.TypeBytes, Len: gen.Dist{Median: 1, Sigma: 1, Max: 2}, Optional: 0.5},
	{Name: "blobs", Type: gen.TypeBytes, Len: gen.Dist{Median: 1, Sigma: 1, Max: 3}, Repeat: list(2, 1, 0, 4)},

	{Name: "zero32", Type: gen.TypeInt32},
	{Name: "maybe_zero32", Type: gen.TypeInt32, Optional: 0.5},
	{Name: "small32", Type: gen.TypeInt32, Min: -3, Max: 2},
	{Name: "ints32", Type: gen.TypeInt32, Ints: []int64{math.MinInt32, -1, 0, 1, math.MaxInt32}, Repeat: list(3, 1, 0, 8)},

	{Name: "extreme64", Type: gen.TypeInt64, Ints: []int64{math.MinInt64, -1, 0, math.MaxInt64}},
	{Name: "maybe64", Type: gen.TypeInt64, Ints: []int64{0, -1}, Optional: 0.5},
	{Name: "wide64", Type: gen.TypeInt64, Min: -1<<61 + 1, Max: 1 << 61},
	{Name: "small64s", Type: gen.TypeInt64, Min: -2, Max: 2, Repeat: list(2, 1, 0, 6)},

	{Name: "small_s64", Type: gen.TypeSInt64, Median: 1, Sigma: 2, Signed: true},
	{Name: "maybe_s64", Type: gen.TypeSInt64, Median: 0.3, Sigma: 1, Signed: true, Optional: 0.5},
	{Name: "s64s", Type: gen.TypeSInt64, Median: 1e15, Sigma: 4, Signed: true, Repeat: list(2, 1, 0, 6)},

	{Name: "zero_double", Type: gen.TypeDouble, Median: 1e-8},
	{Name: "maybe_double", Type: gen.TypeDouble, Median: 1e-7, Sigma: 3, Optional: 0.5},
	{Name: "huge_doubles", Type: gen.TypeDouble, Median: 1 << 50, Sigma: 4, Repeat: list(8, 0, 8, 8)},
	{Name: "doubles", Type: gen.TypeDouble, Median: 1, Sigma: 4, Repeat: list(2, 1, 0, 6)},

	{Name: "flag", Type: gen.TypeBool},
	{Name: "maybe_flag", Type: gen.TypeBool, Optional: 0.5},
	{Name: "flags", Type: gen.TypeBool, Repeat: list(3, 1, 0, 8)},

	{Name: "color", Type: gen.TypeEnum, TypeName: "Color", Symbols: []string{"RED", "GREEN"}},
	{Name: "shades", Type: gen.TypeEnum, TypeName: "Shade", Symbols: []string{"PALE", "DARK", "DEEP"}, Weights: []float64{1, 2, 3}, Repeat: list(2, 1, 0, 6)},

	{Name: "maybe_at", Type: gen.TypeTimestamp, Window: 2000, Optional: 0.5},
	{Name: "first_day", Type: gen.TypeDate, Lag: 1},
	{Name: "at_base", Type: gen.TypeTimestamp, Window: 1},
	{Name: "ats", Type: gen.TypeTimestamp, Window: 3000, Repeat: list(2, 1, 0, 6)},
	{Name: "maybe_day", Type: gen.TypeDate, Optional: 0.5},
	{Name: "days", Type: gen.TypeDate, Lag: 3650, Repeat: list(2, 1, 0, 6)},

	{Name: "zero_amount", Type: gen.TypeMoney, Median: 0.4},
	{Name: "fee_of_zero", Type: gen.TypeMoney, Fee: true},
	{Name: "amount", Type: gen.TypeMoney, Median: 150, Sigma: 3, Signed: true},
	{Name: "maybe_fee", Type: gen.TypeMoney, Fee: true, Optional: 0.5},
	{Name: "amounts", Type: gen.TypeMoney, Median: 1e6, Sigma: 2, Signed: true, Repeat: list(2, 1, 0, 6)},

	{Name: "zeros", Type: gen.TypeRecord, TypeName: "Zeros", Fields: []gen.Field{
		{Name: "zero", Type: gen.TypeInt32},
		{Name: "none", Type: gen.TypeString, Source: gen.SourceToken, Alphabet: "x"},
	}},
	{Name: "inner", Type: gen.TypeRecord, TypeName: "Inner", Fields: []gen.Field{
		{Name: "word", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictWord, Card: 10, Optional: 0.5},
		{Name: "nums", Type: gen.TypeInt32, Min: -5, Max: 5, Repeat: list(1, 1, 0, 3), Optional: 0.5},
	}},
	{Name: "items", Type: gen.TypeRecord, TypeName: "Item", Repeat: list(2, 1, 0, 6), Fields: []gen.Field{
		{Name: "at", Type: gen.TypeTimestamp, Window: 86_400_000},
		{Name: "due", Type: gen.TypeDate, Lag: 30, Optional: 0.5},
		{Name: "deep", Type: gen.TypeRecord, TypeName: "Deep", Fields: []gen.Field{
			{Name: "user", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictUser, Card: 20},
			{Name: "terminal", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictTerminal, Card: 20, Optional: 0.5},
			{Name: "account", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictIBAN, Card: 20, Repeat: list(1, 1, 0, 3)},
			{Name: "bank", Type: gen.TypeString, Source: gen.SourceDict, Dict: gen.DictBIC, Card: 20},
			{Name: "fee", Type: gen.TypeMoney, Fee: true},
		}},
	}},
	{Name: "long_list", Type: gen.TypeInt32, Min: 0, Max: 1000, Repeat: list(70, 0.3, 0, 200), Optional: 0.2},
}}
