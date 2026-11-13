package gen

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
)

// Type is a field's type in a Schema.
type Type int

// The zero Type is unset, so that a schema file can't leave a type out by
// mistake.
const (
	TypeIdentity  Type = iota + 1 // the record's identity: protobuf fixed64, Avro fixed(8), JSON hex string
	TypeString                    // text from Source
	TypeBytes                     // random bytes, their length from Len
	TypeInt32                     // uniform in [Min, Max], or one of Ints
	TypeInt64                     // uniform in [Min, Max], or one of Ints
	TypeSInt64                    // lognormal around Median, negative half the time if Signed
	TypeDouble                    // lognormal around Median, rounded to 6 decimals
	TypeBool                      // true with probability 0.1
	TypeEnum                      // one of Symbols by Weights
	TypeTimestamp                 // milliseconds in a Window from the base time
	TypeDate                      // the record's timestamp date plus 0 to Lag days
	TypeMoney                     // a currency and a lognormal amount in minor units, or a fee of the record's amount
	TypeRecord                    // a nested record of Fields
)

var typeNames = []string{"", "identity", "string", "bytes", "int32", "int64", "sint64", "double", "bool", "enum", "timestamp", "date", "money", "record"}

// Source is where a string field's text comes from.
type Source int

// The zero Source is unset: only string fields have one.
const (
	SourceKey    Source = iota + 1 // the record's key
	SourceToken                    // random characters from Alphabet, the count from Len
	SourceDict                     // an entry of a generated dictionary of Card entries, Zipf-chosen with Skew
	SourceText                     // Zipf-chosen words and reference numbers, the word count from Len, cut at Len.Max bytes
	SourceChoice                   // one of Symbols by Weights
)

var sourceNames = []string{"", "key", "token", "dict", "text", "choice"}

// DictKind is the kind of entries a generated dictionary holds.
type DictKind int

// The zero DictKind is unset: only dict strings have one.
const (
	DictName     DictKind = iota + 1 // company names, like "Kalomi Ixjen GmbH"
	DictIBAN                         // IBAN-shaped account numbers
	DictBIC                          // BIC-shaped bank codes
	DictWord                         // words made of syllables
	DictUser                         // user and service account names
	DictTerminal                     // card terminal IDs
)

var dictNames = []string{"", "name", "iban", "bic", "word", "user", "terminal"}

func enumText(names []string, i int) ([]byte, error) {
	if i == 0 {
		return []byte{}, nil
	}
	if i < 0 || i >= len(names) {
		return nil, fmt.Errorf("unknown value %d", i)
	}
	return []byte(names[i]), nil
}

func parseEnum(names []string, kind string, text []byte) (int, error) {
	s := string(text)
	if s == "" {
		return 0, nil
	}
	if i := slices.Index(names, strings.ToLower(s)); i > 0 {
		return i, nil
	}
	return 0, fmt.Errorf("unknown %s %q, want one of %s", kind, s, strings.Join(names[1:], ", "))
}

// String returns t's name, as in a schema file.
func (t Type) String() string {
	b, err := enumText(typeNames, int(t))
	if err != nil {
		return fmt.Sprintf("Type(%d)", int(t))
	}
	return string(b)
}

// MarshalText returns t's name.
func (t Type) MarshalText() ([]byte, error) { return enumText(typeNames, int(t)) }

// UnmarshalText parses a type's name.
func (t *Type) UnmarshalText(text []byte) error {
	if strings.EqualFold(string(text), "map") {
		return errors.New(`there is no map type: use a list of records, such as {key, value}, so that Avro encoders write it in a fixed order`)
	}
	i, err := parseEnum(typeNames, "type", text)
	*t = Type(i)
	return err
}

// String returns s's name, as in a schema file.
func (s Source) String() string {
	b, err := enumText(sourceNames, int(s))
	if err != nil {
		return fmt.Sprintf("Source(%d)", int(s))
	}
	return string(b)
}

// MarshalText returns s's name.
func (s Source) MarshalText() ([]byte, error) { return enumText(sourceNames, int(s)) }

// UnmarshalText parses a source's name.
func (s *Source) UnmarshalText(text []byte) error {
	i, err := parseEnum(sourceNames, "source", text)
	*s = Source(i)
	return err
}

// String returns d's name, as in a schema file.
func (d DictKind) String() string {
	b, err := enumText(dictNames, int(d))
	if err != nil {
		return fmt.Sprintf("DictKind(%d)", int(d))
	}
	return string(b)
}

// MarshalText returns d's name.
func (d DictKind) MarshalText() ([]byte, error) { return enumText(dictNames, int(d)) }

// UnmarshalText parses a dictionary kind's name.
func (d *DictKind) UnmarshalText(text []byte) error {
	i, err := parseEnum(dictNames, "dict", text)
	*d = DictKind(i)
	return err
}

// Dist is a lognormal distribution of lengths around Median, rounded and
// clamped to [Min, Max]. With a zero Sigma every length is Median.
type Dist struct {
	Median float64 `json:"median"`
	Sigma  float64 `json:"sigma,omitempty"`
	Min    int     `json:"min"`
	Max    int     `json:"max"`
}

// Field is one field of a record: its name and type, and how its values
// are drawn. Which of the drawing parameters apply depends on Type and
// Source; Validate rejects the ones that don't.
type Field struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
	// TypeName names an enum's or a record's type in .proto and Avro IDL.
	TypeName string `json:"type_name,omitempty"`
	// Optional is the probability that the field is absent: proto3
	// optional, an Avro union with null, and a JSON field left out. For a
	// list it is the probability that the list is empty.
	Optional float64 `json:"optional,omitempty"`
	// Repeat makes the field a list, its length drawn from Repeat.
	Repeat *Dist `json:"repeat,omitempty"`

	Source   Source   `json:"source,omitempty"`
	Len      Dist     `json:"length,omitzero"`    // TypeBytes, SourceToken, SourceText
	Alphabet string   `json:"alphabet,omitempty"` // SourceToken
	Dict     DictKind `json:"dict,omitempty"`     // SourceDict
	Card     int      `json:"card,omitempty"`     // SourceDict: the dictionary's entries
	Skew     float64  `json:"skew,omitempty"`     // SourceDict: the Zipf exponent

	Symbols []string  `json:"symbols,omitempty"` // TypeEnum, SourceChoice
	Ints    []int64   `json:"ints,omitempty"`    // TypeInt32, TypeInt64: a weighted choice instead of [Min, Max]
	Weights []float64 `json:"weights,omitempty"` // of Symbols or Ints; equal if nil

	Min    int64   `json:"min,omitempty"`    // TypeInt32, TypeInt64
	Max    int64   `json:"max,omitempty"`    // TypeInt32, TypeInt64
	Median float64 `json:"median,omitempty"` // TypeSInt64, TypeDouble, TypeMoney (in minor units)
	Sigma  float64 `json:"sigma,omitempty"`  // TypeSInt64, TypeDouble, TypeMoney
	Signed bool    `json:"signed,omitempty"` // TypeSInt64, TypeMoney: negative with probability 1/2
	// Fee makes a money field a fraction (0.1% to 3%) of the record's last
	// drawn amount, in its currency.
	Fee    bool    `json:"fee,omitempty"`
	Window int64   `json:"window_ms,omitempty"` // TypeTimestamp
	Lag    int     `json:"lag_days,omitempty"`  // TypeDate
	Fields []Field `json:"fields,omitempty"`    // TypeRecord
}

// Schema is a record type: the struct a client holds before it marshals a
// value. Its first field is the record's identity, named "id".
//
// Generating values compiles a schema once per seed and caches the result
// by the Schema's address, so a Schema must not change after its first
// use.
type Schema struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

const (
	maxDepth  = 8                       // records nested in records
	maxLen    = 1 << 20                 // a list's, string's or bytes' length
	maxCard   = 1 << 22                 // a dictionary's entries
	maxWindow = 100 * 365 * day         // a timestamp's window
	maxSigma  = 4                       // lognormal spread
	maxMedian = float64(int64(1) << 50) // lognormal median
	maxRange  = uint64(1) << 62         // Max-Min of an integer field
	maxLag    = 3650                    // a date's lag in days
)

// reserved are type names the emitters define themselves.
var reserved = []string{"Id", "Money", "Date"}

// Validate reports whether s can be generated and emitted: the identity
// comes first and is named "id", names are identifiers that generated
// code can't mix up, enum symbols are Avro identifiers, type names are
// unique, every parameter fits its type and source, and a fee follows the
// amount it is a fraction of.
func (s *Schema) Validate() error {
	if s == nil {
		return errors.New("no schema")
	}
	if !isIdent(s.Name) {
		return fmt.Errorf("schema name %q is not an identifier", s.Name)
	}
	if len(s.Fields) == 0 || s.Fields[0].Type != TypeIdentity || s.Fields[0].Name != "id" {
		return fmt.Errorf("%s: the first field must be the identity, named id", s.Name)
	}
	if f := s.Fields[0]; f.Optional != 0 || f.Repeat != nil {
		return fmt.Errorf("%s.id: the identity can't be optional or a list", s.Name)
	}
	v := validator{types: map[string]string{}, protoNames: map[string]bool{}}
	for _, name := range append([]string{s.Name}, reserved...) {
		v.types[fold(name)] = name
		v.protoNames[name] = true
	}
	return v.record(s.Name, s.Fields, 0)
}

// fold returns the form of a name that generated code may give it: protoc
// and the Go generators change its case and drop or add underscores, so
// two names that fold alike may clash there.
func fold(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

// validator carries what Validate learns as it walks a schema in the
// order values are drawn.
type validator struct {
	types      map[string]string // type names defined so far, by fold
	protoNames map[string]bool   // names in the .proto package's scope: types and enum values
	amounts    bool              // a money amount has been drawn, so a fee can follow
}

func (v *validator) record(path string, fs []Field, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("%s: records nest more than %d deep", path, maxDepth)
	}
	if len(fs) == 0 {
		return fmt.Errorf("%s: no fields", path)
	}
	names := map[string]string{} // by fold
	for i := range fs {
		f := &fs[i]
		p := path + "." + f.Name
		if !isIdent(f.Name) {
			return fmt.Errorf("%s: field name %q is not an identifier", path, f.Name)
		}
		if other, ok := names[fold(f.Name)]; ok {
			return fmt.Errorf("%s: fields %s and %s would clash in generated code, which changes case and underscores", path, other, f.Name)
		}
		names[fold(f.Name)] = f.Name
		if f.Type == TypeIdentity && (depth > 0 || i > 0) {
			return fmt.Errorf("%s: only the first field can be the identity", p)
		}
		if err := v.field(p, f, depth); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) field(p string, f *Field, depth int) error {
	if !(f.Optional >= 0 && f.Optional < 1) {
		return fmt.Errorf("%s: optional %v is not in [0, 1)", p, f.Optional)
	}
	if f.Repeat != nil {
		if err := checkDist(f.Repeat); err != nil {
			return fmt.Errorf("%s: repeat: %w", p, err)
		}
	}
	if f.Source != 0 && f.Type != TypeString {
		return fmt.Errorf("%s: a source on a %s field", p, f.Type)
	}
	if (f.Type == TypeEnum || f.Type == TypeRecord) != (f.TypeName != "") {
		return fmt.Errorf("%s: enums and records, and only they, need a type_name", p)
	}
	if f.TypeName != "" {
		if !isIdent(f.TypeName) {
			return fmt.Errorf("%s: type name %q is not an identifier", p, f.TypeName)
		}
		if other, ok := v.types[fold(f.TypeName)]; ok {
			return fmt.Errorf("%s: type name %s clashes with %s in generated code, or is reserved", p, f.TypeName, other)
		}
		v.types[fold(f.TypeName)] = f.TypeName
		if err := v.protoName(p, f.TypeName); err != nil {
			return err
		}
	}
	switch f.Type {
	case TypeIdentity, TypeBool:
	case TypeString:
		return v.str(p, f)
	case TypeBytes:
		if err := checkDist(&f.Len); err != nil {
			return fmt.Errorf("%s: length: %w", p, err)
		}
	case TypeInt32, TypeInt64:
		return checkInts(p, f)
	case TypeSInt64, TypeDouble:
		return checkLognormal(p, f.Median, f.Sigma)
	case TypeEnum:
		if len(f.Symbols) == 0 {
			return fmt.Errorf("%s: an enum with no symbols", p)
		}
		for i, s := range f.Symbols {
			if !isIdent(s) || s == "UNSPECIFIED" {
				return fmt.Errorf("%s: enum symbol %q is not an Avro identifier, or is UNSPECIFIED", p, s)
			}
			if slices.Contains(f.Symbols[:i], s) {
				return fmt.Errorf("%s: enum symbol %s appears twice", p, s)
			}
		}
		// Protobuf enum values share the package's scope.
		prefix := upperSnake(f.TypeName) + "_"
		for _, s := range append([]string{"UNSPECIFIED"}, f.Symbols...) {
			if err := v.protoName(p, prefix+s); err != nil {
				return err
			}
		}
		return checkWeights(p, f.Weights, len(f.Symbols))
	case TypeTimestamp:
		if f.Window <= 0 || f.Window > maxWindow {
			return fmt.Errorf("%s: window %d ms is not in (0, 100 years]", p, f.Window)
		}
	case TypeDate:
		if f.Lag < 0 || f.Lag > maxLag {
			return fmt.Errorf("%s: lag %d days is not in [0, %d]", p, f.Lag, maxLag)
		}
	case TypeMoney:
		if f.Fee {
			if !v.amounts {
				return fmt.Errorf("%s: a fee before any amount", p)
			}
			return nil
		}
		v.amounts = true
		return checkLognormal(p, f.Median, f.Sigma)
	case TypeRecord:
		return v.record(p, f.Fields, depth+1)
	default:
		return fmt.Errorf("%s: unknown type %v", p, f.Type)
	}
	return nil
}

// protoName adds name to the .proto package's scope, unless it is there.
func (v *validator) protoName(p, name string) error {
	if v.protoNames[name] {
		return fmt.Errorf("%s: %s is defined twice in the .proto file", p, name)
	}
	v.protoNames[name] = true
	return nil
}

func (v *validator) str(p string, f *Field) error {
	switch f.Source {
	case SourceKey:
	case SourceToken:
		if f.Alphabet == "" || !printable(f.Alphabet) {
			return fmt.Errorf("%s: the alphabet must be printable ASCII", p)
		}
		if err := checkDist(&f.Len); err != nil {
			return fmt.Errorf("%s: length: %w", p, err)
		}
	case SourceDict:
		if f.Dict <= 0 || int(f.Dict) >= len(dictNames) {
			return fmt.Errorf("%s: unknown dict %v", p, f.Dict)
		}
		if f.Card < 1 || f.Card > maxCard {
			return fmt.Errorf("%s: card %d is not in [1, %d]", p, f.Card, maxCard)
		}
		if !(f.Skew >= 0 && f.Skew <= 10) {
			return fmt.Errorf("%s: skew %v is not in [0, 10]", p, f.Skew)
		}
	case SourceText:
		if err := checkDist(&f.Len); err != nil {
			return fmt.Errorf("%s: length: %w", p, err)
		}
		if f.Len.Max < 1 {
			return fmt.Errorf("%s: text needs a length max of at least 1", p)
		}
	case SourceChoice:
		if len(f.Symbols) == 0 {
			return fmt.Errorf("%s: a choice with no symbols", p)
		}
		for _, s := range f.Symbols {
			if !printable(s) {
				return fmt.Errorf("%s: choice %q is not printable ASCII", p, s)
			}
		}
		return checkWeights(p, f.Weights, len(f.Symbols))
	default:
		return fmt.Errorf("%s: a string needs a source", p)
	}
	return nil
}

func checkDist(d *Dist) error {
	switch {
	case d.Min < 0 || d.Max < d.Min || d.Max > maxLen:
		return fmt.Errorf("[%d, %d] is not a range within [0, %d]", d.Min, d.Max, maxLen)
	case !(d.Median >= 0 && d.Median <= maxLen):
		return fmt.Errorf("median %v is not in [0, %d]", d.Median, maxLen)
	case !(d.Sigma >= 0 && d.Sigma <= maxSigma):
		return fmt.Errorf("sigma %v is not in [0, %d]", d.Sigma, maxSigma)
	}
	return nil
}

func checkLognormal(p string, median, sigma float64) error {
	if !(median > 0 && median <= maxMedian) {
		return fmt.Errorf("%s: median %v is not in (0, 2^50]", p, median)
	}
	if !(sigma >= 0 && sigma <= maxSigma) {
		return fmt.Errorf("%s: sigma %v is not in [0, %d]", p, sigma, maxSigma)
	}
	return nil
}

func checkInts(p string, f *Field) error {
	lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
	if f.Type == TypeInt32 {
		lo, hi = math.MinInt32, math.MaxInt32
	}
	if f.Ints != nil {
		if f.Min != 0 || f.Max != 0 {
			return fmt.Errorf("%s: both ints and a [min, max] range", p)
		}
		if len(f.Ints) == 0 {
			return fmt.Errorf("%s: an empty ints choice", p)
		}
		for _, x := range f.Ints {
			if x < lo || x > hi {
				return fmt.Errorf("%s: %d doesn't fit a %s", p, x, f.Type)
			}
		}
		return checkWeights(p, f.Weights, len(f.Ints))
	}
	if f.Weights != nil {
		return fmt.Errorf("%s: weights without ints", p)
	}
	if f.Min > f.Max || f.Min < lo || f.Max > hi || uint64(f.Max-f.Min) >= maxRange {
		return fmt.Errorf("%s: [%d, %d] is not a %s range narrower than 2^62", p, f.Min, f.Max, f.Type)
	}
	return nil
}

func checkWeights(p string, w []float64, n int) error {
	if w == nil {
		return nil
	}
	if len(w) != n {
		return fmt.Errorf("%s: %d weights for %d choices", p, len(w), n)
	}
	for _, x := range w {
		if !(x > 0 && x < math.Inf(1)) {
			return fmt.Errorf("%s: weight %v is not positive", p, x)
		}
	}
	return nil
}

// isIdent reports whether s is an identifier in protobuf, Avro and Go:
// a letter or underscore, then letters, digits and underscores.
func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range []byte(s) {
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// printable reports whether s is printable ASCII, which every encoding
// writes as is apart from JSON's escapes of '"', '\\', '&', '<' and '>'.
func printable(s string) bool {
	for _, c := range []byte(s) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}
