package gen

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/zeebo/xxh3"

	"poc/e2e/harness"
)

// Encoding is how a record is marshaled into a value.
type Encoding int

const (
	// Protobuf is proto3, as protoc-gen-go marshals the message ProtoFile
	// declares.
	Protobuf Encoding = iota
	// Avro is Avro binary, as avroc-gen-go and avro-go marshal the record
	// AvroIDL declares.
	Avro
	// JSON is what encoding/json writes for a Go struct with the schema's
	// fields in order, optional fields and lists omitempty.
	JSON
)

var encodingNames = []string{"protobuf", "avro", "json"}

// String returns "protobuf", "avro" or "json".
func (e Encoding) String() string {
	if e < 0 || int(e) >= len(encodingNames) {
		return fmt.Sprintf("Encoding(%d)", int(e))
	}
	return encodingNames[e]
}

// ParseEncoding parses an encoding's name, ignoring case.
func ParseEncoding(s string) (Encoding, error) {
	i := slices.Index(encodingNames, strings.ToLower(s))
	if i < 0 {
		return 0, fmt.Errorf("unknown encoding %q, want protobuf, avro or json", s)
	}
	return Encoding(i), nil
}

// MarshalText returns e's name.
func (e Encoding) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

// UnmarshalText parses an encoding's name.
func (e *Encoding) UnmarshalText(text []byte) error {
	v, err := ParseEncoding(string(text))
	*e = v
	return err
}

// Values is how one side's values are made: records of Schema marshaled
// with Encoding. A nil Schema means the legacy generator, whose values are
// Config.ValueSize bytes: a header of side, key and index, then a repeated
// 8-byte pattern.
type Values struct {
	Schema   *Schema
	Encoding Encoding
}

// legacyHeader is the side, key index and record index that every legacy
// value starts with.
const legacyHeader = 1 + 8 + 1

// appendLegacy appends a legacy value.
func appendLegacy(dst []byte, seed uint64, size int, side harness.Side, k, j int) []byte {
	n := max(size, legacyHeader)
	dst = slices.Grow(dst, n)
	start := len(dst)
	dst = dst[:start+n]
	v := dst[start:]
	v[0] = byte(side)
	binary.LittleEndian.PutUint64(v[1:], uint64(k))
	v[9] = byte(j)
	fill := xxh3.HashSeed(v[:legacyHeader], seed)
	for i := legacyHeader; i < n; i++ {
		v[i] = byte(fill >> (8 * (i % 8)))
	}
	return dst
}

// compiled is a schema ready to generate from with one seed: its
// dictionaries and alias tables. It is shared read-only.
type compiled struct {
	fields []cfield
	words  [][]byte // SourceText's words
	wpick  alias
}

// cfield is a field with what it draws from.
type cfield struct {
	*Field
	dict   [][]byte // SourceDict
	pick   alias    // SourceDict, SourceChoice, TypeEnum, Ints
	syms   [][]byte // SourceChoice, TypeEnum
	jname  []byte   // `"name":` in JSON
	fields []cfield // TypeRecord
}

var (
	currencies   = [][]byte{[]byte("USD"), []byte("EUR"), []byte("GBP"), []byte("JPY"), []byte("CAD"), []byte("CHF"), []byte("AUD")}
	currencyPick = newWeighted([]float64{50, 25, 10, 5, 4, 3, 3})
)

// compileKey identifies a compiled schema: Schemas are compiled once per
// seed and must not change after.
type compileKey struct {
	s    *Schema
	seed uint64
}

type compileEntry struct {
	once sync.Once
	c    *compiled
	err  error
}

var compileCache struct {
	sync.Mutex
	m map[compileKey]*compileEntry
}

// compile returns s compiled for seed, building it on first use.
func compile(s *Schema, seed uint64) (*compiled, error) {
	compileCache.Lock()
	if compileCache.m == nil {
		compileCache.m = map[compileKey]*compileEntry{}
	}
	e := compileCache.m[compileKey{s, seed}]
	if e == nil {
		e = &compileEntry{}
		compileCache.m[compileKey{s, seed}] = e
	}
	compileCache.Unlock()
	e.once.Do(func() {
		if e.err = s.Validate(); e.err == nil {
			e.c = build(s, seed)
		}
	})
	return e.c, e.err
}

// build draws a schema's dictionaries: the text words first, then each
// field's in schema order.
func build(s *Schema, seed uint64) *compiled {
	r := rng{s: seed ^ 0x9e3779b97f4a7c15}
	c := &compiled{words: makeDict(&r, DictWord, 3000)}
	c.wpick = newZipf(len(c.words), 1.07)
	c.fields = buildFields(&r, s.Fields)
	return c
}

func buildFields(r *rng, fs []Field) []cfield {
	out := make([]cfield, len(fs))
	for i := range fs {
		f := &fs[i]
		c := cfield{Field: f, jname: []byte(`"` + f.Name + `":`)}
		switch {
		case f.Type == TypeString && f.Source == SourceDict:
			c.dict = makeDict(r, f.Dict, f.Card)
			c.pick = newZipf(f.Card, f.Skew)
		case f.Type == TypeEnum || f.Type == TypeString && f.Source == SourceChoice:
			for _, s := range f.Symbols {
				c.syms = append(c.syms, []byte(s))
			}
			c.pick = weights(f.Weights, len(f.Symbols))
		case f.Ints != nil:
			c.pick = weights(f.Weights, len(f.Ints))
		case f.Type == TypeRecord:
			c.fields = buildFields(r, f.Fields)
		}
		out[i] = c
	}
	return out
}

func weights(w []float64, n int) alias {
	if w == nil {
		return newUniform(n)
	}
	return newWeighted(w)
}

// ValueGen generates values. It holds all the scratch state generation
// needs, so it is not safe for concurrent use: each goroutine makes its
// own with Config.ValueGen.
type ValueGen struct {
	seed      uint64
	valueSize int
	keyFormat KeyFormat
	sides     [2]sideGen
	e         encoder
}

// sideGen is how one side's values are made; a nil c means legacy values.
type sideGen struct {
	c   *compiled
	enc Encoding
}

// ValueGen returns a generator of c's values. It panics if c is not valid
// (see Validate). Dictionaries are built once per schema and seed and
// shared, so making a ValueGen is cheap.
func (c Config) ValueGen() *ValueGen {
	if err := c.Validate(); err != nil {
		panic("gen: " + err.Error())
	}
	g := &ValueGen{seed: c.Seed, valueSize: c.ValueSize, keyFormat: c.KeyFormat}
	for i, v := range []Values{c.Left, c.Right} {
		if v.Schema == nil {
			continue
		}
		sc, err := compile(v.Schema, c.Seed)
		if err != nil {
			panic("gen: " + err.Error())
		}
		g.sides[i] = sideGen{c: sc, enc: v.Encoding}
	}
	return g
}

// Append appends the value of key k's j-th record on side to dst. The
// value is a pure function of the Config's Seed, the side's Values, its
// KeyFormat, side, k and j. It allocates nothing when dst has room.
func (g *ValueGen) Append(dst []byte, side harness.Side, k, j int) []byte {
	if side != harness.Left && side != harness.Right {
		panic(fmt.Sprintf("gen: side %d", side))
	}
	s := &g.sides[side-1]
	if s.c == nil {
		return appendLegacy(dst, g.seed, g.valueSize, side, k, j)
	}
	e := &g.e
	e.id = identity(g.seed, side, k, j)
	e.r = rng{s: mix(e.id)}
	e.enc = s.enc
	e.c = s.c
	e.state = state{}
	e.key = g.keyFormat.append(e.key[:0], g.seed, k)
	e.b = dst
	e.record(s.c.fields, 0)
	dst = e.b
	e.b = nil
	return dst
}

// Value returns the value of key k's j-th record on side. It makes a
// ValueGen for each call: generate many values with one ValueGen.
func (c Config) Value(side harness.Side, k, j int) []byte {
	return c.ValueGen().Append(nil, side, k, j)
}
