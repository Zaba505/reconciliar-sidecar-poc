package reference

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	avro "github.com/z5labs/avro-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"poc/e2e/gen"
	avbankline "poc/e2e/gen/reference/avro/bankline"
	avcardsettlement "poc/e2e/gen/reference/avro/cardsettlement"
	avenvelope "poc/e2e/gen/reference/avro/envelope"
	avkitchensink "poc/e2e/gen/reference/avro/kitchensink"
	avpayment "poc/e2e/gen/reference/avro/payment"
	avtiny "poc/e2e/gen/reference/avro/tiny"
	pbbankline "poc/e2e/gen/reference/pb/bankline"
	pbcardsettlement "poc/e2e/gen/reference/pb/cardsettlement"
	pbenvelope "poc/e2e/gen/reference/pb/envelope"
	pbkitchensink "poc/e2e/gen/reference/pb/kitchensink"
	pbpayment "poc/e2e/gen/reference/pb/payment"
	pbtiny "poc/e2e/gen/reference/pb/tiny"
	"poc/e2e/harness"
)

func TestSchemas_are_up_to_date(t *testing.T) {
	want, err := Emit()
	if err != nil {
		t.Fatal(err)
	}
	have, err := filepath.Glob("schema/*")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range have {
		if want[filepath.ToSlash(path)] == nil {
			t.Errorf("%s is no schema's: run make reference", path)
		}
	}
	for _, path := range slices.Sorted(maps.Keys(want)) {
		got, err := os.ReadFile(filepath.FromSlash(path))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			t.Errorf("%s is missing: run make reference", path)
		case err != nil:
			t.Error(err)
		case !bytes.Equal(got, want[path]):
			t.Errorf("%s differs from what the schemas emit: run make reference", path)
		}
	}
}

func TestWrite_repairs_drift(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "schema"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "schema", "old.proto")
	edited := filepath.Join(dir, "schema", "tiny.avdl")
	for _, path := range []string{stale, edited} {
		if err := os.WriteFile(path, []byte("drift"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := Write(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Write left %s: %v", stale, err)
	}
	want, err := Emit()
	if err != nil {
		t.Fatal(err)
	}
	for path, b := range want {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(path)))
		if err != nil || !bytes.Equal(got, b) {
			t.Errorf("%s: %v, or it differs from Emit's", path, err)
		}
	}
}

// avroRecord is a record type avroc-gen-go generated.
type avroRecord interface {
	avro.BinaryMarshaler
	avro.BinaryUnmarshaler
}

// generated holds, for each schema, a value of the message protoc-gen-go
// generated for it and of the record avroc-gen-go generated.
var generated = map[string]struct {
	proto proto.Message
	avro  avroRecord
}{
	"Payment":        {new(pbpayment.Payment), new(avpayment.Payment)},
	"BankLine":       {new(pbbankline.BankLine), new(avbankline.BankLine)},
	"CardSettlement": {new(pbcardsettlement.CardSettlement), new(avcardsettlement.CardSettlement)},
	"Envelope":       {new(pbenvelope.Envelope), new(avenvelope.Envelope)},
	"Tiny":           {new(pbtiny.Tiny), new(avtiny.Tiny)},
	"KitchenSink":    {new(pbkitchensink.KitchenSink), new(avkitchensink.KitchenSink)},
}

// Each schema is checked on at least minValues values, and more until
// they show every case it has (see cases), up to maxValues. Their keys are
// spread over keys, the most decimal keys allow.
const (
	minValues = 4000
	maxValues = 100_000
	keys      = 1_000_000_000_000
)

// TestValues_match_generated_code checks every schema's values in every
// encoding against what generated code makes of them:
//   - protobuf: proto.Unmarshal into the protoc-gen-go message leaves no
//     unknown fields at any depth, and proto.Marshal gives the same bytes;
//   - Avro: UnmarshalAvroBinary into the avroc-gen-go record reads every
//     byte, and MarshalAvroBinary gives the same bytes;
//   - JSON: it is valid, it decodes into the Go struct a client would hold
//     (see jsonType) with no unknown fields, and json.Marshal gives the
//     same bytes.
//
// A round trip accepts any bytes in a field, so the three decoded records
// must also hold the same values, field by field. That checks what the
// round trips can't: that an Avro money amount, two's complement bytes,
// is the decimal the JSON shows; that enum indexes are in range; and that
// each encoding's identity field holds the record's Identity.
//
// The values cover both sides, both key formats, j from 0 to 3 and keys
// spread over 10^12, and they go on until every rare case is seen: each
// optional field absent and present, each list empty, non-empty and long,
// each enum symbol, and each sign of a signed field.
func TestValues_match_generated_code(t *testing.T) {
	for _, s := range Schemas() {
		t.Run(s.Name, func(t *testing.T) {
			ref, ok := generated[s.Name]
			if !ok {
				t.Fatalf("no generated code for %s: run make reference and add it to generated", s.Name)
			}
			var (
				gens [2][3]*gen.ValueGen // by key format and encoding
				cfgs [2]gen.Config       // by key format, for Identity, which ignores the encoding
			)
			for kf := range 2 {
				for enc := range 3 {
					v := gen.Values{Schema: s, Encoding: gen.Encoding(enc)}
					c := gen.Config{Seed: uint64(20 + kf), Keys: keys, Left: v, Right: v, KeyFormat: gen.KeyFormat(kf)}
					if err := c.Validate(); err != nil {
						t.Fatal(err)
					}
					cfgs[kf] = c
					gens[kf][enc] = c.ValueGen()
				}
			}
			jtype := jsonType(s.Fields)
			missing := map[string]bool{}
			for _, c := range cases(s.Fields, "") {
				missing[c] = true
			}
			failed := map[string]int{}
			fail := func(check, what string, err error) {
				if failed[check]++; failed[check] == 1 {
					t.Errorf("%s, %s: %v", check, what, err)
				}
			}

			var buf []byte
			n := 0
			for ; n < minValues || len(missing) > 0 && n < maxValues; n++ {
				// Every 16 values take each key format, side and j; k
				// jumps around all the keys.
				kf, side, j := n/8%2, harness.Side(1+n%2), n/2%4
				k := int(uint64(n) * 0x9e3779b97f4a7c15 % keys)
				what := fmt.Sprintf("%v keys, side %d, k %d, j %d", gen.KeyFormat(kf), side, k, j)
				var leaves [3][]leaf
				for enc, g := range gens[kf] {
					buf = g.Append(buf[:0], side, k, j)
					var err error
					switch gen.Encoding(enc) {
					case gen.Protobuf:
						leaves[enc], err = checkProto(ref.proto.ProtoReflect().Type().New().Interface(), s.Fields, buf)
					case gen.Avro:
						rec := reflect.New(reflect.TypeOf(ref.avro).Elem()).Interface().(avroRecord)
						leaves[enc], err = checkAvro(rec, s.Fields, buf)
					case gen.JSON:
						leaves[enc], err = checkJSON(jtype, s.Fields, buf)
					}
					if err != nil {
						fail(gen.Encoding(enc).String(), what, err)
					}
				}

				id := fmt.Sprintf("%016x", cfgs[kf].Identity(side, k, j))
				base := -1 // the first encoding that decoded
				for enc, ls := range leaves {
					switch {
					case ls == nil:
						continue
					case ls[0].value != id:
						fail("identity", what, fmt.Errorf("%v decodes the identity %s, want %s", gen.Encoding(enc), ls[0].value, id))
					case base < 0:
						base = enc
					default:
						if i := firstDifference(leaves[base], ls); i >= 0 {
							fail("same record", what, fmt.Errorf("%v holds %s, but %v holds %s",
								gen.Encoding(enc), at(ls, i), gen.Encoding(base), at(leaves[base], i)))
						}
					}
				}
				if base >= 0 {
					for _, c := range observe(leaves[base]) {
						delete(missing, c)
					}
				}
			}

			t.Logf("checked %d values", n)
			for _, check := range slices.Sorted(maps.Keys(failed)) {
				if failed[check] > 1 {
					t.Errorf("%s: %d of %d values failed", check, failed[check], n)
				}
			}
			if len(missing) > 0 {
				t.Errorf("%d values never showed: %s", n, strings.Join(slices.Sorted(maps.Keys(missing)), ", "))
			}
		})
	}
}

// checkProto unmarshals v into m, a message of fields fs, and marshals it
// back. It returns m's leaves if v unmarshaled.
func checkProto(m proto.Message, fs []gen.Field, v []byte) ([]leaf, error) {
	if err := proto.Unmarshal(v, m); err != nil {
		return nil, err
	}
	leaves := protoLeaves(nil, m.ProtoReflect(), fs, "")
	if path := unknown(m.ProtoReflect(), string(m.ProtoReflect().Descriptor().Name())); path != "" {
		return leaves, fmt.Errorf("unknown fields in %s", path)
	}
	out, err := proto.Marshal(m)
	if err != nil {
		return leaves, err
	}
	if !bytes.Equal(out, v) {
		return leaves, fmt.Errorf("proto.Marshal gives different bytes: %s", diff(out, v))
	}
	return leaves, nil
}

// unknown returns the path of the first message in m, m included, that
// holds unknown fields, or "" if none does.
func unknown(m protoreflect.Message, path string) string {
	if len(m.GetUnknown()) > 0 {
		return path
	}
	fds := m.Descriptor().Fields()
	for i := range fds.Len() {
		fd := fds.Get(i)
		if fd.Message() == nil || !m.Has(fd) {
			continue
		}
		p := path + "." + string(fd.Name())
		if !fd.IsList() {
			if u := unknown(m.Get(fd).Message(), p); u != "" {
				return u
			}
			continue
		}
		l := m.Get(fd).List()
		for e := range l.Len() {
			if u := unknown(l.Get(e).Message(), fmt.Sprintf("%s[%d]", p, e)); u != "" {
				return u
			}
		}
	}
	return ""
}

// checkAvro unmarshals v into rec, a record of fields fs, and marshals it
// back. It returns rec's leaves if v unmarshaled.
func checkAvro(rec avroRecord, fs []gen.Field, v []byte) ([]leaf, error) {
	r := avro.NewBinaryReader(bytes.NewReader(v))
	if err := rec.UnmarshalAvroBinary(r); err != nil {
		return nil, err
	}
	leaves := avroLeaves(nil, reflect.ValueOf(rec).Elem(), fs, "")
	if r.Offset() != int64(len(v)) {
		return leaves, fmt.Errorf("the record ends at byte %d of %d", r.Offset(), len(v))
	}
	var out bytes.Buffer
	if err := avro.MarshalBinary(&out, rec); err != nil {
		return leaves, err
	}
	if !bytes.Equal(out.Bytes(), v) {
		return leaves, fmt.Errorf("MarshalAvroBinary gives different bytes: %s", diff(out.Bytes(), v))
	}
	return leaves, nil
}

// checkJSON decodes v into a new value of typ, the jsonType of fields fs,
// and marshals it back. It returns the value's leaves if v decoded.
func checkJSON(typ reflect.Type, fs []gen.Field, v []byte) ([]leaf, error) {
	if !json.Valid(v) {
		return nil, fmt.Errorf("invalid JSON %q", v)
	}
	p := reflect.New(typ)
	d := json.NewDecoder(bytes.NewReader(v))
	d.DisallowUnknownFields()
	if err := d.Decode(p.Interface()); err != nil {
		return nil, err
	}
	leaves := jsonLeaves(nil, p.Elem(), fs, "")
	if d.InputOffset() != int64(len(v)) {
		return leaves, fmt.Errorf("the object ends at byte %d of %d", d.InputOffset(), len(v))
	}
	out, err := json.Marshal(p.Interface())
	if err != nil {
		return leaves, err
	}
	if !bytes.Equal(out, v) {
		return leaves, fmt.Errorf("json.Marshal gives different bytes: %s", diff(out, v))
	}
	return leaves, nil
}

// money is how a client's Go struct holds a money amount for JSON.
type money struct {
	CurrencyCode string `json:"currency_code"`
	Amount       string `json:"amount"`
}

// jsonType returns the Go struct a client would marshal records of fs from
// with encoding/json: the fields in order, optional ones as omitempty
// pointers, lists as omitempty slices, the identity, dates and enums as
// strings, timestamps as time.Time and bytes as []byte.
func jsonType(fs []gen.Field) reflect.Type {
	sfs := make([]reflect.StructField, len(fs))
	for i, f := range fs {
		var t reflect.Type
		switch f.Type {
		case gen.TypeIdentity, gen.TypeString, gen.TypeEnum, gen.TypeDate:
			t = reflect.TypeFor[string]()
		case gen.TypeBytes:
			t = reflect.TypeFor[[]byte]()
		case gen.TypeInt32:
			t = reflect.TypeFor[int32]()
		case gen.TypeInt64, gen.TypeSInt64:
			t = reflect.TypeFor[int64]()
		case gen.TypeDouble:
			t = reflect.TypeFor[float64]()
		case gen.TypeBool:
			t = reflect.TypeFor[bool]()
		case gen.TypeTimestamp:
			t = reflect.TypeFor[time.Time]()
		case gen.TypeMoney:
			t = reflect.TypeFor[money]()
		case gen.TypeRecord:
			t = jsonType(f.Fields)
		default:
			panic(fmt.Sprintf("%s: a %v field", f.Name, f.Type))
		}
		tag := f.Name
		switch {
		case f.Repeat != nil:
			t, tag = reflect.SliceOf(t), tag+",omitempty"
		case f.Optional > 0:
			t, tag = reflect.PointerTo(t), tag+",omitempty"
		}
		sfs[i] = reflect.StructField{Name: fmt.Sprintf("F%d", i), Type: t, Tag: reflect.StructTag(fmt.Sprintf("json:%q", tag))}
	}
	return reflect.StructOf(sfs)
}

// A leaf is one value of a decoded record, in a form every encoding
// shares. Its path names the field, like "counterparty.bic", with "[]"
// for an item of a list. A list has a leaf for its length, like "[3]",
// before its items'; a record has one, "{}", before its fields'; and an
// absent optional field has the value "null".
type leaf struct {
	f     *gen.Field
	path  string
	value string
}

// protoLeaves appends the leaves of m, a message of fields fs, to out. Field
// i of fs is field number i+1 of m.
func protoLeaves(out []leaf, m protoreflect.Message, fs []gen.Field, path string) []leaf {
	fds := m.Descriptor().Fields()
	for i := range fs {
		f := &fs[i]
		p := path + f.Name
		fd := fds.ByNumber(protoreflect.FieldNumber(i + 1))
		switch {
		case fd == nil:
			out = append(out, leaf{f, p, "undeclared"})
		case f.Repeat != nil:
			l := m.Get(fd).List()
			out = append(out, leaf{f, p, fmt.Sprintf("[%d]", l.Len())})
			for e := range l.Len() {
				out = protoValue(out, f, l.Get(e), p+"[]")
			}
		case (f.Optional > 0 || fd.Message() != nil) && !m.Has(fd):
			// A message field has presence even if it isn't optional:
			// a client's struct has a non-nil pointer, so the value
			// must hold it, if only as a tag and a length of 0.
			out = append(out, leaf{f, p, "null"})
		default:
			out = protoValue(out, f, m.Get(fd), p)
		}
	}
	return out
}

// protoValue appends the leaves of v, one value of f, at path p.
func protoValue(out []leaf, f *gen.Field, v protoreflect.Value, p string) []leaf {
	// get reads a field of a Timestamp, Date or Money message by name.
	get := func(name protoreflect.Name) protoreflect.Value {
		m := v.Message()
		return m.Get(m.Descriptor().Fields().ByName(name))
	}
	var s string
	switch f.Type {
	case gen.TypeIdentity:
		s = fmt.Sprintf("%016x", v.Uint())
	case gen.TypeString:
		s = v.String()
	case gen.TypeBytes:
		s = fmt.Sprintf("%x", v.Bytes())
	case gen.TypeInt32, gen.TypeInt64, gen.TypeSInt64:
		s = strconv.FormatInt(v.Int(), 10)
	case gen.TypeDouble:
		s = strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case gen.TypeBool:
		s = strconv.FormatBool(v.Bool())
	case gen.TypeEnum:
		s = symbol(f, int(v.Enum())-1)
	case gen.TypeTimestamp:
		sec, nanos := get("seconds").Int(), get("nanos").Int()
		s = strconv.FormatInt(sec*1000+nanos/1e6, 10)
		if nanos%1e6 != 0 {
			s = fmt.Sprintf("%d s %d ns", sec, nanos)
		}
	case gen.TypeDate:
		s = fmt.Sprintf("%04d-%02d-%02d", get("year").Int(), get("month").Int(), get("day").Int())
	case gen.TypeMoney:
		cur, units, nanos := get("currency_code").String(), get("units").Int(), get("nanos").Int()
		if nanos%1e7 != 0 || units < 0 && nanos > 0 || units > 0 && nanos < 0 {
			s = fmt.Sprintf("%s with %d units and %d nanos", cur, units, nanos)
			break
		}
		minor := new(big.Int).Mul(big.NewInt(units), big.NewInt(100))
		s = cur + " " + decimal(minor.Add(minor, big.NewInt(nanos/1e7)))
	case gen.TypeRecord:
		return protoLeaves(append(out, leaf{f, p, "{}"}), v.Message(), f.Fields, p+".")
	}
	return append(out, leaf{f, p, s})
}

// avroLeaves appends the leaves of v, a record avroc-gen-go generated for
// fs, to out: its fields are fs's, in order.
func avroLeaves(out []leaf, v reflect.Value, fs []gen.Field, path string) []leaf {
	for i := range fs {
		f := &fs[i]
		p := path + f.Name
		fv := v.Field(i)
		switch {
		case f.Repeat != nil:
			out = append(out, leaf{f, p, fmt.Sprintf("[%d]", fv.Len())})
			for e := range fv.Len() {
				out = avroValue(out, f, fv.Index(e), p+"[]")
			}
		case f.Optional > 0:
			// A union { null, T } is an interface holding a struct with no
			// fields for null, or with the value in its one field.
			switch u := fv.Elem(); {
			case !u.IsValid():
				out = append(out, leaf{f, p, "no union"})
			case u.NumField() == 0:
				out = append(out, leaf{f, p, "null"})
			default:
				out = avroValue(out, f, u.Field(0), p)
			}
		default:
			out = avroValue(out, f, fv, p)
		}
	}
	return out
}

// avroValue appends the leaves of v, one value of f, at path p.
func avroValue(out []leaf, f *gen.Field, v reflect.Value, p string) []leaf {
	var s string
	switch f.Type {
	case gen.TypeIdentity:
		var id [8]byte
		reflect.Copy(reflect.ValueOf(id[:]), v)
		s = fmt.Sprintf("%016x", binary.LittleEndian.Uint64(id[:]))
	case gen.TypeString:
		s = v.String()
	case gen.TypeBytes:
		s = fmt.Sprintf("%x", v.Bytes())
	case gen.TypeInt32, gen.TypeInt64, gen.TypeSInt64, gen.TypeTimestamp:
		s = strconv.FormatInt(v.Int(), 10)
	case gen.TypeDouble:
		s = strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case gen.TypeBool:
		s = strconv.FormatBool(v.Bool())
	case gen.TypeEnum:
		s = symbol(f, int(v.Int()))
	case gen.TypeDate:
		s = time.UnixMilli(v.Int() * 86_400_000).UTC().Format(time.DateOnly)
	case gen.TypeMoney:
		cur, amount := v.FieldByName("CurrencyCode").String(), v.FieldByName("Amount").Bytes()
		if n, err := twosComplement(amount); err != nil {
			s = fmt.Sprintf("%s with the amount %x: %v", cur, amount, err)
		} else {
			s = cur + " " + decimal(n)
		}
	case gen.TypeRecord:
		return avroLeaves(append(out, leaf{f, p, "{}"}), v, f.Fields, p+".")
	}
	return append(out, leaf{f, p, s})
}

// jsonLeaves appends the leaves of v, a value of jsonType(fs), to out.
func jsonLeaves(out []leaf, v reflect.Value, fs []gen.Field, path string) []leaf {
	for i := range fs {
		f := &fs[i]
		p := path + f.Name
		fv := v.Field(i)
		switch {
		case f.Repeat != nil:
			out = append(out, leaf{f, p, fmt.Sprintf("[%d]", fv.Len())})
			for e := range fv.Len() {
				out = jsonValue(out, f, fv.Index(e), p+"[]")
			}
		case f.Optional > 0 && fv.IsNil():
			out = append(out, leaf{f, p, "null"})
		case f.Optional > 0:
			out = jsonValue(out, f, fv.Elem(), p)
		default:
			out = jsonValue(out, f, fv, p)
		}
	}
	return out
}

// jsonValue appends the leaves of v, one value of f, at path p.
func jsonValue(out []leaf, f *gen.Field, v reflect.Value, p string) []leaf {
	var s string
	switch f.Type {
	case gen.TypeIdentity, gen.TypeString, gen.TypeDate:
		s = v.String()
	case gen.TypeBytes:
		s = fmt.Sprintf("%x", v.Bytes())
	case gen.TypeInt32, gen.TypeInt64, gen.TypeSInt64:
		s = strconv.FormatInt(v.Int(), 10)
	case gen.TypeDouble:
		s = strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case gen.TypeBool:
		s = strconv.FormatBool(v.Bool())
	case gen.TypeEnum:
		s = symbol(f, slices.Index(f.Symbols, v.String()))
	case gen.TypeTimestamp:
		ts := v.Interface().(time.Time)
		s = strconv.FormatInt(ts.UnixMilli(), 10)
		if ts.Nanosecond()%1e6 != 0 {
			s = ts.Format(time.RFC3339Nano)
		}
	case gen.TypeMoney:
		m := v.Interface().(money)
		s = m.CurrencyCode + " " + m.Amount
	case gen.TypeRecord:
		return jsonLeaves(append(out, leaf{f, p, "{}"}), v, f.Fields, p+".")
	}
	return append(out, leaf{f, p, s})
}

// symbol returns enum f's i-th symbol, or says i is out of range.
func symbol(f *gen.Field, i int) string {
	if i < 0 || i >= len(f.Symbols) {
		return fmt.Sprintf("index %d of %d symbols", i, len(f.Symbols))
	}
	return f.Symbols[i]
}

// twosComplement decodes an Avro decimal's unscaled value: big-endian two's
// complement in as few bytes as hold it, as Java's BigInteger.toByteArray
// writes it.
func twosComplement(b []byte) (*big.Int, error) {
	switch {
	case len(b) == 0:
		return nil, errors.New("no bytes")
	case len(b) > 1 && (b[0] == 0 && b[1] < 0x80 || b[0] == 0xff && b[1] >= 0x80):
		return nil, errors.New("a redundant sign byte")
	}
	n := new(big.Int).SetBytes(b)
	if b[0] >= 0x80 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return n, nil
}

// decimal formats n hundredths as the JSON encoding writes an amount, like
// "-0.05".
func decimal(n *big.Int) string {
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

// cases returns the rare cases a schema's values must show between them,
// so that each path through the encoders is checked: each optional field
// absent and present; each list empty, non-empty and, if it may be, 64
// items long, for a two-byte Avro block count; each enum symbol; each
// sign of a signed field; and a double of 10^21 or more, which JSON writes
// with an exponent, if one is at most 4 sigmas out.
func cases(fs []gen.Field, path string) []string {
	var out []string
	for i := range fs {
		f := &fs[i]
		p := path + f.Name
		switch {
		case f.Repeat != nil:
			if f.Optional > 0 || f.Repeat.Min == 0 {
				out = append(out, p+" empty")
			}
			out = append(out, p+" non-empty")
			if f.Repeat.Max >= 64 {
				out = append(out, p+" 64+ items")
			}
			p += "[]"
		case f.Optional > 0:
			out = append(out, p+" absent", p+" present")
		}
		if f.Signed {
			out = append(out, p+" negative", p+" positive")
		}
		if f.Type == gen.TypeEnum {
			for _, sym := range f.Symbols {
				out = append(out, p+" "+sym)
			}
		}
		if f.Type == gen.TypeDouble && f.Median*math.Exp(4*f.Sigma) >= 1e21 {
			out = append(out, p+" 1e21+")
		}
		if f.Type == gen.TypeRecord {
			out = append(out, cases(f.Fields, p+".")...)
		}
	}
	return out
}

// observe returns the cases leaves show, named as cases names them.
func observe(leaves []leaf) []string {
	var out []string
	for _, l := range leaves {
		f, p, v := l.f, l.path, l.value
		if f.Repeat != nil && !strings.HasSuffix(p, "[]") {
			var n int
			fmt.Sscanf(v, "[%d]", &n)
			switch {
			case n == 0:
				out = append(out, p+" empty")
			case n >= 64:
				out = append(out, p+" non-empty", p+" 64+ items")
			default:
				out = append(out, p+" non-empty")
			}
			continue
		}
		if v == "null" {
			out = append(out, p+" absent")
			continue
		}
		out = append(out, p+" present")
		if f.Type == gen.TypeEnum {
			out = append(out, p+" "+v)
		}
		if x, err := strconv.ParseFloat(v, 64); f.Type == gen.TypeDouble && err == nil && x >= 1e21 {
			out = append(out, p+" 1e21+")
		}
		if f.Signed {
			_, num, _ := strings.Cut(v, " ") // a money amount follows its currency
			if num == "" {
				num = v
			}
			if strings.HasPrefix(num, "-") {
				out = append(out, p+" negative")
			} else if strings.Trim(num, "0.") != "" {
				out = append(out, p+" positive")
			}
		}
	}
	return out
}

// firstDifference returns the index of the first leaf where a and b
// differ, or -1 if they don't.
func firstDifference(a, b []leaf) int {
	for i := range max(len(a), len(b)) {
		if i >= len(a) || i >= len(b) || a[i].path != b[i].path || a[i].value != b[i].value {
			return i
		}
	}
	return -1
}

// at describes leaf i of ls.
func at(ls []leaf, i int) string {
	if i >= len(ls) {
		return "nothing more"
	}
	s := ls[i].path + " = " + ls[i].value
	if len(s) > 120 {
		s = s[:120] + "..."
	}
	return s
}

// diff describes where got first differs from want.
func diff(got, want []byte) string {
	i := 0
	for i < min(len(got), len(want)) && got[i] == want[i] {
		i++
	}
	lo := max(0, i-24)
	return fmt.Sprintf("byte %d differs (%d bytes, want %d):\n got ...%q...\nwant ...%q...",
		i, len(got), len(want), got[lo:min(len(got), i+40)], want[lo:min(len(want), i+40)])
}
