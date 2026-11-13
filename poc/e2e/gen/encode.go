package gen

import (
	"math"
	"strconv"
	"time"
)

// day is a day in milliseconds.
const day = 86400000

// base is the start of every timestamp window: 2026-10-01 UTC, in ms.
var base = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// state is what a record's later fields draw from its earlier ones: a date
// follows the timestamp, a fee the amount and its currency.
type state struct {
	ts     int64
	amount int64
	cur    []byte
}

// encoder draws one record and appends it to b in one encoding. Every
// encoding draws the same numbers in the same order, so a record's content
// doesn't depend on its encoding.
type encoder struct {
	enc Encoding
	c   *compiled
	r   rng
	id  uint64
	key []byte
	state
	b    []byte
	nest [][]byte // protobuf: a buffer per depth of length-delimited values
	tmp  []byte   // string content being drawn
	raw  []byte   // bytes being drawn, for JSON's base64
}

// mode is how a protobuf value is written: a singular field with implicit
// presence leaves out its zero value, proto3 optional writes it, an element
// of a packed list has no tag, and an element of another list always has
// one.
type mode int

const (
	implicit mode = iota
	explicit
	packedElem
	listElem
)

// packable reports whether protobuf packs a list of t.
func packable(t Type) bool {
	switch t {
	case TypeInt32, TypeInt64, TypeSInt64, TypeDouble, TypeBool, TypeEnum:
		return true
	}
	return false
}

func (e *encoder) record(fs []cfield, depth int) {
	if e.enc == JSON {
		e.b = append(e.b, '{')
	}
	first := true
	for i := range fs {
		f := &fs[i]
		num := i + 1
		present := !e.r.chance(f.Optional)
		if f.Repeat != nil {
			n := 0
			if present {
				n = e.r.length(*f.Repeat)
			}
			e.list(f, num, n, depth, &first)
			continue
		}
		switch e.enc {
		case Protobuf:
			if !present {
				continue
			}
		case Avro:
			if f.Optional > 0 {
				if !present {
					e.b = append(e.b, 0) // union index 0: null
					continue
				}
				e.b = append(e.b, 2) // union index 1, zigzag
			}
		case JSON:
			if !present {
				continue
			}
			e.jsonField(f, &first)
		}
		m := implicit
		if f.Optional > 0 {
			m = explicit
		}
		e.value(f, num, depth, m)
	}
	if e.enc == JSON {
		e.b = append(e.b, '}')
	}
}

func (e *encoder) list(f *cfield, num, n, depth int, first *bool) {
	switch e.enc {
	case Protobuf:
		if packable(f.Type) {
			parent := e.open(depth)
			for range n {
				e.value(f, num, depth+1, packedElem)
			}
			e.close(depth, parent, num, true)
			return
		}
		for range n {
			e.value(f, num, depth, listElem)
		}
	case Avro:
		if n > 0 {
			e.uv(zigzag(int64(n)))
			for range n {
				e.value(f, num, depth, listElem)
			}
		}
		e.b = append(e.b, 0)
	case JSON:
		if n == 0 {
			return
		}
		e.jsonField(f, first)
		e.b = append(e.b, '[')
		for i := range n {
			if i > 0 {
				e.b = append(e.b, ',')
			}
			e.value(f, num, depth, listElem)
		}
		e.b = append(e.b, ']')
	}
}

// value draws one value of f and appends it.
func (e *encoder) value(f *cfield, num, depth int, m mode) {
	r := &e.r
	switch f.Type {
	case TypeIdentity:
		e.identity(num)
	case TypeString:
		e.str(num, m, e.text(f))
	case TypeBytes:
		e.bytes(num, m, r.length(f.Len))
	case TypeInt32, TypeInt64:
		var v int64
		if f.Ints != nil {
			v = f.Ints[f.pick.pick(r)]
		} else {
			v = f.Min + int64(r.uint64n(uint64(f.Max-f.Min)+1))
		}
		e.int(f.Type, num, m, v)
	case TypeSInt64:
		v := toInt(r.lognormal(f.Median, f.Sigma))
		if f.Signed && r.intn(2) == 0 {
			v = -v
		}
		e.int(f.Type, num, m, v)
	case TypeBool:
		var v int64
		if r.chance(0.1) {
			v = 1
		}
		e.int(f.Type, num, m, v)
	case TypeEnum:
		v := f.pick.pick(r)
		if e.enc == JSON {
			e.jstr(f.syms[v])
			return
		}
		e.int(f.Type, num, m, int64(v))
	case TypeDouble:
		e.double(num, m, math.Round(r.lognormal(f.Median, f.Sigma)*1e6)/1e6)
	case TypeTimestamp:
		ms := base + int64(r.intn(int(f.Window)))
		e.ts = ms
		e.timestamp(num, depth, ms)
	case TypeDate:
		ts := e.ts
		if ts == 0 {
			ts = base + int64(r.intn(30*day))
		}
		e.date(num, depth, ts/day+int64(r.intn(f.Lag+1)))
	case TypeMoney:
		var minor int64
		if f.Fee {
			// A fee is 0.1% to 3% of the amount, in two steps so that an
			// amount of up to 2^62 doesn't overflow.
			bp := int64(10 + r.intn(290))
			minor = e.amount/10000*bp + e.amount%10000*bp/10000
		} else {
			e.cur = currencies[currencyPick.pick(r)]
			minor = toInt(r.lognormal(f.Median, f.Sigma))
			if f.Signed && r.intn(2) == 0 {
				minor = -minor
			}
			e.amount = minor
		}
		e.money(num, depth, e.cur, minor)
	case TypeRecord:
		if e.enc == Protobuf {
			parent := e.open(depth)
			e.record(f.fields, depth+1)
			e.close(depth, parent, num, false)
			return
		}
		e.record(f.fields, depth+1)
	}
}

// text draws a string field's content.
func (e *encoder) text(f *cfield) []byte {
	r := &e.r
	switch f.Source {
	case SourceKey:
		return e.key
	case SourceDict:
		return f.dict[f.pick.pick(r)]
	case SourceChoice:
		return f.syms[f.pick.pick(r)]
	case SourceToken:
		b := e.tmp[:0]
		for range r.length(f.Len) {
			b = append(b, f.Alphabet[r.intn(len(f.Alphabet))])
		}
		e.tmp = b
		return b
	case SourceText:
		n := r.length(f.Len)
		b := e.tmp[:0]
		for w := range n {
			if w > 0 {
				b = append(b, ' ')
			}
			if r.chance(0.12) {
				b = append(b, "INV-"...)
				b = strconv.AppendInt(b, int64(r.intn(10_000_000)), 10)
			} else {
				b = append(b, e.c.words[e.c.wpick.pick(r)]...)
			}
			if len(b) >= f.Len.Max {
				b = b[:f.Len.Max]
				break
			}
		}
		e.tmp = b
		return b
	}
	return nil
}

// randomBytes appends n random bytes to b, 8 from each draw.
func (e *encoder) randomBytes(b []byte, n int) []byte {
	for o := 0; o < n; o += 8 {
		x := e.r.next()
		for i := range min(8, n-o) {
			b = append(b, byte(x>>(8*i)))
		}
	}
	return b
}

// zigzag maps signed to unsigned integers as protobuf's sint64 and Avro's
// int and long do.
func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

// The rest dispatch a drawn value to its encoding.

func (e *encoder) identity(num int) {
	switch e.enc {
	case Protobuf:
		e.protoIdentity(num)
	case Avro:
		e.avroIdentity()
	case JSON:
		e.jsonIdentity()
	}
}

func (e *encoder) str(num int, m mode, s []byte) {
	switch e.enc {
	case Protobuf:
		e.protoBytes(num, m, s)
	case Avro:
		e.avroBytes(s)
	case JSON:
		e.jstr(s)
	}
}

// bytes draws n random bytes and appends them. Protobuf and Avro write
// them in place after their length; JSON needs them whole for base64.
func (e *encoder) bytes(num int, m mode, n int) {
	switch e.enc {
	case Protobuf:
		if n == 0 && m == implicit {
			return
		}
		e.tag(num, wireBytes)
		e.uv(uint64(n))
		e.b = e.randomBytes(e.b, n)
	case Avro:
		e.uv(zigzag(int64(n)))
		e.b = e.randomBytes(e.b, n)
	case JSON:
		e.raw = e.randomBytes(e.raw[:0], n)
		e.jsonBase64(e.raw)
	}
}

// int appends an integer, bool or enum index of type t.
func (e *encoder) int(t Type, num int, m mode, v int64) {
	switch e.enc {
	case Protobuf:
		e.protoInt(t, num, m, v)
	case Avro:
		e.avroInt(t, v)
	case JSON:
		e.jsonInt(t, v)
	}
}

func (e *encoder) double(num int, m mode, x float64) {
	switch e.enc {
	case Protobuf:
		e.protoDouble(num, m, x)
	case Avro:
		e.avroDouble(x)
	case JSON:
		e.jsonDouble(x)
	}
}

func (e *encoder) timestamp(num, depth int, ms int64) {
	switch e.enc {
	case Protobuf:
		e.protoTimestamp(num, depth, ms)
	case Avro:
		e.uv(zigzag(ms))
	case JSON:
		e.jsonTimestamp(ms)
	}
}

// date appends a date of days since the epoch.
func (e *encoder) date(num, depth int, days int64) {
	switch e.enc {
	case Protobuf:
		e.protoDate(num, depth, days)
	case Avro:
		e.uv(zigzag(days))
	case JSON:
		e.jsonDate(days)
	}
}

// money appends an amount of minor units (cents) in currency cur.
func (e *encoder) money(num, depth int, cur []byte, minor int64) {
	switch e.enc {
	case Protobuf:
		e.protoMoney(num, depth, cur, minor)
	case Avro:
		e.avroMoney(cur, minor)
	case JSON:
		e.jsonMoney(cur, minor)
	}
}
