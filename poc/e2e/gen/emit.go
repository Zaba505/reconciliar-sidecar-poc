package gen

import (
	"fmt"
	"strings"
)

// ProtoFile returns s as a proto3 .proto file in package gen.<name> with
// the given go_package, importing google/protobuf/timestamp.proto only if
// s has a timestamp. It declares what the protobuf encoder writes:
// fields numbered in order, proto3 optional for optional fields, enums with
// an X_UNSPECIFIED zero, google.protobuf.Timestamp, and Date and Money
// messages shaped like google.type.Date and google.type.Money. s must be
// valid.
func ProtoFile(s *Schema, goPackage string) string {
	var defs []string
	timestamps := false
	defined := map[string]bool{}
	define := func(name, body string) {
		if !defined[name] {
			defined[name] = true
			defs = append(defs, body)
		}
	}
	var message func(name string, fs []Field)
	message = func(name string, fs []Field) {
		var b strings.Builder
		fmt.Fprintf(&b, "message %s {\n", name)
		for i, f := range fs {
			var t string
			switch f.Type {
			case TypeIdentity:
				t = "fixed64"
			case TypeString:
				t = "string"
			case TypeBytes:
				t = "bytes"
			case TypeInt32:
				t = "int32"
			case TypeInt64:
				t = "int64"
			case TypeSInt64:
				t = "sint64"
			case TypeDouble:
				t = "double"
			case TypeBool:
				t = "bool"
			case TypeEnum:
				t = f.TypeName
				p := upperSnake(f.TypeName) + "_"
				var e strings.Builder
				fmt.Fprintf(&e, "enum %s {\n  %sUNSPECIFIED = 0;\n", f.TypeName, p)
				for j, sym := range f.Symbols {
					fmt.Fprintf(&e, "  %s%s = %d;\n", p, sym, j+1)
				}
				e.WriteString("}\n")
				define(f.TypeName, e.String())
			case TypeTimestamp:
				t = "google.protobuf.Timestamp"
				timestamps = true
			case TypeDate:
				t = "Date"
				define("Date", "message Date {\n  int32 year = 1;\n  int32 month = 2;\n  int32 day = 3;\n}\n")
			case TypeMoney:
				t = "Money"
				define("Money", "message Money {\n  string currency_code = 1;\n  int64 units = 2;\n  int32 nanos = 3;\n}\n")
			case TypeRecord:
				message(f.TypeName, f.Fields)
				t = f.TypeName
			}
			label := ""
			switch {
			case f.Repeat != nil:
				label = "repeated "
			case f.Optional > 0:
				label = "optional "
			}
			fmt.Fprintf(&b, "  %s%s %s = %d;\n", label, t, f.Name, i+1)
		}
		b.WriteString("}\n")
		define(name, b.String())
	}
	message(s.Name, s.Fields)
	var b strings.Builder
	fmt.Fprintf(&b, "syntax = \"proto3\";\n\npackage gen.%s;\n\n", strings.ToLower(s.Name))
	if timestamps {
		b.WriteString("import \"google/protobuf/timestamp.proto\";\n\n")
	}
	fmt.Fprintf(&b, "option go_package = %q;\n\n%s", goPackage, strings.Join(defs, "\n"))
	return b.String()
}

// upperSnake turns CamelCase into CAMEL_CASE, for enum value prefixes.
func upperSnake(s string) string {
	var b strings.Builder
	for i, c := range s {
		if i > 0 && c >= 'A' && c <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(c)
	}
	return strings.ToUpper(b.String())
}

// AvroIDL returns s as Avro IDL in namespace. It declares what the Avro
// encoder writes. avroc's IDL parser has no logical types, so timestamps
// are long (ms), dates int (days) and money amounts bytes (a decimal at
// scale 2): the bytes on the wire are the same as with the logical types.
// The identity is fixed Id(8) and optional fields are unions with null
// first. s must be valid.
func AvroIDL(s *Schema, namespace string) string {
	var defs []string
	defined := map[string]bool{}
	define := func(name, body string) {
		if !defined[name] {
			defined[name] = true
			defs = append(defs, body)
		}
	}
	var record func(name string, fs []Field)
	var typ func(f *Field) string
	record = func(name string, fs []Field) {
		var b strings.Builder
		fmt.Fprintf(&b, "record %s {\n", name)
		for i := range fs {
			t := typ(&fs[i])
			switch {
			case fs[i].Repeat != nil:
				t = "array<" + t + ">"
			case fs[i].Optional > 0:
				t = "union { null, " + t + " }"
			}
			fmt.Fprintf(&b, "  %s %s;\n", t, fs[i].Name)
		}
		b.WriteString("}\n")
		define(name, b.String())
	}
	typ = func(f *Field) string {
		switch f.Type {
		case TypeIdentity:
			define("Id", "fixed Id(8);\n")
			return "Id"
		case TypeString:
			return "string"
		case TypeBytes:
			return "bytes"
		case TypeInt32:
			return "int"
		case TypeInt64, TypeSInt64, TypeTimestamp:
			return "long"
		case TypeDouble:
			return "double"
		case TypeBool:
			return "boolean"
		case TypeEnum:
			define(f.TypeName, fmt.Sprintf("enum %s {\n  %s\n}\n", f.TypeName, strings.Join(f.Symbols, ",\n  ")))
			return f.TypeName
		case TypeDate:
			return "int"
		case TypeMoney:
			define("Money", "record Money {\n  string currency_code;\n  bytes amount;\n}\n")
			return "Money"
		case TypeRecord:
			record(f.TypeName, f.Fields)
			return f.TypeName
		}
		panic(fmt.Sprintf("gen: AvroIDL of a %v field", f.Type))
	}
	record(s.Name, s.Fields)
	return fmt.Sprintf("namespace %s;\n\nschema %s;\n\n%s", namespace, s.Name, strings.Join(defs, "\n"))
}
