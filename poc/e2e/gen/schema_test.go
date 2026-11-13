package gen

import (
	"strings"
	"testing"
)

func TestValidate_rejects_names_generated_code_would_clash_on(t *testing.T) {
	id := Field{Name: "id", Type: TypeIdentity}
	enum := func(name, typeName string, symbols ...string) Field {
		return Field{Name: name, Type: TypeEnum, TypeName: typeName, Symbols: symbols}
	}
	record := func(name, typeName string) Field {
		return Field{Name: name, Type: TypeRecord, TypeName: typeName, Fields: []Field{{Name: "b", Type: TypeBool}}}
	}
	tests := map[string]struct {
		fields []Field
		want   string // in the error
	}{
		// protoc: the JSON names of foo_bar and fooBar are both fooBar.
		"same JSON name": {[]Field{{Name: "foo_bar", Type: TypeBool}, {Name: "fooBar", Type: TypeBool}}, "fields foo_bar and fooBar"},
		// Go generators: both fields are named Foo.
		"same Go name": {[]Field{{Name: "Foo", Type: TypeBool}, {Name: "foo", Type: TypeBool}}, "fields Foo and foo"},
		// Go generators: both types are named FooBar.
		"same Go type name": {[]Field{enum("x", "FooBar", "A"), record("y", "Foo_Bar")}, "Foo_Bar clashes with FooBar"},
		"reserved, folded":  {[]Field{record("x", "MONEY")}, "MONEY clashes with Money"},
		// protoc: enum values are the package's, so A's B_C and AB's C
		// are both A_B_C.
		"same enum value": {[]Field{enum("x", "A", "B_C"), enum("y", "AB", "C")}, "A_B_C is defined twice"},
		// protoc: A's B_UNSPECIFIED is AB's zero value.
		"same zero value": {[]Field{enum("x", "A", "B_UNSPECIFIED"), enum("y", "AB", "X")}, "A_B_UNSPECIFIED is defined twice"},
		"value as a type": {[]Field{enum("x", "E", "R"), record("y", "E_R")}, "E_R is defined twice"},
	}
	for name, tt := range tests {
		s := &Schema{Name: "S", Fields: append([]Field{id}, tt.fields...)}
		if err := s.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: Validate gives %v, want an error with %q", name, err, tt.want)
		}
	}
	ok := &Schema{Name: "S", Fields: []Field{id, enum("x", "A", "B"), enum("y", "AB", "C"), record("z", "Fee"), record("w", "FeeKind")}}
	if err := ok.Validate(); err != nil {
		t.Errorf("Validate rejected names that don't clash: %v", err)
	}
}
