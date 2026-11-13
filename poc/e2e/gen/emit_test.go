package gen

import (
	"strings"
	"testing"
)

// protoc warns of an unused import, and protoc-gen-go then imports
// timestamppb for nothing.
func TestProtoFile_imports_timestamp_only_when_used(t *testing.T) {
	const imp = `import "google/protobuf/timestamp.proto";`
	for _, s := range Profiles() {
		uses := strings.Contains(ProtoFile(s, "x"), "google.protobuf.Timestamp ")
		if got := strings.Contains(ProtoFile(s, "x"), imp); got != uses {
			t.Errorf("%s: imports timestamp.proto: %v, uses a timestamp: %v", s.Name, got, uses)
		}
	}
	nested := &Schema{Name: "N", Fields: []Field{{Name: "id", Type: TypeIdentity},
		{Name: "r", Type: TypeRecord, TypeName: "R", Fields: []Field{{Name: "at", Type: TypeTimestamp, Window: 1, Repeat: &Dist{Max: 2}}}}}}
	if !strings.Contains(ProtoFile(nested, "x"), imp) {
		t.Error("a timestamp list in a nested record doesn't import timestamp.proto")
	}
	if strings.Contains(ProtoFile(Tiny, "x"), imp) {
		t.Error("Tiny, which has no timestamp, imports timestamp.proto")
	}
}
