// Package reference exists only to hold reference code, the tests that use
// it, and the schemas it is generated from. Its tests check that package
// gen marshals values exactly as code generated from its own schemas
// would.
//
// schema holds the .proto and .avdl of each profile and of a test schema
// with every type in every form, emitted by gen.ProtoFile and
// gen.AvroIDL. pb holds what protoc-gen-go generates from the .proto
// files, and avro what avroc-gen-go generates from the .avdl files, as
// avroc.json directs. Run make reference to regenerate all of it after
// changing a profile or the emitters: cmd/emit writes schema and
// avroc.json from Emit, which doesn't need the generated code to compile,
// and then protoc and avroc generate pb and avro.
package reference
