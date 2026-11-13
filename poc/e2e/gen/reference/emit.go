package reference

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"poc/e2e/gen"
)

// Schemas returns the schemas the reference code is generated from: the
// profiles, then a test schema with every type in every form.
func Schemas() []*gen.Schema {
	return append(gen.Profiles(), kitchenSink)
}

// Emit returns the files make reference emits from Schemas, by
// slash-separated path in this directory: each schema's .proto and .avdl
// in schema/, and avroc.json, the manifest that has avroc turn each .avdl
// into a Go package in avro/.
func Emit() (map[string][]byte, error) {
	type generator struct {
		Name    string            `json:"name"`
		Out     string            `json:"out"`
		Inputs  []string          `json:"inputs"`
		Options map[string]string `json:"options"`
	}
	var manifest struct {
		Generators []generator `json:"generators"`
	}
	files := map[string][]byte{}
	for _, s := range Schemas() {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		name := strings.ToLower(s.Name)
		files["schema/"+name+".proto"] = []byte(gen.ProtoFile(s, "poc/e2e/gen/reference/pb/"+name))
		files["schema/"+name+".avdl"] = []byte(gen.AvroIDL(s, "gen."+name))
		manifest.Generators = append(manifest.Generators, generator{
			Name:    "go",
			Out:     "avro/" + name,
			Inputs:  []string{"schema/" + name + ".avdl"},
			Options: map[string]string{"package_name": name},
		})
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	files["avroc.json"] = append(b, '\n')
	return files, nil
}

// Write writes the files Emit returns into dir, the directory of this
// package, and removes the files in dir/schema that Emit no longer
// returns. It doesn't import the generated code, so it works when that
// code is missing or doesn't compile.
func Write(dir string) error {
	files, err := Emit()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "schema"), 0o755); err != nil {
		return err
	}
	have, err := filepath.Glob(filepath.Join(dir, "schema", "*"))
	if err != nil {
		return err
	}
	for _, path := range have {
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if files[filepath.ToSlash(rel)] == nil {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	for path, b := range files {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(path)), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}
