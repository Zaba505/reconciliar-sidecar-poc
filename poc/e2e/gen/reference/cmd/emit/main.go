// Command emit writes the reference schemas' .proto and .avdl files and
// avroc.json into package reference's directory, for make reference to
// generate code from. It removes schema files no schema emits any more.
//
//	go run ./e2e/gen/reference/cmd/emit -dir e2e/gen/reference
package main

import (
	"flag"
	"fmt"
	"os"

	"poc/e2e/gen/reference"
)

func main() {
	dir := flag.String("dir", ".", "package reference's directory")
	flag.Parse()
	if err := reference.Write(*dir); err != nil {
		fmt.Fprintln(os.Stderr, "emit:", err)
		os.Exit(1)
	}
}
