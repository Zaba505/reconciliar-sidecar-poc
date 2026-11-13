// Command gencert writes a test CA and a server certificate for localhost,
// for running the sidecar image by hand.
package main

import (
	"flag"
	"fmt"
	"os"

	"poc/e2e/harness"
)

func main() {
	dir := flag.String("dir", ".local/tls", "directory to write ca.pem, server.pem and server-key.pem into")
	flag.Parse()
	if _, err := harness.WriteCerts(*dir); err != nil {
		fmt.Fprintln(os.Stderr, "gencert:", err)
		os.Exit(1)
	}
}
