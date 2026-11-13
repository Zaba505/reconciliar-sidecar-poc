//go:build e2e

// Package e2e tests the recon-sidecar image as a running container.
package e2e

import (
	"os"
	"testing"

	"poc/e2e/harness"
)

func TestMain(m *testing.M) {
	os.Exit(harness.Main(m))
}
