// Command recon-sidecar is the reconciliation join sidecar.
package main

import (
	"github.com/z5labs/humus"

	"poc/internal/app"
)

func main() {
	humus.Main(app.Init, humus.WithTelemetry(app.Telemetry))
}
