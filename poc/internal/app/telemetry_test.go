package app

import (
	"slices"
	"testing"

	"github.com/z5labs/bedrock"
	"github.com/z5labs/humus/telemetry"
	"go.opentelemetry.io/otel/log"
)

func TestTelemetrySettings(t *testing.T) {
	cases := []struct {
		env     map[string]string
		metrics telemetry.Exporter
		level   log.Severity
	}{
		{nil, telemetry.ExporterNone, log.SeverityInfo},
		{map[string]string{"RECON_METRICS_EXPORTER": "console", "RECON_LOG_LEVEL": "debug"}, telemetry.ExporterConsole, log.SeverityDebug},
		{map[string]string{"RECON_METRICS_EXPORTER": "otlp", "RECON_LOG_LEVEL": "warn"}, telemetry.ExporterOTLP, log.SeverityWarn},
		{map[string]string{"RECON_LOG_LEVEL": "error"}, telemetry.ExporterNone, log.SeverityError},
	}
	for _, c := range cases {
		s, err := telemetrySettings(t.Context(), bedrock.NewStaticEnvironment(c.env))
		if err != nil {
			t.Errorf("%v: %v", c.env, err)
			continue
		}
		if s.ServiceName != "recon-sidecar" || s.Metrics != c.metrics || s.LogLevel != c.level || s.Logs != telemetry.ExporterConsole {
			t.Errorf("%v: service %q, metrics %q, level %v, logs %q; want recon-sidecar, %q, %v, console",
				c.env, s.ServiceName, s.Metrics, s.LogLevel, s.Logs, c.metrics, c.level)
		}
	}
}

func TestTelemetrySettings_rejects_a_bad_value(t *testing.T) {
	for name, value := range map[string]string{
		"RECON_METRICS_EXPORTER": "prometheus",
		"RECON_LOG_LEVEL":        "verbose",
	} {
		_, err := telemetrySettings(t.Context(), bedrock.NewStaticEnvironment(map[string]string{name: value}))
		if got := failedNames(err); !slices.Equal(got, []string{name}) {
			t.Errorf("%s=%s: failed variables = %q, want [%s]: %v", name, value, got, name, err)
		}
	}
}
