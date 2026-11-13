package app

import (
	"context"

	"github.com/z5labs/bedrock"
	"github.com/z5labs/bedrock/config"
	"github.com/z5labs/humus/telemetry"
	"go.opentelemetry.io/otel/log"
)

// serviceName is the service.name of the sidecar's telemetry.
const serviceName = "recon-sidecar"

// logLevels maps RECON_LOG_LEVEL's values onto OpenTelemetry severities.
var logLevels = map[string]log.Severity{
	"debug": log.SeverityDebug,
	"info":  log.SeverityInfo,
	"warn":  log.SeverityWarn,
	"error": log.SeverityError,
}

// Telemetry is the sidecar's telemetry.Builder. Logs go to stdout as JSON.
// RECON_METRICS_EXPORTER sets where metrics go, and RECON_LOG_LEVEL the
// lowest severity logged.
func Telemetry(ctx context.Context, p bedrock.Process, dial telemetry.Dialer) (telemetry.SDK, error) {
	s, err := telemetrySettings(ctx, p.Environment)
	if err != nil {
		return telemetry.SDK{}, err
	}
	return telemetry.New(ctx, s, dial, p.Stdout)
}

// telemetrySettings reads the telemetry variables over telemetry's defaults.
func telemetrySettings(ctx context.Context, env bedrock.Environment) (telemetry.Settings, error) {
	type vars struct {
		metrics telemetry.Exporter
		level   log.Severity
	}
	v, err := config.Struct(func(r *config.Resolver) vars {
		return vars{
			metrics: r.Get(config.Env(env, "RECON_METRICS_EXPORTER").
				Map(func(s string) telemetry.Exporter { return telemetry.Exporter(s) }).
				Default(telemetry.ExporterNone).
				Validate(telemetry.CheckExporter)),
			level: r.Get(config.Env(env, "RECON_LOG_LEVEL").
				Parse(config.OneOf("debug", "info", "warn", "error")).
				Map(func(s string) log.Severity { return logLevels[s] }).
				Default(log.SeverityInfo)),
		}
	}).Read(ctx)
	if err != nil {
		return telemetry.Settings{}, err
	}
	s := telemetry.DefaultSettings()
	s.ServiceName = serviceName
	s.Metrics = v.metrics
	s.LogLevel = v.level
	return s, nil
}
