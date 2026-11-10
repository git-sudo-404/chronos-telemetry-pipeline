module github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator

go 1.25.3

require (
	github.com/git-sudo-404/chronos-telemetry-pipeline/services/telemetry-schemas v0.0.0
	google.golang.org/protobuf v1.36.12
)

replace github.com/git-sudo-404/chronos-telemetry-pipeline/services/telemetry-schemas => ../telemetry-schemas
