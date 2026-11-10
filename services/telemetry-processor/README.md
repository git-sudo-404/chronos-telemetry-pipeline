# Telemetry Processor

Asynchronous post-ingestion consumer for telemetry streams. This service will
decode frames with the telemetry schema package, apply semantic validation
(including speed and GPS jump checks), and route corrupt packets to a dead
letter quarantine tier.
