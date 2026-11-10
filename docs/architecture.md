# Architecture and engineering notes

## Data flow
1. A producer publishes JSON trade events to Azure Event Hubs.
2. Databricks Structured Streaming reads the event stream.
3. Bronze persists raw payloads and Event Hubs metadata in Delta on ADLS Gen2.
4. Silver parses the contract, validates fields, and deduplicates using a documented business key.
5. Gold computes event-time aggregates such as trade count and notional by symbol and five-minute window.

## Reliability
- Use a stable checkpoint path per streaming query.
- Define event-time semantics and watermark duration from business SLAs and observed lateness.
- Quarantine invalid payloads with a reason.
- Test restart/replay behavior before claiming end-to-end exactly-once processing.

## Security
Use managed identity where supported or secret scopes. Apply least-privilege access to Event Hubs and ADLS. Never store secrets in source control.

## Operational metrics
Track input/processed rows per second, micro-batch duration, processing latency, state-store size, failed records and checkpoint progress.

## Decisions to validate
Connector/runtime compatibility, Event Hubs partition count, storage layout, watermark duration, checkpoint retention, DLT versus standard streaming, and alert thresholds.
