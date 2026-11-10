# Architecture and engineering notes

## Data flow
1. Event producer publishes JSON trade events to Azure Event Hubs.
2. Databricks Structured Streaming reads the event stream.
3. Bronze persists raw payload and Event Hubs metadata in Delta.
4. Silver parses the event contract, applies data-quality rules and deduplicates according to a defined business key.
5. Gold computes event-time aggregates, such as trade count and notional by symbol and five-minute window.

## Reliability considerations
- Use a stable checkpoint path per streaming query.
- Define event-time semantics and watermark duration from observed lateness and business SLAs.
- Specify deduplication keys and replay behavior.
- Quarantine invalid payloads with a reason rather than silently dropping them.
- Test restart/replay behavior before claiming exactly-once outcomes across the full pipeline.

## Security
Use managed identity where supported or Databricks secret scopes for credentials. Apply least-privilege access to Event Hubs and ADLS. Keep secrets out of notebooks, logs and Git.

## Operational metrics
Track input rows/sec, processed rows/sec, micro-batch duration, processing latency, state-store size, failed records and checkpoint progress.

## Decisions to validate
Event Hubs connector/runtime compatibility, partition count, storage layout, watermark duration, checkpoint retention, DLT versus standard streaming, and alert thresholds.
