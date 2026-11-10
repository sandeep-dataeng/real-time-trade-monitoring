# Test plan

**Status: planned; tests have not yet been executed.**

- Valid JSON events parse into expected Spark data types.
- Missing trade ID, symbol or event time is classified as invalid.
- Unsupported side values are rejected.
- Zero/negative quantity and negative price are rejected.
- Duplicate trade IDs follow a documented deduplication policy.
- Late events within the watermark are included as expected.
- Events beyond the watermark follow the documented late-data policy.
- Restarting from checkpoint does not create unintended duplicate output.
- Invalid payloads are persisted to quarantine with a reason.
- Bronze, Silver and Gold counts/aggregates reconcile for a fixed synthetic batch.
