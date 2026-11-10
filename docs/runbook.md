# Local and cloud execution runbook

## 1. Generate synthetic events
Install the producer requirements and configure the two producer environment variables. Run `python producer/trade_event_producer.py`. This sends generated JSON events to your own Event Hub.

## 2. Start Bronze
In a Databricks notebook or job, install a connector version compatible with the cluster runtime, configure the required environment/secrets and run `src/stream_to_bronze.py`.

## 3. Start Silver and Gold
Configure `SILVER_PATH`, `QUARANTINE_PATH`, `GOLD_PATH`, and `CHECKPOINT_ROOT`. Run `src/run_silver_gold.py` after Bronze is available.

## 4. Validate
- Confirm raw payloads appear in Bronze.
- Check accepted and quarantined records in Silver/quarantine.
- Compare Gold trade counts and notional against the synthetic event set.
- Stop and restart streams to verify checkpoint recovery.
- Introduce malformed and late events to validate error paths.

## Important implementation notes
- The scripts are starter templates; validate Spark/Delta versions, module import paths, streaming query semantics and connector configuration in your target workspace.
- The current sample pipeline is append-oriented. For production-grade corrections, late updates, evolving schemas and replay, define explicit event identity and table update semantics.
- The code has not been run against an Azure subscription from this repository setup session.
