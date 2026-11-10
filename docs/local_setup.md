# Run the streaming project locally (no Azure subscription)

This local mode uses Spark's streaming file source to simulate an event feed. It exercises Structured Streaming and Delta Lake locally; it does **not** connect to Azure Event Hubs or ADLS Gen2.

## Requirements
- Python 3.11
- Java 8, 11 or 17 compatible with the selected Spark runtime (Java 17 recommended for this setup)
- Enough local disk space for generated events, Delta data and checkpoints

## 1. Create an environment
```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-local.txt
```

## 2. Start the Bronze stream
Open PowerShell terminal 1:
```powershell
python src/local_file_stream.py
```

## 3. Start Silver and Gold
Open PowerShell terminal 2:
```powershell
python src/local_silver_gold.py
```

## 4. Generate events
Open PowerShell terminal 3:
```powershell
python local_simulator/generate_events.py
```

The generator writes one JSON file per event to `runtime/input`. Spark ingests new files and writes Delta tables under `runtime/delta`.

## 5. Inspect the results
Use a separate Python/Spark session to read the Delta paths:
- `runtime/delta/bronze`
- `runtime/delta/silver`
- `runtime/delta/quarantine`
- `runtime/delta/gold`

## 6. Reset a test run
Stop all three processes with Ctrl+C. To run from scratch, delete the local `runtime` directory. Do not delete checkpoints while a query is active.

## Important
The local file source is a learning substitute for the event broker. The cloud adapter files remain in the repository as unverified starter code. A real Azure Event Hubs/ADLS deployment requires an Azure subscription and a compatible Databricks runtime/connector.
