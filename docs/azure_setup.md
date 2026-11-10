# Azure setup checklist

This guide describes the resources required; it does not create them.

## Required resources
1. Azure Event Hubs namespace and event hub.
2. Databricks workspace with a Spark runtime compatible with the Event Hubs connector.
3. ADLS Gen2 storage account and container.
4. A managed identity/service principal or secret-backed access configuration.

## Event Hubs
- Create an event hub dedicated to this portfolio project.
- Use a least-privilege SAS policy with Send permission for the producer.
- Use a separate least-privilege Listen permission for the consumer.
- Keep connection strings in local environment variables or Databricks secret scopes.

## Databricks
- Install a connector artifact compatible with the selected Databricks Runtime.
- Confirm the connector's required options and connection-string encryption approach for that exact version.
- Configure storage access to ADLS Gen2 using managed identity or a supported credential mechanism.
- Set durable checkpoint paths in ADLS Gen2; do not store checkpoints on driver-local storage.

## Local producer
Install dependencies from `producer/requirements.txt`, set `EVENTHUB_CONNECTION_STRING` and `EVENTHUB_NAME`, then run:
```bash
python producer/trade_event_producer.py
```

## Security
Never paste credentials into notebooks, source files, screenshots or commits. Rotate any credential accidentally exposed in Git history.
