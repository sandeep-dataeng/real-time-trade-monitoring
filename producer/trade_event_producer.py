"""Send synthetic trade events to Azure Event Hubs.

Required environment variables:
  EVENTHUB_CONNECTION_STRING
  EVENTHUB_NAME

Optional:
  EVENTS_PER_SECOND (default: 2)
  DURATION_SECONDS (default: 60)
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient


def make_trade_event():
    symbol = random.choice(["MSFT", "AAPL", "NVDA", "AMZN", "GOOGL"])
    return {
        "trade_id": str(uuid.uuid4()),
        "symbol": symbol,
        "side": random.choice(["BUY", "SELL"]),
        "quantity": random.randint(1, 500),
        "price": round(random.uniform(50, 900), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "portfolio-simulator",
    }


def main():
    connection_string = os.environ["EVENTHUB_CONNECTION_STRING"]
    eventhub_name = os.environ["EVENTHUB_NAME"]
    rate = max(1, int(os.getenv("EVENTS_PER_SECOND", "2")))
    duration = max(1, int(os.getenv("DURATION_SECONDS", "60")))

    client = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name,
    )
    interval = 1.0 / rate
    deadline = time.monotonic() + duration
    sent = 0

    try:
        while time.monotonic() < deadline:
            batch = client.create_batch()
            while time.monotonic() < deadline:
                payload = json.dumps(make_trade_event())
                try:
                    batch.add(EventData(payload))
                    sent += 1
                except ValueError:
                    break
                if len(batch) >= 50:
                    break
            if len(batch):
                client.send_batch(batch)
            time.sleep(interval)
    finally:
        client.close()
    print(f"Sent approximately {sent} synthetic events.")


if __name__ == "__main__":
    main()
