"""Generate one synthetic JSON event file per interval for local streaming tests."""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(os.getenv("EVENT_INPUT_DIR", "runtime/input"))
EVENTS_PER_SECOND = max(1, int(os.getenv("EVENTS_PER_SECOND", "2")))
DURATION_SECONDS = max(1, int(os.getenv("DURATION_SECONDS", "60")))
SYMBOLS = ["MSFT", "AAPL", "NVDA", "AMZN", "GOOGL"]

def make_event():
    return {
        "trade_id": str(uuid.uuid4()),
        "symbol": random.choice(SYMBOLS),
        "side": random.choice(["BUY", "SELL"]),
        "quantity": random.randint(1, 500),
        "price": round(random.uniform(50, 900), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "local-portfolio-simulator",
    }

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    interval = 1.0 / EVENTS_PER_SECOND
    end_at = time.monotonic() + DURATION_SECONDS
    sequence = 0
    while time.monotonic() < end_at:
        event = make_event()
        # Atomic rename prevents Spark from reading a partially-written JSON file.
        temp_path = OUTPUT_DIR / f".event_{sequence:08d}.tmp"
        final_path = OUTPUT_DIR / f"event_{sequence:08d}.json"
        temp_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
        temp_path.replace(final_path)
        sequence += 1
        time.sleep(interval)
    print(f"Generated {sequence} synthetic events in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
