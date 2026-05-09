from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def build_envelope(
    symbol: str,
    exchange: str,
    event_type: str,
    event_timestamp_ms: int,
    payload: dict[str, Any],
) -> bytes:
    """Return a UTF-8 encoded JSON envelope ready to publish to Kafka."""
    ingest_timestamp_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    event_timestamp_utc = _ms_to_iso8601_utc(event_timestamp_ms)

    envelope: dict[str, Any] = {
        "symbol": symbol,
        "exchange": exchange,
        "event_type": event_type,
        "ts_event": event_timestamp_utc,
        "ts_ingest": ingest_timestamp_utc,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=False).encode("utf-8")


def _ms_to_iso8601_utc(epoch_milliseconds: int) -> str:
    return datetime.fromtimestamp(
        epoch_milliseconds / 1000.0, tz=timezone.utc
    ).isoformat(timespec="milliseconds")
