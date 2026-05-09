from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from ingest.envelope import build_envelope, _ms_to_iso8601_utc


# ── _ms_to_iso8601_utc ────────────────────────────────────────────────────────

class TestMsToIso8601Utc:
    def test_converts_known_epoch_to_correct_iso_string(self) -> None:
        # 2024-01-15 12:00:00.123 UTC
        epoch_ms = 1705320000123
        result = _ms_to_iso8601_utc(epoch_ms)
        assert result == "2024-01-15T12:00:00.123+00:00"

    def test_output_contains_utc_offset(self) -> None:
        result = _ms_to_iso8601_utc(0)
        assert "+00:00" in result

    def test_millisecond_precision_is_preserved(self) -> None:
        result = _ms_to_iso8601_utc(1705320000456)
        assert result.endswith(".456+00:00")


# ── build_envelope ─────────────────────────────────────────────────────────────

FIXED_INGEST_DATETIME = datetime(2024, 1, 15, 12, 0, 0, 789000, tzinfo=timezone.utc)
AGG_TRADE_PAYLOAD = {
    "aggregate_trade_id": 123456,
    "price": "42000.50",
    "quantity": "0.015",
}


class TestBuildEnvelope:
    def _decode_envelope(self, envelope_bytes: bytes) -> dict:
        return json.loads(envelope_bytes.decode("utf-8"))

    @patch("ingest.envelope.datetime")
    def test_returns_valid_utf8_encoded_json(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        result = build_envelope(
            symbol="BTCUSDT",
            exchange="binance_futures",
            event_type="agg_trade",
            event_timestamp_ms=1705320000123,
            payload=AGG_TRADE_PAYLOAD,
        )

        assert isinstance(result, bytes)
        parsed = self._decode_envelope(result)
        assert isinstance(parsed, dict)

    @patch("ingest.envelope.datetime")
    def test_all_required_envelope_fields_present(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        result = build_envelope(
            symbol="ETHUSDT",
            exchange="binance_futures",
            event_type="agg_trade",
            event_timestamp_ms=1705320000000,
            payload=AGG_TRADE_PAYLOAD,
        )

        parsed = self._decode_envelope(result)
        for required_field in ("symbol", "exchange", "event_type", "ts_event", "ts_ingest", "payload"):
            assert required_field in parsed, f"Campo faltante: {required_field}"

    @patch("ingest.envelope.datetime")
    def test_symbol_and_exchange_are_set_correctly(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        result = build_envelope(
            symbol="SOLUSDT",
            exchange="binance_futures",
            event_type="depth_update",
            event_timestamp_ms=1705320000000,
            payload={},
        )

        parsed = self._decode_envelope(result)
        assert parsed["symbol"] == "SOLUSDT"
        assert parsed["exchange"] == "binance_futures"
        assert parsed["event_type"] == "depth_update"

    @patch("ingest.envelope.datetime")
    def test_ts_event_matches_event_timestamp_ms(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        event_timestamp_ms = 1705320000123
        result = build_envelope(
            symbol="BTCUSDT",
            exchange="binance_futures",
            event_type="agg_trade",
            event_timestamp_ms=event_timestamp_ms,
            payload={},
        )

        parsed = self._decode_envelope(result)
        assert parsed["ts_event"] == _ms_to_iso8601_utc(event_timestamp_ms)

    @patch("ingest.envelope.datetime")
    def test_ts_ingest_uses_current_utc_time(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        result = build_envelope(
            symbol="BTCUSDT",
            exchange="binance_futures",
            event_type="agg_trade",
            event_timestamp_ms=1705320000000,
            payload={},
        )

        parsed = self._decode_envelope(result)
        assert "2024-01-15" in parsed["ts_ingest"]

    @patch("ingest.envelope.datetime")
    def test_payload_is_preserved_exactly(self, mock_datetime) -> None:
        mock_datetime.now.return_value = FIXED_INGEST_DATETIME
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        expected_payload = {"price": "99999.99", "quantity": "1.0", "is_buyer_market_maker": True}
        result = build_envelope(
            symbol="BTCUSDT",
            exchange="binance_futures",
            event_type="agg_trade",
            event_timestamp_ms=1705320000000,
            payload=expected_payload,
        )

        parsed = self._decode_envelope(result)
        assert parsed["payload"] == expected_payload
