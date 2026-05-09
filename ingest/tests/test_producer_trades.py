from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.producer_trades import (
    _build_agg_trade_payload,
    _process_websocket_messages,
)

# Mensaje crudo de Binance tal como llega del combined stream endpoint
SAMPLE_BINANCE_AGG_TRADE_OUTER = {
    "stream": "btcusdt@aggTrade",
    "data": {
        "e": "aggTrade",
        "E": 1705320000200,
        "s": "BTCUSDT",
        "a": 987654,
        "p": "42000.50",
        "q": "0.015",
        "nq": "0.010",
        "f": 111111,
        "l": 111115,
        "T": 1705320000123,
        "m": False,
    },
}

SAMPLE_BINANCE_AGG_TRADE_DATA = SAMPLE_BINANCE_AGG_TRADE_OUTER["data"]


# ── _build_agg_trade_payload ──────────────────────────────────────────────────

class TestBuildAggTradePayload:
    def test_all_payload_fields_are_present(self) -> None:
        payload = _build_agg_trade_payload(SAMPLE_BINANCE_AGG_TRADE_DATA)
        expected_fields = [
            "aggregate_trade_id",
            "price",
            "quantity",
            "quantity_excluding_rpi",
            "first_trade_id",
            "last_trade_id",
            "trade_execution_time_ms",
            "is_buyer_market_maker",
        ]
        for field in expected_fields:
            assert field in payload, f"Campo faltante: {field}"

    def test_price_and_quantity_are_preserved_as_strings(self) -> None:
        payload = _build_agg_trade_payload(SAMPLE_BINANCE_AGG_TRADE_DATA)
        assert payload["price"] == "42000.50"
        assert payload["quantity"] == "0.015"

    def test_aggregate_trade_id_matches_raw_field_a(self) -> None:
        payload = _build_agg_trade_payload(SAMPLE_BINANCE_AGG_TRADE_DATA)
        assert payload["aggregate_trade_id"] == 987654

    def test_is_buyer_market_maker_is_boolean(self) -> None:
        payload = _build_agg_trade_payload(SAMPLE_BINANCE_AGG_TRADE_DATA)
        assert isinstance(payload["is_buyer_market_maker"], bool)

    def test_missing_nq_field_defaults_to_zero_string(self) -> None:
        raw_without_nq = {k: v for k, v in SAMPLE_BINANCE_AGG_TRADE_DATA.items() if k != "nq"}
        payload = _build_agg_trade_payload(raw_without_nq)
        assert payload["quantity_excluding_rpi"] == "0"


# ── _process_websocket_messages ───────────────────────────────────────────────

class TestProcessWebsocketMessages:
    def _make_mock_websocket(self, messages: list[str]) -> AsyncMock:
        mock_ws = AsyncMock()
        mock_ws.__aiter__.return_value = iter(messages)
        return mock_ws

    def _make_mock_kafka_producer(self) -> MagicMock:
        mock_producer = MagicMock()
        mock_producer.produce = MagicMock()
        mock_producer.poll = MagicMock()
        return mock_producer

    @pytest.mark.asyncio
    async def test_valid_agg_trade_message_is_published_to_kafka(self) -> None:
        raw_frame = json.dumps(SAMPLE_BINANCE_AGG_TRADE_OUTER)
        mock_ws = self._make_mock_websocket([raw_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-trades")

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "crypto-trades"
        assert call_kwargs["key"] == b"BTCUSDT"

    @pytest.mark.asyncio
    async def test_kafka_message_value_is_valid_envelope_json(self) -> None:
        raw_frame = json.dumps(SAMPLE_BINANCE_AGG_TRADE_OUTER)
        mock_ws = self._make_mock_websocket([raw_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-trades")

        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        envelope = json.loads(call_kwargs["value"].decode("utf-8"))
        assert envelope["symbol"] == "BTCUSDT"
        assert envelope["exchange"] == "binance_futures"
        assert envelope["event_type"] == "agg_trade"
        assert "ts_event" in envelope
        assert "ts_ingest" in envelope
        assert "payload" in envelope

    @pytest.mark.asyncio
    async def test_non_agg_trade_event_type_is_skipped(self) -> None:
        non_trade_frame = json.dumps({
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "s": "BTCUSDT"},
        })
        mock_ws = self._make_mock_websocket([non_trade_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-trades")

        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_symbol_message_is_skipped(self) -> None:
        unknown_symbol_frame = json.dumps({
            "stream": "unknownusdt@aggTrade",
            "data": {**SAMPLE_BINANCE_AGG_TRADE_DATA, "s": "UNKNOWNUSDT"},
        })
        mock_ws = self._make_mock_websocket([unknown_symbol_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-trades")

        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_messages_all_published(self) -> None:
        frames = [
            json.dumps({**SAMPLE_BINANCE_AGG_TRADE_OUTER, "data": {**SAMPLE_BINANCE_AGG_TRADE_DATA, "s": symbol}})
            for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        ]
        mock_ws = self._make_mock_websocket(frames)
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-trades")

        assert mock_kafka_producer.produce.call_count == 3
