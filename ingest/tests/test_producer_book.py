from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ingest.producer_book import (
    _build_depth_update_payload,
    _process_websocket_messages,
)

SAMPLE_BINANCE_DEPTH_UPDATE_OUTER = {
    "stream": "btcusdt@depth@100ms",
    "data": {
        "e": "depthUpdate",
        "E": 1705320000200,
        "T": 1705320000150,
        "s": "BTCUSDT",
        "U": 100001,
        "u": 100010,
        "pu": 100000,
        "b": [["41999.00", "0.500"], ["41998.00", "1.200"]],
        "a": [["42001.00", "0.300"], ["42002.00", "0.000"]],
    },
}

SAMPLE_BINANCE_DEPTH_UPDATE_DATA = SAMPLE_BINANCE_DEPTH_UPDATE_OUTER["data"]


# ── _build_depth_update_payload ───────────────────────────────────────────────

class TestBuildDepthUpdatePayload:
    def test_all_payload_fields_are_present(self) -> None:
        payload = _build_depth_update_payload(SAMPLE_BINANCE_DEPTH_UPDATE_DATA)
        expected_fields = [
            "first_update_id",
            "final_update_id",
            "previous_final_update_id",
            "bids",
            "asks",
        ]
        for field in expected_fields:
            assert field in payload, f"Campo faltante: {field}"

    def test_update_ids_match_raw_fields(self) -> None:
        payload = _build_depth_update_payload(SAMPLE_BINANCE_DEPTH_UPDATE_DATA)
        assert payload["first_update_id"] == 100001
        assert payload["final_update_id"] == 100010
        assert payload["previous_final_update_id"] == 100000

    def test_bids_and_asks_are_lists(self) -> None:
        payload = _build_depth_update_payload(SAMPLE_BINANCE_DEPTH_UPDATE_DATA)
        assert isinstance(payload["bids"], list)
        assert isinstance(payload["asks"], list)

    def test_bid_entries_are_price_quantity_pairs(self) -> None:
        payload = _build_depth_update_payload(SAMPLE_BINANCE_DEPTH_UPDATE_DATA)
        first_bid = payload["bids"][0]
        assert len(first_bid) == 2
        assert first_bid[0] == "41999.00"
        assert first_bid[1] == "0.500"

    def test_zero_quantity_ask_is_preserved(self) -> None:
        payload = _build_depth_update_payload(SAMPLE_BINANCE_DEPTH_UPDATE_DATA)
        # Cantidad "0.000" indica eliminar el nivel — debe conservarse tal cual
        zero_qty_ask = next(entry for entry in payload["asks"] if entry[1] == "0.000")
        assert zero_qty_ask[0] == "42002.00"


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
    async def test_valid_depth_update_is_published_to_kafka(self) -> None:
        raw_frame = json.dumps(SAMPLE_BINANCE_DEPTH_UPDATE_OUTER)
        mock_ws = self._make_mock_websocket([raw_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "crypto-book"
        assert call_kwargs["key"] == b"BTCUSDT"

    @pytest.mark.asyncio
    async def test_kafka_message_value_is_valid_envelope_json(self) -> None:
        raw_frame = json.dumps(SAMPLE_BINANCE_DEPTH_UPDATE_OUTER)
        mock_ws = self._make_mock_websocket([raw_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        envelope = json.loads(call_kwargs["value"].decode("utf-8"))
        assert envelope["symbol"] == "BTCUSDT"
        assert envelope["exchange"] == "binance_futures"
        assert envelope["event_type"] == "depth_update"
        assert "ts_event" in envelope
        assert "ts_ingest" in envelope
        assert "payload" in envelope

    @pytest.mark.asyncio
    async def test_ts_event_uses_transaction_time_field_T(self) -> None:
        raw_frame = json.dumps(SAMPLE_BINANCE_DEPTH_UPDATE_OUTER)
        mock_ws = self._make_mock_websocket([raw_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        envelope = json.loads(call_kwargs["value"].decode("utf-8"))
        # T = 1705320000150 → 2024-01-15T12:00:00.150+00:00
        assert "2024-01-15" in envelope["ts_event"]
        assert ".150" in envelope["ts_event"]

    @pytest.mark.asyncio
    async def test_non_depth_update_event_type_is_skipped(self) -> None:
        non_depth_frame = json.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade", "s": "BTCUSDT"},
        })
        mock_ws = self._make_mock_websocket([non_depth_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_symbol_message_is_skipped(self) -> None:
        unknown_symbol_frame = json.dumps({
            "stream": "unknownusdt@depth@100ms",
            "data": {**SAMPLE_BINANCE_DEPTH_UPDATE_DATA, "s": "UNKNOWNUSDT"},
        })
        mock_ws = self._make_mock_websocket([unknown_symbol_frame])
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_symbols_all_published(self) -> None:
        frames = [
            json.dumps({
                **SAMPLE_BINANCE_DEPTH_UPDATE_OUTER,
                "data": {**SAMPLE_BINANCE_DEPTH_UPDATE_DATA, "s": symbol},
            })
            for symbol in ["BTCUSDT", "ETHUSDT"]
        ]
        mock_ws = self._make_mock_websocket(frames)
        mock_kafka_producer = self._make_mock_kafka_producer()

        await _process_websocket_messages(mock_ws, mock_kafka_producer, "crypto-book")

        assert mock_kafka_producer.produce.call_count == 2
