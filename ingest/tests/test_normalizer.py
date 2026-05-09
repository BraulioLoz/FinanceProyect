from __future__ import annotations

import pytest

from ingest.normalizer import (
    BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL,
    build_binance_futures_multi_stream_url,
    build_binance_futures_stream_names,
    normalize_binance_futures_symbol,
)

CONFIGURED_SYMBOLS = list(BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL.keys())


# ── normalize_binance_futures_symbol ─────────────────────────────────────────

class TestNormalizeBinanceFuturesSymbol:
    @pytest.mark.parametrize("symbol", CONFIGURED_SYMBOLS)
    def test_all_configured_symbols_return_non_none(self, symbol: str) -> None:
        assert normalize_binance_futures_symbol(symbol) is not None

    @pytest.mark.parametrize("symbol", CONFIGURED_SYMBOLS)
    def test_configured_symbols_map_to_uppercase_without_slash(self, symbol: str) -> None:
        result = normalize_binance_futures_symbol(symbol)
        assert result == symbol.upper()
        assert "/" not in result

    def test_unknown_symbol_returns_none(self) -> None:
        result = normalize_binance_futures_symbol("UNKNOWNUSDT")
        assert result is None

    def test_lowercase_input_is_accepted(self) -> None:
        result = normalize_binance_futures_symbol("btcusdt")
        assert result == "BTCUSDT"

    def test_mixed_case_input_is_accepted(self) -> None:
        result = normalize_binance_futures_symbol("BtcUsDt")
        assert result == "BTCUSDT"

    def test_unknown_symbol_logs_warning(self, caplog) -> None:
        import logging
        with caplog.at_level(logging.WARNING, logger="ingest.normalizer"):
            normalize_binance_futures_symbol("FAKEUSDT")
        # El símbolo va en record.args vía extra={}; verificamos que se emitió un WARNING
        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warning_records) == 1
        assert warning_records[0].raw_symbol == "FAKEUSDT"


# ── build_binance_futures_stream_names ────────────────────────────────────────

class TestBuildBinanceFuturesStreamNames:
    def test_agg_trade_stream_names_use_lowercase_symbol(self) -> None:
        result = build_binance_futures_stream_names(["BTCUSDT", "ETHUSDT"], "aggTrade")
        assert result == ["btcusdt@aggTrade", "ethusdt@aggTrade"]

    def test_depth_stream_names_use_lowercase_symbol(self) -> None:
        result = build_binance_futures_stream_names(["SOLUSDT"], "depth@100ms")
        assert result == ["solusdt@depth@100ms"]

    def test_empty_symbols_list_returns_empty_list(self) -> None:
        result = build_binance_futures_stream_names([], "aggTrade")
        assert result == []

    def test_ten_symbols_produce_ten_stream_names(self) -> None:
        result = build_binance_futures_stream_names(CONFIGURED_SYMBOLS, "aggTrade")
        assert len(result) == len(CONFIGURED_SYMBOLS)


# ── build_binance_futures_multi_stream_url ────────────────────────────────────

class TestBuildBinanceFuturesMultiStreamUrl:
    BASE_URL = "wss://fstream.binance.com/stream"

    def test_single_stream_produces_correct_url(self) -> None:
        stream_names = ["btcusdt@aggTrade"]
        result = build_binance_futures_multi_stream_url(self.BASE_URL, stream_names)
        assert result == "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade"

    def test_multiple_streams_are_joined_with_slash(self) -> None:
        stream_names = ["btcusdt@aggTrade", "ethusdt@aggTrade"]
        result = build_binance_futures_multi_stream_url(self.BASE_URL, stream_names)
        assert result == "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"

    def test_url_starts_with_base_url(self) -> None:
        result = build_binance_futures_multi_stream_url(self.BASE_URL, ["btcusdt@depth@100ms"])
        assert result.startswith(self.BASE_URL)

    def test_url_contains_streams_query_param(self) -> None:
        result = build_binance_futures_multi_stream_url(self.BASE_URL, ["btcusdt@aggTrade"])
        assert "?streams=" in result
