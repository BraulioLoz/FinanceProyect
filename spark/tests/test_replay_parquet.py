"""
Tests para spark/replayer/replay_parquet.py.

Estrategia:
- _build_replay_feature_envelope y _build_replay_prediction_envelope: funciones puras.
- _get_sort_column: lógica simple, sin Spark.
- _iter_rows_with_throttle: rows en memoria, verificar orden y envelopes generados.
- _parse_arguments: verificar defaults y parsing de CLI.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from spark.replayer.replay_parquet import (
    SOURCE_KAFKA_TOPICS,
    SOURCE_PARQUET_DIRS,
    _build_replay_feature_envelope,
    _build_replay_prediction_envelope,
    _get_sort_column,
    _iter_rows_with_throttle,
    _parse_arguments,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_feature_row(**overrides):
    defaults = {
        "symbol": "BTCUSDT",
        "window_start": datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "vwap": 42000.5,
        "price_volatility": 100.0,
        "total_volume": 10.0,
        "trade_count": 50,
        "avg_price": 42000.0,
        "avg_spread_proxy": 2.0,
        "avg_best_bid_price": 41999.0,
        "avg_best_ask_price": 42001.0,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_prediction_row(**overrides):
    defaults = {
        "symbol": "BTCUSDT",
        "ts_event": "2024-01-15T12:00:00+00:00",
        "prediction": 1,
        "vwap": 42000.5,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ── _build_replay_feature_envelope ────────────────────────────────────────────

class TestBuildReplayFeatureEnvelope:
    def test_returns_valid_json_string(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row())
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_all_envelope_fields_present(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row())
        parsed = json.loads(result)
        for field in ("symbol", "exchange", "event_type", "ts_event", "ts_ingest", "payload"):
            assert field in parsed

    def test_event_type_is_feature_row(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row())
        parsed = json.loads(result)
        assert parsed["event_type"] == "feature_row"

    def test_symbol_is_preserved(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row(symbol="ETHUSDT"))
        parsed = json.loads(result)
        assert parsed["symbol"] == "ETHUSDT"

    def test_payload_contains_vwap(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row(vwap=42500.0))
        parsed = json.loads(result)
        assert abs(parsed["payload"]["vwap"] - 42500.0) < 0.001

    def test_payload_contains_replayed_flag(self) -> None:
        result = _build_replay_feature_envelope(_make_feature_row())
        parsed = json.loads(result)
        assert parsed["payload"]["_replayed"] is True

    def test_ts_event_comes_from_window_start(self) -> None:
        window_start = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = _build_replay_feature_envelope(_make_feature_row(window_start=window_start))
        parsed = json.loads(result)
        assert "2024-01-15" in parsed["ts_event"]

    def test_ts_ingest_is_current_date(self) -> None:
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = _build_replay_feature_envelope(_make_feature_row())
        parsed = json.loads(result)
        assert current_date in parsed["ts_ingest"]


# ── _build_replay_prediction_envelope ────────────────────────────────────────

class TestBuildReplayPredictionEnvelope:
    def test_returns_valid_json_string(self) -> None:
        result = _build_replay_prediction_envelope(_make_prediction_row())
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_event_type_is_prediction(self) -> None:
        result = _build_replay_prediction_envelope(_make_prediction_row())
        parsed = json.loads(result)
        assert parsed["event_type"] == "prediction"

    def test_payload_contains_replayed_flag(self) -> None:
        result = _build_replay_prediction_envelope(_make_prediction_row())
        parsed = json.loads(result)
        assert parsed["payload"]["_replayed"] is True

    def test_payload_contains_price_direction_prediction(self) -> None:
        result = _build_replay_prediction_envelope(_make_prediction_row(prediction=0))
        parsed = json.loads(result)
        assert parsed["payload"]["price_direction_prediction"] == 0


# ── _get_sort_column ──────────────────────────────────────────────────────────

class TestGetSortColumn:
    def test_features_source_sorts_by_window_start(self) -> None:
        assert _get_sort_column("features") == "window_start"

    def test_trades_source_sorts_by_ts_event(self) -> None:
        assert _get_sort_column("trades") == "ts_event"

    def test_predictions_source_sorts_by_ts_event_timestamp(self) -> None:
        assert _get_sort_column("predictions") == "ts_event_timestamp"

    def test_unknown_source_falls_back_to_window_start(self) -> None:
        assert _get_sort_column("unknown_source") == "window_start"


# ── _iter_rows_with_throttle ──────────────────────────────────────────────────

class TestIterRowsWithThrottle:
    def _make_mock_df_with_rows(self, rows: list) -> MagicMock:
        mock_df = MagicMock()
        mock_df.orderBy.return_value.collect.return_value = rows
        return mock_df

    def test_yields_one_tuple_per_row(self) -> None:
        rows = [_make_feature_row(symbol="BTCUSDT"), _make_feature_row(symbol="ETHUSDT")]
        mock_df = self._make_mock_df_with_rows(rows)

        results = list(_iter_rows_with_throttle(mock_df, "features", replay_speed_factor=0.0))
        assert len(results) == 2

    def test_each_result_is_symbol_and_json_tuple(self) -> None:
        rows = [_make_feature_row(symbol="BTCUSDT")]
        mock_df = self._make_mock_df_with_rows(rows)

        results = list(_iter_rows_with_throttle(mock_df, "features", replay_speed_factor=0.0))
        symbol_key, envelope_json = results[0]
        assert symbol_key == "BTCUSDT"
        parsed = json.loads(envelope_json)
        assert parsed["symbol"] == "BTCUSDT"

    def test_no_throttle_when_speed_is_zero(self) -> None:
        rows = [
            _make_feature_row(window_start=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)),
            _make_feature_row(window_start=datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc)),
        ]
        mock_df = self._make_mock_df_with_rows(rows)

        with patch("spark.replayer.replay_parquet.time.sleep") as mock_sleep:
            list(_iter_rows_with_throttle(mock_df, "features", replay_speed_factor=0.0))
            mock_sleep.assert_not_called()

    def test_empty_dataframe_yields_nothing(self) -> None:
        mock_df = self._make_mock_df_with_rows([])
        results = list(_iter_rows_with_throttle(mock_df, "features", replay_speed_factor=0.0))
        assert results == []

    def test_envelope_json_contains_replayed_flag(self) -> None:
        rows = [_make_feature_row()]
        mock_df = self._make_mock_df_with_rows(rows)

        results = list(_iter_rows_with_throttle(mock_df, "features", replay_speed_factor=0.0))
        _, envelope_json = results[0]
        parsed = json.loads(envelope_json)
        assert parsed["payload"]["_replayed"] is True


# ── SOURCE_PARQUET_DIRS y SOURCE_KAFKA_TOPICS ─────────────────────────────────

class TestSourceMappings:
    def test_all_expected_sources_have_parquet_dirs(self) -> None:
        for source in ("features", "trades", "predictions"):
            assert source in SOURCE_PARQUET_DIRS

    def test_all_expected_sources_have_kafka_topics(self) -> None:
        for source in ("features", "trades", "predictions"):
            assert source in SOURCE_KAFKA_TOPICS

    def test_features_kafka_topic_is_crypto_features(self) -> None:
        assert SOURCE_KAFKA_TOPICS["features"] == "crypto-features"

    def test_predictions_kafka_topic_is_crypto_pred(self) -> None:
        assert SOURCE_KAFKA_TOPICS["predictions"] == "crypto-pred"


# ── _parse_arguments ──────────────────────────────────────────────────────────

class TestParseArguments:
    def test_default_source_is_features(self) -> None:
        with patch("sys.argv", ["replay_parquet.py"]):
            args = _parse_arguments()
        assert args.source == "features"

    def test_default_speed_is_one(self) -> None:
        with patch("sys.argv", ["replay_parquet.py"]):
            args = _parse_arguments()
        assert args.replay_speed_factor == 1.0

    def test_default_dry_run_is_false(self) -> None:
        with patch("sys.argv", ["replay_parquet.py"]):
            args = _parse_arguments()
        assert args.dry_run is False

    def test_speed_flag_is_parsed(self) -> None:
        with patch("sys.argv", ["replay_parquet.py", "--speed", "2.5"]):
            args = _parse_arguments()
        assert args.replay_speed_factor == 2.5

    def test_dry_run_flag_is_parsed(self) -> None:
        with patch("sys.argv", ["replay_parquet.py", "--dry-run"]):
            args = _parse_arguments()
        assert args.dry_run is True

    def test_symbol_filter_is_parsed(self) -> None:
        with patch("sys.argv", ["replay_parquet.py", "--symbol", "BTCUSDT"]):
            args = _parse_arguments()
        assert args.symbol_filter == "BTCUSDT"

    def test_date_filter_is_parsed(self) -> None:
        with patch("sys.argv", ["replay_parquet.py", "--date", "2024-01-15"]):
            args = _parse_arguments()
        assert args.date_filter == "2024-01-15"

    def test_batch_size_is_parsed(self) -> None:
        with patch("sys.argv", ["replay_parquet.py", "--batch-size", "1000"]):
            args = _parse_arguments()
        assert args.kafka_publish_batch_size == 1000
