"""
Tests para spark/jobs/streaming_features.py.

Estrategia:
- Funciones puras (_build_feature_envelope): sin Spark.
- Transformaciones DataFrame (_parse_trade_stream, _compute_trade_features, etc.):
  DataFrames pequeños en memoria con SparkSession local.
- Escrituras a Kafka y Parquet (_write_batch_to_kafka_and_parquet): mockeadas.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import sys

# PySpark 3.5.x no soporta Python 3.13 — los tests de DataFrame se marcan como skip.
# Los tests de funciones puras corren en cualquier versión.
PYSPARK_INCOMPATIBLE_PYTHON = sys.version_info >= (3, 13)
PYSPARK_SKIP_REASON = "PySpark 3.5.x requiere Python < 3.13; correr en WSL con Python 3.11"

from spark.jobs.streaming_features import (
    ENVELOPE_WITH_TRADE_PAYLOAD_SCHEMA,
    _build_feature_envelope,
    _compute_book_features,
    _compute_trade_features,
    _parse_book_stream,
    _parse_trade_stream,
    _write_batch_to_kafka_and_parquet,
)

# ── Datos de prueba ────────────────────────────────────────────────────────────

SAMPLE_AGG_TRADE_ENVELOPE = json.dumps({
    "symbol": "BTCUSDT",
    "exchange": "binance_futures",
    "event_type": "agg_trade",
    "ts_event": "2024-01-15T12:00:00.123+00:00",
    "ts_ingest": "2024-01-15T12:00:00.200+00:00",
    "payload": {
        "aggregate_trade_id": 111,
        "price": "42000.50",
        "quantity": "0.5",
        "quantity_excluding_rpi": "0.5",
        "first_trade_id": 1,
        "last_trade_id": 1,
        "trade_execution_time_ms": 1705320000123,
        "is_buyer_market_maker": False,
    },
})

SAMPLE_DEPTH_ENVELOPE = json.dumps({
    "symbol": "BTCUSDT",
    "exchange": "binance_futures",
    "event_type": "depth_update",
    "ts_event": "2024-01-15T12:00:00.150+00:00",
    "ts_ingest": "2024-01-15T12:00:00.200+00:00",
    "payload": {
        "first_update_id": 100,
        "final_update_id": 110,
        "previous_final_update_id": 99,
        "bids": '[[\"41999.00\", \"1.0\"]]',
        "asks": '[[\"42001.00\", \"0.5\"]]',
    },
})

RAW_STREAM_SCHEMA = StructType([StructField("raw_json", StringType(), True)])

PARSED_TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("ts_event_timestamp", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("is_buyer_market_maker", BooleanType(), True),
])


# ── _build_feature_envelope ───────────────────────────────────────────────────

class TestBuildFeatureEnvelope:
    def test_returns_valid_json_string(self) -> None:
        result = _build_feature_envelope(
            row_symbol="BTCUSDT",
            window_start_iso="2024-01-15T12:00:00+00:00",
            payload={"vwap": 42000.5},
        )
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_all_envelope_fields_present(self) -> None:
        result = _build_feature_envelope("ETHUSDT", "2024-01-15T12:00:00+00:00", {})
        parsed = json.loads(result)
        for required_field in ("symbol", "exchange", "event_type", "ts_event", "ts_ingest", "payload"):
            assert required_field in parsed, f"Campo faltante: {required_field}"

    def test_event_type_is_feature_row(self) -> None:
        result = _build_feature_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", {})
        parsed = json.loads(result)
        assert parsed["event_type"] == "feature_row"

    def test_symbol_is_set_correctly(self) -> None:
        result = _build_feature_envelope("SOLUSDT", "2024-01-15T12:00:00+00:00", {})
        parsed = json.loads(result)
        assert parsed["symbol"] == "SOLUSDT"

    def test_ts_event_matches_window_start(self) -> None:
        window_start = "2024-01-15T12:00:00+00:00"
        result = _build_feature_envelope("BTCUSDT", window_start, {})
        parsed = json.loads(result)
        assert parsed["ts_event"] == window_start

    def test_payload_is_preserved(self) -> None:
        payload = {"vwap": 42000.5, "total_volume": 1.5, "trade_count": 10}
        result = _build_feature_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", payload)
        parsed = json.loads(result)
        assert parsed["payload"] == payload

    def test_ts_ingest_is_recent_utc(self) -> None:
        from datetime import timedelta
        before = datetime.now(timezone.utc) - timedelta(milliseconds=1)
        result = _build_feature_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", {})
        after = datetime.now(timezone.utc) + timedelta(milliseconds=1)
        parsed = json.loads(result)
        ts_ingest = datetime.fromisoformat(parsed["ts_ingest"])
        assert before <= ts_ingest <= after


# ── _parse_trade_stream ───────────────────────────────────────────────────────

@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestParseTradeStream:
    def test_parses_symbol_correctly(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_AGG_TRADE_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_trade_stream(raw_df)
        row = result_df.collect()[0]
        assert row.symbol == "BTCUSDT"

    def test_parses_price_as_double(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_AGG_TRADE_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_trade_stream(raw_df)
        row = result_df.collect()[0]
        assert isinstance(row.price, float)
        assert abs(row.price - 42000.50) < 0.001

    def test_parses_quantity_as_double(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_AGG_TRADE_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_trade_stream(raw_df)
        row = result_df.collect()[0]
        assert isinstance(row.quantity, float)
        assert abs(row.quantity - 0.5) < 0.0001

    def test_output_has_ts_event_timestamp_column(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_AGG_TRADE_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_trade_stream(raw_df)
        assert "ts_event_timestamp" in result_df.columns

    def test_invalid_json_row_produces_null_symbol(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([("not-valid-json",)], RAW_STREAM_SCHEMA)
        result_df = _parse_trade_stream(raw_df)
        row = result_df.collect()[0]
        assert row.symbol is None

    def test_multiple_rows_all_parsed(self, spark: SparkSession) -> None:
        eth_envelope = SAMPLE_AGG_TRADE_ENVELOPE.replace("BTCUSDT", "ETHUSDT")
        raw_df = spark.createDataFrame(
            [(SAMPLE_AGG_TRADE_ENVELOPE,), (eth_envelope,)],
            RAW_STREAM_SCHEMA,
        )
        result_df = _parse_trade_stream(raw_df)
        symbols = {row.symbol for row in result_df.collect()}
        assert symbols == {"BTCUSDT", "ETHUSDT"}


# ── _parse_book_stream ────────────────────────────────────────────────────────

@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestParseBookStream:
    def test_parses_symbol_correctly(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_DEPTH_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_book_stream(raw_df)
        row = result_df.collect()[0]
        assert row.symbol == "BTCUSDT"

    def test_parses_best_bid_price_as_double(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_DEPTH_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_book_stream(raw_df)
        row = result_df.collect()[0]
        assert isinstance(row.best_bid_price, float)
        assert abs(row.best_bid_price - 41999.0) < 0.001

    def test_parses_best_ask_price_as_double(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_DEPTH_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_book_stream(raw_df)
        row = result_df.collect()[0]
        assert isinstance(row.best_ask_price, float)
        assert abs(row.best_ask_price - 42001.0) < 0.001

    def test_output_has_ts_event_timestamp_column(self, spark: SparkSession) -> None:
        raw_df = spark.createDataFrame([(SAMPLE_DEPTH_ENVELOPE,)], RAW_STREAM_SCHEMA)
        result_df = _parse_book_stream(raw_df)
        assert "ts_event_timestamp" in result_df.columns


# ── _compute_trade_features ───────────────────────────────────────────────────

@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestComputeTradeFeatures:
    def _make_trade_df(self, spark: SparkSession) -> object:
        rows = [
            ("BTCUSDT", "2024-01-15T12:00:00+00:00", 42000.0, 1.0, False),
            ("BTCUSDT", "2024-01-15T12:00:10+00:00", 42100.0, 2.0, True),
            ("BTCUSDT", "2024-01-15T12:00:20+00:00", 42050.0, 1.5, False),
            ("ETHUSDT", "2024-01-15T12:00:05+00:00", 2500.0,  3.0, False),
        ]
        return (
            spark.createDataFrame(rows, PARSED_TRADE_SCHEMA)
            .withColumn("ts_event_timestamp", F.to_timestamp("ts_event_timestamp"))
        )

    def test_vwap_column_present_in_output(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        assert "vwap" in result_df.columns

    def test_price_volatility_column_present(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        assert "price_volatility" in result_df.columns

    def test_total_volume_column_present(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        assert "total_volume" in result_df.columns

    def test_trade_count_column_present(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        assert "trade_count" in result_df.columns

    def test_output_has_window_start_and_symbol(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        assert "window_start" in result_df.columns
        assert "symbol" in result_df.columns

    def test_vwap_is_weighted_by_quantity(self, spark: SparkSession) -> None:
        # Un solo trade: price=42000, qty=1 → VWAP debe ser 42000
        single_trade_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("ts_event_timestamp", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("is_buyer_market_maker", BooleanType(), True),
        ])
        single_row = [("BTCUSDT", "2024-01-15T12:00:00+00:00", 42000.0, 1.0, False)]
        single_df = (
            spark.createDataFrame(single_row, single_trade_schema)
            .withColumn("ts_event_timestamp", F.to_timestamp("ts_event_timestamp"))
        )
        result = _compute_trade_features(single_df, "1 minute", "1 minute").collect()
        assert len(result) == 1
        assert abs(result[0].vwap - 42000.0) < 0.001

    def test_results_grouped_by_symbol(self, spark: SparkSession) -> None:
        trade_df = self._make_trade_df(spark)
        result_df = _compute_trade_features(trade_df, "1 minute", "1 minute")
        symbols = {row.symbol for row in result_df.collect()}
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols


# ── _compute_book_features ────────────────────────────────────────────────────

@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestComputeBookFeatures:
    PARSED_BOOK_SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("ts_event_timestamp", StringType(), True),
        StructField("best_bid_price", DoubleType(), True),
        StructField("best_ask_price", DoubleType(), True),
    ])

    def _make_book_df(self, spark: SparkSession) -> object:
        rows = [
            ("BTCUSDT", "2024-01-15T12:00:00+00:00", 41999.0, 42001.0),
            ("BTCUSDT", "2024-01-15T12:00:10+00:00", 42000.0, 42002.0),
        ]
        return (
            spark.createDataFrame(rows, self.PARSED_BOOK_SCHEMA)
            .withColumn("ts_event_timestamp", F.to_timestamp("ts_event_timestamp"))
        )

    def test_avg_spread_proxy_column_present(self, spark: SparkSession) -> None:
        book_df = self._make_book_df(spark)
        result_df = _compute_book_features(book_df, "1 minute", "1 minute")
        assert "avg_spread_proxy" in result_df.columns

    def test_avg_best_bid_and_ask_columns_present(self, spark: SparkSession) -> None:
        book_df = self._make_book_df(spark)
        result_df = _compute_book_features(book_df, "1 minute", "1 minute")
        assert "avg_best_bid_price" in result_df.columns
        assert "avg_best_ask_price" in result_df.columns

    def test_spread_proxy_is_ask_minus_bid(self, spark: SparkSession) -> None:
        # bid=41999, ask=42001 → spread=2; bid=42000, ask=42002 → spread=2; avg=2
        book_df = self._make_book_df(spark)
        result = _compute_book_features(book_df, "1 minute", "1 minute").collect()
        assert len(result) == 1
        assert abs(result[0].avg_spread_proxy - 2.0) < 0.001

    def test_output_has_symbol_and_window_start(self, spark: SparkSession) -> None:
        book_df = self._make_book_df(spark)
        result_df = _compute_book_features(book_df, "1 minute", "1 minute")
        assert "symbol" in result_df.columns
        assert "window_start" in result_df.columns
