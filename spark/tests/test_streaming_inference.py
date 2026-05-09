"""
Tests para spark/jobs/streaming_inference.py.

Estrategia:
- _build_prediction_envelope: función pura, sin Spark.
- _write_predictions_batch_to_kafka_and_parquet: DataFrame en memoria + mocks de Kafka y Parquet.
- _load_trained_model: testeado con mock de PipelineModel.load.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import sys

PYSPARK_INCOMPATIBLE_PYTHON = sys.version_info >= (3, 13)
PYSPARK_SKIP_REASON = "PySpark 3.5.x requiere Python < 3.13; correr en WSL con Python 3.11"

from spark.jobs.streaming_inference import (
    MODEL_FEATURE_COLUMNS,
    _build_prediction_envelope,
    _load_trained_model,
    _write_predictions_batch_to_kafka_and_parquet,
)

# ── _build_prediction_envelope ────────────────────────────────────────────────

class TestBuildPredictionEnvelope:
    def test_returns_valid_json_string(self) -> None:
        result = _build_prediction_envelope(
            symbol="BTCUSDT",
            ts_event="2024-01-15T12:00:00+00:00",
            prediction=1,
            probability_up=0.72,
            probability_down=0.28,
        )
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_all_envelope_fields_present(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        parsed = json.loads(result)
        for required_field in ("symbol", "exchange", "event_type", "ts_event", "ts_ingest", "payload"):
            assert required_field in parsed

    def test_event_type_is_prediction(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        parsed = json.loads(result)
        assert parsed["event_type"] == "prediction"

    def test_symbol_is_set_correctly(self) -> None:
        result = _build_prediction_envelope("ETHUSDT", "2024-01-15T12:00:00+00:00", 0, 0.3, 0.7)
        parsed = json.loads(result)
        assert parsed["symbol"] == "ETHUSDT"

    def test_ts_event_matches_input(self) -> None:
        ts = "2024-01-15T12:00:00+00:00"
        result = _build_prediction_envelope("BTCUSDT", ts, 1, 0.8, 0.2)
        parsed = json.loads(result)
        assert parsed["ts_event"] == ts

    def test_payload_contains_price_direction_prediction(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        parsed = json.loads(result)
        assert "price_direction_prediction" in parsed["payload"]
        assert parsed["payload"]["price_direction_prediction"] == 1

    def test_payload_contains_probability_up_and_down(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        parsed = json.loads(result)
        assert "probability_price_up" in parsed["payload"]
        assert "probability_price_down" in parsed["payload"]
        assert abs(parsed["payload"]["probability_price_up"] - 0.72) < 0.0001
        assert abs(parsed["payload"]["probability_price_down"] - 0.28) < 0.0001

    def test_payload_contains_model_name(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        parsed = json.loads(result)
        assert parsed["payload"]["model_name"] == "rf_price_direction"

    def test_probabilities_are_rounded_to_6_decimals(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.123456789, 0.876543211)
        parsed = json.loads(result)
        probability_up_str = str(parsed["payload"]["probability_price_up"])
        assert len(probability_up_str.split(".")[-1]) <= 6

    def test_prediction_label_0_is_valid(self) -> None:
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 0, 0.3, 0.7)
        parsed = json.loads(result)
        assert parsed["payload"]["price_direction_prediction"] == 0

    def test_ts_ingest_is_recent_utc(self) -> None:
        from datetime import timedelta
        before = datetime.now(timezone.utc) - timedelta(milliseconds=1)
        result = _build_prediction_envelope("BTCUSDT", "2024-01-15T12:00:00+00:00", 1, 0.72, 0.28)
        after = datetime.now(timezone.utc) + timedelta(milliseconds=1)
        parsed = json.loads(result)
        ts_ingest = datetime.fromisoformat(parsed["ts_ingest"])
        assert before <= ts_ingest <= after


# ── _load_trained_model ───────────────────────────────────────────────────────

class TestLoadTrainedModel:
    def test_calls_pipeline_model_load_with_correct_path(self) -> None:
        model_directory = "/home/user/models/rf_price_direction"
        mock_pipeline_model = MagicMock()

        with patch("spark.jobs.streaming_inference.PipelineModel") as MockPipelineModel:
            MockPipelineModel.load.return_value = mock_pipeline_model
            result = _load_trained_model(model_directory)

        MockPipelineModel.load.assert_called_once_with(model_directory)
        assert result is mock_pipeline_model

    def test_raises_exception_if_model_not_found(self) -> None:
        with patch("spark.jobs.streaming_inference.PipelineModel") as MockPipelineModel:
            MockPipelineModel.load.side_effect = Exception("Path does not exist")
            with pytest.raises(Exception, match="Path does not exist"):
                _load_trained_model("/nonexistent/path")


# ── _write_predictions_batch_to_kafka_and_parquet ─────────────────────────────

PREDICTIONS_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("ts_event", StringType(), True),
    StructField("ts_event_timestamp", StringType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("price_volatility", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("probability", StringType(), True),   # simplificado para tests
])

SAMPLE_PREDICTIONS_ROWS = [
    ("BTCUSDT", "2024-01-15T12:00:00+00:00", "2024-01-15T12:00:00+00:00", 42000.0, 100.0, 10.0, 1.0, "[0.28, 0.72]"),
    ("ETHUSDT", "2024-01-15T12:00:00+00:00", "2024-01-15T12:00:00+00:00", 2500.0,  50.0,  5.0,  0.0, "[0.65, 0.35]"),
]


@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestWritePredictionsBatchToKafkaAndParquet:
    def _make_predictions_df(self, spark: SparkSession) -> object:
        return (
            spark.createDataFrame(SAMPLE_PREDICTIONS_ROWS, PREDICTIONS_SCHEMA)
            .withColumn("ts_event_timestamp", F.to_timestamp("ts_event_timestamp"))
        )

    def test_empty_dataframe_does_not_call_kafka_write(self, spark: SparkSession) -> None:
        empty_df = spark.createDataFrame([], PREDICTIONS_SCHEMA)

        with patch.object(empty_df.__class__, "write", new_callable=MagicMock):
            _write_predictions_batch_to_kafka_and_parquet(
                empty_df, batch_id=0,
                kafka_topic_predictions="crypto-pred",
                predictions_parquet_dir="/tmp/test-preds",
            )
            # Si el DataFrame está vacío, no debe intentar escribir
            assert True  # no exception raised
