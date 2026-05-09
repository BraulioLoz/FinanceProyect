"""
Tests para spark/jobs/batch_train.py.

Estrategia:
- _add_lag_features: DataFrame pequeño en memoria, verificar columnas lag y label.
- _build_ml_pipeline: verificar stages del Pipeline sin entrenarlo.
- run_batch_training: no se testea en CI (requiere Parquet real y tiempo de entrenamiento).
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import sys

PYSPARK_INCOMPATIBLE_PYTHON = sys.version_info >= (3, 13)
PYSPARK_SKIP_REASON = "PySpark 3.5.x requiere Python < 3.13; correr en WSL con Python 3.11"

from spark.jobs.batch_train import (
    NUMERIC_FEATURE_COLUMNS,
    _add_lag_features,
    _build_ml_pipeline,
)

# ── Schema y datos de prueba ───────────────────────────────────────────────────

FEATURES_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("window_start", StringType(), True),
    StructField("window_end", StringType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("price_volatility", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("trade_count", LongType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("avg_spread_proxy", DoubleType(), True),
    StructField("avg_best_bid_price", DoubleType(), True),
    StructField("avg_best_ask_price", DoubleType(), True),
])

# 5 ventanas por símbolo para tener suficientes lags y labels
SAMPLE_FEATURE_ROWS = [
    ("BTCUSDT", "2024-01-15T12:00:00+00:00", "2024-01-15T12:01:00+00:00", 42000.0, 100.0, 10.0, 50, 42000.0, 2.0, 41999.0, 42001.0),
    ("BTCUSDT", "2024-01-15T12:00:30+00:00", "2024-01-15T12:01:30+00:00", 42100.0, 120.0, 12.0, 60, 42100.0, 2.1, 42099.0, 42101.0),
    ("BTCUSDT", "2024-01-15T12:01:00+00:00", "2024-01-15T12:02:00+00:00", 42050.0, 110.0, 11.0, 55, 42050.0, 1.9, 42049.0, 42051.0),
    ("BTCUSDT", "2024-01-15T12:01:30+00:00", "2024-01-15T12:02:30+00:00", 42200.0, 130.0, 13.0, 65, 42200.0, 2.2, 42199.0, 42201.0),
    ("BTCUSDT", "2024-01-15T12:02:00+00:00", "2024-01-15T12:03:00+00:00", 42150.0, 115.0, 11.5, 58, 42150.0, 2.0, 42149.0, 42151.0),
]


def _make_features_df(spark: SparkSession) -> object:
    return (
        spark.createDataFrame(SAMPLE_FEATURE_ROWS, FEATURES_SCHEMA)
        .withColumn("window_start", F.to_timestamp("window_start"))
        .withColumn("window_end", F.to_timestamp("window_end"))
    )


# ── _add_lag_features ─────────────────────────────────────────────────────────

@pytest.mark.skipif(PYSPARK_INCOMPATIBLE_PYTHON, reason=PYSPARK_SKIP_REASON)
class TestAddLagFeatures:
    def test_vwap_lag_1_column_is_added(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        assert "vwap_lag_1" in result_df.columns

    def test_vwap_lag_2_column_is_added(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        assert "vwap_lag_2" in result_df.columns

    def test_price_direction_label_column_is_added(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        assert "price_direction_label" in result_df.columns

    def test_price_direction_label_is_binary(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        label_values = {row.price_direction_label for row in result_df.collect()}
        assert label_values.issubset({0, 1})

    def test_rows_with_null_lags_are_dropped(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        # Con 5 filas y lag_2, se pierden las 2 primeras + la última (sin lead)
        assert result_df.count() < df.count()

    def test_vwap_lag_1_is_previous_window_vwap(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df).orderBy("window_start")
        rows = result_df.collect()
        # La primera fila válida (con lag_1 y lag_2) es la 3ra ventana (vwap=42050).
        # Su lag_1 es el VWAP de la 2da ventana: 42100.
        assert abs(rows[0].vwap_lag_1 - 42100.0) < 0.001

    def test_label_is_1_when_next_vwap_is_higher(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df).orderBy("window_start")
        rows = result_df.collect()
        # Primera fila válida: vwap=42050, next_vwap=42200 → label=1
        first_valid_row = rows[0]
        assert first_valid_row.price_direction_label == 1

    def test_all_numeric_feature_columns_present_after_lag(self, spark: SparkSession) -> None:
        df = _make_features_df(spark)
        result_df = _add_lag_features(df)
        for column_name in NUMERIC_FEATURE_COLUMNS:
            assert column_name in result_df.columns, f"Columna faltante: {column_name}"


# ── _build_ml_pipeline ────────────────────────────────────────────────────────

class TestBuildMlPipeline:
    def test_returns_pipeline_instance(self) -> None:
        pipeline = _build_ml_pipeline()
        assert isinstance(pipeline, Pipeline)

    def test_pipeline_has_three_stages(self) -> None:
        pipeline = _build_ml_pipeline()
        assert len(pipeline.getStages()) == 3

    def test_first_stage_is_vector_assembler(self) -> None:
        pipeline = _build_ml_pipeline()
        assert isinstance(pipeline.getStages()[0], VectorAssembler)

    def test_second_stage_is_standard_scaler(self) -> None:
        pipeline = _build_ml_pipeline()
        assert isinstance(pipeline.getStages()[1], StandardScaler)

    def test_third_stage_is_random_forest_classifier(self) -> None:
        pipeline = _build_ml_pipeline()
        assert isinstance(pipeline.getStages()[2], RandomForestClassifier)

    def test_vector_assembler_uses_correct_input_columns(self) -> None:
        pipeline = _build_ml_pipeline()
        assembler: VectorAssembler = pipeline.getStages()[0]
        assert set(assembler.getInputCols()) == set(NUMERIC_FEATURE_COLUMNS)

    def test_random_forest_has_100_trees(self) -> None:
        pipeline = _build_ml_pipeline()
        classifier: RandomForestClassifier = pipeline.getStages()[2]
        assert classifier.getNumTrees() == 100

    def test_random_forest_label_col_is_price_direction_label(self) -> None:
        pipeline = _build_ml_pipeline()
        classifier: RandomForestClassifier = pipeline.getStages()[2]
        assert classifier.getLabelCol() == "price_direction_label"

    def test_scaler_input_is_assembler_output(self) -> None:
        pipeline = _build_ml_pipeline()
        assembler: VectorAssembler = pipeline.getStages()[0]
        scaler: StandardScaler = pipeline.getStages()[1]
        assert scaler.getInputCol() == assembler.getOutputCol()

    def test_classifier_features_col_is_scaler_output(self) -> None:
        pipeline = _build_ml_pipeline()
        scaler: StandardScaler = pipeline.getStages()[1]
        classifier: RandomForestClassifier = pipeline.getStages()[2]
        assert classifier.getFeaturesCol() == scaler.getOutputCol()
