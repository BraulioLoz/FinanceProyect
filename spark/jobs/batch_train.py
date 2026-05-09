"""
batch_train.py — Entrenamiento batch: Parquet de features → modelo RandomForest en MLlib

Lee las features generadas por streaming_features.py, construye lag-features,
genera el label de dirección del precio y entrena un clasificador binario.

Uso:
    bash infra/scripts/run_spark_cpu.sh spark/jobs/batch_train.py
    bash infra/scripts/run_spark_gpu.sh spark/jobs/batch_train.py
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

load_dotenv()

# ── Variables de entorno ───────────────────────────────────────────────────────

DATA_DIR: str = os.environ["DATA_DIR"]
MODEL_DIR: str = os.environ["MODEL_DIR"]

FEATURES_PARQUET_DIR: str = f"{DATA_DIR}/features"
MODEL_OUTPUT_DIR: str = f"{MODEL_DIR}/rf_price_direction"

TRAIN_RATIO: float = 0.8
TEST_RATIO: float = 0.2
RANDOM_SEED: int = 42

# Columnas de features numéricas usadas para entrenamiento
NUMERIC_FEATURE_COLUMNS: list[str] = [
    "vwap",
    "price_volatility",
    "total_volume",
    "trade_count",
    "avg_price",
    "avg_spread_proxy",
    "avg_best_bid_price",
    "avg_best_ask_price",
    # lag features (ventana anterior)
    "vwap_lag_1",
    "price_volatility_lag_1",
    "total_volume_lag_1",
    "avg_spread_proxy_lag_1",
    # lag features (dos ventanas atrás)
    "vwap_lag_2",
    "price_volatility_lag_2",
]


def _load_features(spark: SparkSession) -> object:
    print(f"Leyendo features desde: {FEATURES_PARQUET_DIR}")
    return spark.read.parquet(FEATURES_PARQUET_DIR)


def _add_lag_features(features_df: object) -> object:
    """Agrega lag features ordenando por window_start dentro de cada símbolo."""
    symbol_time_window = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
    )

    lag_1_window = symbol_time_window.rowsBetween(-1, -1)
    lag_2_window = symbol_time_window.rowsBetween(-2, -2)

    return (
        features_df
        .withColumn("vwap_lag_1", F.lag("vwap", 1).over(symbol_time_window))
        .withColumn("price_volatility_lag_1", F.lag("price_volatility", 1).over(symbol_time_window))
        .withColumn("total_volume_lag_1", F.lag("total_volume", 1).over(symbol_time_window))
        .withColumn("avg_spread_proxy_lag_1", F.lag("avg_spread_proxy", 1).over(symbol_time_window))
        .withColumn("vwap_lag_2", F.lag("vwap", 2).over(symbol_time_window))
        .withColumn("price_volatility_lag_2", F.lag("price_volatility", 2).over(symbol_time_window))
        # Label: 1 si el VWAP de la siguiente ventana es mayor al actual, 0 si baja
        .withColumn(
            "price_direction_label",
            F.when(F.lead("vwap", 1).over(symbol_time_window) > F.col("vwap"), 1).otherwise(0).cast(IntegerType()),
        )
        # Eliminar filas con lags nulos (primeras ventanas de cada símbolo) o sin label
        .dropna(subset=NUMERIC_FEATURE_COLUMNS + ["price_direction_label"])
    )


def _build_ml_pipeline() -> Pipeline:
    vector_assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURE_COLUMNS,
        outputCol="raw_features",
        handleInvalid="skip",
    )

    standard_scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True,
    )

    random_forest_classifier = RandomForestClassifier(
        featuresCol="scaled_features",
        labelCol="price_direction_label",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=100,
        maxDepth=10,
        seed=RANDOM_SEED,
    )

    return Pipeline(stages=[vector_assembler, standard_scaler, random_forest_classifier])


def _print_evaluation_metrics(
    predictions_df: object,
    accuracy_evaluator: MulticlassClassificationEvaluator,
    f1_evaluator: MulticlassClassificationEvaluator,
    auc_evaluator: BinaryClassificationEvaluator,
) -> None:
    accuracy = accuracy_evaluator.evaluate(predictions_df)
    f1_score = f1_evaluator.evaluate(predictions_df)
    auc_roc = auc_evaluator.evaluate(predictions_df)

    print("\n" + "=" * 50)
    print("MÉTRICAS DEL MODELO (test set)")
    print("=" * 50)
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  F1-score:  {f1_score:.4f}")
    print(f"  AUC-ROC:   {auc_roc:.4f}")
    print("=" * 50 + "\n")


def run_batch_training(spark: SparkSession) -> None:
    features_df = _load_features(spark)

    print(f"Total de filas de features: {features_df.count()}")
    print(f"Símbolos: {[row.symbol for row in features_df.select('symbol').distinct().collect()]}")

    features_with_lags_df = _add_lag_features(features_df)
    print(f"Filas después de lag features y dropna: {features_with_lags_df.count()}")

    train_df, test_df = features_with_lags_df.randomSplit(
        [TRAIN_RATIO, TEST_RATIO], seed=RANDOM_SEED
    )
    print(f"Train: {train_df.count()} filas | Test: {test_df.count()} filas")

    ml_pipeline = _build_ml_pipeline()
    print("Entrenando RandomForestClassifier...")
    trained_pipeline_model = ml_pipeline.fit(train_df)

    predictions_df = trained_pipeline_model.transform(test_df)

    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="price_direction_label",
        predictionCol="prediction",
        metricName="accuracy",
    )
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="price_direction_label",
        predictionCol="prediction",
        metricName="f1",
    )
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="price_direction_label",
        rawPredictionCol="probability",
        metricName="areaUnderROC",
    )

    _print_evaluation_metrics(predictions_df, accuracy_evaluator, f1_evaluator, auc_evaluator)

    print(f"Guardando modelo en: {MODEL_OUTPUT_DIR}")
    trained_pipeline_model.write().overwrite().save(MODEL_OUTPUT_DIR)
    print("Modelo guardado correctamente.")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("crypto-batch-train")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_batch_training(spark)
