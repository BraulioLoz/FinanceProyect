"""
Fixtures compartidos para todos los tests de Spark.
La SparkSession se crea una sola vez por sesión de pytest (scope=session)
para evitar el overhead de inicialización en cada test.
"""
from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

# PySpark debe usar el mismo ejecutable de Python que está corriendo pytest.
# Sin esto, en Windows el worker no puede conectarse al driver.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("crypto-spark-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-test-checkpoints")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()
