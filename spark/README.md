# spark/ — Procesamiento Spark Structured Streaming

Módulo de procesamiento: streaming online, entrenamiento batch y scoring en tiempo real. La línea CPU/streaming usa **PySpark 3.5.4**; los jobs con **RAPIDS** en GPU usan **PySpark 3.5.2** y `rapids-4-spark_2.12-24.10.1.jar` (ver `CLAUDE.md` → Versiones pin).

---

## Benchmark CPU vs GPU (Parquet)

| Script | Rol |
|--------|-----|
| `spark/jobs/gen_synth_parquet.py` | Genera una vez `~/data/crypto/synth.parquet` (modo CPU). |
| `spark/jobs/gpu_smoke_test.py` | Lee Parquet y ejecuta agregaciones; mismo código para CPU (`run_spark_cpu.sh`) y GPU (`run_spark_gpu.sh`). |
| `spark/replayer/inflate_features.py` | Infla features reales para pruebas de escala con `batch_aggregate.py`. |

---

## Configuración recomendada para el host

Host: ASUS TUF F15 — i7-12700H / 32 GB RAM / RTX 4070 Laptop 8 GB VRAM.

Ver archivos de configuración en `spark/conf/` y valores en `.env.example`. Resumen:

| Parámetro | CPU | GPU |
|-----------|-----|-----|
| `spark.driver.memory` | `8g` | `8g` |
| `spark.executor.memory` | `16g` | `16g` |
| `spark.executor.cores` | `10` | `10` |
| `spark.sql.shuffle.partitions` | `20` | `20` |
| `spark.rapids.memory.pinnedPool.size` | — | `2g` |
| `spark.executor.resource.gpu.amount` | — | `1` |

---

## Topics Kafka y particiones

| Topic | Contenido | Particiones | Replication |
|-------|-----------|:-----------:|:-----------:|
| `crypto-trades` | Trades normalizados | 8 | 1 |
| `crypto-book` | Actualizaciones de libro | 8 | 1 |
| `crypto-features` | Features por ventana (output de streaming) | 8 | 1 |
| `crypto-pred` | Predicciones del modelo | 8 | 1 |

- 8 particiones en desarrollo; ajustar si el throughput sostenido supera ~4 000 msg/s por topic.
- Replication factor 1 (broker único local).

---

## Separación de jobs

### 1. Streaming (online)

Archivo de referencia: `spark/jobs/streaming_features.py`

- Lee `crypto-trades` y `crypto-book` desde Kafka.
- Parsea el envelope JSON y extrae `ts_event` como campo temporal.
- Aplica `withWatermark("ts_event", "10 seconds")` antes de cualquier `window(...)`.
- Calcula por ventana (ej. 1 s / 5 s / 1 min):
  - **VWAP** (Volume Weighted Average Price)
  - **Volatilidad móvil** (desviación estándar de precios)
  - Volumen acumulado, spread proxy (si hay datos de libro)
  - **Detección de anomalías:** STREAM-LOF o DBSCAN online sobre vectores de features (con muestreo si el throughput lo exige).
- Escribe features a `crypto-features` (Kafka) y a Parquet en `$DATA_DIR/features/`.
- Cada query tiene su propio `checkpointLocation`: `$SPARK_CHECKPOINT_BASE/streaming_features/`.

### 2. Batch (entrenamiento ML)

Archivo de referencia: `spark/jobs/batch_train.py`

- Lee Parquet histórico desde `$DATA_DIR/features/`.
- Construye features de ventanas previas para predecir si el precio sube o baja en los próximos 5 minutos.
- Entrena con **Spark MLlib**: `RandomForestClassifier` o `GBTClassifier`.
- Guarda el modelo en `$MODEL_DIR/`.
- Se ejecuta de forma manual o programada (no streaming).

### 3. Inferencia en tiempo real

Archivo de referencia: `spark/jobs/streaming_inference.py`

- Lee `crypto-features` desde Kafka.
- Carga el modelo entrenado desde `$MODEL_DIR/`.
- Aplica scoring sobre cada micro-batch.
- Escribe predicciones a `crypto-pred` (Kafka) y a Parquet en `$DATA_DIR/predictions/`.
- Checkpoint propio: `$SPARK_CHECKPOINT_BASE/streaming_inference/`.

---

## Métricas para `reports/`

Documentar en `reports/<fecha>/<modo>/` (modo = `cpu` o `gpu`):

| Métrica | Fuente |
|---------|--------|
| Input Rate (msg/s) | Spark UI → Streaming tab |
| Processing Rate (msg/s) | Spark UI → Streaming tab |
| Batch Duration p95 (ms) | Spark UI → Streaming tab |
| Cola de batches pendientes | Spark UI → Streaming tab |
| Shuffle Read / Write (bytes) | Spark UI → Stages |
| GC Time (ms) | Spark UI → Executors |
| Scheduler Delay (ms) | Spark UI → Streaming tab |
| Spill (disco, bytes) | Spark UI → Stages |

Capturar capturas de pantalla de `http://localhost:4040` al finalizar cada corrida.

---

## Comparativa CPU vs GPU

- Usar **el mismo dataset** (mismo replay desde `spark/replayer/`) para ambas corridas.
- Cambiar solo la configuración:
  - CPU: `spark/conf/spark-cpu.conf`
  - GPU: `spark/conf/spark-gpu.conf` + JARs `rapids-4-spark`
- Guardar resultados en `reports/<fecha>/cpu_*/` y `reports/<fecha>/gpu_*/`.
- La tabla comparativa final incluye todas las métricas de arriba para ambos modos.

---

## Replayer

`spark/replayer/` contiene un script que lee Parquet histórico e inyecta mensajes a Kafka a alta velocidad configurable, permitiendo reproducir la misma carga en corridas CPU y GPU sucesivas.

---

## Estructura de archivos esperada

```
spark/
  README.md
  jobs/
    streaming_features.py    # features + anomalías online
    batch_train.py           # entrenamiento ML offline
    streaming_inference.py   # scoring en tiempo real
  replayer/
    replay_parquet.py        # inyecta Parquet → Kafka
  conf/
    spark-cpu.conf           # configuración sin RAPIDS
    spark-gpu.conf           # configuración con RAPIDS + CUDA
```
