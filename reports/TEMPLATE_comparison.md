# Comparativa CPU vs GPU — Apache Spark Structured Streaming

> Copiar esta plantilla a `reports/<YYYY-MM-DD>/comparison_table.md` antes de cada benchmark.
> Capturas de Spark UI en `reports/<YYYY-MM-DD>/cpu/` y `reports/<YYYY-MM-DD>/gpu/`.

## Entorno

| Campo | Valor |
|-------|-------|
| Fecha | |
| Host | ASUS TUF F15 — i7-12700H / 32 GB / RTX 4070 Laptop 8 GB VRAM |
| PySpark | 3.5.4 |
| Python | 3.10.x (WSL Ubuntu) |
| RAPIDS JAR | (completar versión) |
| CUDA | 12.x |
| Dataset | Replay Parquet — `<fuente>` — `<fecha_datos>` |
| Duración benchmark | minutos |

---

## Parámetros Spark

| Parámetro | CPU | GPU |
|-----------|-----|-----|
| `spark.driver.memory` | 8g | 8g |
| `spark.executor.memory` | 16g | 16g |
| `spark.executor.cores` | 10 | 10 |
| `spark.sql.shuffle.partitions` | 20 | 20 |
| `spark.rapids.memory.pinnedPool.size` | — | 2g |
| `spark.executor.resource.gpu.amount` | — | 1 |

---

## Métricas — Streaming Features

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Input Rate (msgs/s) | | | |
| Processing Rate (msgs/s) | | | |
| Batch Duration p50 (ms) | | | |
| Batch Duration p95 (ms) | | | |
| Cola de batches (max) | | | |
| Shuffle Read (MB) | | | |
| Shuffle Write (MB) | | | |
| GC Time (ms) | | | |
| Scheduler Delay (ms) | | | |
| Spill (MB) | | | |

## Métricas — Batch Training

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Tiempo total entrenamiento (s) | | | |
| Shuffle Read (MB) | | | |
| GC Time (ms) | | | |

## Métricas — Streaming Inference

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Input Rate (msgs/s) | | | |
| Processing Rate (msgs/s) | | | |
| Batch Duration p95 (ms) | | | |
| Scheduler Delay (ms) | | | |

---

## Capturas Spark UI

- `cpu/spark_ui_streaming_features.png`
- `cpu/spark_ui_batch_train.png`
- `cpu/spark_ui_inference.png`
- `gpu/spark_ui_streaming_features.png`
- `gpu/spark_ui_batch_train.png`
- `gpu/spark_ui_inference.png`

---

## Conclusiones

(Completar tras el benchmark)
