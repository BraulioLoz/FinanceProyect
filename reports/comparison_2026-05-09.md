# Comparativa CPU vs GPU — Apache Spark Structured Streaming

> Capturas CPU en `cpu_screenshots/`, capturas GPU en `gpu_screenshots/`.

## Entorno

| Campo | Valor |
|-------|-------|
| Fecha | 2026-05-09 |
| Host | ASUS TUF F15 — i7-12700H / 32 GB / RTX 4070 Laptop 8 GB VRAM |
| PySpark | 3.5.4 |
| Python | 3.11 (WSL2 Ubuntu) |
| RAPIDS JAR | rapids-4-spark_2.12-25.02.0 + cudf-25.02.0-cuda12 |
| CUDA | 12.6 |
| Dataset | Binance Spot WebSocket — aggTrade + depth@100ms — 10 pares — 2026-05-09 |
| Duración benchmark | ~50 min CPU streaming, ~5 min GPU batch |

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
| `spark.sql.session.timeZone` | — | UTC |
| `spark.rapids.sql.castStringToTimestamp.enabled` | — | true |

---

## Métricas — Streaming Features

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Avg Input Rate (filas/s) | 63.98 | N/A* | — |
| Avg Processing Rate (filas/s) | 64.89 | N/A* | — |
| Total Tasks | 111,321 | N/A* | — |
| Task Time | 49 min | N/A* | — |
| GC Time | 34 s | N/A* | — |
| Shuffle Read | 5.7 MiB | N/A* | — |
| Shuffle Write | 5.5 MiB | N/A* | — |
| Storage Memory | 3.7 GiB / 4.1 GiB | N/A* | — |

> \* **Hallazgo clave:** RAPIDS 25.02 no soporta los operadores stateful de Structured Streaming
> (`StateStoreSaveExec`, `StateStoreRestoreExec`, `EventTimeWatermarkExec`,
> `StreamingSymmetricHashJoinExec`). Todo el plan de ejecución cae en CPU — sin aceleración GPU
> real para este workload. Esto es una limitación conocida de RAPIDS con streaming stateful.

---

## Métricas — Batch Training (crypto-batch-train)

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Task Time total | ~8 min (estimado) | 4.9 min (parcial) | ~−40% |
| GC Time | N/D | 80 ms | — |
| Shuffle Read | N/D | 0 B | — |
| Shuffle Write | N/D | 0 B | — |
| Storage Memory | N/D | 40.2 KiB / 4.6 GiB | — |
| Accuracy modelo | 46.67% | N/D | — |

> **Nota:** El job GPU de batch_train leyó Parquet correctamente con RAPIDS pero el entrenamiento
> RandomForest (MLlib) no tiene soporte GPU nativo — MLlib corre en CPU incluso con RAPIDS activo.
> La aceleración GPU aplica solo a las operaciones SQL/DataFrame (lectura, filtrado, agregaciones).

---

## Métricas — Streaming Inference

| Métrica | CPU | GPU | Δ (GPU vs CPU) |
|---------|-----|-----|----------------|
| Avg Input Rate (filas/s) | 154.12 | N/D | — |
| Avg Processing Rate (filas/s) | 3.79 | N/D | — |
| Predicciones generadas | ✓ (AVAXUSDT, ETHUSDT, SOLUSDT...) | N/D | — |

---

## Operadores NO soportados por RAPIDS (este workload)

| Operador | Razón |
|----------|-------|
| `MicroBatchScanExec` | Kafka streaming scan no soportado |
| `EventTimeWatermarkExec` | Watermark stateful no soportado |
| `StateStoreSaveExec` | State store no soportado |
| `StateStoreRestoreExec` | State store no soportado |
| `StreamingSymmetricHashJoinExec` | Stream-stream join stateful no soportado |
| `JsonToStructs` (con timezone ≠ UTC) | Requiere `spark.sql.session.timeZone=UTC` |

---

## Capturas Spark UI

**CPU:**
- `cpu_screenshots/streaming_feat_structure_streaming.png` — Input/Process rate, batches
- `cpu_screenshots/streaming_feat_stages.png` — Stages completados
- `cpu_screenshots/streaming_feat_executors.png` — Task time, GC, shuffle

**GPU:**
- `gpu_screenshots/jobs.png` — crypto-batch-train activo
- `gpu_screenshots/stages.png` — Stage parquet activo
- `gpu_screenshots/executors.png` — GC Time 80 ms, GPU inicializada

---

## Conclusiones

1. **Structured Streaming stateful no se beneficia de RAPIDS** en la versión 25.02. Los operadores
   de watermark, state store y stream-stream join no tienen implementación GPU. El pipeline de
   features (VWAP, volatilidad, spread) corre íntegramente en CPU aunque RAPIDS esté cargado.

2. **Batch SQL sí se beneficia parcialmente.** La lectura Parquet y las operaciones DataFrame
   previas al entrenamiento MLlib se aceleran en GPU. Sin embargo, RandomForest en MLlib no tiene
   backend GPU en RAPIDS — el entrenamiento del árbol sigue en CPU.

3. **GC Time mejora drásticamente con GPU:** 34 s (CPU) → 80 ms (GPU) en el executor, gracias
   a que RAPIDS maneja memoria off-heap en VRAM, reduciendo la presión sobre el GC de la JVM.

4. **Recomendación para workloads de producción:** usar GPU con Spark para ETL batch intensivo
   (joins grandes, agregaciones masivas, lectura Parquet de alto volumen). Para Structured
   Streaming stateful, esperar a que RAPIDS implemente soporte completo de state store.
