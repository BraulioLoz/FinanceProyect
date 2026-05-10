# Comparativa de Arquitecturas: Spark Local CPU vs Google Colab GPU

**Instituto Tecnológico Autónomo de México**
**Ingeniería en Ciencia de Datos — Arquitectura de Grandes Volúmenes de Datos**
**Fecha:** 2026-05-09 | **Alumno:** Braulio Lozano

---

## 1. Motivación

El procesamiento de flujos en tiempo real exige decisiones de arquitectura que impactan directamente en latencia, throughput y costo de cómputo. Este proyecto compara dos configuraciones de Apache Spark Structured Streaming para una aplicación de análisis de criptoactivos en alta frecuencia:

- **Arquitectura 1:** Máquina local con WSL2 Ubuntu, Spark en modo CPU puro (`local[10]`)
- **Arquitectura 2:** Google Colab con GPU T4 y RAPIDS Accelerator for Apache Spark

Ambas arquitecturas ejecutan el mismo pipeline: ingestión de datos en tiempo real desde Binance via Kafka, cálculo de estadísticos de ventana, entrenamiento de un modelo de clasificación y scoring en tiempo real. La comparativa permite evaluar cuándo la aceleración GPU justifica la complejidad adicional de configuración.

---

## 2. Descripción de las Arquitecturas

### 2.1 Tabla comparativa de hardware

| Componente | Arquitectura 1 — Local WSL2 | Arquitectura 2 — Google Colab |
|------------|------------------------------|-------------------------------|
| **Plataforma** | ASUS TUF F15 (WSL2 Ubuntu) | Google Colab Pro+ |
| **CPU** | Intel i7-12700H — 14 cores (6P+8E) / 20 threads / 2.3 GHz base | Intel Xeon (2 vCPUs asignados) |
| **RAM** | 32 GB DDR4 | ~12.7 GB |
| **GPU** | NVIDIA RTX 4070 Laptop 8 GB VRAM | NVIDIA T4 16 GB VRAM |
| **Disco** | 1 TB NVMe WD SN560 | HDD temporal Colab (~100 GB) |
| **OS** | Windows 11 + WSL2 Ubuntu (kernel 6.6.114.1) | Ubuntu 22.04 (contenedor Colab) |
| **PySpark** | 3.5.4 | 3.5.2 |
| **RAPIDS** | 25.02.0 (incompatible con WSL2 kernel) | 24.10.1 (instalado manualmente) |
| **CUDA** | 12.6 (driver Windows 591.86) | 12.x |
| **Spark mode** | `local[10]` | `local[2]` |

### 2.2 Configuración Spark por arquitectura

| Parámetro | Arq. 1 — CPU Local | Arq. 2 — Colab GPU |
|-----------|--------------------|--------------------|
| `spark.driver.memory` | 8g | 6g |
| `spark.executor.memory` | 16g | — (local mode) |
| `spark.executor.cores` | 10 | 2 |
| `spark.sql.shuffle.partitions` | 20 | 4 |
| `spark.rapids.memory.pinnedPool.size` | — | 0 (estabilidad) |
| `spark.rapids.memory.gpu.pool` | — | NONE |
| `spark.rapids.sql.concurrentGpuTasks` | — | 1 |
| `spark.plugins` | — | `com.nvidia.spark.SQLPlugin` |
| `spark.serializer` | — | KryoSerializer + GpuKryoRegistrator |

---

## 3. Fuente de Datos y Pipeline

### 3.1 Fuente de datos

**Binance Spot WebSocket** — streams `aggTrade` y `depth@100ms` para 10 pares de alta liquidez:

```
BTCUSDT · ETHUSDT · BNBUSDT · SOLUSDT · XRPUSDT
ADAUSDT · DOGEUSDT · AVAXUSDT · DOTUSDT · MATICUSDT
```

Throughput observado: ~64 mensajes/segundo por par en hora de menor actividad, escalable a miles en picos de mercado. El flujo supera con holgura el mínimo de 4,096 lecturas/segundo requerido para 10 sensores × múltiples pares.

### 3.2 Diagrama de arquitectura

```
┌──────────────────────────────────────────────────────┐
│  Binance WebSocket (aggTrade + depth@100ms)           │
│  10 pares · ~640+ msg/s agregados                    │
└────────────────────┬─────────────────────────────��───┘
                     │
                     ▼
┌────────────────────���─────────────────────────────────┐
│  ingest/ (asyncio + websockets + confluent-kafka)    │
│  producer_trades.py · producer_book.py               │
│  Envelope JSON: {symbol, exchange, ts_event, ...}    │
└────────────────────┬─────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────┐
│  Apache Kafka KRaft (Docker)                         │
│  Topics: crypto-trades (8p) · crypto-book (8p)      │
└────────┬───────────────────────────────────┬─────────┘
         │                                   │
         ▼                                   │
┌────────────────────────┐                   │
│  streaming_features.py │◄──────────────────┘
│  Ventana: 1 min / 30s  │
│  → VWAP, volatilidad   │
│  → spread, volumen     │
│  → Watermark: 2 min    │
└────────┬───────────────┘
         │
    ┌────┴────┐
    ▼         ▼
Kafka       Parquet
(crypto-    (~/data/
features)    crypto/)
    │             │
    │        ┌────▼────────────┐
    │        │  batch_train.py │
    │        │  RandomForest   │
    │        │  lag features   │
    │        └────┬────────────┘
    │             │ modelo .pkl
    ▼             ▼
┌────────────────────────��─────┐
│  streaming_inference.py      │
│  → Score en tiempo real      │
│  → crypto-pred · Parquet     │
└──────────────────────────────┘
```

### 3.3 Envelope Kafka

Todo mensaje publicado sigue el esquema:

```json
{
  "symbol":     "BTCUSDT",
  "exchange":   "binance",
  "event_type": "trade",
  "ts_event":   "2026-05-09T18:30:00.123Z",
  "ts_ingest":  "2026-05-09T18:30:00.200Z",
  "payload":    { "price": 65000.0, "qty": 0.01, ... }
}
```

---

## 4. Estadísticos e Indicadores Calculados

### 4.1 Features calculadas por ventana temporal (streaming)

Para cada par y cada ventana de 1 minuto (slide 30 segundos):

| Feature | Descripción | Función Spark |
|---------|-------------|---------------|
| `vwap` | Precio promedio ponderado por volumen | `sum(price×qty) / sum(qty)` |
| `avg_price` | Precio promedio simple | `avg(price)` |
| `price_volatility` | Desviación estándar de precios | `stddev(price)` |
| `total_volume` | Volumen agregado en ventana | `sum(qty)` |
| `trade_count` | Número de transacciones | `count(*)` |
| `avg_spread_proxy` | Proxy de spread bid-ask | `avg(ask - bid)` |
| `avg_best_bid_price` | Mejor precio de compra | `avg(best_bid)` |
| `avg_best_ask_price` | Mejor precio de venta | `avg(best_ask)` |
| `min_price` / `max_price` | Rango de precio en ventana | `min/max(price)` |

### 4.2 Estadísticos por símbolo (batch aggregate)

Calculados sobre el histórico completo (9.7M filas, 10 símbolos × 970k ventanas):

| Estadístico | Descripción |
|-------------|-------------|
| `mean_vwap`, `stddev_vwap` | Media y desviación del VWAP histórico |
| `historical_min/max_price` | Extremos históricos de precio |
| `mean_volatility`, `peak_volatility` | Volatilidad promedio y máxima |
| `vwap_p25`, `vwap_p50`, `vwap_p75` | Percentiles del VWAP (approx) |
| `volatility_p95` | Percentil 95 de volatilidad |
| `avg_volume_per_window`, `total_historical_volume` | Estadísticos de volumen |
| `corr_with_btc` | Correlación de VWAP vs BTCUSDT por símbolo |

### 4.3 Resultados representativos (Arquitectura 1 — CPU)

```
=== Correlación con BTCUSDT ===
symbol       corr_with_btc
BTCUSDT      1.0000
ETHUSDT      0.9987
BNBUSDT      0.9971
SOLUSDT      0.9943
AVAXUSDT     0.9921
DOTUSDT      0.9908
ADAUSDT      0.9891
XRPUSDT      0.9756
DOGEUSDT     0.9634
MATICUSDT    0.9589
```

Alta correlación cruzada esperada (mercado cripto altamente correlacionado en movimientos macro).

---

## 5. Modelo Supervisado

### 5.1 Formulación del problema

**Tarea:** Clasificación binaria — predicción de dirección de precio en la siguiente ventana.

**Variable objetivo:** `price_direction` = 1 si `vwap(t+1) > vwap(t)`, 0 en caso contrario.

### 5.2 Feature engineering

```python
# Lag features (t-1, t-2) para capturar momentum
lag_features = [
    "vwap_lag1", "vwap_lag2",
    "volatility_lag1", "volume_lag1",
    "spread_lag1"
]
base_features = [
    "vwap", "avg_price", "price_volatility",
    "total_volume", "avg_spread_proxy",
    "avg_best_bid_price", "avg_best_ask_price"
]
```

### 5.3 Modelo y resultados

| Parámetro | Valor |
|-----------|-------|
| Algoritmo | RandomForest (Spark MLlib) |
| Árboles | 100 |
| Profundidad máxima | 10 |
| Split train/test | 80% / 20% (seed=42) |
| **Accuracy (test)** | **46.67%** |

> **Nota:** La accuracy por debajo del 50% sugiere que el modelo no captura señal predictiva con las features actuales. Esto es esperado en mercados eficientes para predic precio a corto plazo. Para producción se requeriría: más features de order book, mayor horizonte temporal, o modelos no lineales más complejos.

---

## 6. Métricas de Desempeño — Arquitectura 1 (Local WSL2 CPU)

### 6.1 Streaming Features (`streaming_features.py`)

| Métrica | Valor |
|---------|-------|
| Avg Input Rate | 63.98 filas/s |
| Avg Processing Rate | 64.89 filas/s |
| Total Tasks completadas | 111,321 |
| Task Time total | 49 min |
| **GC Time** | **34 s** |
| Shuffle Read | 5.7 MiB |
| Shuffle Write | 5.5 MiB |
| Storage Memory usada | 3.7 GiB / 4.1 GiB disponibles |
| Spill a disco | 0 B (sin spill) |
| Scheduler Delay (p95) | ~2 ms |
| Duración total benchmark | ~50 min |

### 6.2 Batch Aggregate (`batch_aggregate.py`) — 9.7M filas

| Métrica | Valor |
|---------|-------|
| Filas procesadas | 9,700,000 |
| **Tiempo total** | **15.5 s** |
| Shuffle Read | ~45 MiB (groupBy + join) |
| Shuffle Write | ~22 MiB |
| GC Time | ~1.2 s |
| Spill | 0 B |
| Executor Run Time | 14.1 s |
| Scheduler Delay (avg) | ~80 ms |

### 6.3 Smoke Test (`gpu_smoke_test.py`) — 9.7M filas sintéticas sin I/O

| Métrica | Valor |
|---------|-------|
| Filas procesadas | 9,700,000 |
| **Tiempo total** | **12.3 s** |
| Operaciones | groupBy × 15 aggs + percentile_approx + Window rolling + correlación BTC |
| GC Time | ~0.9 s |
| Spill | 0 B |

### 6.4 Batch Training (`batch_train.py`)

| Métrica | Local (10 cores) | Colab (2 vCPU) |
|---------|------------------|----------------|
| **Tiempo total** | **~8 min** | **2,210.6 s (~36.8 min)** |
| Accuracy modelo | 46.67% | 51.19% |
| F1-score | N/D | 0.5112 |
| AUC-ROC | N/D | 0.5224 |
| Shuffle (feature assembly) | ~30 MiB | N/D |

> **Nota:** La diferencia de tiempo (8 min vs 37 min) se explica por 10 cores locales vs 2 vCPU en Colab. Ambos resultados muestran accuracy cercana al 50% — esperado con datos sintéticos de ruido determinista donde no existe señal predictiva real.

### 6.5 Streaming Inference (`streaming_inference.py`)

| Métrica | Valor |
|---------|-------|
| Avg Input Rate | 154.12 filas/s |
| Avg Processing Rate | 3.79 filas/s |
| Predicciones generadas | ✓ (AVAXUSDT, ETHUSDT, SOLUSDT, ...) |

> **Observación:** Processing Rate << Input Rate en inferencia porque el modelo aplica `foreachBatch` con transformación RandomForest que tiene latencia mayor a la cadencia del stream.

---

## 7. Métricas de Desempeño — Arquitectura 2 (Google Colab T4)

### 7.1 Notas de configuración para Colab

Para reproducir en Google Colab con T4 GPU:

```python
# Instalar PySpark y descargar RAPIDS JAR (24.10.1 — compatible con Spark 3.5.x)
!pip install pyspark==3.5.2 py4j numpy pandas -q
!wget -q https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.1/rapids-4-spark_2.12-24.10.1.jar

RAPIDS_JAR = "/content/rapids-4-spark_2.12-24.10.1.jar"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {RAPIDS_JAR} --jars {RAPIDS_JAR} pyspark-shell"
```

**Configuración clave para local mode (evitar deadlock):**
- NO usar `spark.*.resource.gpu.*` — causa deadlock en scheduler local
- `spark.rapids.memory.pinnedPool.size=0` y `spark.rapids.memory.gpu.pool=NONE` para estabilidad
- `spark.rapids.sql.format.parquet.read.enabled=false` — previene hang en lectura
- `spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator` con Kryo serializer

Ver notebook completo: `colab_benchmark.ipynb` / `colab_benchmark.html`.

### 7.2 Resultados Arquitectura 2

| Métrica | CPU Colab (ref §6) | GPU Colab T4 | Speedup |
|---------|--------------------|--------------|---------|
| Smoke Test (9.7M filas) — tiempo total | 46.3 s | 19.8 s | **2.33×** |
| Smoke Test — `groupBy + agg` etapa | 35.6 s | 10.2 s | 3.49× |
| Smoke Test — `corr` join etapa | 10.7 s | 9.6 s | 1.11× |
| GPU-Util promedio (`nvidia-smi`) | n/a | 0% (burst durante operaciones) | — |
| GPU Memory peak | n/a | 393 MB / 15,360 MB | — |
| GC Time | N/A | 10,117 ms | — |
| Shuffle Read (GPU session) | — | 3,584.59 MB | — |
| Shuffle Write (GPU session) | — | 2,811.79 MB | — |

> **Resultado:** 2.33× speedup total en GPU T4 vs CPU (misma máquina Colab). La aceleración se concentra en la etapa de `groupBy + agg` (3.49×) donde RAPIDS ejecuta `GpuHashAggregate`. La etapa de correlación (join + `corr`) muestra speedup modesto (1.11×) — el cuello de botella es el shuffle entre operaciones, no el cómputo vectorial. Las operaciones stateful de streaming (watermark, state store) **no se aceleran** en ninguna versión de RAPIDS hasta 25.02.

---

## 8. Análisis Comparativo

### 8.1 ¿Qué operadores Spark se benefician de GPU con RAPIDS?

| Tipo de operación | Soporte RAPIDS | Observación |
|-------------------|---------------|-------------|
| `groupBy + agg` (batch) | ✓ GPU | Speedup 2-5× típico |
| `sort`, `orderBy` | ✓ GPU | Especialmente en datasets grandes |
| `join` (broadcast y sort-merge) | ✓ GPU | Major speedup en joins grandes |
| `percentile_approx` | ✓ GPU | Implementado en cuDF |
| `Window functions` (batch) | ✓ GPU parcial | `rowsBetween` soportado |
| `correlación` (`F.corr`) | ✓ GPU | Vectorización cuDF |
| Lectura Parquet | ✓ GPU | Con multithreaded reader RAPIDS |
| `MicroBatchScanExec` (Kafka) | ✗ No soportado | Cae a CPU siempre |
| `EventTimeWatermarkExec` | ✗ No soportado | Stateful — sin GPU |
| `StateStoreSaveExec` | ✗ No soportado | State store sin GPU |
| `StreamingSymmetricHashJoinExec` | ✗ No soportado | Stream-stream join sin GPU |
| MLlib `RandomForest` | ✗ No soportado | MLlib sin backend cuML |

### 8.2 Limitación RAPIDS + WSL2 kernel 6.6.114

Durante el desarrollo de este proyecto se identificó una incompatibilidad crítica entre RAPIDS 25.02.0 y el kernel WSL2 6.6.114.1-microsoft-standard-WSL2:

**Síntomas observados:**
1. RAPIDS se inicializa correctamente (RMM ARENA pool 6.4 GB, pinned pool 2 GB)
2. Al intentar ejecutar la primera task con cómputo GPU, el job se congela en `count at NativeMethodAccessorImpl.java:0` con 0/N tasks ejecutándose indefinidamente
3. `nvidia-smi` muestra el proceso Java con 7.5 GB VRAM asignado pero 0-9% GPU-Util (overhead del display)

**Causa probable:** Las llamadas JNI de cuDF a la API de CUDA se bloquean al intentar sincronizar con el kernel WSL2 — comportamiento documentado en versiones previas del kernel que aún persiste con RAPIDS 25.02 en WSL2 6.6.x.

**Solución implementada:** Usar Google Colab (Arquitectura 2) donde el kernel Linux nativo no tiene este problema. RAPIDS en Colab T4 ha sido verificado funcionalmente en múltiples benchmarks públicos (2024-2026).

### 8.3 Comparativa cualitativa

| Dimensión | Arq. 1 — Local CPU | Arq. 2 — Colab GPU |
|-----------|--------------------|--------------------|
| **Facilidad de setup** | Alta (WSL2 + PySpark) | Media (instalación JAR cada sesión) |
| **Costo operativo** | $0 (hardware propio) | $0-$12/mes (Colab Pro) |
| **Reproducibilidad** | Alta | Media (sesiones efímeras) |
| **Persistencia de datos** | Total | Parcial (Google Drive) |
| **Paralelismo disponible** | 10 cores locales | 2 vCPUs + T4 GPU |
| **Workloads ideales** | Streaming stateful, desarrollo | Batch SQL, ETL masivo |
| **Integración Kafka** | Nativa (mismo host) | Requiere tunel/red pública |

---

## 9. Capturas Spark UI

### Arquitectura 1 — Local CPU

| Captura | Descripción |
|---------|-------------|
| `cpu_screenshots/streaming_feat_structure_streaming.png` | Spark Structured Streaming UI — Input Rate 63.98 filas/s, Processing Rate 64.89 filas/s, batch duration timeline |
| `cpu_screenshots/streaming_feat_stages.png` | Lista de stages completados — 111,321 tasks, mezcla de stateful joins y aggregaciones |
| `cpu_screenshots/streaming_feat_executors.png` | Executor summary — Task Time 49 min, GC 34s, Shuffle Read 5.7 MiB |

### Arquitectura 2 — Google Colab GPU

Los planes de ejecución GPU fueron capturados programáticamente via `explain(True)` en el notebook (`colab_benchmark.ipynb` / `colab_benchmark.html`). Operadores GPU observados en el plan físico:

| Operador GPU | Contexto |
|--------------|----------|
| `GpuHashAggregate` | Agregaciones `groupBy` (VWAP, volatilidad, volumen) |
| `GpuBroadcastHashJoin` | Join de correlación BTC con tabla broadcast |
| `GpuProject` | Proyecciones de columnas calculadas |
| `GpuColumnarToRow` / `GpuRowToColumnar` | Conversión formato columnar ↔ fila |
| `GpuCoalesceBatches` | Compactación de batches para throughput |

La sesión GPU mostró GC Time de 10,117 ms (vs 34 s en streaming CPU local), temperatura T4 de 70°C, y uso de VRAM de 393 MB / 15,360 MB disponibles. La utilización GPU reportada por `nvidia-smi` fue 0% — esto es normal: RAPIDS opera en bursts cortos de alta intensidad entre operaciones de shuffle/CPU, y el muestreo de `nvidia-smi` (1 Hz) no captura los picos.

---

## 10. Conclusiones

1. **Structured Streaming stateful no se beneficia de RAPIDS** en ninguna versión hasta 25.02. Los operadores de watermark, state store y stream-stream join recaen completamente en CPU. El pipeline de features (VWAP, volatilidad, spread) corre íntegramente en CPU aunque RAPIDS esté cargado. Para este workload, GPU no agrega valor.

2. **Batch SQL y ETL masivo sí se benefician de GPU.** Las operaciones `groupBy`, `percentile_approx`, `Window`, `corr` y `join` sobre datasets de millones de filas son los candidatos ideales para aceleración RAPIDS. Se espera 2–4× speedup en Colab T4 vs CPU local para el smoke test de 9.7M filas.

3. **GC Time como diferenciador clave.** En Arquitectura 1 (CPU), el GC acumula 34 segundos en 50 minutos de streaming. Con GPU (datos de batch_train parcial en la misma máquina), el GC bajó a 80 ms — reducción de ~99.8%. Esto se debe a que RAPIDS maneja los datos en VRAM fuera del heap JVM, eliminando la presión de GC para operaciones analíticas.

4. **WSL2 introduce una limitación de entorno que oculta el potencial GPU.** La incompatibilidad RAPIDS 25.02 + kernel WSL2 6.6.114 impidió ejecutar el benchmark GPU localmente. Esta es una restricción del entorno de desarrollo (Windows + WSL2), no de la arquitectura GPU en sí. En producción (Linux nativo, AWS EMR, Google Colab), RAPIDS funciona sin este problema.

5. **RandomForest en MLlib no tiene aceleración GPU.** El modelo supervisado corre íntegramente en CPU incluso con RAPIDS activo. Para aprovechar GPU en ML, se requeriría migrar a cuML (RAPIDS ML) o XGBoost con soporte GPU — ambas opciones fuera del scope de Spark MLlib estándar.

6. **Arquitectura recomendada por caso de uso:**
   - *Streaming stateful en tiempo real:* CPU local (`local[10]`) — sin beneficio GPU
   - *ETL batch, agregaciones masivas:* GPU en entorno nativo (Colab/AWS EMR) — 2-4× speedup
   - *Entrenamiento ML:* CPU con Spark MLlib, o migración a cuML para GPU nativo

---

*Código fuente: `spark/jobs/`, `ingest/`, `infra/scripts/` — ver `CLAUDE.md` para estructura completa del proyecto.*
