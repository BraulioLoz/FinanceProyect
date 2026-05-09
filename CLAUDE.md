# CLAUDE.md — Proyecto: Cripto HF + Kafka + Spark (CPU vs GPU)

> Asistentes LLM: leer este archivo completo antes de proponer cualquier cambio. La fuente de verdad del diseño es `contexto.md`. No inventar stack alternativo al definido aquí.

---

## Índice de documentación por dominio

| Módulo | Documento |
|--------|-----------|
| Ingesta WebSocket → Kafka | [ingest/README.md](ingest/README.md) |
| Procesamiento Spark | [spark/README.md](spark/README.md) |
| Infraestructura Docker / WSL | [infra/README.md](infra/README.md) |

---

## Contexto académico

- **Curso / institución:** ITAM — Arquitectura de Grandes Volúmenes de Datos.
- **Objetivo principal:** comparar **Apache Spark Structured Streaming en CPU vs GPU** (NVIDIA RAPIDS Accelerator for Apache Spark, `rapids-4-spark`) en la misma máquina local (WSL2 Ubuntu en Windows), midiendo throughput y tiempos con **Spark Application UI** (`http://localhost:4040`).
- **Entregables:** capturas de Spark UI, tabla comparativa CPU vs GPU, carpeta `reports/<fecha>/<modo>/`, dashboard en **Power BI Desktop** (Windows) leyendo Parquet desde `\\wsl$\Ubuntu\home\<usuario>\data\crypto`.
- **Dominio:** análisis multivariado de criptoactivos en alta frecuencia — trades y libro de órdenes de al menos 10 pares de alta liquidez.

---

## Host de desarrollo

**ASUS TUF Gaming F15 FX507ZI** — toda la comparativa CPU vs GPU corre en esta máquina:

| Componente | Especificación |
|------------|---------------|
| CPU | Intel i7-12700H — 14 cores (6P + 8E) / 20 threads / 2.3 GHz base |
| RAM | 32 GB |
| GPU | NVIDIA RTX 4070 Laptop GPU — 8 GB VRAM |
| Disco | 1 TB NVMe WD SN560 |
| OS | Windows 11 + **WSL2 Ubuntu** |

**Configuración Spark recomendada para este host:**

| Parámetro | CPU | GPU |
|-----------|-----|-----|
| `spark.driver.memory` | `8g` | `8g` |
| `spark.executor.memory` | `16g` | `16g` |
| `spark.executor.cores` | `10` | `10` |
| `spark.sql.shuffle.partitions` | `20` | `20` |
| `spark.rapids.memory.pinnedPool.size` | — | `2g` |
| `spark.executor.resource.gpu.amount` | — | `1` |

---

## Stack fijo

| Pieza | Elección |
|-------|----------|
| Lenguaje | Python **3.11+** (wheels de WSL) |
| Spark | **3.5.x** — pin: `pyspark==3.5.4` (último parche estable) |
| Kafka | **Docker KRaft** (sin Zookeeper) — `infra/docker-compose.kafka.yml` |
| GPU Spark | `rapids-4-spark` compatible con Spark 3.5.x + **CUDA 12.x** en WSL2 |
| Ingesta | `asyncio` + `websockets` → `confluent-kafka` |
| Fuente de datos | **Binance USDS-Margined Futures WebSocket** — streams `aggTrade` y `depth@100ms` |
| ML | **Spark MLlib** preferido; justificar cualquier lib extra |
| Almacenamiento | Parquet local `~/data/crypto/` en WSL |
| Visualización | **Power BI Desktop** (Windows) vía `\\wsl$\Ubuntu\home\<usuario>\data\crypto` |
| Estado/offsets | Sin Postgres. SQLite opcional si hace falta |

---

## Arquitectura de referencia

```
WebSocket_trades ──┐
                   ├─→ ingest/ (productores asyncio) ─→ Kafka KRaft (Docker)
WebSocket_book  ───┘                                          │
                                                              ▼
                                              Spark Structured Streaming
                                            ┌─────────────────────────────┐
                                            │ Streaming: features, anomalías │
                                            │ Batch: entrenamiento ML       │
                                            │ Inferencia: scoring streaming │
                                            └────────────┬────────────────┘
                                                         ▼
                                               Parquet ~/data/crypto/
                                                         │
                                                         ▼
                                             Power BI Desktop (Windows)
```

Replayer: `spark/replayer/` lee Parquet histórico e inyecta a Kafka para reproducir cargas.

---

## Contrato Kafka

### Topics

| Topic | Contenido |
|-------|-----------|
| `crypto-trades` | Ejecuciones / trades normalizados |
| `crypto-book` | Actualizaciones de libro (snapshots o deltas) |
| `crypto-features` | Features por ventana (salida de Spark) |
| `crypto-pred` | Predicciones / scores del modelo |

- **Particiones:** 8 por topic en desarrollo.
- **Replication factor:** 1 (broker único).

### Envelope JSON obligatorio

Todo mensaje publicado a Kafka debe ser un objeto JSON UTF-8 con estos campos mínimos:

```json
{
  "symbol":     "BTCUSDT",
  "exchange":   "binance",
  "event_type": "trade",
  "ts_event":   "2024-01-15T12:00:00.123Z",
  "ts_ingest":  "2024-01-15T12:00:00.456Z",
  "payload":    { ... }
}
```

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `symbol` | string | Ej. `BTCUSDT` — usar siempre la misma convención sin `/` |
| `exchange` | string | Ej. `binance`, `kraken` |
| `event_type` | string | `trade`, `book_ticker`, `depth_update`, `feature_row`, `prediction` |
| `ts_event` | string ISO-8601 UTC | Timestamp del evento en el exchange; si no existe, mejor esfuerzo |
| `ts_ingest` | string ISO-8601 UTC | Timestamp de ingesta al sistema |
| `payload` | object | Carga útil normalizada (precio, cantidad, niveles, etc.) |

Los consumers Spark parsean el envelope y usan `ts_event` como campo de watermark.

---

## Reglas Spark

1. Toda agregación temporal usa `window(...) + withWatermark(...)` sobre `ts_event`.
2. Cada query de streaming tiene su propio `checkpointLocation` único bajo `$SPARK_CHECKPOINT_BASE/<nombre_query>/`.
3. Comparativa CPU vs GPU: misma versión PySpark, mismos datos (mismo replay), mismos parámetros; solo cambian los archivos `spark/conf/spark-cpu.conf` / `spark/conf/spark-gpu.conf` y los JARs RAPIDS.
4. Métricas a documentar en `reports/<fecha>/<modo>/`: Input Rate, Processing Rate, Batch Duration (p95), cola de batches, Shuffle Read/Write, GC Time, Scheduler Delay, Spill.

---

## Versiones pin

```
python       == 3.11.*
pyspark      == 3.5.4
kafka-python >= 2.0.0   # o confluent-kafka >= 2.3.0
websockets   >= 12.0
rapids-4-spark <ver>    # confirmar build compatible con Spark 3.5.x + CUDA 12.x
cuda         12.x        # toolkit en WSL (no instalar nvidia-driver-* con apt)
```

---

## Estructura de carpetas

```
PROYECTO/
  ingest/               # Productores WebSocket → Kafka
  spark/
    jobs/               # Structured Streaming, batch training, inferencia
    replayer/           # Replay Parquet → Kafka para benchmarks
    conf/               # spark-cpu.conf, spark-gpu.conf
  infra/
    docker-compose.kafka.yml
    scripts/            # create_topics.sh, run_spark_cpu.sh, run_spark_gpu.sh
  data/                 # gitignored — Parquet local (~/data/crypto/ en WSL)
  models/               # gitignored — modelos entrenados
  reports/              # capturas Spark UI, tablas comparativas
  .env.example          # plantilla de variables de entorno
  .env                  # gitignored — valores reales
  contexto.md           # fuente de verdad del diseño
  CLAUDE.md             # este archivo
```

---

## Lista "no usar" (salvo petición explícita del usuario)

- Google Colab, ngrok, Google Drive como datalake.
- Amazon S3 u otro object storage cloud como ruta principal de datos.
- PostgreSQL u otra BD operacional por defecto.
- Apache Pulsar, AWS Kinesis u otro broker en lugar de Kafka.
- Databricks, EMR u otras plataformas cloud gestionadas.
- Omitir la comparativa CPU/GPU — el curso la exige.

---

## Convenciones de código

- **Type hints** obligatorios en todas las funciones públicas.
- Formatter: `black` + `isort`; linter: `ruff`.
- Tests de productores con mocks de Kafka (`unittest.mock` o `pytest-mock`); nunca depender de un broker real en CI.
- Dependencias nuevas deben justificarse en el PR con referencia al curso.
- Nombres de variables de entorno: `UPPER_SNAKE_CASE`; leer siempre desde `.env` vía `python-dotenv`.
- Commits en inglés o español consistente; usar prefijos `feat:`, `fix:`, `docs:`, `chore:`.
- No commitear `.env`, `data/`, `models/`, `*.parquet`, checkpoints de Spark.
