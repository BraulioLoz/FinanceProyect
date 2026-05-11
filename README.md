# Cripto HF: Spark CPU vs GPU — Benchmark en Tiempo Real

**Instituto Tecnologico Autonomo de Mexico (ITAM)**
Ingenieria en Ciencia de Datos — Arquitectura de Grandes Volumenes de Datos

---

## Descripcion

Proyecto que compara Apache Spark Structured Streaming en **CPU** vs **GPU** (NVIDIA RAPIDS Accelerator) para procesamiento de criptoactivos en alta frecuencia. El pipeline ingiere datos en tiempo real desde Binance via Kafka, calcula features estadisticas por ventana temporal, entrena un modelo de clasificacion (RandomForest MLlib) y genera predicciones en streaming.

La comparativa mide throughput, latencia y uso de recursos entre:
- **Arquitectura 1:** Maquina local WSL2 — Intel i7-12700H (10 cores) + RTX 4070 8GB
- **Arquitectura 2:** Google Colab — 2 vCPU + NVIDIA T4 16GB con RAPIDS 24.10.1

---

## Arquitectura

```
WebSocket (Binance)          Kafka KRaft (Docker)         Spark Structured Streaming
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────────────┐
│ aggTrade 10 pares│───────►│ crypto-trades (8p)│───────►│ streaming_features.py   │
│ depth@100ms     │───────►│ crypto-book   (8p)│         │ → VWAP, volatilidad,    │
└─────────────────┘         └──────────────────┘         │   spread, volumen       │
                                                          └───────────┬─────────────┘
                                                                      │
                                                          ┌───────────▼─────────────┐
                                                          │ Parquet ~/data/crypto/   │
                                                          └───────────┬─────────────┘
                                                                      │
                                                          ┌───────────▼─────────────┐
                                                          │ batch_train.py          │
                                                          │ RandomForest (MLlib)    │
                                                          └───────────┬─────────────┘
                                                                      │
                                                          ┌───────────▼─────────────┐
                                                          │ streaming_inference.py  │
                                                          │ → Scoring en tiempo real│
                                                          └─────────────────────────┘
```

---

## Stack Tecnologico

| Componente | Tecnologia |
|------------|-----------|
| Lenguaje | Python 3.11 |
| Procesamiento | PySpark 3.5.4 (CPU) / 3.5.2 (GPU) |
| Aceleracion GPU | rapids-4-spark 24.10.1 + CUDA 12.x |
| Broker | Apache Kafka KRaft (Docker, `cp-kafka:7.6.1`) |
| Ingesta | asyncio + websockets + confluent-kafka |
| Fuente de datos | Binance USDS-Margined Futures WebSocket |
| ML | Spark MLlib — RandomForestClassifier |
| Almacenamiento | Apache Parquet (local) |
| Dashboard | Streamlit + Plotly (indicadores en tiempo real) |

---

## Estructura del Proyecto

```
FinanceProyect/
├── ingest/                  # Productores WebSocket → Kafka
│   ├── producer_trades.py
│   └── producer_book.py
├── spark/
│   ├── jobs/                # Jobs de Spark
│   │   ├── streaming_features.py    # Features en streaming
│   │   ├── batch_train.py           # Entrenamiento RandomForest
│   │   ├── streaming_inference.py   # Inferencia en tiempo real
│   │   ├── gpu_smoke_test.py        # Benchmark CPU vs GPU
│   │   └── gen_synth_parquet.py     # Generador de datos sinteticos
│   ├── replayer/            # Replay Parquet → Kafka
│   └── conf/                # spark-cpu.conf, spark-gpu.conf
├── infra/
│   ├── docker-compose.kafka.yml
│   └── scripts/             # run_spark_cpu.sh, run_spark_gpu.sh
├── dashboard/
│   ├── app.py               # Dashboard Streamlit (indicadores TR)
│   └── requirements.txt
├── reports/                 # Reporte comparativo + capturas Spark UI
├── colab_benchmark.ipynb    # Notebook Colab (benchmark GPU T4)
├── colab_benchmark.html     # Notebook exportado con resultados
├── CLAUDE.md                # Documentacion tecnica del proyecto
└── contexto.md              # Fuente de verdad del diseno
```

---

## Resultados Clave

### Benchmark: Smoke Test — 9.7M filas sinteticas (10 simbolos)

| Metrica | CPU Colab (2 vCPU) | GPU Colab T4 | Speedup |
|---------|-------------------|--------------|---------|
| Tiempo total | 46.3 s | 19.8 s | **2.33x** |
| `groupBy + agg` | 35.6 s | 10.2 s | **3.49x** |
| `corr` join | 10.7 s | 9.6 s | 1.11x |

### Modelo Supervisado (RandomForest — direccion de precio)

| Metrica | Valor |
|---------|-------|
| Accuracy | ~50% |
| F1-score | 0.51 |
| AUC-ROC | 0.52 |

> Accuracy cercana al 50% es esperada: datos sinteticos con ruido determinista y mercados eficientes donde la prediccion a corto plazo es fundamentalmente dificil.

---

## Dashboard de Indicadores en Tiempo Real

Dashboard interactivo (Streamlit + Plotly) que visualiza los indicadores estadisticos del trafico de criptoactivos calculados por el pipeline Spark.

**Paneles incluidos:**

| Panel | Indicador | Descripcion |
|-------|-----------|-------------|
| 1 | VWAP | Precio promedio ponderado por volumen (linea temporal) |
| 2 | Volatilidad | Desviacion estandar del precio por ventana |
| 3 | Volumen / Trades | Volumen total y numero de transacciones por simbolo |
| 4 | Spread Bid-Ask | Proxy de liquidez (mejor ask - mejor bid) |
| 5 | Correlacion BTC | Correlacion Pearson de VWAP vs BTCUSDT por par |
| 6 | Estadisticas | Media, stddev, percentiles (P25/P50/P75) por simbolo |

**Ejecutar:**

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

El dashboard se abre en `http://localhost:8501`. Lee los Parquet generados por Spark (`~/data/crypto/features/`). Si no hay datos disponibles, muestra datos sinteticos de demostracion. Incluye auto-refresh cada 30 segundos para actualizacion en vivo.

---

## Como Reproducir

### Local (CPU) — WSL2 Ubuntu

```bash
# 1. Levantar Kafka
docker compose -f infra/docker-compose.kafka.yml up -d

# 2. Crear topics
bash infra/scripts/create_topics.sh

# 3. Iniciar productores (en terminales separadas)
python ingest/producer_trades.py
python ingest/producer_book.py

# 4. Ejecutar streaming features
bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_features.py

# 5. Generar datos sinteticos para benchmark
bash infra/scripts/run_spark_cpu.sh spark/jobs/gen_synth_parquet.py

# 6. Entrenar modelo
bash infra/scripts/run_spark_cpu.sh spark/jobs/batch_train.py

# 7. Benchmark CPU vs GPU (solo CPU local)
bash infra/scripts/run_spark_cpu.sh spark/jobs/gpu_smoke_test.py
```

### Google Colab (GPU T4)

1. Subir `colab_benchmark.ipynb` a Google Colab
2. Seleccionar runtime GPU (T4)
3. Ejecutar todas las celdas secuencialmente
4. El notebook instala PySpark 3.5.2 + RAPIDS 24.10.1 automaticamente

---

## Requisitos

- Python 3.11+
- Java 11 (para Spark)
- Docker (para Kafka KRaft)
- CUDA 12.x (solo si se intenta GPU local)
- Google Colab con GPU T4 (para benchmark GPU)
- Streamlit + Plotly (para dashboard — ver `dashboard/requirements.txt`)

---

## Autores

- **Braulio Lozano** — Pipeline local (ingesta, streaming, batch, infra WSL2)
- **Juan Casas** — Benchmark GPU Colab, reporte comparativo

---

*Proyecto Final — Arquitectura de Grandes Volumenes de Datos, ITAM 2026*
