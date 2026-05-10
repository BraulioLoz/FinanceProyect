# Guía para Casas — Segunda Arquitectura (Claude Code)

Pega estos prompts en tu Claude Code **en orden**. Cada uno es una instrucción completa.

---

## Paso 1 — Configurar el entorno

```
Tengo un proyecto de Spark Structured Streaming con Kafka para el curso de Arquitectura del ITAM.
Necesito configurar el entorno en mi máquina (segunda arquitectura para comparar con la de mi compañero).

1. Verifica que tengo instalado: Python 3.11, Java 11+, Docker Desktop.
2. Si falta algo, dame los comandos de instalación para mi OS.
3. Clona o confirma que tengo el repo en la ruta correcta.
4. Crea un archivo .env basado en .env.example con estos valores:
   - KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   - BINANCE_FUTURES_WS_BASE_URL=wss://stream.binance.com:9443/stream
   - KAFKA_TOPIC_TRADES=crypto-trades
   - KAFKA_TOPIC_BOOK=crypto-book
   - KAFKA_TOPIC_FEATURES=crypto-features
   - KAFKA_TOPIC_PREDICTIONS=crypto-pred
   - SPARK_CHECKPOINT_BASE=/tmp/spark-checkpoints
   - DATA_DIR=<ruta a tu carpeta home>/data/crypto
5. Instala las dependencias Python: pip install -r requirements.txt
```

---

## Paso 2 — Levantar Kafka

```
Levanta Kafka con Docker usando el archivo infra/docker-compose.kafka.yml.
Luego crea los topics necesarios con el script infra/scripts/create_topics.sh.
Verifica que los 4 topics existen: crypto-trades, crypto-book, crypto-features, crypto-pred.
```

---

## Paso 3 — Correr los productores

```
Corre los dos productores de datos en paralelo (en terminales separadas):
- python3.11 -m ingest.producer_trades
- python3.11 -m ingest.producer_book

Verifica que llegan mensajes a Kafka con:
docker exec kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-trades --max-messages 3

Si no llegan mensajes en 30 segundos, diagnostica el problema.
```

---

## Paso 4 — Correr Spark Structured Streaming (CPU)

```
Corre el job de streaming features en modo CPU:
bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_features.py

Espera 5 minutos y luego abre http://localhost:4040 en el navegador.
Ve a la pestaña "Structured Streaming" y toma captura de pantalla.
Ve a "Executors" y toma captura de pantalla.
Ve a "Stages" y toma captura de pantalla.
Guarda las capturas en una carpeta llamada screenshots_<tu_nombre>/
```

---

## Paso 5 — Capturar métricas para comparativa

```
De las capturas de Spark UI que tomé, extrae estos valores y ponlos en un archivo metrics.md:
- Avg Input/sec (Structured Streaming)
- Avg Process/sec (Structured Streaming)
- Task Time total (Executors)
- GC Time (Executors)
- Shuffle Read (Executors)
- Shuffle Write (Executors)
- Storage Memory usada

También incluye las especificaciones de tu máquina:
- CPU (modelo, cores, frecuencia)
- RAM (GB)
- GPU (si tienes)
- OS
- Versión Python, PySpark, Java
```

---

## Paso 6 — Entrenar modelo y hacer inferencia

```
Ya tengo suficientes datos en Parquet. Corre en orden:
1. bash infra/scripts/run_spark_cpu.sh spark/jobs/batch_train.py
   (toma captura de Jobs y Executors antes de que termine)
2. bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_inference.py
   (verifica que llegan predicciones: docker exec kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-pred --max-messages 3)
```

---

## Paso 7 — Comparar con la arquitectura de Braulio

```
Tengo un archivo reports/comparison_2026-05-09.md con las métricas de la primera arquitectura.
Agrega las métricas de mi máquina a ese archivo en columnas adicionales y genera una tabla
comparativa final con conclusiones sobre cuál arquitectura tiene mejor desempeño y por qué.
```
