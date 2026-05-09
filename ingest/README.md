# ingest/ — Ingesta WebSocket → Kafka

Módulo responsable de conectarse a **Binance USDS-Margined Futures** vía WebSocket, normalizar los mensajes y publicarlos a Kafka usando el [envelope JSON estándar](../CLAUDE.md#envelope-json-obligatorio).

---

## Modelo de ingesta

```
Binance Futures WebSocket (wss://fstream.binance.com)
       │
       │  mensajes crudos: aggTrade / depthUpdate
       ▼
  asyncio event loop
       │
       ├─ extraer campos crudos (p, q, T, b, a, …)
       ├─ normalizar símbolo (campo s → BTCUSDT)
       ├─ construir envelope JSON estándar
       │
       ▼
  Kafka Producer (confluent-kafka)
       │
       ├─→ topic: crypto-trades   (key = symbol)
       └─→ topic: crypto-book     (key = symbol)
```

- Un productor por tipo de stream (trades / book) con su propia conexión WebSocket.
- Se usa `asyncio` para manejar los 10 pares simultáneos sin bloqueo.
- La clave (key) del mensaje Kafka es el campo `symbol` en bytes UTF-8 — garantiza que todos los mensajes de un par vayan siempre a la misma partición.

---

## Binance USDS-Margined Futures — detalles de la API

### URLs de conexión

| Propósito | URL |
|-----------|-----|
| Streams de mercado (market data) | `wss://fstream.binance.com/stream?streams=<stream1>/<stream2>/...` |
| WebSocket API (órdenes, sesión) | `wss://ws-fapi.binance.com/ws-fapi/v1` |

Para datos públicos de trades y libro solo se necesita la URL de market streams.

### Formato de suscripción multi-stream

Conectar una sola vez y pasar múltiples streams en la URL:

```
wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@depth@100ms/ethusdt@depth@100ms/...
```

- Símbolos en **minúsculas** en la URL; el campo `s` en la respuesta viene en mayúsculas (`BTCUSDT`).
- Combinar trades y book de todos los pares en una sola conexión (10 pares × 2 streams = 20 streams — muy por debajo del límite).

### Límites de conexión

| Parámetro | Valor |
|-----------|-------|
| Duración máxima de conexión | 24 horas — reconectar al expirar |
| Ping del servidor | cada 3 minutos |
| Timeout si no hay pong | 10 minutos — servidor cierra la conexión |
| Rate limit ping/pong | 5 por segundo máximo |
| Streams por conexión | Sin límite documentado explícito para market streams públicos |

---

## Streams utilizados

### 1. Aggregate Trade Stream — `<symbol>@aggTrade`

Trades de mercado agregados, update cada **100 ms**.

**Campos del mensaje crudo de Binance:**

| Campo Binance | Tipo | Descripción |
|---------------|------|-------------|
| `e` | string | Tipo de evento — siempre `"aggTrade"` |
| `E` | long (ms) | Timestamp del evento en Binance |
| `s` | string | Símbolo — ej. `"BTCUSDT"` |
| `a` | long | ID del trade agregado |
| `p` | string | Precio del trade |
| `q` | string | Cantidad total agregada |
| `nq` | string | Cantidad excluyendo órdenes RPI |
| `f` | long | ID del primer trade individual en el agregado |
| `l` | long | ID del último trade individual en el agregado |
| `T` | long (ms) | Timestamp de ejecución del trade — usar como `ts_event` |
| `m` | bool | `true` si el comprador es el market maker |

**Mapeo al envelope interno:**

| Campo envelope | Fuente Binance | Notas |
|----------------|----------------|-------|
| `symbol` | `s` | Ya en formato `BTCUSDT` |
| `exchange` | constante | `"binance_futures"` |
| `event_type` | constante | `"agg_trade"` |
| `ts_event` | `T` | Convertir de ms a ISO-8601 UTC |
| `ts_ingest` | `datetime.utcnow()` | Generado al recibir |
| `payload.aggregate_trade_id` | `a` | |
| `payload.price` | `p` | string decimal |
| `payload.quantity` | `q` | string decimal |
| `payload.quantity_excluding_rpi` | `nq` | string decimal |
| `payload.first_trade_id` | `f` | |
| `payload.last_trade_id` | `l` | |
| `payload.trade_execution_time_ms` | `T` | epoch ms, redundante con ts_event |
| `payload.is_buyer_market_maker` | `m` | bool |

### 2. Diff Book Depth Stream — `<symbol>@depth@100ms`

Actualizaciones diferenciales del libro de órdenes, update cada **100 ms**.

**Campos del mensaje crudo de Binance:**

| Campo Binance | Tipo | Descripción |
|---------------|------|-------------|
| `e` | string | Tipo de evento — siempre `"depthUpdate"` |
| `E` | long (ms) | Timestamp del evento en Binance |
| `T` | long (ms) | Timestamp de transacción — usar como `ts_event` |
| `s` | string | Símbolo — ej. `"BTCUSDT"` |
| `U` | long | Primer update ID en este evento |
| `u` | long | Último update ID en este evento |
| `pu` | long | Último update ID del evento anterior (para consistencia) |
| `b` | array | Bids actualizados — `[[price_string, qty_string], ...]` |
| `a` | array | Asks actualizados — `[[price_string, qty_string], ...]` |

Nota: cantidad `"0"` en `b` o `a` indica eliminar ese nivel de precio.

**Mapeo al envelope interno:**

| Campo envelope | Fuente Binance | Notas |
|----------------|----------------|-------|
| `symbol` | `s` | |
| `exchange` | constante | `"binance_futures"` |
| `event_type` | constante | `"depth_update"` |
| `ts_event` | `T` | Convertir de ms a ISO-8601 UTC |
| `ts_ingest` | `datetime.utcnow()` | |
| `payload.first_update_id` | `U` | Para mantener libro local consistente |
| `payload.final_update_id` | `u` | |
| `payload.previous_final_update_id` | `pu` | |
| `payload.bids` | `b` | lista de `[price, qty]` strings |
| `payload.asks` | `a` | lista de `[price, qty]` strings |

---

## Pares suscritos (10 pares)

```
BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT,
ADAUSDT, DOGEUSDT, MATICUSDT, AVAXUSDT, DOTUSDT
```

Todos son pares de Binance USDS-Margined Futures de alta liquidez. En la URL de suscripción van en minúsculas.

---

## Normalización de símbolos

- Convención interna: **sin barra**, mayúsculas. Ej: `BTCUSDT`.
- Binance Futures ya entrega el campo `s` en este formato — sin transformación necesaria.
- Si se agrega otro exchange en el futuro, mantener un diccionario `BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL` en `ingest/normalizer.py`.
- Si el símbolo recibido no está en la lista configurada, loguear advertencia y descartar el mensaje.

---

## Política de reintentos y reconexión

1. **Backoff exponencial:** esperar `min(2 ** reconnect_attempt_count, 60)` segundos entre intentos.
2. **Jitter:** agregar `random.uniform(0, 1)` al tiempo de espera para evitar thundering herd.
3. **Máximo de intentos:** configurable vía `WEBSOCKET_MAX_RETRIES` (0 = sin límite para productores de larga duración).
4. **Reconexión a las 24 h:** el servidor cierra la conexión tras 24 horas — el productor debe manejar este cierre limpio y reconectar.
5. **Pong obligatorio:** responder siempre al ping del servidor (cada 3 min); el servidor desconecta si no recibe pong en 10 min.
6. **Reconexión limpia:** al reconectar, re-suscribirse a todos los pares del grupo sin duplicar streams.
7. **Logging estructurado:** registrar cada desconexión con timestamp, razón y número de intento en JSON a stdout.

---

## Variables de entorno relevantes

Ver [.env.example](../.env.example) para la lista completa.

| Variable | Uso |
|----------|-----|
| `KAFKA_BOOTSTRAP_SERVERS` | Host:puerto del broker Kafka |
| `EXCHANGE_DEFAULT` | Exchange a usar (`binance_futures`) |
| `SYMBOLS` | Lista de pares separados por coma |
| `BINANCE_FUTURES_WS_BASE_URL` | URL base de streams de mercado |
| `WEBSOCKET_PING_INTERVAL_SECONDS` | Segundos entre pings al exchange (default: 180) |
| `WEBSOCKET_PONG_TIMEOUT_SECONDS` | Segundos para timeout de pong (default: 600) |
| `WEBSOCKET_MAX_RETRIES` | Máximo de reintentos (0 = sin límite) |

---

## Estructura de archivos esperada

```
ingest/
  README.md               # este archivo
  producer_trades.py      # productor asyncio para aggTrade → crypto-trades
  producer_book.py        # productor asyncio para depthUpdate → crypto-book
  normalizer.py           # BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL y helpers
  envelope.py             # construcción y validación del envelope JSON estándar
  health.py               # endpoint FastAPI opcional para /health y /metrics
  requirements.txt        # dependencias del módulo
```
