"""
Productor de libro de órdenes: suscribe a <symbol>@depth@100ms en Binance USDS-Margined Futures
y publica mensajes normalizados al topic crypto-book en Kafka.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from typing import Any

import websockets
from confluent_kafka import Producer

from ingest.envelope import build_envelope
from ingest.normalizer import (
    build_binance_futures_multi_stream_url,
    build_binance_futures_stream_names,
    normalize_binance_futures_symbol,
)

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
)
logger = logging.getLogger(__name__)

EXCHANGE_NAME = "binance_futures"
EVENT_TYPE_DEPTH_UPDATE = "depth_update"
DEPTH_STREAM_SUFFIX = "depth@100ms"

MAX_CONNECTION_LIFETIME_SECONDS = int(
    os.getenv("WEBSOCKET_MAX_CONNECTION_LIFETIME_SECONDS", "82800")
)
MAX_RETRIES = int(os.getenv("WEBSOCKET_MAX_RETRIES", "0"))


def _build_depth_update_payload(raw_message: dict[str, Any]) -> dict[str, Any]:
    return {
        "first_update_id": raw_message["U"],
        "final_update_id": raw_message["u"],
        "previous_final_update_id": raw_message["pu"],
        "bids": raw_message["b"],   # [[price_string, qty_string], ...]
        "asks": raw_message["a"],   # [[price_string, qty_string], ...]
    }


def _on_kafka_delivery_report(error: Exception | None, message: Any) -> None:
    if error is not None:
        logger.error("Error al entregar mensaje a Kafka", extra={"error": str(error)})


def _create_kafka_producer(kafka_bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": kafka_bootstrap_servers})


async def _process_websocket_messages(
    websocket_connection: websockets.WebSocketClientProtocol,
    kafka_producer: Producer,
    kafka_topic_book: str,
) -> None:
    async for raw_text_frame in websocket_connection:
        outer = json.loads(raw_text_frame)
        raw_message: dict[str, Any] = outer.get("data", outer)

        if raw_message.get("e") != "depthUpdate":
            continue

        internal_symbol = normalize_binance_futures_symbol(raw_message["s"])
        if internal_symbol is None:
            continue

        payload = _build_depth_update_payload(raw_message)
        # Usar T (transaction time) como ts_event para el libro
        envelope_bytes = build_envelope(
            symbol=internal_symbol,
            exchange=EXCHANGE_NAME,
            event_type=EVENT_TYPE_DEPTH_UPDATE,
            event_timestamp_ms=raw_message["T"],
            payload=payload,
        )

        kafka_producer.produce(
            topic=kafka_topic_book,
            key=internal_symbol.encode("utf-8"),
            value=envelope_bytes,
            callback=_on_kafka_delivery_report,
        )
        kafka_producer.poll(0)


async def run_book_producer(
    binance_futures_ws_base_url: str,
    kafka_bootstrap_servers: str,
    kafka_topic_book: str,
    symbols: list[str],
) -> None:
    stream_names = build_binance_futures_stream_names(symbols, DEPTH_STREAM_SUFFIX)
    websocket_url = build_binance_futures_multi_stream_url(
        binance_futures_ws_base_url, stream_names
    )

    kafka_producer = _create_kafka_producer(kafka_bootstrap_servers)
    reconnect_attempt_count = 0

    while True:
        try:
            logger.info(
                "Conectando a Binance Futures WebSocket (book)",
                extra={"url": websocket_url, "attempt": reconnect_attempt_count},
            )
            connection_start_time = time.monotonic()

            async with websockets.connect(websocket_url) as websocket_connection:
                reconnect_attempt_count = 0
                logger.info("Conexión establecida — escuchando depth@100ms")

                while True:
                    elapsed_seconds = time.monotonic() - connection_start_time
                    if elapsed_seconds >= MAX_CONNECTION_LIFETIME_SECONDS:
                        logger.info("Reconexión programada por límite de 24 h")
                        break

                    try:
                        await asyncio.wait_for(
                            _process_websocket_messages(
                                websocket_connection, kafka_producer, kafka_topic_book
                            ),
                            timeout=30.0,
                        )
                    except asyncio.TimeoutError:
                        continue

        except Exception as connection_error:
            reconnect_attempt_count += 1
            if MAX_RETRIES > 0 and reconnect_attempt_count > MAX_RETRIES:
                logger.error("Máximo de reintentos alcanzado — deteniendo productor")
                break

            backoff_seconds = min(2 ** reconnect_attempt_count, 60) + random.uniform(0, 1)
            logger.warning(
                "Conexión perdida — reintentando",
                extra={
                    "error": str(connection_error),
                    "attempt": reconnect_attempt_count,
                    "backoff_seconds": round(backoff_seconds, 2),
                },
            )
            await asyncio.sleep(backoff_seconds)
        finally:
            kafka_producer.flush()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    binance_futures_ws_base_url = os.environ["BINANCE_FUTURES_WS_BASE_URL"]
    kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    kafka_topic_book = os.getenv("KAFKA_TOPIC_BOOK", "crypto-book")
    symbols_raw = os.getenv(
        "SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,MATICUSDT,AVAXUSDT,DOTUSDT",
    )
    symbols = [symbol.strip() for symbol in symbols_raw.split(",")]

    asyncio.run(
        run_book_producer(
            binance_futures_ws_base_url=binance_futures_ws_base_url,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic_book=kafka_topic_book,
            symbols=symbols,
        )
    )
