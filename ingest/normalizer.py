from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Binance Futures ya entrega los símbolos en el formato interno (BTCUSDT).
# Este mapa se usa si en el futuro se agregan exchanges con formatos distintos.
BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL: dict[str, str] = {
    "BTCUSDT": "BTCUSDT",
    "ETHUSDT": "ETHUSDT",
    "SOLUSDT": "SOLUSDT",
    "BNBUSDT": "BNBUSDT",
    "XRPUSDT": "XRPUSDT",
    "ADAUSDT": "ADAUSDT",
    "DOGEUSDT": "DOGEUSDT",
    "MATICUSDT": "MATICUSDT",
    "AVAXUSDT": "AVAXUSDT",
    "DOTUSDT": "DOTUSDT",
}


def normalize_binance_futures_symbol(raw_symbol: str) -> str | None:
    """
    Map a raw Binance Futures symbol to the internal convention.
    Returns None and logs a warning if the symbol is not in the configured list.
    """
    internal_symbol = BINANCE_FUTURES_SYMBOL_TO_INTERNAL_SYMBOL.get(raw_symbol.upper())
    if internal_symbol is None:
        logger.warning(
            "Símbolo no configurado, mensaje descartado",
            extra={"raw_symbol": raw_symbol},
        )
    return internal_symbol


def build_binance_futures_stream_names(
    symbols: list[str],
    stream_suffix: str,
) -> list[str]:
    """
    Build Binance stream name list for a given suffix.
    Binance requires lowercase symbols in stream names.
    Example: ['btcusdt@aggTrade', 'ethusdt@aggTrade']
    """
    return [f"{symbol.lower()}@{stream_suffix}" for symbol in symbols]


def build_binance_futures_multi_stream_url(
    base_url: str,
    stream_names: list[str],
) -> str:
    """Combine base URL and stream names into the combined stream endpoint."""
    combined_streams = "/".join(stream_names)
    return f"{base_url}?streams={combined_streams}"
