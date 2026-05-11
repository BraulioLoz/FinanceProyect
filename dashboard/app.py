"""
Dashboard de Indicadores de Trafico Cripto en Tiempo Real.

Lee Parquet generado por el pipeline Spark (streaming_features / batch_aggregate)
y muestra indicadores estadisticos interactivos.

Ejecutar: streamlit run app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", str(Path.home() / "data" / "crypto")))
FEATURES_DIR = DATA_DIR / "features"
AGGREGATES_DIR = DATA_DIR / "aggregates"
SYMBOL_STATS_DIR = AGGREGATES_DIR / "symbol_stats"
BTC_CORR_DIR = AGGREGATES_DIR / "btc_correlations"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "MATICUSDT", "AVAXUSDT", "DOTUSDT",
]


def generate_synthetic_features() -> pd.DataFrame:
    """Generate synthetic feature data when no Parquet is available."""
    np.random.seed(42)
    rows = []
    base_prices = {
        "BTCUSDT": 65000, "ETHUSDT": 3200, "SOLUSDT": 145,
        "BNBUSDT": 580, "XRPUSDT": 0.52, "ADAUSDT": 0.45,
        "DOGEUSDT": 0.15, "MATICUSDT": 0.72, "AVAXUSDT": 35, "DOTUSDT": 7.2,
    }
    timestamps = pd.date_range("2026-05-10 00:00", periods=200, freq="30s")

    for symbol in SYMBOLS:
        bp = base_prices[symbol]
        price_walk = bp + np.cumsum(np.random.randn(len(timestamps)) * bp * 0.001)
        for i, ts in enumerate(timestamps):
            p = price_walk[i]
            vol = abs(np.random.randn()) * bp * 0.002
            volume = abs(np.random.randn()) * 50 + 10
            spread = abs(np.random.randn()) * bp * 0.0001
            rows.append({
                "symbol": symbol,
                "window_start": ts,
                "vwap": p,
                "avg_price": p * (1 + np.random.randn() * 0.0001),
                "price_volatility": vol,
                "total_volume": volume,
                "trade_count": int(np.random.poisson(80)),
                "min_price": p - vol,
                "max_price": p + vol,
                "avg_spread_proxy": spread,
                "avg_best_bid_price": p - spread / 2,
                "avg_best_ask_price": p + spread / 2,
            })
    return pd.DataFrame(rows)


def generate_synthetic_stats() -> pd.DataFrame:
    """Generate synthetic symbol statistics."""
    np.random.seed(42)
    base_prices = {
        "BTCUSDT": 65000, "ETHUSDT": 3200, "SOLUSDT": 145,
        "BNBUSDT": 580, "XRPUSDT": 0.52, "ADAUSDT": 0.45,
        "DOGEUSDT": 0.15, "MATICUSDT": 0.72, "AVAXUSDT": 35, "DOTUSDT": 7.2,
    }
    rows = []
    for sym, bp in base_prices.items():
        rows.append({
            "symbol": sym,
            "mean_vwap": bp,
            "stddev_vwap": bp * 0.02,
            "historical_min_price": bp * 0.9,
            "historical_max_price": bp * 1.1,
            "mean_volatility": bp * 0.005,
            "peak_volatility": bp * 0.03,
            "vwap_p25": bp * 0.97,
            "vwap_p50": bp,
            "vwap_p75": bp * 1.03,
            "volatility_p95": bp * 0.02,
            "avg_volume_per_window": 45.0,
            "total_historical_volume": 45000.0,
        })
    return pd.DataFrame(rows)


def generate_synthetic_correlations() -> pd.DataFrame:
    """Generate synthetic BTC correlations."""
    corrs = {
        "BTCUSDT": 1.0, "ETHUSDT": 0.9987, "BNBUSDT": 0.9971,
        "SOLUSDT": 0.9943, "AVAXUSDT": 0.9921, "DOTUSDT": 0.9908,
        "ADAUSDT": 0.9891, "XRPUSDT": 0.9756, "DOGEUSDT": 0.9634,
        "MATICUSDT": 0.9589,
    }
    return pd.DataFrame([
        {"symbol": k, "corr_with_btc": v} for k, v in corrs.items()
    ])


@st.cache_data(ttl=30)
def load_features() -> pd.DataFrame:
    """Load feature Parquet or fall back to synthetic data."""
    if FEATURES_DIR.exists():
        parquet_files = list(FEATURES_DIR.rglob("*.parquet"))
        if parquet_files:
            dfs = [pd.read_parquet(f) for f in parquet_files[:50]]
            return pd.concat(dfs, ignore_index=True)
    return generate_synthetic_features()


@st.cache_data(ttl=60)
def load_symbol_stats() -> pd.DataFrame:
    """Load symbol statistics or fall back to synthetic."""
    if SYMBOL_STATS_DIR.exists():
        parquet_files = list(SYMBOL_STATS_DIR.rglob("*.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[0])
    return generate_synthetic_stats()


@st.cache_data(ttl=60)
def load_correlations() -> pd.DataFrame:
    """Load BTC correlations or fall back to synthetic."""
    if BTC_CORR_DIR.exists():
        parquet_files = list(BTC_CORR_DIR.rglob("*.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[0])
    return generate_synthetic_correlations()


def main() -> None:
    st.set_page_config(
        page_title="Crypto HF - Indicadores en Tiempo Real",
        page_icon="$",
        layout="wide",
    )

    st.title("Indicadores de Trafico Cripto en Tiempo Real")
    st.caption("Pipeline: Binance WebSocket -> Kafka -> Spark Structured Streaming -> Parquet")

    # Sidebar
    st.sidebar.header("Configuracion")
    selected_symbols = st.sidebar.multiselect(
        "Simbolos", SYMBOLS, default=["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    )
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)

    if auto_refresh:
        st.sidebar.info("Refrescando cada 30 segundos...")
        import time
        time.sleep(0.1)
        st.rerun()

    # Load data
    df = load_features()
    stats_df = load_symbol_stats()
    corr_df = load_correlations()

    if df.empty:
        st.warning("No hay datos disponibles. Ejecuta el pipeline primero.")
        return

    df_filtered = df[df["symbol"].isin(selected_symbols)].copy()
    if df_filtered.empty:
        st.warning("No hay datos para los simbolos seleccionados.")
        return

    df_filtered["window_start"] = pd.to_datetime(df_filtered["window_start"])
    df_filtered = df_filtered.sort_values(["symbol", "window_start"])

    # --- Panel 1: VWAP ---
    st.header("1. VWAP (Precio Promedio Ponderado por Volumen)")
    fig_vwap = px.line(
        df_filtered, x="window_start", y="vwap", color="symbol",
        title="VWAP por Ventana Temporal",
        labels={"window_start": "Tiempo", "vwap": "VWAP (USD)", "symbol": "Par"},
    )
    fig_vwap.update_layout(height=400)
    st.plotly_chart(fig_vwap, use_container_width=True)

    # --- Panel 2: Volatilidad ---
    st.header("2. Volatilidad (Desviacion Estandar del Precio)")
    fig_vol = px.line(
        df_filtered, x="window_start", y="price_volatility", color="symbol",
        title="Volatilidad por Ventana",
        labels={"window_start": "Tiempo", "price_volatility": "Volatilidad (USD)", "symbol": "Par"},
    )
    fig_vol.update_layout(height=350)
    st.plotly_chart(fig_vol, use_container_width=True)

    # --- Panel 3: Volumen y Trade Count ---
    st.header("3. Volumen y Numero de Trades")
    col1, col2 = st.columns(2)

    with col1:
        fig_volume = px.bar(
            df_filtered.groupby("symbol")["total_volume"].sum().reset_index(),
            x="symbol", y="total_volume", color="symbol",
            title="Volumen Total por Simbolo",
            labels={"total_volume": "Volumen Total", "symbol": "Par"},
        )
        st.plotly_chart(fig_volume, use_container_width=True)

    with col2:
        fig_trades = px.bar(
            df_filtered.groupby("symbol")["trade_count"].sum().reset_index(),
            x="symbol", y="trade_count", color="symbol",
            title="Total Trades por Simbolo",
            labels={"trade_count": "Trades", "symbol": "Par"},
        )
        st.plotly_chart(fig_trades, use_container_width=True)

    # --- Panel 4: Spread Bid-Ask ---
    st.header("4. Spread Bid-Ask (Proxy de Liquidez)")
    fig_spread = px.line(
        df_filtered, x="window_start", y="avg_spread_proxy", color="symbol",
        title="Evolucion del Spread Bid-Ask",
        labels={"window_start": "Tiempo", "avg_spread_proxy": "Spread (USD)", "symbol": "Par"},
    )
    fig_spread.update_layout(height=350)
    st.plotly_chart(fig_spread, use_container_width=True)

    # --- Panel 5: Correlacion con BTC ---
    st.header("5. Correlacion con BTCUSDT")
    corr_filtered = corr_df[corr_df["symbol"].isin(SYMBOLS)].sort_values(
        "corr_with_btc", ascending=False
    )
    fig_corr = px.bar(
        corr_filtered, x="symbol", y="corr_with_btc", color="corr_with_btc",
        color_continuous_scale="RdYlGn",
        title="Correlacion de VWAP con BTCUSDT (Pearson)",
        labels={"corr_with_btc": "Correlacion", "symbol": "Par"},
    )
    fig_corr.update_layout(height=350)
    st.plotly_chart(fig_corr, use_container_width=True)

    # --- Panel 6: Estadisticas Resumen ---
    st.header("6. Estadisticas Resumen por Simbolo")
    stats_filtered = stats_df[stats_df["symbol"].isin(selected_symbols)]
    if not stats_filtered.empty:
        display_cols = [
            "symbol", "mean_vwap", "stddev_vwap",
            "historical_min_price", "historical_max_price",
            "mean_volatility", "vwap_p25", "vwap_p50", "vwap_p75",
        ]
        available_cols = [c for c in display_cols if c in stats_filtered.columns]
        st.dataframe(
            stats_filtered[available_cols].set_index("symbol"),
            use_container_width=True,
        )
    else:
        st.info("Estadisticas no disponibles para los simbolos seleccionados.")

    # Footer
    st.divider()
    st.caption(
        "Datos: Parquet generado por Spark Structured Streaming "
        f"| Directorio: `{DATA_DIR}` "
        "| Ventana: 1 min / slide 30s"
    )


if __name__ == "__main__":
    main()
