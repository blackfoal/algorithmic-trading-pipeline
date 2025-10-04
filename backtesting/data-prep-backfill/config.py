"""
Shared configuration utilities for data-prep-backfill
Centralizes environment reading for DB config and symbols.
"""

import os
from typing import Dict, List


DEFAULT_FREQUENCIES: List[str] = ["15m", "30m", "1h"]


def get_db_config(strict: bool = True) -> Dict[str, str]:
    """Load database configuration from environment.

    If strict is True, raise if any required variable is missing.
    """
    required_keys = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    env: Dict[str, str] = {}
    missing: List[str] = []
    for key in required_keys:
        value = os.getenv(key)
        if value is None:
            missing.append(key)
        else:
            env[key] = value

    if strict and missing:
        raise RuntimeError(
            f"Missing required DB environment variables: {', '.join(missing)}"
        )

    # Defaults only used when strict is False
    if not strict:
        env.setdefault("POSTGRES_HOST", "postgres-backtesting")
        env.setdefault("POSTGRES_PORT", "5432")
        env.setdefault("POSTGRES_DB", "backtesting")
        env.setdefault("POSTGRES_USER", "murat_binance")
        env.setdefault("POSTGRES_PASSWORD", "murat_binance")

    return {
        "host": env.get("POSTGRES_HOST", "postgres-backtesting" if not strict else ""),
        "port": int(env.get("POSTGRES_PORT", "5433" if not strict else "0")),
        "database": env.get("POSTGRES_DB", "backtesting" if not strict else ""),
        "user": env.get("POSTGRES_USER", "murat_binance" if not strict else ""),
        "password": env.get("POSTGRES_PASSWORD", "murat_binance" if not strict else ""),
    }


def get_symbols(strict: bool = True) -> List[str]:
    """Return symbols from SYMBOLS env, preserving CCXT format (e.g., BTC/USDT).

    If strict and SYMBOLS is missing or empty, raise an error.
    """
    symbols_str = os.getenv("SYMBOLS", "" if strict else "")
    if strict and not symbols_str:
        raise RuntimeError("SYMBOLS environment variable is not set")

    if not symbols_str:
        return []

    symbols: List[str] = []
    for part in symbols_str.split(","):
        symbol = part.strip().upper()
        if symbol:
            symbols.append(symbol)
    return symbols


def to_db_symbol(symbol: str) -> str:
    """Convert CCXT-style symbol (BTC/USDT) to DB-style (BTCUSDT)."""
    return symbol.replace("/", "")


