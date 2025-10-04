#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from typing import List, Tuple, Optional

import numpy as np
import psycopg2
import psycopg2.extras


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def compute_ema(series: np.ndarray, period: int) -> np.ndarray:
    alpha = 2.0 / (period + 1.0)
    ema = np.empty_like(series, dtype=np.float64)
    ema[0] = series[0]
    for i in range(1, series.shape[0]):
        ema[i] = alpha * series[i] + (1.0 - alpha) * ema[i - 1]
    return ema


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'backtesting'),
        user=os.getenv('POSTGRES_USER', 'murat_binance'),
        password=os.getenv('POSTGRES_PASSWORD', 'murat_binance'),
    )


def list_periods(conn) -> List[Tuple[str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM periods ORDER BY start_time")
        return [row[0] for row in cur.fetchall()]


def list_symbols_for_period(conn, period_id: str, frequency: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM ticker_data
            WHERE period_id = %s AND frequency = %s
            ORDER BY symbol
            """,
            (period_id, frequency)
        )
        return [row[0] for row in cur.fetchall()]


def fetch_closes(conn, period_id: str, symbol: str, frequency: str) -> Tuple[np.ndarray, np.ndarray]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.ts, t.close
            FROM ticker_data t
            JOIN periods p ON t.period_id = p.id
            WHERE t.period_id = %s AND t.symbol = %s AND t.frequency = %s
                AND t.ts >= COALESCE(p.buffer_start_time, p.start_time)
                AND t.ts <= p.end_time
            ORDER BY t.ts
            """,
            (period_id, symbol, frequency)
        )
        rows = cur.fetchall()
    ts = np.array([r[0] for r in rows], dtype=object)
    closes = np.array([float(r[1]) for r in rows], dtype=np.float64)
    return ts, closes


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS macd_indicators (
                symbol text NOT NULL,
                ts timestamp NOT NULL,
                period_id text NOT NULL,
                frequency text NOT NULL CHECK (frequency IN ('15m','30m','1h')),
                close numeric(20,8) NOT NULL,
                ema_12 numeric(20,8) NOT NULL,
                ema_26 numeric(20,8) NOT NULL,
                macd_line numeric(20,8) NOT NULL,
                macd_signal numeric(20,8) NOT NULL,
                macd_histogram numeric(20,8) NOT NULL,
                PRIMARY KEY (symbol, ts, period_id, frequency)
            )
            """
        )
        conn.commit()


def upsert_macd(conn, period_id: str, symbol: str, frequency: str, ts_arr: np.ndarray, close_arr: np.ndarray,
                ema12: np.ndarray, ema26: np.ndarray, macd: np.ndarray, signal: np.ndarray, hist: np.ndarray):
    # Get the actual period start time to filter out buffer data
    with conn.cursor() as cur:
        cur.execute("SELECT start_time FROM periods WHERE id = %s", (period_id,))
        period_start = cur.fetchone()[0]
    
    # Filter data to only include actual period (not buffer)
    mask = ts_arr >= period_start
    filtered_ts = ts_arr[mask]
    filtered_close = close_arr[mask]
    filtered_ema12 = ema12[mask]
    filtered_ema26 = ema26[mask]
    filtered_macd = macd[mask]
    filtered_signal = signal[mask]
    filtered_hist = hist[mask]
    
    # Delete existing records for this symbol, period, and frequency first
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM macd_indicators WHERE symbol = %s AND period_id = %s AND frequency = %s",
            (symbol, period_id, frequency)
        )
    
    # Insert only the filtered data
    values = [
        (
            symbol,
            filtered_ts[i],
            period_id,
            frequency,
            round(float(filtered_close[i]), 8),
            round(float(filtered_ema12[i]), 8),
            round(float(filtered_ema26[i]), 8),
            round(float(filtered_macd[i]), 8),
            round(float(filtered_signal[i]), 8),
            round(float(filtered_hist[i]), 8),
        ) for i in range(len(filtered_ts))
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO macd_indicators (
                symbol, ts, period_id, frequency, close, ema_12, ema_26, macd_line, macd_signal, macd_histogram
            ) VALUES %s
            """,
            values,
            page_size=500,
        )
        conn.commit()


def compute_and_store_for_period(conn, period_id: str, frequency: str):
    symbols = list_symbols_for_period(conn, period_id, frequency)
    total = 0
    logger.info(f"Computing MACD for period {period_id} ({frequency}) using Standard EMA (12,26,9) methodology")
    for symbol in symbols:
        ts, closes = fetch_closes(conn, period_id, symbol, frequency)
        if ts.size < 30:
            logger.warning(f"{symbol}: insufficient data ({ts.size} points), skipping")
            continue
        ema12 = compute_ema(closes, 12)
        ema26 = compute_ema(closes, 26)
        macd = ema12 - ema26
        signal = compute_ema(macd, 9)
        hist = macd - signal
        upsert_macd(conn, period_id, symbol, frequency, ts, closes, ema12, ema26, macd, signal, hist)
        total += ts.size
        logger.info(f"{period_id} {symbol} ({frequency}): {ts.size} rows (MACD line: {macd[-1]:.2f}, Signal: {signal[-1]:.2f}, Hist: {hist[-1]:.2f})")
    logger.info(f"Period {period_id} ({frequency}): upserted {total} rows total")


def main():
    parser = argparse.ArgumentParser(description="Compute MACD in Python and upsert to DB")
    parser.add_argument('--period', help='Specific period ID to compute')
    parser.add_argument('--all', action='store_true', help='Compute for all periods')
    parser.add_argument('--frequency', help='Frequency to compute (15m, 30m, 1h)', default='15m')
    args = parser.parse_args()

    # Validate frequency
    if args.frequency not in {'15m', '30m', '1h'}:
        print("❌ Invalid frequency. Must be one of: 15m, 30m, 1h")
        sys.exit(1)

    conn = get_db_conn()
    try:
        ensure_table(conn)
        if args.period:
            compute_and_store_for_period(conn, args.period, args.frequency)
        elif args.all:
            for pid in list_periods(conn):
                compute_and_store_for_period(conn, pid, args.frequency)
        else:
            print("Please specify --period PERIOD_ID or --all")
            sys.exit(2)
    finally:
        conn.close()


if __name__ == '__main__':
    main()


