from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Union


PRODUCT_SOURCE_CONFIG = {
    "SHFE-CU": {"akshare_symbol": "CU0", "yahoo_symbol": "HG=F"},
    "SHFE-AL": {"akshare_symbol": "AL0"},
    "SHFE-AU": {"akshare_symbol": "AU0", "yahoo_symbol": "GC=F"},
    "SHFE-AG": {"akshare_symbol": "AG0", "yahoo_symbol": "SI=F"},
    "INE-SC": {"akshare_symbol": "SC0", "yahoo_symbol": "CL=F"},
    "DCE-I": {"akshare_symbol": "I0"},
    "DCE-M": {"akshare_symbol": "M0"},
    "DCE-Y": {"akshare_symbol": "Y0"},
    "CZCE-TA": {"akshare_symbol": "TA0"},
    "CZCE-MA": {"akshare_symbol": "MA0"},
    "CFFEX-IF": {"akshare_symbol": "IF0"},
}


def get_connection(db_path: Union[Path, str]) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_market_data_table(db_path: Union[Path, str]) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                futures_id TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(futures_id, trading_date)
            )
            """
        )
        conn.commit()


def get_latest_market_date(db_path: Union[Path, str], futures_id: str) -> Optional[str]:
    ensure_market_data_table(db_path)
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT trading_date
            FROM market_data
            WHERE futures_id = ?
            ORDER BY trading_date DESC
            LIMIT 1
            """,
            (futures_id,),
        ).fetchone()
    return row["trading_date"] if row else None


def load_daily_bars(
    db_path: Union[Path, str],
    futures_id: str,
    limit: int = 120,
) -> list[dict]:
    ensure_market_data_table(db_path)
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT futures_id, trading_date, open_price, high_price, low_price, close_price, volume, source
            FROM market_data
            WHERE futures_id = ?
            ORDER BY trading_date DESC
            LIMIT ?
            """,
            (futures_id, limit),
        ).fetchall()

    ordered_rows = list(reversed(rows))
    return [
        {
            "futuresId": row["futures_id"],
            "date": row["trading_date"],
            "open": float(row["open_price"]),
            "high": float(row["high_price"]),
            "low": float(row["low_price"]),
            "close": float(row["close_price"]),
            "volume": float(row["volume"]) if row["volume"] is not None else None,
            "source": row["source"],
        }
        for row in ordered_rows
    ]


def upsert_market_data(
    db_path: Union[Path, str], futures_id: str, bars: Iterable[dict], source: str
) -> int:
    ensure_market_data_table(db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inserted = 0
    with get_connection(db_path) as conn:
        for bar in bars:
            trading_date = normalize_trade_date(bar.get("date") or bar.get("trading_date"))
            if not trading_date:
                continue
            conn.execute(
                """
                INSERT INTO market_data (
                    futures_id, trading_date, open_price, high_price, low_price, close_price,
                    volume, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(futures_id, trading_date) DO UPDATE SET
                    open_price = excluded.open_price,
                    high_price = excluded.high_price,
                    low_price = excluded.low_price,
                    close_price = excluded.close_price,
                    volume = excluded.volume,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                (
                    futures_id,
                    trading_date,
                    float(bar["open"]),
                    float(bar["high"]),
                    float(bar["low"]),
                    float(bar["close"]),
                    float(bar["volume"]) if bar.get("volume") is not None else None,
                    source,
                    now,
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


def refresh_market_data_if_needed(
    db_path: Union[Path, str],
    futures_id: str,
    lookback_days: int = 180,
) -> list[dict]:
    latest_date = get_latest_market_date(db_path, futures_id)
    today = date.today().isoformat()
    needs_refresh = latest_date is None or latest_date == today

    if needs_refresh:
        extract_and_store_daily_data(db_path=db_path, futures_id=futures_id, lookback_days=lookback_days)

    return load_daily_bars(db_path=db_path, futures_id=futures_id, limit=lookback_days)


def extract_and_store_daily_data(
    db_path: Union[Path, str],
    futures_id: str,
    lookback_days: int = 180,
) -> list[dict]:
    source_config = PRODUCT_SOURCE_CONFIG.get(futures_id, {})
    akshare_bars = fetch_from_akshare(source_config.get("akshare_symbol"), lookback_days)
    if akshare_bars:
        upsert_market_data(db_path, futures_id, akshare_bars, source="akshare")

    yahoo_bars = fetch_from_yahoo(source_config.get("yahoo_symbol"), lookback_days)
    if yahoo_bars:
        upsert_market_data(db_path, futures_id, yahoo_bars, source="yahoo_finance")

    return load_daily_bars(db_path=db_path, futures_id=futures_id, limit=lookback_days)


def fetch_from_akshare(symbol: Optional[str], lookback_days: int) -> list[dict]:
    if not symbol:
        return []

    try:
        import akshare as ak
    except Exception:
        return []

    data_frame = None
    candidates = [
        lambda: ak.futures_zh_daily_sina(symbol=symbol),
        lambda: ak.get_futures_daily(start_date=None, end_date=None, market=symbol),
    ]

    for loader in candidates:
        try:
            data_frame = loader()
            if data_frame is not None and not data_frame.empty:
                break
        except Exception:
            continue

    if data_frame is None or data_frame.empty:
        return []

    records = []
    for _, row in data_frame.tail(lookback_days).iterrows():
        records.append(
            {
                "date": normalize_trade_date(
                    row.get("date")
                    or row.get("日期")
                    or row.get("trade_date")
                    or row.get("交易日期")
                ),
                "open": row.get("open") or row.get("开盘价") or row.get("开盘"),
                "high": row.get("high") or row.get("最高价") or row.get("最高"),
                "low": row.get("low") or row.get("最低价") or row.get("最低"),
                "close": row.get("close") or row.get("收盘价") or row.get("收盘"),
                "volume": row.get("volume") or row.get("成交量"),
            }
        )
    return [bar for bar in records if is_complete_bar(bar)]


def fetch_from_yahoo(symbol: Optional[str], lookback_days: int) -> list[dict]:
    if not symbol:
        return []

    try:
        import yfinance as yf
    except Exception:
        return []

    try:
        history = yf.Ticker(symbol).history(period=f"{max(lookback_days, 30)}d", interval="1d")
    except Exception:
        return []

    if history is None or history.empty:
        return []

    records = []
    for index, row in history.tail(lookback_days).iterrows():
        records.append(
            {
                "date": normalize_trade_date(index),
                "open": row.get("Open"),
                "high": row.get("High"),
                "low": row.get("Low"),
                "close": row.get("Close"),
                "volume": row.get("Volume"),
            }
        )
    return [bar for bar in records if is_complete_bar(bar)]


def normalize_trade_date(value) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None
    text = text.replace("/", "-")
    if len(text) >= 10:
        return text[:10]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def is_complete_bar(bar: dict) -> bool:
    required = ("date", "open", "high", "low", "close")
    return all(bar.get(field) not in (None, "") for field in required)
