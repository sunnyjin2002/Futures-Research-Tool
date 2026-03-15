from __future__ import annotations

import functools
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Union


MANUAL_SOURCE_CONFIG = {
    "SHFE-CU": {"yahoo_symbol": "HG=F"},
    "SHFE-AU": {"yahoo_symbol": "GC=F"},
    "SHFE-AG": {"yahoo_symbol": "SI=F"},
    "INE-SC": {"yahoo_symbol": "CL=F"},
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
    lookback_days: int = 365,
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
    lookback_days: int = 365,
) -> list[dict]:
    source_config = resolve_source_config(futures_id)
    akshare_bars = fetch_from_akshare(
        futures_id=futures_id,
        exchange=source_config["exchange"],
        code=source_config["code"],
        lookback_days=lookback_days,
    )
    if akshare_bars:
        upsert_market_data(db_path, futures_id, akshare_bars, source="akshare")

    yahoo_bars = fetch_from_yahoo(source_config.get("yahoo_symbol"), lookback_days)
    if yahoo_bars:
        upsert_market_data(db_path, futures_id, yahoo_bars, source="yahoo_finance")

    bars = load_daily_bars(db_path=db_path, futures_id=futures_id, limit=lookback_days)
    if not bars:
        raise ValueError(f"未能抓取 {futures_id} 的行情数据，请检查 AkShare/Yahoo Finance 代码映射或网络。")
    return bars


def resolve_source_config(futures_id: str) -> dict:
    exchange, code = split_futures_id(futures_id)
    manual = MANUAL_SOURCE_CONFIG.get(futures_id, {})
    return {
        "exchange": exchange,
        "code": code,
        "yahoo_symbol": manual.get("yahoo_symbol"),
    }


def split_futures_id(futures_id: str) -> tuple[str, str]:
    if "-" in futures_id:
        exchange, code = futures_id.split("-", 1)
        return exchange.upper(), code.upper()
    return "", futures_id.upper()


@functools.lru_cache(maxsize=1)
def get_akshare_main_contract_map() -> dict:
    try:
        import akshare as ak
    except Exception:
        return {}

    try:
        data_frame = ak.futures_display_main_sina()
    except Exception:
        return {}

    if data_frame is None or data_frame.empty:
        return {}

    symbol_map = {}
    for _, row in data_frame.iterrows():
        symbol = str(row.get("symbol", "")).strip()
        exchange = str(row.get("exchange", "")).strip().upper()
        name = str(row.get("name", "")).strip()
        if not symbol:
            continue

        base_code = extract_base_code_from_symbol(symbol)
        symbol_map[(exchange, base_code)] = {
            "symbol": symbol,
            "exchange": exchange,
            "name": name,
        }
    return symbol_map


def extract_base_code_from_symbol(symbol: str) -> str:
    normalized = str(symbol).strip().upper()
    while normalized and normalized[-1].isdigit():
        normalized = normalized[:-1]
    return normalized


def map_exchange_for_akshare(exchange: str) -> str:
    mapping = {
        "SHFE": "SHFE",
        "INE": "INE",
        "DCE": "DCE",
        "CZCE": "CZCE",
        "CFFEX": "CFFEX",
        "GFEX": "GFEX",
    }
    return mapping.get(exchange.upper(), exchange.upper())


def fetch_from_akshare(
    futures_id: str,
    exchange: str,
    code: str,
    lookback_days: int,
) -> list[dict]:
    try:
        import akshare as ak
    except Exception:
        return []

    symbol_map = get_akshare_main_contract_map()
    lookup_key = (map_exchange_for_akshare(exchange), code.upper())
    symbol_entry = symbol_map.get(lookup_key)
    symbol = symbol_entry["symbol"] if symbol_entry else f"{code.upper()}0"

    end_dt = date.today()
    start_dt = date.fromordinal(end_dt.toordinal() - lookback_days)

    try:
        data_frame = ak.futures_main_sina(
            symbol=symbol,
            start_date=start_dt.strftime("%Y%m%d"),
            end_date=end_dt.strftime("%Y%m%d"),
        )
    except Exception:
        return []

    if data_frame is None or data_frame.empty:
        return []

    records = []
    for _, row in data_frame.tail(lookback_days).iterrows():
        records.append(
            {
                "date": normalize_trade_date(row.get("日期") or row.get("date")),
                "open": row.get("开盘价") or row.get("open"),
                "high": row.get("最高价") or row.get("high"),
                "low": row.get("最低价") or row.get("low"),
                "close": row.get("收盘价") or row.get("close"),
                "volume": row.get("成交量") or row.get("volume"),
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
