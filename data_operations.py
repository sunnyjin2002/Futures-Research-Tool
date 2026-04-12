from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from data_extraction import ensure_market_data_table


def get_connection(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path | str) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                futures_id TEXT NOT NULL,
                futures_code TEXT NOT NULL,
                futures_name TEXT NOT NULL,
                exchange_name TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_label TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                runtime_seconds REAL,
                prediction_payload TEXT,
                error_message TEXT
            )
            """
        )
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()}
        if "futures_id" not in columns:
            conn.execute("ALTER TABLE predictions ADD COLUMN futures_id TEXT")
            conn.execute(
                """
                UPDATE predictions
                SET futures_id = CASE exchange_name
                    WHEN '上海期货交易所' THEN 'SHFE-' || futures_code
                    WHEN '上海国际能源交易中心' THEN 'INE-' || futures_code
                    WHEN '大连商品交易所' THEN 'DCE-' || futures_code
                    WHEN '郑州商品交易所' THEN 'CZCE-' || futures_code
                    WHEN '中国金融期货交易所' THEN 'CFFEX-' || futures_code
                    WHEN '广州期货交易所' THEN 'GFEX-' || futures_code
                    ELSE futures_code
                END
                WHERE futures_id IS NULL
                """
            )
        if "error_message" not in columns:
            conn.execute("ALTER TABLE predictions ADD COLUMN error_message TEXT")
        conn.commit()

    ensure_market_data_table(db_path)
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                futures_id TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                ma5 REAL,
                ma10 REAL,
                ma20 REAL,
                boll_upper REAL,
                boll_middle REAL,
                boll_lower REAL,
                macd_diff REAL,
                macd_dea REAL,
                macd_hist REAL,
                kdj_k REAL,
                kdj_d REAL,
                kdj_j REAL,
                updated_at TEXT NOT NULL,
                UNIQUE(futures_id, trading_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_refresh_log (
                product_id TEXT PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                latest_trading_date TEXT,
                loaded_rows INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.commit()


def serialize_prediction(row: sqlite3.Row) -> dict:
    payload = json.loads(row["prediction_payload"]) if row["prediction_payload"] else None
    return {
        "id": row["id"],
        "futuresId": row["futures_id"],
        "futuresCode": row["futures_code"],
        "futuresName": row["futures_name"],
        "exchangeName": row["exchange_name"],
        "modelId": row["model_id"],
        "modelLabel": row["model_label"],
        "status": row["status"],
        "createdAt": row["created_at"],
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
        "runtimeSeconds": row["runtime_seconds"],
        "predictionPayload": payload,
        "errorMessage": row["error_message"],
    }


def serialize_market_row(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "futuresId": row["futures_id"],
        "tradingDate": row["trading_date"],
        "openPrice": row["open_price"],
        "highPrice": row["high_price"],
        "lowPrice": row["low_price"],
        "closePrice": row["close_price"],
        "volume": row["volume"],
        "source": row["source"],
        "updatedAt": row["updated_at"],
    }


def fetch_prediction_by_id(db_path: Path | str, prediction_id: int | str) -> Optional[dict]:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM predictions WHERE id = ?", (prediction_id,)).fetchone()
    if not row:
        return None
    return serialize_prediction(row)


def insert_prediction_record(db_path: Path | str, product: dict, model: dict, now_text: str) -> int:
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO predictions (
                futures_id, futures_code, futures_name, exchange_name, model_id, model_label,
                status, created_at, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product["id"],
                product["code"],
                product["name"],
                product["exchange"],
                model["id"],
                model["label"],
                "running",
                now_text,
                now_text,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def list_model_statuses(db_path: Path | str, product_id: str, models: list[dict]) -> list[dict]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT p.*
            FROM predictions p
            JOIN (
                SELECT model_id, MAX(id) AS latest_id
                FROM predictions
                WHERE futures_id = ? AND status = 'completed'
                GROUP BY model_id
            ) latest
            ON p.id = latest.latest_id
            ORDER BY p.model_label
            """,
            (product_id,),
        ).fetchall()

    latest_by_model = {row["model_id"]: serialize_prediction(row) for row in rows}
    model_statuses = []
    for model in models:
        latest = latest_by_model.get(model["id"])
        model_statuses.append(
            {
                "id": model["id"],
                "label": model["label"],
                "lastUpdated": latest["completedAt"] if latest else None,
                "prediction": latest,
            }
        )
    return model_statuses


def upsert_market_refresh_log(
    db_path: Path | str,
    product_id: str,
    refreshed_at: str,
    latest_trading_date: Optional[str],
    loaded_rows: int,
) -> None:
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO market_refresh_log (product_id, refreshed_at, latest_trading_date, loaded_rows)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                refreshed_at = excluded.refreshed_at,
                latest_trading_date = excluded.latest_trading_date,
                loaded_rows = excluded.loaded_rows
            """,
            (product_id, refreshed_at, latest_trading_date, loaded_rows),
        )
        conn.commit()


def get_market_refresh_status(db_path: Path | str, product_id: str) -> dict:
    with get_connection(db_path) as conn:
        market_row = conn.execute(
            """
            SELECT
                MAX(trading_date) AS latest_trading_date,
                MAX(updated_at) AS latest_updated_at,
                COUNT(*) AS row_count
            FROM market_data
            WHERE futures_id = ?
            """,
            (product_id,),
        ).fetchone()
        refresh_row = conn.execute(
            """
            SELECT refreshed_at, latest_trading_date, loaded_rows
            FROM market_refresh_log
            WHERE product_id = ?
            """,
            (product_id,),
        ).fetchone()

    latest_trading_date = market_row["latest_trading_date"] if market_row else None
    latest_updated_at = market_row["latest_updated_at"] if market_row else None
    row_count = int(market_row["row_count"]) if market_row and market_row["row_count"] is not None else 0
    if refresh_row:
        latest_updated_at = refresh_row["refreshed_at"] or latest_updated_at
        if not latest_trading_date:
            latest_trading_date = refresh_row["latest_trading_date"]

    return {
        "productId": product_id,
        "latestTradingDate": latest_trading_date,
        "latestUpdatedAt": latest_updated_at,
        "rowCount": row_count,
    }


def upsert_market_indicators(
    db_path: Path | str,
    product_id: str,
    candles: list[dict],
    indicators: dict,
    updated_at: Optional[str] = None,
) -> None:
    if not candles:
        return
    now = updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection(db_path) as conn:
        for index, candle in enumerate(candles):
            conn.execute(
                """
                INSERT INTO market_indicators (
                    futures_id, trading_date, ma5, ma10, ma20,
                    boll_upper, boll_middle, boll_lower,
                    macd_diff, macd_dea, macd_hist,
                    kdj_k, kdj_d, kdj_j, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(futures_id, trading_date) DO UPDATE SET
                    ma5 = excluded.ma5,
                    ma10 = excluded.ma10,
                    ma20 = excluded.ma20,
                    boll_upper = excluded.boll_upper,
                    boll_middle = excluded.boll_middle,
                    boll_lower = excluded.boll_lower,
                    macd_diff = excluded.macd_diff,
                    macd_dea = excluded.macd_dea,
                    macd_hist = excluded.macd_hist,
                    kdj_k = excluded.kdj_k,
                    kdj_d = excluded.kdj_d,
                    kdj_j = excluded.kdj_j,
                    updated_at = excluded.updated_at
                """,
                (
                    product_id,
                    candle["label"],
                    indicators["ma5"][index],
                    indicators["ma10"][index],
                    indicators["ma20"][index],
                    indicators["boll"]["upper"][index],
                    indicators["boll"]["middle"][index],
                    indicators["boll"]["lower"][index],
                    indicators["macd"]["diff"][index],
                    indicators["macd"]["dea"][index],
                    indicators["macd"]["hist"][index],
                    indicators["kdj"]["k"][index],
                    indicators["kdj"]["d"][index],
                    indicators["kdj"]["j"][index],
                    now,
                ),
            )
        conn.commit()


def load_market_indicators(db_path: Path | str, product_id: str, trading_dates: list[str]) -> Optional[dict]:
    if not trading_dates:
        return None
    placeholders = ", ".join("?" for _ in trading_dates)
    with get_connection(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM market_indicators
            WHERE futures_id = ? AND trading_date IN ({placeholders})
            """,
            (product_id, *trading_dates),
        ).fetchall()

    if len(rows) != len(trading_dates):
        return None

    row_map = {row["trading_date"]: row for row in rows}
    ordered_rows = [row_map.get(date) for date in trading_dates]
    if any(row is None for row in ordered_rows):
        return None

    def to_float_or_none(value) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    return {
        "ma5": [to_float_or_none(row["ma5"]) for row in ordered_rows],
        "ma10": [to_float_or_none(row["ma10"]) for row in ordered_rows],
        "ma20": [to_float_or_none(row["ma20"]) for row in ordered_rows],
        "boll": {
            "upper": [to_float_or_none(row["boll_upper"]) for row in ordered_rows],
            "middle": [to_float_or_none(row["boll_middle"]) for row in ordered_rows],
            "lower": [to_float_or_none(row["boll_lower"]) for row in ordered_rows],
        },
        "macd": {
            "diff": [to_float_or_none(row["macd_diff"]) for row in ordered_rows],
            "dea": [to_float_or_none(row["macd_dea"]) for row in ordered_rows],
            "hist": [to_float_or_none(row["macd_hist"]) for row in ordered_rows],
        },
        "kdj": {
            "k": [to_float_or_none(row["kdj_k"]) for row in ordered_rows],
            "d": [to_float_or_none(row["kdj_d"]) for row in ordered_rows],
            "j": [to_float_or_none(row["kdj_j"]) for row in ordered_rows],
        },
    }


def get_database_snapshot(db_path: Path | str, limit: Optional[int] = 100) -> dict:
    with get_connection(db_path) as conn:
        prediction_query = """
            SELECT * FROM predictions
            ORDER BY id DESC
        """
        market_query = """
            SELECT * FROM market_data
            ORDER BY trading_date DESC, id DESC
        """

        prediction_rows = conn.execute(
            prediction_query if limit is None else f"{prediction_query}\nLIMIT ?",
            () if limit is None else (limit,),
        ).fetchall()
        prediction_count = conn.execute("SELECT COUNT(*) AS count FROM predictions").fetchone()["count"]

        market_rows = conn.execute(
            market_query if limit is None else f"{market_query}\nLIMIT ?",
            () if limit is None else (limit,),
        ).fetchall()
        market_count = conn.execute("SELECT COUNT(*) AS count FROM market_data").fetchone()["count"]

    return {
        "predictions": {
            "count": prediction_count,
            "rows": [serialize_prediction(row) for row in prediction_rows],
        },
        "marketData": {
            "count": market_count,
            "rows": [serialize_market_row(row) for row in market_rows],
        },
    }


def delete_database_rows(db_path: Path | str, payload: dict) -> dict:
    prediction_ids = [int(item) for item in payload.get("predictionIds", [])]
    market_data_ids = [int(item) for item in payload.get("marketDataIds", [])]

    deleted_predictions = 0
    deleted_market_data = 0

    with get_connection(db_path) as conn:
        if prediction_ids:
            placeholders = ", ".join("?" for _ in prediction_ids)
            cursor = conn.execute(
                f"DELETE FROM predictions WHERE id IN ({placeholders})",
                prediction_ids,
            )
            deleted_predictions = cursor.rowcount

        if market_data_ids:
            placeholders = ", ".join("?" for _ in market_data_ids)
            cursor = conn.execute(
                f"DELETE FROM market_data WHERE id IN ({placeholders})",
                market_data_ids,
            )
            deleted_market_data = cursor.rowcount

        conn.commit()

    return {
        "deletedPredictions": deleted_predictions,
        "deletedMarketData": deleted_market_data,
    }
