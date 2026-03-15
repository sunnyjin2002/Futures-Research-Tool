from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from data_extraction import ensure_market_data_table, refresh_market_data_if_needed
from models import ARIMAPredictor, GARCHPredictor


@dataclass
class PredictionRunner:
    db_path: Path
    horizon: int = 5

    def run(
        self,
        futures_id: str,
        futures_code: str,
        futures_name: str,
        exchange_name: str,
        model_id: str,
    ) -> dict:
        ensure_market_data_table(self.db_path)
        market_data = refresh_market_data_if_needed(self.db_path, futures_id, lookback_days=240)
        if len(market_data) < 10:
            raise ValueError(f"{futures_name} 的历史日线数据不足，暂时无法运行 {model_id} 模型。")

        closes = [bar["close"] for bar in market_data]
        model = self._build_model(model_id)

        started_at = time.time()
        predicted_prices = model.predict(closes)
        runtime_seconds = round(time.time() - started_at, 2)

        last_date = market_data[-1]["date"]
        payload = [
            {
                "dayOffset": step,
                "label": f"T+{step}",
                "price": round(float(price), 2),
                "sourceDate": last_date,
                "futuresId": futures_id,
                "futuresCode": futures_code,
                "futuresName": futures_name,
                "exchangeName": exchange_name,
                "modelId": model_id,
            }
            for step, price in enumerate(predicted_prices, start=1)
        ]
        return {
            "predictionPayload": payload,
            "runtimeSeconds": runtime_seconds,
            "inputRows": len(market_data),
            "lastTradingDate": last_date,
        }

    def _build_model(self, model_id: str):
        if model_id == "arima":
            return ARIMAPredictor(horizon=self.horizon)
        if model_id == "garch":
            return GARCHPredictor(horizon=self.horizon)
        raise ValueError(f"尚未实现模型：{model_id}")


def run_prediction_job_for_record(db_path: Union[Path, str], prediction_id: int) -> None:
    db_path = Path(db_path)
    runner = PredictionRunner(db_path=db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM predictions WHERE id = ?", (prediction_id,)).fetchone()

    if row is None:
        return

    try:
        result = runner.run(
            futures_id=row["futures_id"],
            futures_code=row["futures_code"],
            futures_name=row["futures_name"],
            exchange_name=row["exchange_name"],
            model_id=row["model_id"],
        )
        status = "completed"
        error_message = None
        prediction_payload = json.dumps(result["predictionPayload"], ensure_ascii=False)
        runtime_seconds = result["runtimeSeconds"]
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        prediction_payload = None
        runtime_seconds = None
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE predictions
            SET status = ?, completed_at = ?, runtime_seconds = ?, prediction_payload = ?, error_message = ?
            WHERE id = ?
            """,
            (status, completed_at, runtime_seconds, prediction_payload, error_message, prediction_id),
        )
        conn.commit()
