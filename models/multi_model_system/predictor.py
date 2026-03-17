from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from models.arima_model import ARIMAPredictor


@dataclass
class MultiModelIntegratedPredictor:
    """
    Generalized version of the referenced repo's integrated system:
    technical model + macro model + fundamental model + regime/risk adjustment.
    """

    horizon: int = 30

    def predict(self, closes: Sequence[float]) -> list[float]:
        bars = [{"close": float(value), "volume": None} for value in closes if value is not None]
        return self.predict_from_bars(bars)

    def predict_from_bars(self, bars: Sequence[dict]) -> list[float]:
        frame = self._build_frame(bars)
        if frame.empty:
            return [0.0 for _ in range(self.horizon)]
        if len(frame) < 40:
            return ARIMAPredictor(horizon=self.horizon).predict(frame["close"].tolist())

        technical_path = self._technical_path(frame)
        macro_path = self._macro_path(frame)
        fundamental_path = self._fundamental_path(frame)

        market_state = self._detect_market_state(frame)
        weights = self._dynamic_weights(market_state)
        regime_path = self._apply_regime_adjustment(technical_path, frame, market_state)

        combined = (
            technical_path * weights["technical"]
            + regime_path * weights["enhanced"]
            + fundamental_path * weights["fundamental"]
            + macro_path * weights["macro"]
        )

        combined = self._smooth_extremes(combined, frame["close"].iloc[-1])
        return [round(float(value), 2) for value in combined]

    def _build_frame(self, bars: Sequence[dict]) -> pd.DataFrame:
        rows = []
        for bar in bars:
            close = bar.get("close")
            volume = bar.get("volume")
            if close in (None, ""):
                continue
            rows.append(
                {
                    "close": float(close),
                    "volume": float(volume) if volume not in (None, "") else np.nan,
                }
            )

        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame

        frame["volume"] = frame["volume"].ffill().bfill().fillna(0.0)
        frame["return"] = frame["close"].pct_change().fillna(0.0)
        frame["log_return"] = np.log(frame["close"]).diff().fillna(0.0)
        frame["volatility_10"] = frame["return"].rolling(10).std().bfill().fillna(0.0)
        frame["volatility_20"] = frame["return"].rolling(20).std().bfill().fillna(0.0)
        frame["ma_10"] = frame["close"].rolling(10).mean().bfill()
        frame["ma_20"] = frame["close"].rolling(20).mean().bfill()
        frame["ma_60"] = frame["close"].rolling(60).mean().bfill()
        frame["momentum_5"] = frame["close"].pct_change(5).fillna(0.0)
        frame["momentum_20"] = frame["close"].pct_change(20).fillna(0.0)
        frame["drawdown_20"] = frame["close"] / frame["close"].rolling(20).max().bfill() - 1.0
        frame["volume_trend"] = frame["volume"].pct_change(5).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        frame["price_vs_ma60"] = frame["close"] / frame["ma_60"] - 1.0
        return frame

    def _technical_path(self, frame: pd.DataFrame) -> np.ndarray:
        predictor = ARIMAPredictor(horizon=self.horizon)
        return np.array(predictor.predict(frame["close"].tolist()), dtype=float)

    def _macro_path(self, frame: pd.DataFrame) -> np.ndarray:
        latest_price = float(frame["close"].iloc[-1])
        returns = frame["log_return"].iloc[-min(180, len(frame)) :].to_numpy()

        try:
            from statsmodels.tsa.statespace.sarimax import SARIMAX

            exog = frame[["momentum_20", "volatility_20", "volume_trend"]].iloc[-len(returns) :].fillna(0.0)
            model = SARIMAX(
                returns,
                exog=exog,
                order=(1, 0, 1),
                trend="c",
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False)

            future_exog = self._future_exog(frame, horizon=self.horizon)
            forecast_returns = np.asarray(model.forecast(steps=self.horizon, exog=future_exog), dtype=float)
        except Exception:
            base = float(frame["momentum_20"].iloc[-1]) / 20.0
            sigma = float(frame["volatility_20"].iloc[-1])
            pattern = self._oscillation_pattern(frame["log_return"].tolist(), max(sigma * 0.8, 0.001))
            forecast_returns = np.array(
                [
                    base + pattern[index % len(pattern)] * (0.94**index)
                    for index in range(self.horizon)
                ],
                dtype=float,
            )

        prices = self._returns_to_prices(latest_price, forecast_returns)
        return np.array(prices, dtype=float)

    def _fundamental_path(self, frame: pd.DataFrame) -> np.ndarray:
        latest_price = float(frame["close"].iloc[-1])
        recent = frame.iloc[-min(220, len(frame)) :].copy()
        long_trend = float(recent["ma_60"].iloc[-1] / max(recent["close"].iloc[-1], 1e-9) - 1.0)
        price_gap = float(recent["price_vs_ma60"].iloc[-1])
        volume_trend = float(recent["volume_trend"].rolling(5).mean().iloc[-1])
        slope = float((recent["ma_20"].iloc[-1] - recent["ma_60"].iloc[-1]) / max(recent["ma_60"].iloc[-1], 1e-9))

        try:
            import statsmodels.api as sm

            recent["target"] = recent["close"].shift(-10) / recent["close"] - 1.0
            model_data = recent[["momentum_20", "price_vs_ma60", "volume_trend", "drawdown_20", "target"]].dropna()
            if len(model_data) < 40:
                raise ValueError("insufficient samples")

            exog = sm.add_constant(model_data[["momentum_20", "price_vs_ma60", "volume_trend", "drawdown_20"]], has_constant="add")
            fitted = sm.OLS(model_data["target"], exog).fit()

            future_rows = []
            for step in range(1, self.horizon + 1):
                damping = 0.985**step
                future_rows.append(
                    {
                        "const": 1.0,
                        "momentum_20": float(recent["momentum_20"].iloc[-1]) * damping,
                        "price_vs_ma60": price_gap * (0.97**step),
                        "volume_trend": volume_trend * (0.95**step),
                        "drawdown_20": float(recent["drawdown_20"].iloc[-1]) * (0.98**step),
                    }
                )
            future_exog = pd.DataFrame(future_rows)
            long_horizon_return = np.asarray(fitted.predict(future_exog), dtype=float)
            forecast_returns = long_horizon_return / 10.0
        except Exception:
            anchor_return = (
                slope * 0.35
                - price_gap * 0.18
                + volume_trend * 0.05
                + long_trend * 0.12
            )
            sigma = float(recent["volatility_20"].iloc[-1])
            pattern = self._oscillation_pattern(recent["return"].tolist(), max(sigma * 0.45, 0.0008))
            forecast_returns = np.array(
                [
                    anchor_return + pattern[index % len(pattern)] * (0.96**index)
                    for index in range(self.horizon)
                ],
                dtype=float,
            )

        prices = self._returns_to_prices(latest_price, forecast_returns)
        return np.array(prices, dtype=float)

    def _detect_market_state(self, frame: pd.DataFrame) -> str:
        latest = frame.iloc[-1]
        volatility = float(latest["volatility_20"])
        drawdown = float(latest["drawdown_20"])
        momentum = float(latest["momentum_20"])
        price_gap = float(latest["price_vs_ma60"])

        if drawdown < -0.08 or volatility > 0.028:
            return "crisis"
        if drawdown < -0.04 or momentum < -0.03:
            return "bear"
        if momentum > 0.04 and price_gap > 0.01:
            return "bull"
        return "normal"

    def _dynamic_weights(self, market_state: str) -> dict[str, float]:
        weights = {
            "technical": 0.40,
            "enhanced": 0.40,
            "fundamental": 0.12,
            "macro": 0.08,
        }

        if market_state == "bull":
            weights["technical"] += 0.04
            weights["enhanced"] -= 0.02
            weights["macro"] += 0.02
        elif market_state == "bear":
            weights["enhanced"] += 0.05
            weights["technical"] -= 0.03
            weights["macro"] += 0.01
            weights["fundamental"] -= 0.03
        elif market_state == "crisis":
            weights["enhanced"] += 0.08
            weights["fundamental"] += 0.04
            weights["technical"] -= 0.08
            weights["macro"] -= 0.04

        total = sum(weights.values())
        return {key: value / total for key, value in weights.items()}

    def _apply_regime_adjustment(
        self,
        technical_path: np.ndarray,
        frame: pd.DataFrame,
        market_state: str,
    ) -> np.ndarray:
        latest_price = float(frame["close"].iloc[-1])
        latest_vol = float(frame["volatility_20"].iloc[-1])
        drawdown = float(frame["drawdown_20"].iloc[-1])
        momentum = float(frame["momentum_20"].iloc[-1])

        adjusted = []
        for step, price in enumerate(technical_path, start=1):
            raw_return = price / max(latest_price, 1e-9) - 1.0
            phase_wave = latest_vol * 1.8 * math.sin(step * math.pi / 2.4)

            if market_state == "bull":
                regime_bias = max(momentum, 0.0) * 0.35
            elif market_state == "bear":
                regime_bias = min(momentum, 0.0) * 0.55 + min(drawdown, 0.0) * 0.18
            elif market_state == "crisis":
                regime_bias = min(drawdown, 0.0) * 0.35 - latest_vol * 1.5
            else:
                regime_bias = momentum * 0.12 - drawdown * 0.05

            adjusted_return = raw_return + regime_bias + phase_wave * (0.92**step)
            adjusted.append(latest_price * (1.0 + adjusted_return))

        return np.array(adjusted, dtype=float)

    def _future_exog(self, frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
        latest = frame.iloc[-1]
        rows = []
        for step in range(1, horizon + 1):
            rows.append(
                {
                    "momentum_20": float(latest["momentum_20"]) * (0.96**step),
                    "volatility_20": float(latest["volatility_20"]) * (0.985**step),
                    "volume_trend": float(latest["volume_trend"]) * (0.95**step),
                }
            )
        return pd.DataFrame(rows)

    def _oscillation_pattern(self, values: Sequence[float], fallback_scale: float) -> list[float]:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return [fallback_scale, -fallback_scale, fallback_scale * 0.5, -fallback_scale * 0.5]

        recent = cleaned[-min(8, len(cleaned)) :]
        center = sum(recent) / len(recent)
        centered = [value - center for value in recent]
        amplitude = max((abs(value) for value in centered), default=0.0)
        if amplitude < fallback_scale * 0.3:
            return [fallback_scale, -fallback_scale, fallback_scale * 0.6, -fallback_scale * 0.6]
        return centered

    def _returns_to_prices(self, latest_price: float, forecast_returns: Sequence[float]) -> list[float]:
        price = float(latest_price)
        forecasts = []
        for expected_return in forecast_returns:
            bounded_return = max(min(float(expected_return), 0.06), -0.06)
            price *= math.exp(bounded_return)
            forecasts.append(price)
        return forecasts

    def _smooth_extremes(self, values: np.ndarray, latest_price: float) -> np.ndarray:
        floor = latest_price * 0.72
        cap = latest_price * 1.28
        clipped = np.clip(values, floor, cap)

        smoothed = clipped.copy()
        for index in range(1, len(smoothed)):
            smoothed[index] = smoothed[index - 1] * 0.35 + smoothed[index] * 0.65
        return smoothed
