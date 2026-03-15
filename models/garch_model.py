from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass
class GARCHPredictor:
    p: int = 1
    q: int = 1
    horizon: int = 5

    def predict(self, closes: Sequence[float]) -> list[float]:
        history = [float(value) for value in closes if value is not None]
        if not history:
            return [0.0 for _ in range(self.horizon)]
        if len(history) < 2:
            return [round(history[-1], 2) for _ in range(self.horizon)]

        returns = [
            (history[index] - history[index - 1]) / history[index - 1]
            for index in range(1, len(history))
            if history[index - 1] != 0
        ]
        if not returns:
            return [round(history[-1], 2) for _ in range(self.horizon)]

        try:
            from arch import arch_model

            scaled_returns = [value * 100 for value in returns]
            fitted = arch_model(
                scaled_returns,
                mean="Constant",
                vol="GARCH",
                p=self.p,
                q=self.q,
                dist="normal",
            ).fit(disp="off")
            forecast = fitted.forecast(horizon=self.horizon)
            variance_series = forecast.variance.iloc[-1].tolist()
            average_return = sum(returns[-min(20, len(returns)) :]) / min(20, len(returns))
            latest_price = history[-1]

            predicted = []
            for step, variance in enumerate(variance_series, start=1):
                sigma = math.sqrt(max(float(variance), 0.0)) / 100
                next_price = latest_price * (1 + average_return + 0.15 * sigma)
                predicted.append(round(next_price, 2))
                latest_price = next_price
            return predicted
        except Exception:
            # Fall back to a volatility-adjusted trend when arch is unavailable.
            window = returns[-min(20, len(returns)) :]
            average_return = sum(window) / len(window)
            variance = sum((value - average_return) ** 2 for value in window) / len(window)
            sigma = math.sqrt(max(variance, 0.0))
            latest_price = history[-1]
            predicted = []
            for _ in range(self.horizon):
                latest_price = latest_price * (1 + average_return + 0.15 * sigma)
                predicted.append(round(latest_price, 2))
            return predicted
