from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass
class ARIMAPredictor:
    order: tuple[int, int, int] = (1, 1, 1)
    horizon: int = 5

    def predict(self, closes: Sequence[float]) -> list[float]:
        history = [float(value) for value in closes if value is not None]
        if not history:
            return [0.0 for _ in range(self.horizon)]

        try:
            from statsmodels.tsa.arima.model import ARIMA

            fitted = ARIMA(history, order=self.order).fit()
            forecast = fitted.forecast(steps=self.horizon)
            return [round(float(value), 2) for value in forecast]
        except Exception:
            # Fall back to a drift-style forecast when statsmodels is unavailable.
            if len(history) == 1:
                return [round(history[-1], 2) for _ in range(self.horizon)]

            recent_window = history[-min(5, len(history)) :]
            deltas = [
                recent_window[index] - recent_window[index - 1]
                for index in range(1, len(recent_window))
            ]
            avg_delta = sum(deltas) / len(deltas) if deltas else 0.0
            base = history[-1]
            return [round(base + avg_delta * step, 2) for step in range(1, self.horizon + 1)]
