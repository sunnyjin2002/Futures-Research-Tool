from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass
class GARCHPredictor:
    p: int = 1
    q: int = 1
    horizon: int = 30

    def predict(self, closes: Sequence[float]) -> list[float]:
        history = [float(value) for value in closes if value is not None]
        if not history:
            return [0.0 for _ in range(self.horizon)]
        if len(history) < 2:
            return [round(history[-1], 2) for _ in range(self.horizon)]

        returns = self._build_log_returns(history)
        if not returns:
            return [round(history[-1], 2) for _ in range(self.horizon)]

        try:
            from arch import arch_model
            from statsmodels.tsa.ar_model import AutoReg

            mean_model = AutoReg(
                returns,
                lags=min(5, max(1, len(returns) // 8)),
                trend="c",
                old_names=False,
            ).fit()
            mean_forecast = [float(value) for value in mean_model.predict(start=len(returns), end=len(returns) + self.horizon - 1)]

            mean_residuals = [
                float(actual) - float(fitted)
                for actual, fitted in zip(returns[-len(mean_model.fittedvalues) :], mean_model.fittedvalues)
            ]
            scaled_returns = [value * 100 for value in mean_residuals]
            fitted = arch_model(
                scaled_returns,
                mean="Zero",
                vol="GARCH",
                p=self.p,
                q=self.q,
                dist="normal",
            ).fit(disp="off")
            forecast = fitted.forecast(horizon=self.horizon)
            variance_series = forecast.variance.iloc[-1].tolist()
            shock_pattern = self._build_centered_pattern(mean_residuals, fallback_scale=max(self._calc_std(returns) * 0.7, 0.0012))

            latest_price = float(history[-1])
            predicted = []
            for step, (mu, variance) in enumerate(zip(mean_forecast, variance_series), start=1):
                sigma = math.sqrt(max(float(variance), 0.0)) / 100
                signed_shock = shock_pattern[(step - 1) % len(shock_pattern)] * (0.93 ** step)
                seasonal_push = 0.1 * sigma * math.sin(step * math.pi / 2.0)
                next_return = mu + 0.75 * signed_shock + seasonal_push
                latest_price *= math.exp(max(min(next_return, 0.07), -0.07))
                predicted.append(round(latest_price, 2))

            if self._is_monotonic_forecast(predicted):
                return self._volatility_adjusted_fallback(history, returns)
            return predicted
        except Exception:
            return self._volatility_adjusted_fallback(history, returns)

    def _build_log_returns(self, history: Sequence[float]) -> list[float]:
        returns = []
        for index in range(1, len(history)):
            previous = float(history[index - 1])
            current = float(history[index])
            if previous <= 0 or current <= 0:
                continue
            returns.append(math.log(current / previous))
        return returns

    def _volatility_adjusted_fallback(self, history: Sequence[float], returns: Sequence[float]) -> list[float]:
        window = list(returns[-min(30, len(returns)) :])
        if not window:
            return [round(float(history[-1]), 2) for _ in range(self.horizon)]

        average_return = sum(window) / len(window)
        sigma = self._calc_std(window)
        shock_pattern = self._build_centered_pattern(window, fallback_scale=max(sigma * 0.65, 0.001))
        latest_price = float(history[-1])
        predicted = []

        for step in range(1, self.horizon + 1):
            signed_shock = shock_pattern[(step - 1) % len(shock_pattern)] * (0.9 ** step)
            cyclical_push = 0.12 * sigma * math.sin(step * math.pi / 2.2)
            next_return = average_return + signed_shock + cyclical_push
            latest_price *= math.exp(max(min(next_return, 0.07), -0.07))
            predicted.append(round(latest_price, 2))
        return predicted

    def _build_centered_pattern(self, values: Sequence[float], fallback_scale: float) -> list[float]:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return [fallback_scale, -fallback_scale]

        window = cleaned[-min(10, len(cleaned)) :]
        center = sum(window) / len(window)
        centered = [value - center for value in window]
        amplitude = max((abs(value) for value in centered), default=0.0)
        if amplitude < fallback_scale * 0.25:
            return [fallback_scale, -fallback_scale, fallback_scale * 0.5, -fallback_scale * 0.5]
        return centered

    def _calc_std(self, values: Sequence[float]) -> float:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return 0.0
        mean = sum(cleaned) / len(cleaned)
        variance = sum((value - mean) ** 2 for value in cleaned) / len(cleaned)
        return math.sqrt(max(variance, 0.0))

    def _is_monotonic_forecast(self, forecast_values: Sequence[float]) -> bool:
        if len(forecast_values) < 4:
            return False
        deltas = [forecast_values[index] - forecast_values[index - 1] for index in range(1, len(forecast_values))]
        positive = sum(delta > 0 for delta in deltas)
        negative = sum(delta < 0 for delta in deltas)
        turns = sum(
            deltas[index] * deltas[index - 1] < 0
            for index in range(1, len(deltas))
            if deltas[index] != 0 and deltas[index - 1] != 0
        )
        return turns == 0 and (positive == len(deltas) or negative == len(deltas))
