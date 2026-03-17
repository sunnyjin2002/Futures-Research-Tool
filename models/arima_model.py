from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


@dataclass
class ARIMAPredictor:
    order: tuple[int, int, int] = (1, 1, 1)
    horizon: int = 30

    def predict(self, closes: Sequence[float]) -> list[float]:
        history = [float(value) for value in closes if value is not None]
        if not history:
            return [0.0 for _ in range(self.horizon)]
        if len(history) < 6:
            return self._trend_adjusted_fallback(history)

        try:
            from statsmodels.tsa.arima.model import ARIMA

            returns = self._build_log_returns(history)
            if len(returns) < 6:
                return self._trend_adjusted_fallback(history)

            return_order = (max(self.order[0], 1), 0, max(self.order[2], 0))
            fitted = ARIMA(
                returns,
                order=return_order,
                trend="c",
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit()

            mean_returns = [float(value) for value in fitted.forecast(steps=self.horizon)]
            oscillation = self._build_residual_oscillation(
                residuals=fitted.resid,
                baseline=returns,
                horizon=self.horizon,
                damping=0.92,
                scale=0.85,
            )
            projected_returns = [mean + shock for mean, shock in zip(mean_returns, oscillation)]
            forecast_values = self._rebuild_prices(history[-1], projected_returns)

            if self._is_flat_forecast(forecast_values, history) or self._is_monotonic_forecast(forecast_values):
                return self._trend_adjusted_fallback(history)
            return forecast_values
        except Exception:
            return self._trend_adjusted_fallback(history)

    def _trend_adjusted_fallback(self, history: Sequence[float]) -> list[float]:
        if len(history) == 1:
            base = float(history[-1])
            return [round(base * (1 + 0.0015 * step), 2) for step in range(1, self.horizon + 1)]

        window = list(history[-min(60, len(history)) :])
        deltas = [window[index] - window[index - 1] for index in range(1, len(window))]
        avg_delta = sum(deltas) / len(deltas) if deltas else 0.0
        slope_delta = avg_delta * 0.65
        volatility = math.sqrt(sum(delta * delta for delta in deltas) / max(len(deltas), 1))
        smoothing = min(0.35, max(0.08, volatility / max(abs(window[-1]), 1)))
        wave_pattern = self._build_centered_pattern(deltas, fallback_scale=max(volatility * 0.18, 8.0))
        base = float(history[-1])

        predictions = []
        for step in range(1, self.horizon + 1):
            trend_component = slope_delta * step
            curvature = smoothing * math.log1p(step) * (1 if avg_delta >= 0 else -1)
            wave = wave_pattern[(step - 1) % len(wave_pattern)] * (0.88 ** step)
            price = base + trend_component + curvature + wave
            predictions.append(round(price, 2))
        return predictions

    def _build_log_returns(self, history: Sequence[float]) -> list[float]:
        returns = []
        for index in range(1, len(history)):
            previous = float(history[index - 1])
            current = float(history[index])
            if previous <= 0 or current <= 0:
                continue
            returns.append(math.log(current / previous))
        return returns

    def _rebuild_prices(self, latest_price: float, projected_returns: Sequence[float]) -> list[float]:
        price = float(latest_price)
        forecasts = []
        for expected_return in projected_returns:
            bounded_return = max(min(float(expected_return), 0.06), -0.06)
            price *= math.exp(bounded_return)
            forecasts.append(round(price, 2))
        return forecasts

    def _build_residual_oscillation(
        self,
        residuals: Sequence[float],
        baseline: Sequence[float],
        horizon: int,
        damping: float,
        scale: float,
    ) -> list[float]:
        recent_residuals = [float(value) for value in residuals if value is not None]
        recent_residuals = recent_residuals[-min(12, len(recent_residuals)) :]
        baseline_sigma = self._calc_std(baseline[-min(30, len(baseline)) :])
        fallback_scale = max(baseline_sigma * 0.65, 0.0009)
        pattern = self._build_centered_pattern(recent_residuals, fallback_scale=fallback_scale)

        oscillation = []
        for step in range(horizon):
            seasonal_push = 1.0 + 0.12 * math.sin((step + 1) * math.pi / 2.5)
            oscillation.append(pattern[step % len(pattern)] * (damping**step) * scale * seasonal_push)
        return oscillation

    def _build_centered_pattern(self, values: Sequence[float], fallback_scale: float) -> list[float]:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return [fallback_scale, -fallback_scale]

        window = cleaned[-min(8, len(cleaned)) :]
        center = sum(window) / len(window)
        centered = [value - center for value in window]
        amplitude = max((abs(value) for value in centered), default=0.0)
        if amplitude < fallback_scale * 0.3:
            return [fallback_scale, -fallback_scale, fallback_scale * 0.6, -fallback_scale * 0.6]
        return centered

    def _calc_std(self, values: Sequence[float]) -> float:
        cleaned = [float(value) for value in values if value is not None]
        if not cleaned:
            return 0.0
        mean = sum(cleaned) / len(cleaned)
        variance = sum((value - mean) ** 2 for value in cleaned) / len(cleaned)
        return math.sqrt(max(variance, 0.0))

    def _is_flat_forecast(self, forecast_values: Sequence[float], history: Sequence[float]) -> bool:
        if not forecast_values:
            return True
        latest_close = float(history[-1])
        recent_window = list(history[-min(30, len(history)) :])
        recent_range = max(recent_window) - min(recent_window) if len(recent_window) > 1 else 0.0
        forecast_range = max(forecast_values) - min(forecast_values)
        price_tolerance = max(abs(latest_close) * 0.0015, 1.0)
        range_tolerance = max(recent_range * 0.08, abs(latest_close) * 0.003)

        first_leg = forecast_values[: min(5, len(forecast_values))]
        nearly_same_as_latest = all(abs(value - latest_close) <= price_tolerance for value in first_leg)
        insufficient_variation = forecast_range <= range_tolerance

        return nearly_same_as_latest or insufficient_variation

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
