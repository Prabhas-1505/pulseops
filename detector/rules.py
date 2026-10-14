"""Pure anomaly-detection rules: no DB, no I/O, fully deterministic given inputs."""

from __future__ import annotations

from dataclasses import dataclass

from detector.config import ERROR_RATE_THRESHOLD, EWMA_ALPHA, LATENCY_FLOOR_MS, LATENCY_MULTIPLIER, MIN_SAMPLES
from detector.metrics import WindowMetrics


@dataclass
class Anomaly:
    service: str
    metric_type: str  # "error_rate" | "latency"
    observed_value: float
    baseline_or_threshold: float
    explanation: str


def check_error_rate(m: WindowMetrics, threshold: float = ERROR_RATE_THRESHOLD, min_samples: int = MIN_SAMPLES) -> Anomaly | None:
    if m.count < min_samples:
        return None
    rate = m.error_count / m.count
    if rate <= threshold:
        return None
    return Anomaly(
        service=m.service,
        metric_type="error_rate",
        observed_value=rate,
        baseline_or_threshold=threshold,
        explanation=(
            f"{m.error_count}/{m.count} requests ({rate:.0%}) returned a 5xx status in the last "
            f"window, exceeding the {threshold:.0%} threshold"
        ),
    )


def check_latency(
    m: WindowMetrics,
    baseline_ms: float | None,
    multiplier: float = LATENCY_MULTIPLIER,
    floor_ms: float = LATENCY_FLOOR_MS,
    min_samples: int = MIN_SAMPLES,
) -> Anomaly | None:
    if m.count < min_samples or m.median_latency_ms is None or baseline_ms is None:
        return None
    threshold = max(baseline_ms * multiplier, floor_ms)
    if m.median_latency_ms <= threshold:
        return None
    return Anomaly(
        service=m.service,
        metric_type="latency",
        observed_value=m.median_latency_ms,
        baseline_or_threshold=baseline_ms,
        explanation=(
            f"median latency {m.median_latency_ms:.1f}ms exceeds {multiplier:g}x the EWMA baseline "
            f"({baseline_ms:.1f}ms) -- threshold was {threshold:.1f}ms"
        ),
    )


def update_ewma(old_baseline: float | None, new_value: float, alpha: float = EWMA_ALPHA) -> float:
    if old_baseline is None:
        return new_value
    return alpha * new_value + (1 - alpha) * old_baseline
