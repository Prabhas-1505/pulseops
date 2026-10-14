"""Deterministic unit tests for detector/rules.py -- pure functions, no stack required."""

from datetime import datetime, timezone

from detector.metrics import WindowMetrics
from detector.rules import check_error_rate, check_latency, update_ewma

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _metrics(count, error_count, median_latency_ms, service="svc") -> WindowMetrics:
    return WindowMetrics(
        service=service, window_start=NOW, window_end=NOW, count=count,
        error_count=error_count, median_latency_ms=median_latency_ms,
    )


def test_error_rate_below_min_samples_is_ignored():
    # 1 error out of 2 requests is a 50% rate, well above threshold -- but
    # with only 2 samples this is noise, not signal.
    assert check_error_rate(_metrics(count=2, error_count=1, median_latency_ms=10), min_samples=3) is None


def test_error_rate_above_threshold_fires():
    anomaly = check_error_rate(_metrics(count=10, error_count=5, median_latency_ms=10), threshold=0.2)
    assert anomaly is not None
    assert anomaly.metric_type == "error_rate"
    assert anomaly.observed_value == 0.5


def test_error_rate_below_threshold_does_not_fire():
    assert check_error_rate(_metrics(count=10, error_count=1, median_latency_ms=10), threshold=0.2) is None


def test_latency_cold_start_no_baseline_does_not_fire():
    assert check_latency(_metrics(count=10, error_count=0, median_latency_ms=500), baseline_ms=None) is None


def test_latency_within_normal_range_does_not_fire():
    assert check_latency(_metrics(count=10, error_count=0, median_latency_ms=55), baseline_ms=50, multiplier=2.0) is None


def test_latency_spike_fires():
    anomaly = check_latency(_metrics(count=10, error_count=0, median_latency_ms=150), baseline_ms=50, multiplier=2.0)
    assert anomaly is not None
    assert anomaly.metric_type == "latency"
    assert anomaly.baseline_or_threshold == 50


def test_latency_floor_protects_near_zero_baseline_from_jitter():
    # baseline*multiplier = 2ms would make any jitter look anomalous;
    # the floor should win and suppress this.
    assert check_latency(_metrics(count=10, error_count=0, median_latency_ms=15), baseline_ms=1, multiplier=2.0, floor_ms=20.0) is None


def test_ewma_first_value_initializes_baseline():
    assert update_ewma(None, 100.0) == 100.0


def test_ewma_smooths_toward_new_value():
    updated = update_ewma(old_baseline=100.0, new_value=200.0, alpha=0.3)
    assert updated == 0.3 * 200.0 + 0.7 * 100.0
