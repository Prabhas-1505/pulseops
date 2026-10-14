"""Explainable, hardcoded detection thresholds.

These are intentionally simple and documented inline rather than tuned by
any learning process -- the spec asks for "explainable thresholds," which
means a human can read this file and understand exactly why an alert fired.
"""

import os

WINDOW_SECONDS = int(os.environ.get("DETECTOR_WINDOW_SECONDS", "10"))
TICK_SECONDS = WINDOW_SECONDS

# Don't evaluate a window with too few samples -- a single slow/erroring
# request in an otherwise-quiet window is noise, not a signal.
MIN_SAMPLES = int(os.environ.get("DETECTOR_MIN_SAMPLES", "3"))

# Error rate: flag if more than this fraction of business requests in the
# window returned a 5xx status.
ERROR_RATE_THRESHOLD = float(os.environ.get("DETECTOR_ERROR_RATE_THRESHOLD", "0.2"))

# Latency: flag if the window's median latency exceeds
# max(baseline * LATENCY_MULTIPLIER, LATENCY_FLOOR_MS). The floor exists so
# a near-zero baseline (e.g. right after cold start) doesn't make ordinary
# jitter look like a 2x anomaly.
LATENCY_MULTIPLIER = float(os.environ.get("DETECTOR_LATENCY_MULTIPLIER", "2.0"))
LATENCY_FLOOR_MS = float(os.environ.get("DETECTOR_LATENCY_FLOOR_MS", "20.0"))

# EWMA smoothing factor for the latency baseline: new_baseline =
# alpha * current_window_median + (1 - alpha) * old_baseline.
EWMA_ALPHA = float(os.environ.get("DETECTOR_EWMA_ALPHA", "0.3"))

# Dedup: a new alert with the same (service, metric_type) signature as an
# open incident, within this many seconds of that incident's last_seen_at,
# attaches to the existing incident instead of opening a new one. Also used
# as the lookback window for cross-service trace-overlap correlation.
DEDUP_WINDOW_SECONDS = int(os.environ.get("DETECTOR_DEDUP_WINDOW_SECONDS", str(WINDOW_SECONDS * 3)))

# An open incident with no new alerts for this long is considered resolved.
RESOLUTION_GRACE_SECONDS = int(os.environ.get("DETECTOR_RESOLUTION_GRACE_SECONDS", str(WINDOW_SECONDS * 4)))
