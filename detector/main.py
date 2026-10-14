"""Periodic entrypoint: run one detection tick every TICK_SECONDS, forever."""

import logging
import time
from datetime import datetime, timezone

from detector.config import TICK_SECONDS
from detector.engine import run_once
from services.common.db import build_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("pulseops.detector")


def main() -> None:
    engine = build_engine()
    logger.info("detector started: tick every %ss", TICK_SECONDS)
    while True:
        now = datetime.now(timezone.utc)
        try:
            with engine.begin() as conn:
                alert_ids = run_once(conn, now)
            if alert_ids:
                logger.info("tick at %s: %d new alert(s) %s", now.isoformat(), len(alert_ids), alert_ids)
        except Exception:
            logger.exception("detection tick failed, will retry next tick")
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    main()
