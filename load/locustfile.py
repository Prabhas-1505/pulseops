"""Locust load profile for PulseOps's demo request chain.

Targets the gateway (`--host http://localhost:8000`), which fans out to
orders -> payments on every request -- so this load-tests the whole
synchronous call chain, not just the gateway process itself.

Run:
    locust -f load/locustfile.py --host http://localhost:8000 \
        --users 50 --spawn-rate 10 --run-time 60s --headless \
        --csv load/results/run1 --html load/results/run1.html
"""

import random
import uuid

from locust import HttpUser, between, task


class CheckoutUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def checkout(self):
        payload = {
            "item": f"load-test-{uuid.uuid4().hex[:8]}",
            "amount_cents": random.randint(100, 10000),
        }
        self.client.post("/checkout", json=payload, name="/checkout")
