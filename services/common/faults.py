"""In-memory, per-process fault injection shared by the demo services.

This is intentionally simple for Phase 1: a single mutable config object
guarded by an asyncio lock, exposed through /admin/faults. It is NOT shared
across replicas/processes -- each service instance has its own fault state.
There is no authentication on /admin/faults in Phase 1; the management API
(Phase 4) will own authenticated control surfaces.
"""

import asyncio
import random
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


class FaultConfig(BaseModel):
    latency_ms: int = Field(0, ge=0, description="Extra delay injected on affected requests")
    latency_probability: float = Field(0.0, ge=0.0, le=1.0)
    error_probability: float = Field(0.0, ge=0.0, le=1.0)
    error_status_code: int = Field(500, ge=400, le=599)
    outage: bool = Field(False, description="If true, every request fails immediately with 503")


class _FaultState:
    def __init__(self) -> None:
        self._config = FaultConfig()
        self._lock = asyncio.Lock()

    async def get(self) -> FaultConfig:
        async with self._lock:
            return self._config.model_copy()

    async def set(self, config: FaultConfig) -> FaultConfig:
        async with self._lock:
            self._config = config
            return self._config.model_copy()


fault_state = _FaultState()


async def apply_fault_injection() -> None:
    """Call at the top of a business endpoint. Raises HTTPException or sleeps in place."""
    config = await fault_state.get()

    if config.outage:
        raise HTTPException(status_code=503, detail="service unavailable (fault injection: outage)")

    if config.latency_probability > 0 and random.random() < config.latency_probability:
        start = time.monotonic()
        await asyncio.sleep(config.latency_ms / 1000)
        _ = time.monotonic() - start  # observable via the span duration itself

    if config.error_probability > 0 and random.random() < config.error_probability:
        raise HTTPException(
            status_code=config.error_status_code,
            detail=f"injected failure (fault injection: error_probability={config.error_probability})",
        )


def build_admin_router() -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.get("/faults", response_model=FaultConfig)
    async def get_faults() -> FaultConfig:
        return await fault_state.get()

    @router.post("/faults", response_model=FaultConfig)
    async def set_faults(config: FaultConfig) -> FaultConfig:
        return await fault_state.set(config)

    @router.post("/faults/reset", response_model=FaultConfig)
    async def reset_faults() -> FaultConfig:
        return await fault_state.set(FaultConfig())

    return router
