import os
import uuid

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from pydantic import BaseModel, Field

from services.common.faults import apply_fault_injection, build_admin_router
from services.common.telemetry import setup_telemetry

SERVICE_NAME = os.environ.get("SERVICE_NAME", "orders")
PAYMENTS_BASE_URL = os.environ.get("PAYMENTS_BASE_URL", "http://localhost:8002")

app = FastAPI(title="PulseOps Orders Service")
setup_telemetry(app, SERVICE_NAME)
app.include_router(build_admin_router())


class OrderRequest(BaseModel):
    item: str
    amount_cents: int = Field(gt=0)


class OrderResponse(BaseModel):
    order_id: str
    item: str
    amount_cents: int
    charge_id: str
    status: str


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/orders", response_model=OrderResponse)
async def create_order(req: OrderRequest) -> OrderResponse:
    await apply_fault_injection()

    order_id = f"ord_{uuid.uuid4().hex[:16]}"
    span = trace.get_current_span()
    span.set_attribute("pulseops.order_id", order_id)
    span.set_attribute("pulseops.item", req.item)

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{PAYMENTS_BASE_URL}/charge",
                json={"order_id": order_id, "amount_cents": req.amount_cents},
            )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"payments unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"payments rejected charge: {resp.status_code} {resp.text}")

    charge = resp.json()
    return OrderResponse(
        order_id=order_id,
        item=req.item,
        amount_cents=req.amount_cents,
        charge_id=charge["charge_id"],
        status="confirmed",
    )
