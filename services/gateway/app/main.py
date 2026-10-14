import os
import uuid

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from pydantic import BaseModel, Field

from services.common.faults import apply_fault_injection, build_admin_router
from services.common.telemetry import setup_telemetry

SERVICE_NAME = os.environ.get("SERVICE_NAME", "gateway")
ORDERS_BASE_URL = os.environ.get("ORDERS_BASE_URL", "http://localhost:8001")

app = FastAPI(title="PulseOps API Gateway")
setup_telemetry(app, SERVICE_NAME)
app.include_router(build_admin_router())


class CheckoutRequest(BaseModel):
    item: str
    amount_cents: int = Field(gt=0)


class CheckoutResponse(BaseModel):
    request_id: str
    order_id: str
    charge_id: str
    status: str


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/checkout", response_model=CheckoutResponse)
async def checkout(req: CheckoutRequest) -> CheckoutResponse:
    await apply_fault_injection()

    request_id = f"req_{uuid.uuid4().hex[:16]}"
    span = trace.get_current_span()
    span.set_attribute("pulseops.request_id", request_id)
    span.set_attribute("pulseops.item", req.item)
    span.set_attribute("pulseops.amount_cents", req.amount_cents)

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.post(
                f"{ORDERS_BASE_URL}/orders",
                json={"item": req.item, "amount_cents": req.amount_cents},
            )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"orders unreachable: {exc}") from exc

    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"orders rejected request: {resp.status_code} {resp.text}")

    order = resp.json()
    return CheckoutResponse(
        request_id=request_id,
        order_id=order["order_id"],
        charge_id=order["charge_id"],
        status=order["status"],
    )
