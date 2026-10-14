import os
import uuid

from fastapi import FastAPI
from opentelemetry import trace
from pydantic import BaseModel, Field

from services.common.faults import apply_fault_injection, build_admin_router
from services.common.telemetry import setup_telemetry

SERVICE_NAME = os.environ.get("SERVICE_NAME", "payments")

app = FastAPI(title="PulseOps Payments Service")
setup_telemetry(app, SERVICE_NAME)
app.include_router(build_admin_router())


class ChargeRequest(BaseModel):
    order_id: str
    amount_cents: int = Field(gt=0)


class ChargeResponse(BaseModel):
    charge_id: str
    order_id: str
    amount_cents: int
    status: str


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/charge", response_model=ChargeResponse)
async def charge(req: ChargeRequest) -> ChargeResponse:
    await apply_fault_injection()
    charge_id = f"chg_{uuid.uuid4().hex[:16]}"

    span = trace.get_current_span()
    span.set_attribute("pulseops.charge_id", charge_id)
    span.set_attribute("pulseops.order_id", req.order_id)

    return ChargeResponse(
        charge_id=charge_id,
        order_id=req.order_id,
        amount_cents=req.amount_cents,
        status="captured",
    )
