from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import auth, incidents, services

app = FastAPI(
    title="PulseOps Management API",
    description=(
        "Authenticated API over PulseOps's detected incidents and service health. "
        "JWT bearer auth (register/login below), Redis-backed rate limiting on every route, "
        "paginated incident listing, and an optional AI incident summary with a "
        "deterministic non-LLM fallback."
    ),
    version="0.1.0",
)


# Permissive CORS: this is a local/demo system with no production deployment
# target. A real deployment would restrict this to the dashboard's actual
# origin -- flagged in docs/ARCHITECTURE.md as a known limitation, not an
# oversight.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["health"])
def health() -> dict:
    return {"status": "ok", "service": "api"}


app.include_router(auth.router)
app.include_router(incidents.router)
app.include_router(incidents.stream_router)
app.include_router(services.router)
