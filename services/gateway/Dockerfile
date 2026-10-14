FROM python:3.11-slim

WORKDIR /app

COPY services/requirements.txt services/requirements.txt
RUN pip install --no-cache-dir -r services/requirements.txt

COPY services/__init__.py services/__init__.py
COPY services/common services/common
COPY services/gateway services/gateway

EXPOSE 8000

CMD ["uvicorn", "services.gateway.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
