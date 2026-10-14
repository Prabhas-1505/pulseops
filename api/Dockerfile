FROM python:3.11-slim

WORKDIR /app

COPY api/requirements.txt api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

COPY services/__init__.py services/__init__.py
COPY services/common services/common
COPY processor/__init__.py processor/__init__.py
COPY processor/models.py processor/models.py
COPY detector/__init__.py detector/__init__.py
COPY detector/models.py detector/models.py
COPY detector/config.py detector/config.py
COPY detector/metrics.py detector/metrics.py
COPY api api

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
