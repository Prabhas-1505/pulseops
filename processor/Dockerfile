FROM python:3.11-slim

WORKDIR /app

COPY processor/requirements.txt processor/requirements.txt
RUN pip install --no-cache-dir -r processor/requirements.txt

COPY services/__init__.py services/__init__.py
COPY services/common services/common
COPY processor processor
COPY alembic.ini alembic.ini

CMD ["python", "-m", "processor.main"]
