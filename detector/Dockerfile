FROM python:3.11-slim

WORKDIR /app

COPY detector/requirements.txt detector/requirements.txt
RUN pip install --no-cache-dir -r detector/requirements.txt

COPY services/__init__.py services/__init__.py
COPY services/common services/common
COPY detector detector

CMD ["python", "-m", "detector.main"]
