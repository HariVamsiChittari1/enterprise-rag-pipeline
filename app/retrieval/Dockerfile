FROM python:3.12-slim AS builder

WORKDIR /build
COPY retrieval/requirements.txt .
RUN pip install --no-cache-dir --target=/deps -r requirements.txt

FROM python:3.12-slim

RUN groupadd -r agent && useradd --no-log-init -r -g agent agent
WORKDIR /app

COPY --from=builder /deps /usr/local/lib/python3.12/site-packages
COPY retrieval/ retrieval/
COPY source_registry.py ./source_registry.py
COPY cosmos_connection.py ./cosmos_connection.py

USER agent
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health/live')" || exit 1

CMD ["python", "-m", "uvicorn", "retrieval.main:app", "--host", "0.0.0.0", "--port", "8080"]
