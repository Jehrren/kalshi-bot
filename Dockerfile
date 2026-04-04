FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./

RUN mkdir -p /app/data && chown -R 1001:root /app/data && chmod 750 /app/data

RUN useradd -r -u 1001 -g root kalshi
USER kalshi

CMD ["python", "-u", "main.py"]
