FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/logs && \
    touch /app/logs/results_writer.log && \
    chmod -R 777 /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "results_writer.py"]