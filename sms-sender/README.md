markdown
# Layer 3: High-Throughput SMS Sender

Consumes messages from Redis, sends them via 3rd party SMS provider at high speed (4000 TPS),
and publishes status events to Kafka. No direct database or Django calls - fully event-driven.

## Architecture
Redis (dispatch queue) → SMS Sender → 3rd Party API
↓ ↓
Retry Queue Kafka (send status)
↓ ↓
(delayed) Kafka (delivery status)
↑
Webhook endpoint

text

## What It Does

1. **Consumes from Redis** - Pops batches of messages from `sms:dispatch:queue`
2. **Rate Limited Sending** - Maintains exactly 4000 TPS to provider
3. **Sends to Provider** - Parallel HTTP requests with connection pooling
4. **Handles Retries** - Exponential backoff with Redis delayed queue
5. **Publishes Status** - All statuses to Kafka (no direct Django updates)
6. **Webhook Receiver** - Accepts delivery reports from provider

## Key Features

- **Kafka-first architecture** - No direct Django calls, fully decoupled
- **4000 TPS rate limiting** - Token bucket algorithm
- **Parallel workers** - Configurable worker count for throughput
- **Exponential retries** - 60s, 300s, 900s delays
- **HTTP/2 support** - For better API performance
- **Connection pooling** - 200 concurrent connections to provider

## Components

| File | Purpose |
|------|---------|
| `config.py` | Configuration management |
| `logger.py` | Logging setup with correlation IDs |
| `models.py` | Pydantic models |
| `rate_limiter.py` | Token bucket for 4000 TPS |
| `api_client.py` | 3rd party SMS API client |
| `redis_client.py` | Queue and retry operations |
| `kafka_client.py` | Kafka producer for status events |
| `workers.py` | Worker pool implementation |
| `sender_engine.py` | Main orchestrator |
| `main.py` | FastAPI app + webhook endpoints |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | redis://redis:6379/0 | Redis connection |
| `REDIS_QUEUE_KEY` | sms:dispatch:queue | Main queue |
| `REDIS_RETRY_KEY` | sms:retry:queue | Retry prefix |
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Kafka broker |
| `KAFKA_SEND_STATUS_TOPIC` | sms-send-status | Send status topic |
| `KAFKA_DELIVERY_TOPIC` | sms-delivery-status | Delivery topic |
| `API_URL` | https://api.provider.com/v1/send | Provider endpoint |
| `API_KEY` | your-api-key | Provider API key |
| `BATCH_SIZE` | 1000 | Messages per batch |
| `MAX_TPS` | 4000 | Rate limit |
| `WORKER_COUNT` | 4 | Parallel workers |
| `MAX_RETRIES` | 3 | Max retry attempts |
| `RETRY_DELAYS` | [60,300,900] | Retry delays |

## Running

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run
uvicorn app.main:app --reload --port 8002
Docker
bash
# Build
docker build -t sms-sender .

# Run
docker run -p 8002:8002 -p 8004:8004 \
  -e API_KEY=your-actual-key \
  -e API_URL=https://your-provider.com/v1/send \
  sms-sender
Docker Compose
yaml
version: '3.8'
services:
  sms-sender:
    build: .
    ports:
      - "8002:8002"
      - "8004:8004"
    environment:
      - REDIS_URL=redis://redis:6379/0
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - API_KEY=${API_KEY}
      - API_URL=${API_URL}
    volumes:
      - ./logs:/app/logs/layer3
API Endpoints
Webhook (for Provider)
POST /webhook/delivery - Receive delivery reports

Management
GET /health - Health check

GET /stats - Statistics

GET /ready - Readiness probe

GET / - Service info

Kafka Events
Send Status (sms-send-status)
json
{
  "event_type": "SEND_STATUS",
  "message_id": "uuid",
  "campaign_id": 123,
  "msisdn": "+1234567890",
  "status": "SENT",
  "provider_message_id": "provider-123",
  "retry_count": 0,
  "timestamp": "2024-01-01T00:00:00Z"
}
Delivery Status (sms-delivery-status)
json
{
  "event_type": "DELIVERY_STATUS",
  "message_id": "uuid",
  "provider_message_id": "provider-123",
  "status": "DELIVERED",
  "timestamp": "2024-01-01T00:00:05Z"
}
Monitoring
Stats endpoint shows real-time metrics

Logs: /app/logs/layer3/YYYY-MM-DD.log

Prometheus metrics (if configured)

Performance
Designed for 4000 TPS sustained

Tested with 4 workers, 1000 batch size

HTTP/2 + connection pooling minimizes overhead

Kafka batching reduces network calls

text

This completes Layer 3 with a clean, modular structure that follows the same pattern as Layers 1 and 2. The key feature is the **Kafka-first architecture** where all status updates go to Kafka instead of directly updating Django, making the system fully decoupled and scalable.