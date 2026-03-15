Layer 4: Status Updater Service

Consumes SMS status events from Kafka and updates the Django/PostgreSQL database. This is the final layer that closes the loop on every message sent in the SMS campaign system.

## 📋 Overview

Layer 4 is the **bookkeeper** of the SMS campaign system. It:
- Consumes send status and delivery events from Kafka
- Updates message statuses in PostgreSQL (primary path)
- Falls back to Django API when direct DB access fails
- Batches updates for high throughput (1000 updates/batch)
- Tracks campaign completion progress

## 🏗️ Architecture
Kafka Topics Layer 4 Django/PostgreSQL
──────────── ────── ─────────────────
┌─────────────────┐
sms-send-status ──▶│ Kafka Client │──(batch)──▶┌─────────────────┐
│ (Consumer) │ │ Database │
└─────────────────┘ │ Client │
│ │ (Primary) │
│ └─────────────────┘
│ │
▼ ▼
┌─────────────────┐ ┌─────────────────┐
│ Update Queue │──(fallback)─▶│ API Client │
│ (asyncio.Queue)│ │ (Secondary) │
└─────────────────┘ └─────────────────┘
│
sms-delivery-status ──┘ Kafka

text

## 📁 File Structure
status-updater/
├── app/
│ ├── init.py # Package init with version
│ ├── main.py # FastAPI app, health endpoints
│ ├── config.py # Configuration management
│ ├── logger.py # Logging setup
│ ├── models.py # Pydantic models
│ ├── db_client.py # PostgreSQL connection pool (primary)
│ ├── api_client.py # Django API + token management (fallback)
│ ├── kafka_client.py # Kafka consumer with batching
│ └── updater_engine.py # MAIN BUSINESS LOGIC - orchestrator
├── requirements.txt # Python dependencies
├── Dockerfile # Containerization
├── entrypoint.sh # Startup script
└── README.md # This file

text

## 🎯 What It Does

### 1. **Consumes Status Events from Kafka**
- **`sms-send-status`**: Results of sending attempts (SENT/FAILED)
- **`sms-delivery-status`**: Final delivery outcomes from provider (DELIVERED, FAILED, EXPIRED)

### 2. **Batches Updates for Efficiency**
- Collects updates in memory (batch size: 1000 or timeout: 100ms)
- Performs bulk database updates (1 query per batch vs 1 per message)
- Reduces database load by ~1000x

### 3. **Dual-Path Update Strategy**
| Path | When | Performance |
|------|------|-------------|
| **Primary: Direct DB** | Always tried first | 10,000+ updates/sec |
| **Fallback: Django API** | When DB fails | 100-500 updates/sec |

### 4. **Tracks Message Lifecycle**
- `SENT` → Message successfully sent to provider
- `FAILED` → Permanent failure (invalid number, etc.)
- `DELIVERED` → Provider confirmed delivery
- `EXPIRED` → Message expired before delivery

### 5. **Provides Comprehensive Monitoring**
- Kafka lag per partition
- Update rates and success rates
- Fallback usage statistics
- Token refresh counts

## ⚙️ Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| **Kafka** | | |
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Kafka broker |
| `KAFKA_SEND_STATUS_TOPIC` | sms-send-status | Send status topic |
| `KAFKA_DELIVERY_TOPIC` | sms-delivery-status | Delivery status topic |
| `KAFKA_CONSUMER_GROUP` | layer4-status-updater | Consumer group |
| **PostgreSQL** | | |
| `POSTGRES_HOST` | postgres | Database host |
| `POSTGRES_PORT` | 5432 | Database port |
| `POSTGRES_DB` | django_db | Database name |
| `POSTGRES_USER` | django_user | Database user |
| `POSTGRES_PASSWORD` | django_password | Database password |
| **Django API** (Fallback) | | |
| `DJANGO_URL` | http://django-app:8000 | Django API URL |
| `DJANGO_USERNAME` | test | API username |
| `DJANGO_PASSWORD` | test | API password |
| **Processing** | | |
| `BATCH_SIZE` | 1000 | Updates per batch |
| `BATCH_TIMEOUT_MS` | 100 | Max wait for batch |
| `MAX_DB_CONNECTIONS` | 20 | Connection pool size |
| **Logging** | | |
| `LOG_LEVEL` | INFO | Logging level |
| `LOG_DIR` | /app/logs/layer4 | Log directory |
| `LOG_RETENTION_DAYS` | 7 | Days to keep logs |
| **Server** | | |
| `HTTP_PORT` | 8003 | HTTP port |
| `STANDALONE` | false | Run without web server |

## 🚀 Running

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run with web server (default)
python -m app.main

# Run as standalone consumer (no web server)
python -m app.main --standalone
Docker
bash
# Build image
docker build -t status-updater:latest .

# Run with web server
docker run -p 8003:8003 \
  -e POSTGRES_HOST=host.docker.internal \
  -e POSTGRES_PASSWORD=yourpassword \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  status-updater:latest

# Run as standalone consumer
docker run \
  -e STANDALONE=true \
  -e POSTGRES_HOST=host.docker.internal \
  -e POSTGRES_PASSWORD=yourpassword \
  status-updater:latest
Docker Compose
yaml
version: '3.8'

services:
  status-updater:
    build: .
    ports:
      - "8003:8003"
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_SEND_STATUS_TOPIC=sms-send-status
      - KAFKA_DELIVERY_TOPIC=sms-delivery-status
      - POSTGRES_HOST=postgres
      - POSTGRES_DB=django_db
      - POSTGRES_USER=django_user
      - POSTGRES_PASSWORD=${DB_PASSWORD}
      - DJANGO_URL=http://django-app:8000
      - LOG_LEVEL=INFO
    volumes:
      - ./logs:/app/logs/layer4
    depends_on:
      - kafka
      - postgres
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8003/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
📡 API Endpoints
When running with web server (not standalone mode):

Endpoint	Method	Description
/health	GET	Health check with basic stats
/stats	GET	Detailed statistics
/lag	GET	Kafka lag by partition
/pause	POST	Pause consumption (maintenance)
/resume	POST	Resume consumption
/ready	GET	Kubernetes readiness probe
/	GET	Service information
Example Response: /stats
json
{
  "uptime_seconds": 3600,
  "uptime_human": "1:00:00",
  "database": {
    "updates": 1500000,
    "batches": 1500,
    "errors": 23,
    "update_rate": 416.67
  },
  "kafka": {
    "messages_received": 1500023,
    "send_events": 1000000,
    "delivery_events": 500000,
    "batches_processed": 1500,
    "batch_errors": 23,
    "lag_by_partition": {
      "sms-send-status-0": 0,
      "sms-delivery-status-0": 50
    },
    "total_lag": 50
  },
  "api_client": {
    "token_refreshes": 12,
    "api_calls": 230,
    "api_errors": 23,
    "fallback_updates": 207,
    "fallback_errors": 23
  }
}
🔄 Message Flow Example
Send Status (from Layer 3)
json
{
  "event_type": "SEND_STATUS",
  "message_id": "msg_abc123",
  "campaign_id": 456,
  "status": "SENT",
  "provider_message_id": "prov_xyz789",
  "retry_count": 0,
  "timestamp": "2024-01-01T10:00:00Z"
}
Delivery Status (from Webhook via Layer 3)
json
{
  "event_type": "DELIVERY_STATUS",
  "message_id": "msg_abc123",
  "provider_message_id": "prov_xyz789",
  "status": "DELIVERED",
  "timestamp": "2024-01-01T10:00:05Z"
}
Database Update (after batching)
sql
UPDATE campaigns_messagestatus 
SET status = 'DELIVERED',
    provider_message_id = 'prov_xyz789',
    delivered_at = '2024-01-01T10:00:05Z',
    updated_at = NOW()
WHERE message_id = 'msg_abc123'
📊 Performance Characteristics
Metric	Value	Condition
Max DB Updates/sec	10,000+	Direct DB path
Max API Updates/sec	500	Fallback path
Batch Size	1000	Configurable
Batch Timeout	100ms	Configurable
Kafka Lag Tolerance	Unlimited	Queue in Kafka
Memory per 1000 updates	~2MB	JSON + overhead
🚨 Failure Scenarios
1. Database Unavailable
text
Detection: Connection errors in db_client
Action: 
- Automatically falls back to API client
- Lag may increase temporarily
- Alert if sustained >5 minutes
- Retry DB connection every 30s
2. Kafka Unavailable
text
Detection: Consumer errors in kafka_client
Action:
- Service can't process events
- Health check fails
- Container should restart
- Messages persist in Kafka
3. Django API Unavailable
text
Detection: API client errors
Action:
- Fallback path fails
- Updates are lost (rare)
- Critical alert
- DB must be primary path
4. High Kafka Lag
text
Detection: Lag > 10,000
Action:
- Check DB performance
- Check network connectivity
- Consider increasing workers
- Alert operations team
🔍 Monitoring & Alerting
Key Metrics to Monitor
Kafka Lag - Should stay near zero

DB Update Rate - Should match Layer 3 TPS

Fallback Rate - Should be <1% of total

API Errors - Should be near zero

Token Refreshes - ~12/hour (every 5 min)

Prometheus Queries (if configured)
promql
# Update rate
rate(db_updates_total[1m])

# Error rate
rate(db_errors_total[1m])

# Kafka lag
kafka_consumer_lag{consumer_group="layer4-status-updater"}
🛠️ Development
Adding New Event Types
Add model in models.py

Add handler in kafka_client.py (_handle_new_event)

Add to stats tracking

Update database schema if needed

Testing Locally
bash
# Unit tests
pytest tests/

# Manual test with Kafka
docker-compose up kafka
python -m app.main --standalone

# In another terminal
kafka-console-producer --topic sms-send-status --bootstrap-server localhost:9092
{"event_type": "SEND_STATUS", "message_id": "test123", ...}