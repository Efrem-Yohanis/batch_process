# Layer 2: Campaign Execution Engine

Consumes campaign commands from Kafka, streams recipients directly from PostgreSQL,
builds personalized messages, and pushes them to Redis for dispatch.

## Architecture
Kafka (commands) → Campaign Executor → PostgreSQL (recipients) → Redis (dispatch queue)
↓
Kafka (progress)
↓
Django API (metadata + completion)

text

## What It Does

1. **Listens for Commands** - Consumes START/STOP/PAUSE/RESUME from Kafka
2. **Fetches Metadata** - Gets campaign templates and config from Django API
3. **Streams Recipients** - Direct PostgreSQL access with keyset pagination
4. **Builds Messages** - Language matching + placeholder replacement
5. **Queues for Dispatch** - Pushes batches to Redis with backpressure handling
6. **Tracks Progress** - Updates checkpoints and publishes batch completion

## Components

| File | Purpose |
|------|---------|
| `api_client.py` | Django API communication with auto token refresh |
| `database.py` | PostgreSQL connection pool for recipient streaming |
| `redis_client.py` | Redis queue management with backpressure |
| `kafka_client.py` | Kafka consumer (commands) and producer (progress) |
| `message_builder.py` | Template processing and message construction |
| `campaign_engine.py` | Core processing logic with checkpointing |
| `command_handler.py` | Command routing and campaign status tracking |
| `main.py` | App initialization and health server |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Kafka broker |
| `KAFKA_COMMAND_TOPIC` | sms-commands | Command topic |
| `KAFKA_PROGRESS_TOPIC` | sms-batch-started | Progress topic |
| `CAMPAIGN_MANAGER_URL` | http://django-app:8000 | Django API URL |
| `POSTGRES_DSN` | postgresql://... | Database connection |
| `REDIS_URL` | redis://redis:6379/0 | Redis connection |
| `BATCH_SIZE` | 50000 | Recipients per batch |
| `MAX_QUEUE_SIZE` | 500000 | Backpressure threshold |
| `HEALTH_PORT` | 8003 | Health check port |

## Running

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run
python -m app.main
Docker
bash
# Build
docker build -t campaign-executor .

# Run
docker run -p 8002:8002 -p 8003:8003 \
  -e POSTGRES_DSN=postgresql://user:pass@host/db \
  -e CAMPAIGN_MANAGER_URL=http://django-app:8000 \
  campaign-executor
Docker Compose
yaml
version: '3.8'
services:
  campaign-executor:
    build: .
    ports:
      - "8002:8002"
      - "8003:8003"
    environment:
      - POSTGRES_DSN=postgresql://user:pass@postgres:5432/campaign_db
      - REDIS_URL=redis://redis:6379/0
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    volumes:
      - ./logs:/app/logs/layer2
Health Checks
:8003/health - Overall health status

:8003/ready - Readiness probe for K8s

Monitoring
:8002/stats - Active campaigns and statuses

Logs: /app/logs/layer2/YYYY-MM-DD.log

Flow Example
START command received for campaign 123

Fetch metadata from Django API

Stream recipients from PostgreSQL in batches of 50,000

For each recipient:

Match language to template

Replace {{name}} with actual value

Generate unique message ID

Push batch to Redis queue

Update checkpoint (last_id = 50000)

Publish batch progress to Kafka

Repeat until all recipients processed

Mark campaign complete via Django API

text

This complete implementation follows the exact structure you requested with clean separation of concerns, proper error handling, and comprehensive logging throughout.