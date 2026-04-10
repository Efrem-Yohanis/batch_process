markdown
# SMS Campaign Layer 1 - Command Ingestion Service

Receives campaign commands from Airflow, validates with Django Campaign Manager, and publishes to Kafka.

## Architecture

This service is the entry point for all campaign commands (START, STOP, PAUSE, RESUME, COMPLETE). It:
1. Receives commands via REST API
2. Validates campaign state with Django Campaign Manager
3. Publishes validated commands to Kafka for downstream processing

## Directory Structure
my_campaign_app/
├── app/
│ ├── init.py # Package initialization
│ ├── main.py # FastAPI app entry point
│ ├── config.py # Configuration management
│ ├── logger.py # Logging setup
│ ├── models.py # Pydantic models
│ ├── urls.py # API route definitions
│ ├── services.py # Business logic
│ ├── kafka_client.py # Kafka producer
│ └── api_client.py # External API client with token refresh
├── requirements.txt # Python dependencies
├── Dockerfile # Containerization
└── README.md # This file

text

## Configuration

Environment variables (with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Kafka broker address |
| KAFKA_COMMAND_TOPIC | sms-commands | Kafka topic for commands |
| CAMPAIGN_MANAGER_URL | http://django-app:8000 | Django API URL |
| DJANGO_USERNAME | test | Django username |
| DJANGO_PASSWORD | test | Django password |
| LOG_LEVEL | INFO | Logging level |
| LOG_DIR | /app/logs/layer1 | Log directory |
| LOG_RETENTION_DAYS | 7 | Days to keep logs |
| HOST | 0.0.0.0 | Bind address |
| PORT | 8001 | HTTP port |

## API Endpoints

### Command Endpoints (all POST)

- `/campaign/start` - Start a campaign
- `/campaign/stop` - Stop a campaign
- `/campaign/pause` - Pause a campaign
- `/campaign/resume` - Resume a campaign
- `/campaign/complete` - Complete a campaign

### Utility Endpoints

- `/health` - Health check
- `/info` - Service information
- `/docs` - OpenAPI documentation

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run with uvicorn
uvicorn app.main:app --reload --port 8001
Running with Docker
bash
# Build image
docker build -t sms-layer1 .

# Run container
docker run -p 8001:8001 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e CAMPAIGN_MANAGER_URL=http://django-app:8000 \
  sms-layer1
Logging
Logs are written to /app/logs/layer1/YYYY-MM-DD.log with daily rotation.
Each log entry includes a correlation ID for request tracing.

Testing
bash
# Test start command
curl -X POST http://localhost:8001/campaign/start \
  -H "Content-Type: application/json" \
  -d '{"campaign_id": 123, "reason": "Test"}'

# Check health
curl http://localhost:8001/health