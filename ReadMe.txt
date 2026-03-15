SMS Campaign System - README Files
📁 Main Project README
markdown
# SMS Campaign System - High-Throughput Bulk SMS Platform

A distributed, high-performance SMS campaign management system capable of handling millions of messages with precise timing, rate limiting, and reliable delivery through external SMSC providers.

## 🏗 Architecture Overview

The system consists of 3 main layers:
- **Layer 0**: Django Campaign Manager (Core business logic)
- **Layer 1**: Command Ingestion Service (FastAPI)
- **Layer 2**: Campaign Execution Engine (Async Processor)
- **Layer 3**: SMS Sender Cluster (High-throughput sender)
- **Scheduler**: Apache Airflow for campaign timing
- **Message Queue**: Kafka for commands/events
- **Dispatch Queue**: Redis for message queuing

## 📋 Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)
- 8GB+ RAM recommended
- 20GB+ free disk space

## 🚀 Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd batch_process

# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
🔌 Access Points
Service	URL	Credentials
Django Admin	http://localhost:8000/admin	test / test
Django API	http://localhost:8000/api	-
Layer 1 API	http://localhost:8001/docs	-
Layer 3 Webhook	http://localhost:8002/webhook/delivery	-
Layer 3 Health	http://localhost:8002/health	-
Airflow UI	http://localhost:8080	test / test
Kafka UI	http://localhost:8082	-
Redis Commander	http://localhost:8083	-
📊 System Components
Layer 0: Django Campaign Manager
Campaign CRUD operations

Message template management (5 languages)

Audience management

Campaign scheduling

JWT authentication

Layer 1: Command Ingestion (Port 8001)
Receives commands from Airflow

Validates campaign readiness

Publishes to Kafka

Returns 202 Accepted immediately

Layer 2: Execution Engine
Consumes Kafka commands

Fetches campaign metadata

Streams recipients in batches (50K)

Builds messages with language matching

Pushes to Redis queue

Maintains checkpoints

Layer 3: SMS Sender (Port 8002)
Consumes from Redis in batches

Rate limiting (4000 TPS)

Sends to 3rd party API

Retry logic with backoff

Updates status in Django

Publishes delivery events to Kafka

Scheduler (Airflow Port 8080)
Runs every minute

Checks campaign schedules

Triggers Layer 1 API

🛠 Configuration
Environment Variables
Create a .env file:

env
# Django
DJANGO_USERNAME=test
DJANGO_PASSWORD=test

# 3rd Party API (REQUIRED)
API_URL=https://your-sms-provider.com/api/send
API_KEY=your-api-key
📈 Performance Targets
Layer 1: <100ms response time

Layer 2: 10,000+ recipients/second

Layer 3: 4,000 messages/second

End-to-end: Millions of messages/hour

🔍 Monitoring
Kafka UI: http://localhost:8082

Redis Commander: http://localhost:8083

Logs: ./logs/layer1/, ./logs/layer2/, ./logs/layer3/

Health Checks: Available at /health endpoints

🐛 Troubleshooting
Common Issues
Permission denied for logs

bash
sudo chown -R 1000:1000 ./send_sms_manage/logs
Kafka connection refused

bash
docker-compose restart kafka zookeeper
Layer 2 unhealthy

Check Kafka connection: docker-compose logs kafka

Verify campaigns exist in Django

📚 API Documentation
Layer 1 Endpoints
POST /campaign/start - Start campaign

POST /campaign/stop - Stop campaign

POST /campaign/pause - Pause campaign

POST /campaign/resume - Resume campaign

POST /campaign/complete - Complete campaign

Django API Endpoints
POST /api/token/ - Get JWT token

GET /api/campaigns/ - List campaigns

POST /api/campaigns/ - Create campaign

GET /api/campaigns/{id}/ - Get campaign details

🤝 Contributing
Fork the repository

Create a feature branch

Commit changes

Push to the branch

Create a Pull Request

📄 License
[Your License Here]

👥 Authors
[Your Name/Organization]

text

---

## 📁 **send_sms_manage/README.md** (For Layers 1-3)

```markdown
# SMS Sender Manager - Layers 1, 2, & 3

This module contains the core processing layers for the SMS Campaign System:
- **Layer 1**: Command Ingestion Service (FastAPI)
- **Layer 2**: Campaign Execution Engine (Async Processor)
- **Layer 3**: SMS Sender Cluster (High-throughput sender)

## 📁 File Structure
send_sms_manage/
├── Dockerfile # Multi-stage build for all layers
├── requirements.txt # Python dependencies
├── entrypoint.sh # Container entrypoint script
├── layer1_ingestion.py # Layer 1 FastAPI service
├── layer2_queue_builder.py # Layer 2 execution engine
├── layer3_sender.py # Layer 3 SMS sender
├── token_manager.py # Shared JWT token management
└── logs/ # Log directory (created at runtime)
├── layer1/
├── layer2/
└── layer3/

text

## 🚀 Running Locally

### Prerequisites
- Python 3.11+
- Kafka running on localhost:9092
- Redis running on localhost:6379
- Django API running on localhost:8000

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (Windows PowerShell)
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:REDIS_URL="redis://localhost:6379/0"
$env:DJANGO_URL="http://localhost:8000"
Run Layer 1 (Command Ingestion)
bash
python -m uvicorn layer1_ingestion:app --host 0.0.0.0 --port 8001 --reload
Run Layer 2 (Execution Engine)
bash
python layer2_queue_builder.py
Run Layer 3 (SMS Sender)
bash
python layer3_sender.py
🐳 Docker Deployment
Build Image
bash
docker build -t sms-layers:latest .
Run Containers
bash
# Layer 1
docker run -p 8001:8001 sms-layers:latest uvicorn layer1_ingestion:app --host 0.0.0.0 --port 8001

# Layer 2
docker run sms-layers:latest python layer2_queue_builder.py

# Layer 3
docker run -p 8002:8002 sms-layers:latest python layer3_sender.py
🔧 Configuration
Environment Variables
Variable	Default	Description
LOG_LEVEL	INFO	Logging level
LOG_DIR	/app/logs/layer[1-3]	Log directory
KAFKA_BOOTSTRAP_SERVERS	kafka:9092	Kafka brokers
REDIS_URL	redis://redis:6379/0	Redis connection
DJANGO_URL	http://django-app:8000	Django API URL
DJANGO_USERNAME	test	Django username
DJANGO_PASSWORD	test	Django password
API_URL	(required)	3rd party SMS API
API_KEY	(required)	API authentication
MAX_TPS	4000	Rate limit (Layer 3)
BATCH_SIZE	1000	Messages per batch
📊 Logging
Logs are written to /app/logs/layer[1-3]/YYYY-MM-DD.log with daily rotation and 7-day retention.

View logs
bash
# Docker
docker-compose logs -f sms-layer1

# Local
tail -f logs/layer1/$(date +%Y-%m-%d).log
🔍 Health Checks
Layer 1: GET http://localhost:8001/health

Layer 3: GET http://localhost:8002/health

Layer 3 Stats: GET http://localhost:8002/stats

🎯 Layer-Specific Details
Layer 1: Command Ingestion
Receives HTTP commands from Airflow

Validates with Django API

Publishes to Kafka topic sms-commands

Returns 202 Accepted immediately

Layer 2: Execution Engine
Consumes Kafka commands

Fetches campaign metadata from Django

Streams recipients from PostgreSQL (50K batches)

Builds messages with language matching

Pushes to Redis sms:dispatch:queue

Maintains checkpoints for resume capability

Layer 3: SMS Sender
Consumes Redis queue in batches

Rate limiting (configurable TPS)

Sends to 3rd party API

Retry logic with exponential backoff

Updates status in Django

Publishes delivery events to Kafka

🐛 Troubleshooting
Permission Issues
bash
# Fix log directory permissions
sudo chown -R 1000:1000 ./logs
Kafka Connection
bash
# Test Kafka connection
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
Redis Connection
bash
# Test Redis connection
docker exec redis-dispatch redis-cli ping
Django Connection
bash
# Test Django API
curl http://localhost:8000/api/health
text

---

## 📁 **django_project/README.md**

```markdown
# Django Campaign Manager - Core Business Logic

The Django Campaign Manager is the heart of the SMS Campaign System, providing:
- Campaign CRUD operations
- Message template management (5 languages)
- Audience management
- Campaign scheduling
- JWT authentication
- REST API for all operations

## 📁 File Structure
django_project/
├── manage.py
├── requirements.txt
├── myproject/
│ ├── settings.py
│ ├── urls.py
│ └── wsgi.py
└── scheduler_manager/
├── models.py # Database models
├── views.py # API endpoints
├── serializers.py # DRF serializers
├── urls.py # App URLs
└── management/
└── commands/
└── sms_heartbeat.py

text

## 🚀 Local Development

### Prerequisites
- Python 3.11+
- PostgreSQL 14+

### Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Create superuser
python manage.py createsuperuser

# Run development server
python manage.py runserver
🐳 Docker Deployment
bash
# Build image
docker build -t django-campaign-manager .

# Run container
docker run -p 8000:8000 django-campaign-manager
📊 Database Models
Campaign
id: Primary key

name: Campaign name

status: draft/active/paused/completed/archived

created_by: User reference

created_at/updated_at: Timestamps

Schedule
One-to-one with Campaign

start_date/end_date: Campaign duration

frequency: daily/weekly/monthly

run_days: Days of week to run

send_times/end_times: Time windows

MessageContent
One-to-one with Campaign

content: JSON with 5 languages

default_language: Fallback language

Audience
One-to-one with Campaign

recipients: JSON array of recipients

Statistics: total_count, valid_count, invalid_count

CampaignProgress
Tracks campaign execution

total_messages, sent_count, failed_count

progress_percent

status: ACTIVE/COMPLETED/STOPPED/FAILED

Checkpoint
For resume after crash

last_processed_index

MessageStatus
High-volume message tracking

message_id, campaign_id, batch_id

status, attempts, provider_response

🔌 API Endpoints
Authentication
text
POST /api/token/           # Get JWT token
POST /api/token/refresh/   # Refresh token
Campaigns
text
GET    /api/campaigns/           # List campaigns
POST   /api/campaigns/           # Create campaign
GET    /api/campaigns/{id}/      # Get campaign details
PUT    /api/campaigns/{id}/      # Update campaign
DELETE /api/campaigns/{id}/      # Delete campaign

# Nested resources
GET    /api/campaigns/{id}/schedule/        # Get schedule
POST   /api/campaigns/{id}/schedule/        # Create schedule
GET    /api/campaigns/{id}/message-content/ # Get message content
PATCH  /api/campaigns/{id}/message-content/ # Update message content
GET    /api/campaigns/{id}/audience/        # Get audience summary
POST   /api/campaigns/{id}/audience/        # Create audience

# Actions
POST   /api/campaigns/{id}/start/    # Start campaign
POST   /api/campaigns/{id}/stop/     # Stop campaign
POST   /api/campaigns/{id}/pause/    # Pause campaign
POST   /api/campaigns/{id}/resume/   # Resume campaign
POST   /api/campaigns/{id}/complete/ # Complete campaign
🔧 Configuration
Environment Variables
Variable	Default	Description
DEBUG	True	Debug mode
DATABASE_URL	postgresql://...	PostgreSQL connection
DB_HOST	localhost	Database host
DB_PORT	5432	Database port
DB_NAME	campaign_db	Database name
DB_USER	campaign_user	Database user
DB_PASSWORD	campaign_pass	Database password
📝 Notes
Audience recipients are not returned in API responses (for performance)

Use database_info in audience response for direct DB access

JWT tokens expire in 5 minutes (auto-refresh supported)

text

---

## 📁 **airflow-sms-scheduler/README.md**

```markdown
# Airflow SMS Scheduler

Apache Airflow DAGs for scheduling SMS campaigns. Runs every minute to check campaign schedules and trigger Layer 1.

## 📁 File Structure
airflow-sms-scheduler/
├── dags/
│ └── sms_scheduler_dag.py # Main scheduler DAG
├── logs/ # Airflow logs
└── plugins/ # Custom plugins (optional)

text

## 🚀 Deployment

This service runs in Docker as part of the main docker-compose setup.

### Access Airflow UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

## 📊 DAG: `sms_scheduler_dag`

### Schedule
- Runs every minute (`*/1 * * * *`)

### Tasks
1. **check_schedules**: Fetches active campaigns from Django
2. **execute_actions**: Triggers Layer 1 for campaigns that should start/stop

### Flow
Every minute → Check Django API → If time to start → Call Layer 1 START
→ If time to stop → Call Layer 1 STOP

text

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CAMPAIGN_MANAGER_URL` | http://django-app:8000 | Django API URL |
| `LAYER1_URL` | http://sms-layer1:8001 | Layer 1 API URL |

## 📝 DAG Logic

The scheduler checks:
- Is today a scheduled run day?
- Is current time within send windows?
- Is campaign within date range?

If conditions match, it calls the appropriate Layer 1 endpoint.