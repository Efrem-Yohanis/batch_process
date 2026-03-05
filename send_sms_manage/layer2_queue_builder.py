# import os
import asyncio
import logging
import os
from aiokafka import AIOKafkaConsumer
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Kafka-Consumer-Test")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "sms-commands"

async def simple_consumer():
    """Simplest possible consumer to test connectivity"""
    
    logger.info(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Create consumer with minimal config
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="test-group",
        auto_offset_reset='earliest'
    )
    
    try:
        # Start consumer
        await consumer.start()
        logger.info("✅ SUCCESS: Consumer started!")
        
        # Get assigned partitions
        partitions = consumer.assignment()
        logger.info(f"Assigned partitions: {partitions}")
        
        # Wait for messages
        logger.info(f"Waiting for messages on topic '{TOPIC}'...")
        
        # Try to get one message
        try:
            async for msg in consumer:
                logger.info(f"✅ GOT MESSAGE: {msg.value.decode()}")
                logger.info(f"   Topic: {msg.topic}, Partition: {msg.partition}, Offset: {msg.offset}")
                break  # Exit after first message
            
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            
    except Exception as e:
        logger.error(f"❌ FAILED: Could not connect to Kafka - {e}")
        logger.error("Check if:")
        logger.error("1. Kafka is running")
        logger.error(f"2. {KAFKA_BOOTSTRAP_SERVERS} is correct")
        logger.error("3. Topic 'sms-commands' exists")
        
    finally:
        await consumer.stop()
        logger.info("Consumer stopped")

if __name__ == "__main__":
    asyncio.run(simple_consumer())

    
# import asyncio
# import json
# import logging
# import django
# import redis.asyncio as redis
# from aiokafka import AIOKafkaConsumer
# from django.utils import timezone

# # --- Setup Django ---
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
# django.setup()

# from scheduler_manager.models import CampaignProgress, Checkpoint, Audience, MessageContent

# # --- Config ---
# REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("Hydra-Builder")

# # Global dict to track running tasks so we can STOP them
# running_campaigns = {}

# async def run_hydrator(campaign_id, batch_size):
#     """The 'Brain' loop that processes 3 million records"""
#     r = redis.from_url(REDIS_URL)
#     logger.info(f"🚀 Starting Hydrator for Campaign {campaign_id}")
    
#     try:
#         # 1. Load Context (Templates)
#         # Assuming your model relates Audience to Campaign
#         campaign_meta = MessageContent.objects.filter(campaign_id=campaign_id).first()
#         templates = campaign_meta.content  # Expecting JSON like {"en": "...", "am": "..."}
        
#         checkpoint, _ = Checkpoint.objects.get_or_create(campaign_id=campaign_id)
#         offset = checkpoint.last_processed_index

#         while running_campaigns.get(campaign_id):
#             # 2. Backpressure Check
#             q_len = await r.llen("sms:dispatch:queue")
#             if q_len > 500000:
#                 await asyncio.sleep(2)
#                 continue

#             # 3. Fetch Batch using Django ORM
#             # Optimization: Fetching by ID > offset is faster than OFFSET/LIMIT
#             recipients = Audience.objects.filter(
#                 campaign_id=campaign_id, 
#                 id__gt=offset
#             ).order_by('id')[:batch_size]

#             if not recipients:
#                 logger.info(f"✅ Campaign {campaign_id} fully hydrated.")
#                 break

#             # 4. Pipeline Push to Redis
#             async with r.pipeline(transaction=False) as pipe:
#                 for person in recipients:
#                     lang = person.language or "en"
#                     text = templates.get(lang, templates.get("en"))
                    
#                     payload = {
#                         "msisdn": person.phone_number,
#                         "text": text.replace("{{name}}", person.name),
#                         "campaign_id": campaign_id,
#                         "message_id": person.id
#                     }
#                     await pipe.rpush("sms:dispatch:queue", json.dumps(payload))
#                     offset = person.id # Update local offset

#                 await pipe.execute()

#             # 5. Checkpoint
#             checkpoint.last_processed_index = offset
#             await asyncio.to_thread(checkpoint.save)
#             logger.info(f"📥 Queued {batch_size} messages. Current Offset: {offset}")

#     except Exception as e:
#         logger.error(f"❌ Hydrator Error: {e}")
#     finally:
#         running_campaigns[campaign_id] = False

# async def handle_command(msg):
#     payload = json.loads(msg.value.decode())
#     cid = payload["campaign_id"]
#     # Mapping Layer 1's "command_type" to our logic
#     action = payload.get("command_type", payload.get("action", "")).upper()

#     if action == "START":
#         if not running_campaigns.get(cid):
#             running_campaigns[cid] = True
#             # Fire and forget the hydrator loop
#             asyncio.create_task(run_hydrator(cid, payload.get("batch_size", 1000)))
    
#     elif action == "STOP":
#         running_campaigns[cid] = False
#         logger.info(f"🛑 Stopping Campaign {cid}")

# async def consume():
#     consumer = AIOKafkaConsumer(
#         "sms-commands",
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         group_id="layer2-builders"
#     )
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             await handle_command(msg)
#     finally:
#         await consumer.stop()

# if __name__ == "__main__":
#     asyncio.run(consume())

# """
# Layer 2: Queue Builder
# Consumes START commands from Kafka, reads campaign data from PostgreSQL,
# builds individual messages, stores in Redis
# Run with: python layer2_queue_builder.py
# """
# import os
# import json
# import time
# import uuid
# from datetime import datetime

# from kafka import KafkaConsumer, KafkaProducer
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import redis
# import structlog

# # Configuration
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC", "sms-commands")
# BATCH_STARTED_TOPIC = os.getenv("KAFKA_BATCH_STARTED_TOPIC", "sms-batch-started")

# POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
# POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
# POSTGRES_DB = os.getenv("POSTGRES_DB", "campaign_db")
# POSTGRES_USER = os.getenv("POSTGRES_USER", "campaign_user")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "campaign_pass")

# REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# # Setup logging
# logging = structlog.get_logger()

# class QueueBuilder:
#     def __init__(self):
#         # Kafka consumer for commands
#         self.consumer = KafkaConsumer(
#             COMMAND_TOPIC,
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             group_id="queue-builder-group",
#             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#             auto_offset_reset="earliest",
#             enable_auto_commit=False
#         )
        
#         # Kafka producer for batch tracking
#         self.producer = KafkaProducer(
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )
        
#         # PostgreSQL connection
#         self.pg_conn = psycopg2.connect(
#             host=POSTGRES_HOST,
#             port=POSTGRES_PORT,
#             dbname=POSTGRES_DB,
#             user=POSTGRES_USER,
#             password=POSTGRES_PASSWORD,
#             cursor_factory=RealDictCursor
#         )
        
#         # Redis connection
#         self.redis_client = redis.Redis(
#             host=REDIS_HOST,
#             port=REDIS_PORT,
#             db=REDIS_DB,
#             decode_responses=True
#         )
        
#         logging.info("queue_builder.initialized")

#     def get_campaign_data(self, campaign_id):
#         """Fetch campaign data from PostgreSQL"""
#         with self.pg_conn.cursor() as cur:
#             cur.execute("""
#                 SELECT 
#                     c.id, c.name, c.status,
#                     mc.content as message_content,
#                     mc.default_language,
#                     a.recipients
#                 FROM scheduler_manager_campaign c
#                 LEFT JOIN scheduler_manager_messagecontent mc ON c.id = mc.campaign_id
#                 LEFT JOIN scheduler_manager_audience a ON c.id = a.campaign_id
#                 WHERE c.id = %s
#             """, (campaign_id,))
#             return cur.fetchone()

#     def build_messages(self, campaign):
#         """Build individual messages for each recipient"""
#         messages = []
#         for idx, recipient in enumerate(campaign['recipients']):
#             message = {
#                 "message_id": str(uuid.uuid4()),
#                 "campaign_id": campaign['id'],
#                 "campaign_name": campaign['name'],
#                 "recipient": recipient,
#                 "content": campaign['message_content'],
#                 "language": campaign.get('default_language', 'en'),
#                 "sequence": idx,
#                 "created_at": datetime.utcnow().isoformat()
#             }
#             messages.append(message)
#         return messages

#     def store_in_redis(self, campaign_id, messages, batch_num):
#         """Store messages in Redis sorted set"""
#         key = f"campaign:{campaign_id}:queue"
#         pipeline = self.redis_client.pipeline()
        
#         now = time.time()
#         for idx, msg in enumerate(messages):
#             # Score = timestamp + sequence for ordering
#             score = now + (idx * 0.001)
#             pipeline.zadd(key, {json.dumps(msg): score})
        
#         # Set expiry (7 days max)
#         pipeline.expire(key, 604800)
#         pipeline.execute()
        
#         logging.info("redis.stored", campaign=campaign_id, count=len(messages), batch=batch_num)

#     def publish_batch_started(self, campaign_id, batch_num, message_ids):
#         """Publish batch started event to Kafka"""
#         event = {
#             "event_type": "BATCH_STARTED",
#             "campaign_id": campaign_id,
#             "batch_id": f"{campaign_id}_{batch_num}_{uuid.uuid4().hex[:8]}",
#             "message_ids": message_ids,
#             "message_count": len(message_ids),
#             "timestamp": datetime.utcnow().isoformat()
#         }
#         self.producer.send(BATCH_STARTED_TOPIC, event)
#         logging.info("batch.published", campaign=campaign_id, batch=batch_num)

#     def process_start_command(self, command):
#         """Process a START command"""
#         campaign_id = command['campaign_id']
#         logging.info("processing.start", campaign=campaign_id)
        
#         try:
#             # 1. Get campaign data
#             campaign = self.get_campaign_data(campaign_id)
#             if not campaign:
#                 logging.error("campaign.not_found", campaign=campaign_id)
#                 return
            
#             # 2. Build individual messages
#             all_messages = self.build_messages(campaign)
#             logging.info("messages.built", campaign=campaign_id, count=len(all_messages))
            
#             # 3. Store in Redis in batches
#             for i in range(0, len(all_messages), BATCH_SIZE):
#                 batch = all_messages[i:i+BATCH_SIZE]
#                 batch_num = i // BATCH_SIZE
                
#                 self.store_in_redis(campaign_id, batch, batch_num)
                
#                 # Track batch
#                 message_ids = [m['message_id'] for m in batch]
#                 self.publish_batch_started(campaign_id, batch_num, message_ids)
            
#             # 4. Update campaign status in Redis
#             self.redis_client.set(f"campaign:{campaign_id}:total", len(all_messages))
#             self.redis_client.set(f"campaign:{campaign_id}:status", "QUEUED")
            
#             logging.info("campaign.queued", campaign=campaign_id, total=len(all_messages))
            
#         except Exception as e:
#             logging.error("processing.failed", campaign=campaign_id, error=str(e))

#     def run(self):
#         """Main loop - consume commands"""
#         logging.info("queue_builder.starting")
        
#         try:
#             for message in self.consumer:
#                 command = message.value
#                 logging.info("command.received", command_type=command.get('command_type'))
                
#                 if command.get('command_type') == 'START':
#                     self.process_start_command(command)
#                 elif command.get('command_type') == 'STOP':
#                     logging.info("stop.received", campaign=command.get('campaign_id'))
                
#                 # Commit offset
#                 self.consumer.commit()
                
#         except KeyboardInterrupt:
#             logging.info("queue_builder.stopping")
#         finally:
#             self.consumer.close()
#             self.producer.close()
#             self.pg_conn.close()

# if __name__ == "__main__":
#     builder = QueueBuilder()
#     builder.run()



# -----------

# Now we focus clearly on Layer 2 — this is the most important brain of your SMS system.

# You already decided:

# Kafka = control

# Redis = dispatch queue

# DB = source of truth

# So now let’s define exactly what Layer 2 does.

# 🎯 What Layer 2 Actually Is

# Layer 2 = Campaign Execution Engine

# It sits between:

# Layer 1 (Campaign Control via Kafka)

# Redis (Dispatch Queue)

# Database (State + Recipients)

# It does NOT send SMS.
# It PREPARES and FEEDS the sender system.

# 🧠 Responsibilities of Layer 2

# Layer 2 has 6 main responsibilities:

# 1️⃣ Listen for Campaign Commands (from Kafka)

# It subscribes to a topic like:

# sms-campaign-commands

# Example message:

# {
#   "campaign_id": "CMP001",
#   "action": "START",
#   "batch_size": 50000
# }

# It reacts to:

# START

# STOP

# PAUSE

# RESUME

# It updates campaign state in DB.

# 2️⃣ Validate Campaign Before Running

# Before starting, it checks:

# Is campaign already running?

# Is campaign paused?

# Is campaign stopped?

# Does campaign exist?

# Is sender ID configured?

# Is message template valid?

# If invalid → reject and log.

# 3️⃣ Fetch Recipients in Batches

# It does NOT load 3M users at once.

# It reads from DB in chunks:

# Example:

# SELECT msisdn
# FROM campaign_recipients
# WHERE campaign_id = 'CMP001'
# AND status = 'PENDING'
# LIMIT 50000;

# It processes batch by batch.

# This prevents:

# Memory overflow

# Long DB locks

# Slow system

# 4️⃣ Build Final SMS Payload

# For each recipient:

# Apply template variables

# Add sender ID

# Add campaign_id

# Add message_id (UUID)

# Add retry_count = 0

# Build message like:

# {
#   "message_id": "uuid-123",
#   "campaign_id": "CMP001",
#   "msisdn": "2519xxxxxxx",
#   "text": "Hello John, enjoy 20% cashback!",
#   "retry_count": 0
# }
# 5️⃣ Push Messages to Redis Queue

# It pushes built messages into Redis.

# Example Redis key:

# sms:dispatch:queue

# This is what Layer 3 (SMS sender cluster) consumes.

# Important:
# Layer 2 only pushes.
# It does not wait for sending.

# 6️⃣ Maintain Progress & Checkpoints

# After pushing a batch:

# Mark recipients as QUEUED

# Save last_processed_id

# Update campaign_progress table

# So if the system crashes:

# Layer 2 can resume from last checkpoint.
# How Layer 2 Works Internally

# When START is received:

# while campaign is RUNNING:
#     fetch next batch
#     build messages
#     push to Redis
#     update DB

# When PAUSE:

# Stop fetching new batches

# Keep existing Redis queue intact

# When STOP:

# Stop fetching

# Optionally clear Redis queue for that campaign

# 🧱 What Layer 2 Does NOT Do

# It does NOT:

# Send SMS to SMSC

# Handle delivery reports

# Retry failed SMS

# Store DLRs

# Talk to telecom gateway

# That is Layer 3 and Layer 4.

# ⚙ Layer 2 Is Stateful

# Layer 2 must know:

# Campaign status

# Current batch position

# Is paused?

# Is stopped?

# Is completed?

# So it MUST use DB for state.

# 📊 Example Execution Timeline

# START CMP001

# Layer 2:

# Batch 1 → 50k users → push to Redis
# Batch 2 → 50k users → push
# Batch 3 → 50k users → push

# Until no more recipients.

# Then mark campaign COMPLETED.

# 🧠 Very Important Design Rule

# Layer 2 should NEVER flood Redis uncontrollably.

# You must implement:

# Max Redis queue size check

# Backpressure logic

# Example:

# If Redis queue size > 500k
# → wait 2 seconds
# → continue

# This protects Layer 3.