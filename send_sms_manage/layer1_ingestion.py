import os
import json
import uuid
import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sms-layer1")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC", "sms-commands")

producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("🚀 Starting SMS Layer 1 Ingestion Service...")
    
    connected = False
    for attempt in range(1, 11):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                acks="all" 
            )
            await producer.start()
            logger.info(f"✅ Connected to Kafka on attempt {attempt}")
            connected = True
            break
        except Exception as e:
            logger.warning(f"⚠️ Kafka not ready (Attempt {attempt}/10). Retrying in 5s...")
            await asyncio.sleep(5)
    
    if not connected:
        logger.error("❌ Failed to connect to Kafka after 10 attempts.")

    yield
    
    if producer:
        await producer.stop()
    logger.info("👋 SMS Layer 1 stopped")

app = FastAPI(title="SMS Layer 1 Ingestion", lifespan=lifespan)

class CommandRequest(BaseModel):
    campaign_id: int = Field(..., gt=0)

async def publish_to_kafka(cmd_type: str, campaign_id: int):
    """Internal helper to publish commands to Kafka"""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka not connected")
    
    message = {
        "command_id": f"cmd_{uuid.uuid4().hex[:8]}",
        "command_type": cmd_type,
        "campaign_id": campaign_id,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        await producer.send_and_wait(
            KAFKA_COMMAND_TOPIC,
            key=str(campaign_id).encode(),
            value=json.dumps(message).encode()
        )
        logger.info(f"✅ Kafka ACK: {cmd_type} for Campaign {campaign_id}")
        return {"status": "success", "command": message}
    except Exception as e:
        logger.error(f"❌ Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Broker communication failed")

@app.post("/campaign/start")
async def start_campaign(command: CommandRequest):
    return await publish_to_kafka("START", command.campaign_id)

@app.post("/campaign/stop")
async def stop_campaign(command: CommandRequest):
    return await publish_to_kafka("STOP", command.campaign_id)

@app.get("/health")
async def health():
    return {"status": "up" if producer else "down"}