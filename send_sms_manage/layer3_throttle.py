"""
Layer 3: Throttle Service
Pulls messages from Redis every 100ms, enforces 1000 TPS limit,
creates batches for dispatch
Run with: python layer3_throttle.py
"""
import os
import time
import json
import redis
import structlog
from datetime import datetime
from collections import defaultdict
from kafka import KafkaProducer

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BATCH_READY_TOPIC = os.getenv("KAFKA_BATCH_READY_TOPIC", "sms-batch-ready")

GLOBAL_TPS_LIMIT = int(os.getenv("GLOBAL_TPS_LIMIT", "1000"))
PULL_INTERVAL_MS = int(os.getenv("PULL_INTERVAL_MS", "100"))  # 100ms
BATCH_SIZE = int(os.getenv("THROTTLE_BATCH_SIZE", "100"))

# Setup logging
logging = structlog.get_logger()

class ThrottleService:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Rate limiting state
        self.tokens_remaining = GLOBAL_TPS_LIMIT
        self.last_refill = time.time()
        
        # Stats
        self.stats = defaultdict(int)
        
        logging.info("throttle.initialized", tps_limit=GLOBAL_TPS_LIMIT)

    def find_active_campaigns(self):
        """Find campaigns with messages in Redis"""
        campaigns = []
        cursor = 0
        
        while True:
            cursor, keys = self.redis_client.scan(cursor, match="campaign:*:queue")
            for key in keys:
                campaign_id = key.split(':')[1]
                size = self.redis_client.zcard(key)
                if size > 0:
                    status = self.redis_client.get(f"campaign:{campaign_id}:status")
                    if status not in ['STOPPED', 'COMPLETED']:
                        campaigns.append({
                            'campaign_id': campaign_id,
                            'key': key,
                            'size': size
                        })
            if cursor == 0:
                break
        
        return campaigns

    def refill_tokens(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Add tokens based on elapsed time (max GLOBAL_TPS_LIMIT)
        new_tokens = elapsed * GLOBAL_TPS_LIMIT
        self.tokens_remaining = min(GLOBAL_TPS_LIMIT, self.tokens_remaining + new_tokens)
        self.last_refill = now
        
        return self.tokens_remaining

    def allocate_tokens(self, campaigns):
        """Allocate available tokens across campaigns"""
        if not campaigns or self.tokens_remaining <= 0:
            return []
        
        total_size = sum(c['size'] for c in campaigns)
        allocations = []
        available = self.tokens_remaining
        
        for campaign in campaigns:
            # Proportional allocation based on queue size
            weight = campaign['size'] / total_size if total_size > 0 else 0
            allocated = int(available * weight)
            
            # At least 1 token per campaign if possible
            if allocated < 1 and available >= len(campaigns):
                allocated = 1
            
            # Don't allocate more than queue size
            allocated = min(allocated, campaign['size'])
            
            if allocated > 0:
                allocations.append({
                    'campaign_id': campaign['campaign_id'],
                    'key': campaign['key'],
                    'tokens': allocated
                })
                self.tokens_remaining -= allocated
        
        return allocations

    def pull_messages(self, campaign_key, count):
        """Pull messages from Redis"""
        # Get oldest messages first
        messages_data = self.redis_client.zrange(campaign_key, 0, count - 1)
        
        if not messages_data:
            return []
        
        # Remove from Redis
        self.redis_client.zrem(campaign_key, *messages_data)
        
        # Parse JSON
        messages = [json.loads(m) for m in messages_data]
        return messages

    def create_batch(self, messages, campaign_id):
        """Create a batch for dispatch"""
        return {
            "batch_id": f"BATCH_{campaign_id}_{int(time.time())}_{len(messages)}",
            "campaign_id": campaign_id,
            "messages": messages,
            "message_count": len(messages),
            "created_at": datetime.utcnow().isoformat()
        }

    def process_cycle(self):
        """One processing cycle (runs every 100ms)"""
        # 1. Refill tokens
        self.refill_tokens()
        
        # 2. Find active campaigns
        campaigns = self.find_active_campaigns()
        
        if campaigns:
            # 3. Allocate tokens
            allocations = self.allocate_tokens(campaigns)
            
            # 4. Pull and send messages
            for alloc in allocations:
                messages = self.pull_messages(alloc['key'], alloc['tokens'])
                
                if messages:
                    batch = self.create_batch(messages, alloc['campaign_id'])
                    
                    # Send to Kafka for Layer 4
                    self.producer.send(BATCH_READY_TOPIC, batch)
                    
                    self.stats['messages_pulled'] += len(messages)
                    
                    logging.debug("batch.sent", 
                                 campaign=alloc['campaign_id'],
                                 count=len(messages),
                                 tokens_remaining=self.tokens_remaining)
        
        # Update stats
        self.stats['cycles'] += 1
        if self.stats['cycles'] % 600 == 0:  # ~60 seconds
            logging.info("throttle.stats", 
                        messages_per_sec=self.stats['messages_pulled']/60,
                        campaigns=len(campaigns),
                        tokens_remaining=self.tokens_remaining)
            self.stats['messages_pulled'] = 0

    def run(self):
        """Main loop - runs every 100ms"""
        logging.info("throttle.starting", interval_ms=PULL_INTERVAL_MS)
        
        try:
            while True:
                cycle_start = time.time()
                self.process_cycle()
                
                # Maintain interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, (PULL_INTERVAL_MS / 1000) - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            logging.info("throttle.stopping")
        finally:
            self.producer.close()

if __name__ == "__main__":
    throttle = ThrottleService()
    throttle.run()