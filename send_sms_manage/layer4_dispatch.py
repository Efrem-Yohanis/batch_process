"""
Layer 4: Dispatch Service
Consumes batches from Kafka, sends HTTP requests to SMSC provider,
publishes results back to Kafka
Run with: python layer4_dispatch.py
"""
import os
import json
import time
import requests
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka import KafkaConsumer, KafkaProducer
import structlog

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BATCH_READY_TOPIC = os.getenv("KAFKA_BATCH_READY_TOPIC", "sms-batch-ready")
DELIVERY_RESULTS_TOPIC = os.getenv("KAFKA_DELIVERY_RESULTS_TOPIC", "sms-delivery-results")
RETRY_QUEUE_TOPIC = os.getenv("KAFKA_RETRY_QUEUE_TOPIC", "sms-retry-queue")
DEAD_LETTER_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "sms-dlq")

SMSC_API_URL = os.getenv("SMSC_API_URL", "https://api.twilio.com/2010-04-01/Accounts/ACXXXXXXXXXXXXXXXXXX/Messages.json")
SMSC_API_KEY = os.getenv("SMSC_API_KEY", "")
SMSC_API_SECRET = os.getenv("SMSC_API_SECRET", "")
SMSC_SENDER = os.getenv("SMSC_SENDER", "+1234567890")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF = [int(x) for x in os.getenv("RETRY_BACKOFF", "1,5,30").split(',')]
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "10"))

# Setup logging
logging = structlog.get_logger()

class DispatchService:
    def __init__(self):
        self.consumer = KafkaConsumer(
            BATCH_READY_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="dispatch-group",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset="earliest",
            max_poll_records=5,
            enable_auto_commit=False
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.executor = ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS)
        self.session = requests.Session()
        
        # Configure retry adapter
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=100,
            max_retries=3
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logging.info("dispatch.initialized", workers=CONCURRENT_WORKERS)

    def send_to_smsc(self, message):
        """Send single message to SMSC provider"""
        start_time = time.time()
        
        try:
            # Prepare request (Twilio example)
            response = self.session.post(
                SMSC_API_URL,
                auth=(SMSC_API_KEY, SMSC_API_SECRET),
                data={
                    'To': message['recipient'],
                    'From': SMSC_SENDER,
                    'Body': message['content'].get('text', '') 
                             if isinstance(message['content'], dict) 
                             else str(message['content'])
                },
                timeout=10
            )
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            if response.status_code in [200, 201, 202]:
                result = {
                    'success': True,
                    'status_code': response.status_code,
                    'provider_message_id': response.json().get('sid', ''),
                    'latency_ms': latency_ms
                }
            else:
                result = {
                    'success': False,
                    'status_code': response.status_code,
                    'error': response.text[:200],
                    'latency_ms': latency_ms
                }
                
        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            result = {
                'success': False,
                'error': str(e),
                'latency_ms': latency_ms
            }
        
        return result

    def create_delivery_event(self, message, result, attempt):
        """Create delivery event for Kafka"""
        return {
            "event_id": str(uuid.uuid4()),
            "message_id": message['message_id'],
            "campaign_id": message['campaign_id'],
            "recipient": message['recipient'],
            "status": "SUCCESS" if result['success'] else "FAILED",
            "timestamp": datetime.utcnow().isoformat(),
            "attempt": attempt,
            "latency_ms": result.get('latency_ms', 0),
            "error": result.get('error', ''),
            "status_code": result.get('status_code', 0)
        }

    def process_message(self, message, attempt=1):
        """Process a single message"""
        result = self.send_to_smsc(message)
        event = self.create_delivery_event(message, result, attempt)
        
        if result['success']:
            # Success - publish to results topic
            self.producer.send(DELIVERY_RESULTS_TOPIC, event)
            logging.debug("message.sent", message_id=message['message_id'])
        else:
            if attempt < MAX_RETRIES:
                # Schedule retry
                event['next_retry_in'] = RETRY_BACKOFF[attempt - 1]
                event['retry_count'] = attempt
                self.producer.send(RETRY_QUEUE_TOPIC, event)
                logging.warning("message.retry", 
                              message_id=message['message_id'],
                              attempt=attempt)
            else:
                # Max retries exceeded
                self.producer.send(DEAD_LETTER_TOPIC, event)
                logging.error("message.failed", 
                            message_id=message['message_id'],
                            error=result.get('error'))
        
        return event

    def process_batch(self, batch):
        """Process a batch of messages in parallel"""
        batch_id = batch['batch_id']
        campaign_id = batch['campaign_id']
        messages = batch['messages']
        
        logging.info("processing.batch", 
                    batch_id=batch_id,
                    campaign=campaign_id,
                    count=len(messages))
        
        # Submit all messages to thread pool
        futures = []
        for message in messages:
            future = self.executor.submit(self.process_message, message, 1)
            futures.append(future)
        
        # Wait for all to complete
        success_count = 0
        for future in as_completed(futures):
            event = future.result()
            if event['status'] == 'SUCCESS':
                success_count += 1
        
        logging.info("batch.completed", 
                    batch_id=batch_id,
                    success=success_count,
                    failed=len(messages)-success_count)
        
        # Publish batch completion event
        self.producer.send('sms-batch-completed', {
            "batch_id": batch_id,
            "campaign_id": campaign_id,
            "success_count": success_count,
            "failed_count": len(messages) - success_count,
            "timestamp": datetime.utcnow().isoformat()
        })

    def run(self):
        """Main loop - consume batches from Kafka"""
        logging.info("dispatch.starting")
        
        try:
            for message in self.consumer:
                batch = message.value
                self.process_batch(batch)
                self.consumer.commit()
                
        except KeyboardInterrupt:
            logging.info("dispatch.stopping")
        finally:
            self.executor.shutdown(wait=True)
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    dispatch = DispatchService()
    dispatch.run()