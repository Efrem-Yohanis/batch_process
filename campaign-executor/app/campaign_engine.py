"""Core campaign processing engine."""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from .config import config
from .models import CampaignStatus, BatchProgress
from .api_client import CampaignAPIClient
from .database import DatabaseClient
from .redis_client import RedisQueueClient
from .message_builder import MessageBuilder
from .kafka_client import KafkaProducerClient

logger = logging.getLogger("campaign-executor")

class CampaignEngine:
    """Main campaign processing engine."""
    
    def __init__(self, api_client: CampaignAPIClient, db_client: DatabaseClient,
                 redis_client: RedisQueueClient, kafka_producer: KafkaProducerClient,
                 command_handler=None):
        self.api_client = api_client
        self.db = db_client
        self.redis = redis_client
        self.kafka_producer = kafka_producer
        self.command_handler = command_handler
        self.message_builder = MessageBuilder()
    
    async def process_campaign(self, campaign_id: int, correlation_id: str, 
                               batch_size: Optional[int] = None):
        """
        Main campaign processing function.
        
        Flow:
        1. Fetch campaign metadata from Django API
        2. Get starting checkpoint
        3. Loop through recipients in batches
        4. Build messages
        5. Push to Redis
        6. Update checkpoint
        7. Publish progress
        """
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        batch_size = batch_size or config.BATCH_SIZE
        
        log.info(f"Starting campaign {campaign_id} processing")
        
        try:
            # STEP 1: Get campaign metadata using API client
            metadata, error = await self.api_client.get_campaign_metadata(campaign_id, correlation_id)
            if error or not metadata:
                log.error(f"Failed to fetch campaign metadata: {error}")
                return
            
            log.info(f"Campaign: {metadata.name}")
            log.info(f"Total recipients: {metadata.total_recipients}")
            log.info(f"Templates available: {list(metadata.templates.keys())}")
            
            # STEP 2: Initialize processing
            last_id = metadata.last_processed
            batch_num = 0
            total_processed = 0
            start_time = datetime.now()
            
            if metadata.has_checkpoint:
                log.info(f"Resuming from checkpoint ID: {last_id}")
            
            # STEP 3: Main processing loop
            while True:
                # Check campaign status
                if self.command_handler:
                    status = self.command_handler.get_status(campaign_id)
                    if status != CampaignStatus.RUNNING:
                        log.info(f"Campaign status changed to {status}")
                        break
                
                # Check backpressure
                if await self.redis.check_backpressure():
                    await asyncio.sleep(2)
                    continue
                
                # Fetch next batch
                batch = await self.db.fetch_recipients(campaign_id, last_id, batch_size)
                if not batch:
                    log.info("No more recipients to process")
                    break
                
                log.info(f"Batch {batch_num + 1}: fetched {len(batch)} recipients")
                
                # Build messages
                messages = self.message_builder.build_messages(batch, metadata, campaign_id)
                
                if not messages:
                    log.warning(f"No messages built for batch {batch_num + 1}")
                    last_id = batch[-1]['id']
                    continue
                
                # Push to Redis
                await self.redis.push_messages([m.dict() for m in messages])
                
                # Update checkpoint
                last_id = batch[-1]['id']
                await self.db.update_checkpoint(campaign_id, last_id, len(messages))
                
                # Publish progress
                await self._publish_progress(campaign_id, batch_num, messages, correlation_id, log)
                
                # Update stats
                batch_num += 1
                total_processed += len(messages)
                
                # Log progress
                await self._log_progress(total_processed, metadata.total_recipients, 
                                        start_time, log)
            
            # STEP 4: Campaign complete - mark via API client
            if total_processed > 0:
                success, error = await self.api_client.mark_campaign_complete(
                    campaign_id, correlation_id
                )
                if success:
                    log.info(f"Campaign {campaign_id} marked as complete")
                else:
                    log.error(f"Failed to mark campaign complete: {error}")
            
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = total_processed / elapsed if elapsed > 0 else 0
            log.info(f"Campaign {campaign_id} completed! {total_processed} messages in {elapsed:.0f}s ({rate:.0f} msgs/sec)")
            
        except Exception as e:
            log.error(f"Error processing campaign {campaign_id}: {e}", exc_info=True)
        finally:
            if self.command_handler:
                await self.command_handler.cleanup_campaign(campaign_id)
    
    async def _publish_progress(self, campaign_id: int, batch_num: int,
                                messages: list, correlation_id: str, log):
        """Publish batch progress to Kafka."""
        try:
            if not messages:
                return
                
            progress = BatchProgress(
                campaign_id=campaign_id,
                batch_number=batch_num,
                message_count=len(messages),
                first_message_id=messages[0].message_id,
                last_message_id=messages[-1].message_id,
                first_recipient_id=messages[0].recipient_id,
                last_recipient_id=messages[-1].recipient_id,
                timestamp=datetime.utcnow().isoformat(),
                correlation_id=correlation_id
            )
            
            await self.kafka_producer.publish_progress(progress)
            log.debug(f"Published batch {batch_num} progress")
            
        except Exception as e:
            log.error(f"Error publishing progress: {e}")
    
    async def _log_progress(self, processed: int, total: int, 
                           start_time: datetime, log):
        """Log processing progress and rate."""
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = processed / elapsed if elapsed > 0 else 0
        percentage = (processed / total * 100) if total > 0 else 0
        
        log.info(f"Progress: {processed}/{total} ({percentage:.1f}%) - {rate:.0f} msgs/sec")