"""Message building and template processing."""
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any

from .models import CampaignMetadata, Recipient, SMSMessage

logger = logging.getLogger("campaign-executor")

class MessageBuilder:
    """Builds SMS messages from templates and recipient data."""
    
    def build_messages(self, recipients: List[Dict[str, Any]], 
                       metadata: CampaignMetadata,
                       campaign_id: int) -> List[SMSMessage]:
        """
        Build SMS messages for a batch of recipients.
        
        Steps:
        1. Determine language for each recipient
        2. Get appropriate template
        3. Replace placeholders with variables
        4. Create message objects
        """
        messages = []
        stats = {"matched": 0, "default": 0, "missing": 0}
        
        for recipient_data in recipients:
            recipient = Recipient(**recipient_data)
            
            # Get correct language template
            lang = recipient.language or metadata.default_language
            template = metadata.templates.get(lang)
            
            if not template:
                # Try default language
                template = metadata.templates.get(metadata.default_language, "")
                if template:
                    stats["default"] += 1
                    lang = metadata.default_language
                else:
                    stats["missing"] += 1
                    logger.warning(f"No template found for language {lang} or default")
                    continue
            else:
                stats["matched"] += 1
            
            # Replace placeholders
            text = self._replace_placeholders(template, recipient.variables or {})
            
            # Create message
            message = SMSMessage(
                message_id=str(uuid.uuid4()),
                campaign_id=campaign_id,
                msisdn=recipient.msisdn,
                text=text,
                language=lang,
                retry_count=0,
                timestamp=datetime.utcnow().isoformat(),
                recipient_id=recipient.id
            )
            
            messages.append(message)
        
        # Log statistics
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Message building stats: {stats}")
        
        return messages
    
    def _replace_placeholders(self, template: str, variables: Dict[str, Any]) -> str:
        """
        Replace placeholders in template with actual values.
        
        Placeholder format: {{variable_name}}
        """
        text = template
        for key, value in variables.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in text:
                text = text.replace(placeholder, str(value))
                logger.debug(f"Replaced {placeholder} with {value}")
        return text