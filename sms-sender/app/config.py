"""Configuration management for Layer 3 SMS Sender."""
import os
from typing import List, Dict, Any

class Config:
    """Centralized configuration for the application."""
    
    DEFAULTS = {
        "REDIS_URL": "redis://redis:6379/0",
        "REDIS_QUEUE_KEY": "sms:dispatch:queue",
        "REDIS_RETRY_KEY": "sms:retry:queue",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_SEND_STATUS_TOPIC": "sms-send-status",
        "KAFKA_DELIVERY_TOPIC": "sms-delivery-status",
        "API_URL": "https://api.your-sms-provider.com/v1/send",
        "API_KEY": "your-actual-api-key-here",
        "API_TIMEOUT": "5",
        "LOG_LEVEL": "INFO",
        "LOG_DIR": "/app/logs/layer3",
        "LOG_RETENTION_DAYS": "7",
        "BATCH_SIZE": "1000",
        "MAX_TPS": "4000",
        "WORKER_COUNT": "4",
        "MAX_RETRIES": "3",
        "RETRY_DELAYS": "[60, 300, 900]",
        "HEALTH_PORT": "8004",
        "HTTP_PORT": "8002"
    }
    
    def __init__(self):
        # Apply defaults for missing environment variables
        for key, value in self.DEFAULTS.items():
            if key not in os.environ:
                os.environ[key] = value
        
        # Redis settings
        self.REDIS_URL = os.getenv("REDIS_URL")
        self.REDIS_QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY")
        self.REDIS_RETRY_KEY = os.getenv("REDIS_RETRY_KEY")
        
        # Kafka settings
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.KAFKA_SEND_STATUS_TOPIC = os.getenv("KAFKA_SEND_STATUS_TOPIC")
        self.KAFKA_DELIVERY_TOPIC = os.getenv("KAFKA_DELIVERY_TOPIC")
        
        # 3rd Party API settings
        self.API_URL = os.getenv("API_URL")
        self.API_KEY = os.getenv("API_KEY")
        self.API_TIMEOUT = int(os.getenv("API_TIMEOUT"))
        
        # Processing settings
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
        self.MAX_TPS = int(os.getenv("MAX_TPS"))
        self.WORKER_COUNT = int(os.getenv("WORKER_COUNT"))
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES"))
        
        # Parse retry delays from JSON string
        import json
        self.RETRY_DELAYS = json.loads(os.getenv("RETRY_DELAYS"))
        
        # Logging settings
        self.LOG_LEVEL = os.getenv("LOG_LEVEL")
        self.LOG_DIR = os.getenv("LOG_DIR")
        self.LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS"))
        
        # Server settings
        self.HEALTH_PORT = int(os.getenv("HEALTH_PORT"))
        self.HTTP_PORT = int(os.getenv("HTTP_PORT"))
        
        # Create log directory
        os.makedirs(self.LOG_DIR, exist_ok=True)
    
    def dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary for logging."""
        return {
            "redis_queue": self.REDIS_QUEUE_KEY,
            "kafka_bootstrap": self.KAFKA_BOOTSTRAP_SERVERS,
            "send_topic": self.KAFKA_SEND_STATUS_TOPIC,
            "delivery_topic": self.KAFKA_DELIVERY_TOPIC,
            "api_url": self.API_URL,
            "batch_size": self.BATCH_SIZE,
            "max_tps": self.MAX_TPS,
            "workers": self.WORKER_COUNT,
            "max_retries": self.MAX_RETRIES,
            "log_level": self.LOG_LEVEL,
            "log_dir": self.LOG_DIR,
            "http_port": self.HTTP_PORT,
            "health_port": self.HEALTH_PORT
        }

config = Config()