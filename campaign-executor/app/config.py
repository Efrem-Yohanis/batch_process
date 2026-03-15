"""Configuration management for Layer 2."""
import os
from typing import Dict, Any

class Config:
    """Centralized configuration for the application."""
    
    DEFAULTS = {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_COMMAND_TOPIC": "sms-commands",
        "KAFKA_PROGRESS_TOPIC": "sms-batch-started",
        "KAFKA_CONSUMER_GROUP": "layer2-engine",
        "CAMPAIGN_MANAGER_URL": "http://django-app:8000",
        "DJANGO_USERNAME": "test",
        "DJANGO_PASSWORD": "test",
        "POSTGRES_DSN": "postgresql://campaign_user:campaign_pass@campaign-db:5432/campaign_db",
        "REDIS_URL": "redis://redis:6379/0",
        "REDIS_QUEUE_KEY": "sms:dispatch:queue",
        "LOG_LEVEL": "INFO",
        "LOG_DIR": "/app/logs/layer2",
        "LOG_RETENTION_DAYS": "7",
        "LOG_WHEN": "midnight",
        "LOG_MAX_BYTES": str(100 * 1024 * 1024),
        "BATCH_SIZE": "50000",
        "MAX_QUEUE_SIZE": "500000",
        "HEALTH_PORT": "8003",
        "DB_POOL_MIN_SIZE": "2",
        "DB_POOL_MAX_SIZE": "10",
        "TOKEN_EXPIRY_BUFFER": "60"
    }
    
    def __init__(self):
        # Apply defaults for missing environment variables
        for key, value in self.DEFAULTS.items():
            if key not in os.environ:
                os.environ[key] = value
        
        # Kafka settings
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.KAFKA_COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC")
        self.KAFKA_PROGRESS_TOPIC = os.getenv("KAFKA_PROGRESS_TOPIC")
        self.KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")
        
        # Django API settings
        self.CAMPAIGN_MANAGER_URL = os.getenv("CAMPAIGN_MANAGER_URL")
        self.DJANGO_USERNAME = os.getenv("DJANGO_USERNAME")
        self.DJANGO_PASSWORD = os.getenv("DJANGO_PASSWORD")
        
        # Database settings
        self.POSTGRES_DSN = os.getenv("POSTGRES_DSN")
        self.DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE"))
        self.DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE"))
        
        # Redis settings
        self.REDIS_URL = os.getenv("REDIS_URL")
        self.REDIS_QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY")
        
        # Processing settings
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
        self.MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE"))
        
        # Logging settings
        self.LOG_LEVEL = os.getenv("LOG_LEVEL")
        self.LOG_DIR = os.getenv("LOG_DIR")
        self.LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS"))
        self.LOG_WHEN = os.getenv("LOG_WHEN")
        self.LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES"))
        
        # Server settings
        self.HEALTH_PORT = int(os.getenv("HEALTH_PORT"))
        
        # Token settings
        self.TOKEN_EXPIRY_BUFFER = int(os.getenv("TOKEN_EXPIRY_BUFFER"))
        
        # Create log directory
        os.makedirs(self.LOG_DIR, exist_ok=True)
    
    def dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary for logging."""
        return {
            "kafka_bootstrap": self.KAFKA_BOOTSTRAP_SERVERS,
            "command_topic": self.KAFKA_COMMAND_TOPIC,
            "progress_topic": self.KAFKA_PROGRESS_TOPIC,
            "consumer_group": self.KAFKA_CONSUMER_GROUP,
            "campaign_manager": self.CAMPAIGN_MANAGER_URL,
            "redis_queue": self.REDIS_QUEUE_KEY,
            "batch_size": self.BATCH_SIZE,
            "max_queue_size": self.MAX_QUEUE_SIZE,
            "log_level": self.LOG_LEVEL,
            "log_dir": self.LOG_DIR,
            "health_port": self.HEALTH_PORT
        }

config = Config()