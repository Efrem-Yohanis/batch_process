"""Configuration management for Layer 4 Status Updater."""
import os
from typing import Dict, Any, List
from dataclasses import dataclass, field

@dataclass
class Config:
    """Centralized configuration for the application."""
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_SEND_STATUS_TOPIC: str = os.getenv("KAFKA_SEND_STATUS_TOPIC", "sms-send-status")
    KAFKA_DELIVERY_TOPIC: str = os.getenv("KAFKA_DELIVERY_TOPIC", "sms-delivery-status")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "layer4-status-updater")
    
    # PostgreSQL (Django DB)
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "django_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "django_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "django_password")
    
    # Django API (fallback for complex updates)
    DJANGO_URL: str = os.getenv("DJANGO_URL", "http://django-app:8000")
    DJANGO_USERNAME: str = os.getenv("DJANGO_USERNAME", "test")
    DJANGO_PASSWORD: str = os.getenv("DJANGO_PASSWORD", "test")
    
    # Processing
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    BATCH_TIMEOUT_MS: int = int(os.getenv("BATCH_TIMEOUT_MS", "100"))
    WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", "4"))
    MAX_DB_CONNECTIONS: int = int(os.getenv("MAX_DB_CONNECTIONS", "20"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", "/app/logs/layer4")
    LOG_RETENTION_DAYS: int = int(os.getenv("LOG_RETENTION_DAYS", "7"))
    
    # Server
    HTTP_PORT: int = int(os.getenv("HTTP_PORT", "8003"))
    
    def __post_init__(self):
        """Create log directory after initialization."""
        os.makedirs(self.LOG_DIR, exist_ok=True)
    
    def dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary for logging."""
        return {
            "kafka_bootstrap": self.KAFKA_BOOTSTRAP_SERVERS,
            "send_topic": self.KAFKA_SEND_STATUS_TOPIC,
            "delivery_topic": self.KAFKA_DELIVERY_TOPIC,
            "consumer_group": self.KAFKA_CONSUMER_GROUP,
            "postgres_host": self.POSTGRES_HOST,
            "postgres_db": self.POSTGRES_DB,
            "batch_size": self.BATCH_SIZE,
            "batch_timeout_ms": self.BATCH_TIMEOUT_MS,
            "workers": self.WORKER_COUNT,
            "max_db_connections": self.MAX_DB_CONNECTIONS,
            "log_level": self.LOG_LEVEL,
            "log_dir": self.LOG_DIR,
            "http_port": self.HTTP_PORT
        }

# Global config instance
config = Config()