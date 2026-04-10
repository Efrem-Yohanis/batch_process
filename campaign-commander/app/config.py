"""Application configuration management."""
import os
from typing import Dict, Any

class Config:
    """Centralized configuration for the application."""
    
    # Docker defaults with environment variable override
    DEFAULTS = {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_COMMAND_TOPIC": "sms-commands",
        "CAMPAIGN_MANAGER_URL": "http://django-app:8000",
        "DJANGO_USERNAME": "admin",
        "DJANGO_PASSWORD": "admin123",
        "LOG_LEVEL": "INFO",
        "LOG_DIR": "/app/logs/layer1",
        "LOG_RETENTION_DAYS": "7",
        "LOG_WHEN": "midnight",
        "LOG_MAX_BYTES": str(100 * 1024 * 1024),  # 100MB
        "SERVICE_NAME": "layer1",
        "HOST": "0.0.0.0",
        "PORT": "8001",
        "RELOAD": "true"
    }
    
    def __init__(self):
        # Apply defaults for missing environment variables
        for key, value in self.DEFAULTS.items():
            if key not in os.environ:
                os.environ[key] = value
        
        # Kafka settings
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.KAFKA_COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC")
        
        # External API settings
        self.CAMPAIGN_MANAGER_URL = os.getenv("CAMPAIGN_MANAGER_URL")
        self.DJANGO_USERNAME = os.getenv("DJANGO_USERNAME")
        self.DJANGO_PASSWORD = os.getenv("DJANGO_PASSWORD")
        
        # Logging settings
        self.LOG_LEVEL = os.getenv("LOG_LEVEL")
        self.LOG_DIR = os.getenv("LOG_DIR")
        self.LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS"))
        self.LOG_WHEN = os.getenv("LOG_WHEN")
        self.LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES"))
        self.SERVICE_NAME = os.getenv("SERVICE_NAME")
        
        # Server settings
        self.HOST = os.getenv("HOST")
        self.PORT = int(os.getenv("PORT"))
        self.RELOAD = os.getenv("RELOAD").lower() == "true"
        
        # Token settings
        self.TOKEN_EXPIRY_BUFFER = 60  # Seconds before expiry to refresh
        self.TOKEN_REFRESH_EARLY = 20  # Seconds early to refresh
        
        # Create log directory
        os.makedirs(self.LOG_DIR, exist_ok=True)
    
    def dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary for logging/display."""
        return {
            "kafka_bootstrap_servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "kafka_topic": self.KAFKA_COMMAND_TOPIC,
            "campaign_manager_url": self.CAMPAIGN_MANAGER_URL,
            "log_level": self.LOG_LEVEL,
            "log_dir": self.LOG_DIR,
            "log_retention_days": self.LOG_RETENTION_DAYS,
            "service_name": self.SERVICE_NAME,
            "host": self.HOST,
            "port": self.PORT
        }

# Global config instance
config = Config()