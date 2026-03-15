"""Centralized logging configuration."""
import os
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import datetime

from .config import config

class DailyRotatingFileHandler(TimedRotatingFileHandler):
    """Custom handler that creates daily log files with YYYY-MM-DD.log format."""
    
    def __init__(self, log_dir, when='midnight', interval=1, backupCount=7, encoding=None):
        self.log_dir = log_dir
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.filename = os.path.join(log_dir, f"{self.current_date}.log")
        
        super().__init__(
            filename=self.filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            utc=False
        )
    
    def doRollover(self):
        """Override to handle daily rollover with new filename."""
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.baseFilename = os.path.join(self.log_dir, f"{self.current_date}.log")
        super().doRollover()

class CorrelationIDFilter(logging.Filter):
    """Add correlation_id to log records if not present."""
    
    def filter(self, record):
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = '-'
        return True

def setup_logging():
    """Initialize and configure logging for the application."""
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, config.LOG_LEVEL))
    
    # Console handler (for live viewing)
    console_handler = logging.StreamHandler()
    console_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
    console_handler.setFormatter(logging.Formatter(console_format))
    console_handler.addFilter(CorrelationIDFilter())
    root_logger.addHandler(console_handler)
    
    # Daily rotating file handler
    file_handler = DailyRotatingFileHandler(
        log_dir=config.LOG_DIR,
        when=config.LOG_WHEN,
        interval=1,
        backupCount=config.LOG_RETENTION_DAYS,
        encoding='utf-8'
    )
    file_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(filename)s:%(lineno)d - %(message)s'
    file_handler.setFormatter(logging.Formatter(file_format))
    file_handler.addFilter(CorrelationIDFilter())
    root_logger.addHandler(file_handler)
    
    # Size-based rotation as fallback
    size_handler = RotatingFileHandler(
        filename=os.path.join(config.LOG_DIR, "emergency.log"),
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=3
    )
    size_handler.setLevel(logging.WARNING)
    size_handler.setFormatter(logging.Formatter(file_format))
    size_handler.addFilter(CorrelationIDFilter())
    root_logger.addHandler(size_handler)
    
    # Get logger for this module
    logger = logging.getLogger(config.SERVICE_NAME)
    
    # Log startup
    logger.info("=" * 60)
    logger.info(f"Starting {config.SERVICE_NAME} v{__import__('app').__version__}")
    logger.info("=" * 60)
    logger.info(f"Log level: {config.LOG_LEVEL}")
    logger.info(f"Log directory: {config.LOG_DIR}")
    logger.info(f"Log retention: {config.LOG_RETENTION_DAYS} days")
    logger.info("=" * 60)
    
    return logger

# Initialize logger
logger = setup_logging()