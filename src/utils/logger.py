"""Structured logging configuration for the ETL pipeline."""
import logging
import sys
from datetime import datetime
from typing import Optional
from src.utils.config import Config


class PipelineLogger:
    """Structured logger for ETL pipeline operations."""
    
    _logger: Optional[logging.Logger] = None
    
    @classmethod
    def get_logger(cls, name: str = "etl_pipeline") -> logging.Logger:
        """Get or create a logger instance."""
        if cls._logger is None:
            cls._logger = cls._setup_logger(name)
        return cls._logger
    
    @classmethod
    def _setup_logger(cls, name: str) -> logging.Logger:
        """Set up logger with formatting and handlers."""
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO))
        
        # Avoid duplicate handlers
        if logger.handlers:
            return logger
        
        # Console handler with formatted output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        # Format: [TIMESTAMP] [LEVEL] [MODULE] MESSAGE
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    @classmethod
    def log_etl_stage(cls, stage: str, message: str, **kwargs):
        """Log ETL stage-specific messages with context."""
        logger = cls.get_logger()
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        logger.info(f"[{stage.upper()}] {message} {context if context else ''}")


def get_logger(name: str = "etl_pipeline") -> logging.Logger:
    """Convenience function to get a logger instance."""
    return PipelineLogger.get_logger(name)

