import logging
import logging.handlers
import json
import sys
import time
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps
import traceback
from contextvars import ContextVar, copy_context
from pythonjsonlogger import jsonlogger


# Context variable for async-safe logging context
log_context: ContextVar[Dict[str, Any]] = ContextVar('log_context', default={})


class ContextFilter(logging.Filter):
    def filter(self, record):
        # Get context from contextvars instead of instance variable
        context = log_context.get()
        for key, value in context.items():
            setattr(record, key, value)
        return True


class ErrorDetailsFilter(logging.Filter):
    def filter(self, record):
        if record.exc_info:
            exc_type, exc_value, exc_traceback = record.exc_info
            record.error_type = exc_type.__name__ if exc_type else None
            record.error_message = str(exc_value) if exc_value else None
            record.stacktrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        return True


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.utcnow().isoformat()
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
        
        if hasattr(record, 'duration_ms'):
            log_record['duration_ms'] = record.duration_ms
        
        if hasattr(record, 'request_id'):
            log_record['request_id'] = record.request_id


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
    log_file: Optional[Path] = None,
    rotate_size: int = 10_000_000,
    backup_count: int = 5
) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    root_logger.handlers.clear()
    
    context_filter = ContextFilter()
    error_filter = ErrorDetailsFilter()
    
    if format_type == "json":
        formatter = CustomJsonFormatter(
            '%(timestamp)s %(level)s %(name)s %(message)s',
            json_encoder=json.JSONEncoder
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(context_filter)
    console_handler.addFilter(error_filter)
    root_logger.addHandler(console_handler)
    
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=rotate_size,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(context_filter)
        file_handler.addFilter(error_filter)
        root_logger.addHandler(file_handler)
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class LogContext:
    def __init__(self, **kwargs):
        self.new_context = kwargs
        self.previous_context = None

    def __enter__(self):
        # Save current context
        self.previous_context = log_context.get().copy()
        # Update with new context
        updated_context = self.previous_context.copy()
        updated_context.update(self.new_context)
        log_context.set(updated_context)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous context
        log_context.set(self.previous_context)


def log_execution_time(logger: logging.Logger):
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    duration = (time.time() - start_time) * 1000
                    logger.info(
                        f"{func.__name__} completed",
                        extra={"duration_ms": duration}
                    )
                    return result
                except Exception as e:
                    duration = (time.time() - start_time) * 1000
                    logger.error(
                        f"{func.__name__} failed",
                        extra={"duration_ms": duration},
                        exc_info=True
                    )
                    raise
            
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = (time.time() - start_time) * 1000
                    logger.info(
                        f"{func.__name__} completed",
                        extra={"duration_ms": duration}
                    )
                    return result
                except Exception as e:
                    duration = (time.time() - start_time) * 1000
                    logger.error(
                        f"{func.__name__} failed",
                        extra={"duration_ms": duration},
                        exc_info=True
                    )
                    raise
            
            return sync_wrapper
    
    return decorator