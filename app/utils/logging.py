import logging
import os
from datetime import datetime
from typing import Optional

def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with detailed formatting.
    
    Args:
        name: Logger name (usually __name__)
        level: Optional logging level override
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set level from environment or default to INFO
    log_level = (
        level or 
        os.getenv('LOG_LEVEL', 'INFO')
    ).upper()
    logger.setLevel(getattr(logging, log_level))
    
    # Create formatter
    formatter = logging.Formatter(
        fmt=(
            '%(asctime)s | %(levelname)-8s | '
            '%(name)s:%(funcName)s:%(lineno)d | '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create file handler
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    file_handler = logging.FileHandler(
        os.path.join(
            log_dir,
            f"{datetime.now().strftime('%Y-%m-%d')}.log"
        )
    )
    file_handler.setFormatter(formatter)
    
    # Add handlers if they haven't been added already
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger 