# utils/logging_config.py
 
import logging
from pathlib import Path
 
def get_logger(name: str = "ticket_fraud_logger") -> logging.Logger:
    logger = logging.getLogger(name)
 
    if not logger.handlers:
        logger.setLevel(logging.INFO)
 
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)
 
        # File handler: single persistent log file
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "app.log"
 
        file_handler = logging.FileHandler(log_file, mode="a")
        file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        file_handler.setFormatter(file_formatter)
 
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
 
    return logger