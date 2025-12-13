import os
import sys
import logging
from datetime import datetime

def setup_logging(log_dir: str):
    """
    Sets up logging to file and console with timestamps.
    """
    os.makedirs(log_dir, exist_ok=True)

    # Log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{timestamp}.log")

    # Clear existing handlers to ensure our configuration takes precedence
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers = []

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    
    # Reconfigure pykis logger to use our root logger settings
    # This prevents duplicate logs and ensures consistent formatting
    pykis_logger = logging.getLogger("pykis")
    pykis_logger.handlers = []  # Remove pykis's default handlers
    pykis_logger.propagate = True
    pykis_logger.setLevel(logging.ERROR)

    # Suppress urllib3 and requests logs (only show errors)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file
