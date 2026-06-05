import logging
from pathlib import Path

# Define the hidden Sentinel home directory on the host machine
SENTINEL_HOME = Path.home() / ".sentinel"
LOG_DIR = SENTINEL_HOME / "logs"
REPORT_DIR = SENTINEL_HOME / "reports"

# Ensure the directory structure exists
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    """
    Returns a standard logger that writes exclusively to a rotating log file.
    Ensures absolute silence in the standard terminal output for daemonization.
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Route logs to a dedicated system file, NOT stdout
        log_file = LOG_DIR / "system.log"
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger