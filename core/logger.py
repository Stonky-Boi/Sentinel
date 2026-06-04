import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a standard logger for the Sentinel application.
    Ensures all modules format their output consistently.
    """
    logger = logging.getLogger(name)
    
    # Prevent adding multiple handlers if the logger is requested multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create a clean, production-ready format
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Route logs to standard output
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger