import logging
import time
from logging.handlers import TimedRotatingFileHandler
def create_rotating_log(path):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    
    # add a rotating handler
    handler = TimedRotatingFileHandler(path,
                                       when="m",
                                       interval=1440,
                                       backupCount=5)
    logger.addHandler(handler)
    return logger