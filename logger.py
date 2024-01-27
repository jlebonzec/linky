import logging
import logging.handlers
import os

LOG_PATH = './logs/linky.log'
LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5


def init_log_system() -> logging.Logger:
    """ Initialize logger """
    logger = logging.getLogger('linky')
    logger.setLevel(logging.DEBUG)  # Define minimum severity here
    if not logger.handlers:
        # Create path if it doesn't exist
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

        # Configure handler
        handler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
        formatter = logging.Formatter('[%(asctime)s][%(module)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S %z')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


log = init_log_system()
