import os, logging
from functools import wraps
from datetime import datetime

from utils.fonctions import get_today_date, set_config_as_env_var

set_config = set_config_as_env_var
set_config()
# config logger 
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected.
# WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g., ‘disk space low’). The software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A very serious error, indicating that the program itself may be unable to continue running.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(os.getenv("logs-app-name"))
local_paths = eval(os.getenv("local-paths"))
log_dir = local_paths.get("path-logs-dir")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"run_{get_today_date()}.log")

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)

def log_decorator(func):
    """
    A decorator to log the start, end, and any exceptions of a function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting function: {func.__name__}")
        try:
            d = datetime.now()
            result = func(*args, **kwargs)
            logger.info(f"{func.__name__} - {datetime.now()-d} - Function {func.__name__} completed successfully.")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} - {datetime.now()-d} - Error in function {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper
