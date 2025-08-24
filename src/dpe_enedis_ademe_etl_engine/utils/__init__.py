import os
import sys
import logging
import uuid
import datetime
import logging.handlers
from typing import Union
from functools import wraps
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor

try:
    from ..utils.fonctions import get_env_var, get_today_date
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    sys.path.append(str(parent_dir))
    from utils.fonctions import get_env_var, get_today_date

# configuration for Elasticsearch
# ELASTICSEARCH_HOST = get_env_var('ELASTICSEARCH_HOST', compulsory=True)
# ELASTICSEARCH_PORT = get_env_var('ELASTICSEARCH_PORT', compulsory=True, cast_to_type=int)
# ELASTICSEARCH_INDEX = get_env_var('ELASTICSEARCH_INDEX', compulsory=True)
LOGGER_APP_NAME = get_env_var('ETL_LOGGER_APP_NAME', default_value='dpe_ETL_engine_logger', compulsory=True)
BATCH_CORRELATION_ID = get_env_var('BATCH_CORRELATION_ID', default_value="000000000", compulsory=True)


def get_custom_logger_dict():
    """
    Liste des champs :
    app_name, function_name, timestamp, duration (ms),
    correlation_id, status, severity, details[message]
    """
    return {
        "app_name": "", # est le nom du logger 
        "function_name": "",
        "timestamp": "",
        # "@timestamp": "",
        "duration_ms": -1, # default -1, si pas de duration
        "correlation_id": "",
        "status": "",
        "severity": "",
        "details": {
            "message": "",
            "logger_name": "",
            "module": "",
            "source": "" # flag pour dire si le log provient du decorateur est deja formatté
        }
    }

# class AsyncElasticSearchHandler(logging.Handler):

#     def __init__(self, index: str='', max_workers: int=10):
#         """ LogRecords attributes :
#         ['args', 'created', 'exc_info', 'exc_text', 'filename', 'funcName', 
#         'getMessage', 'levelname', 'levelno', 'lineno', 'module', 'msecs', 
#         'msg', 'name', 'pathname', 'process', 'processName', 'relativeCreated', 
#         'stack_info', 'taskName', 'thread', 'threadName']
#         """
#         super().__init__()
#         self.es = Elasticsearch(f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}")
#         self.index = index
#         self.executor = ThreadPoolExecutor(max_workers=max_workers)
#         # init index if it does not exist
#         if not self.es.indices.exists(index=self.index):
#             self.es.indices.create(index=self.index, ignore=400)
#         if not self.es.ping():
#             raise ConnectionError(f"Could not connect to Elasticsearch at {ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}")

#     def emit(self, record):
#         # override de emit pour envoyer sur le serveur elastic
#         # on fait dans des threads différents à chaque nouveau log qui arrive
#         self.executor.submit(self.log_to_elasticsearch, record)
    
#     def parse_log(self, _log_entry: logging.LogRecord) -> dict[str, Union[str, float, int, datetime.datetime]]:

#         parsed_log = get_custom_logger_dict().copy()
#         parsed_log["timestamp"] = datetime.datetime.now()
#         parsed_log["details"]["module"] = _log_entry.module
#         parsed_log["details"]["source"] = _log_entry.funcName # renvoie wrapper quand c'est un log
#         parsed_log["details"]["logger_name"] = _log_entry.name
#         parsed_log["severity"] = _log_entry.levelname
        
#         try:
#             _log_msg = eval(_log_entry.getMessage()) 
#             # si c'est un str dict, retournera a dict sinon leve excep
#             parsed_log["status"] = _log_msg["status"]
#             parsed_log["app_name"] = _log_msg["app_name"]
#             parsed_log["timestamp"] = _log_msg["timestamp"]
#             parsed_log["function_name"] = _log_msg["function_name"]
#             parsed_log["duration_ms"] = _log_msg["duration_ms"]
#             parsed_log["correlation_id"] = _log_msg["correlation_id"]
#             parsed_log["details"]["message"] = _log_msg["details"]["message"]
#             return parsed_log
#         except:
#             try:
#                 # le getMessage est un str simple dans ce cas
#                 parsed_log["details"]["message"] = _log_entry.getMessage()
#                 parsed_log["timestamp"] = datetime.datetime.fromtimestamp(_log_entry.created)
#             except:
#                 raise
#         return parsed_log

#     def log_to_elasticsearch(self, log_entry):
#         try:
#             self.es.index(
#                 index=self.index, 
#                 id=uuid.uuid4(), 
#                 document=self.parse_log(log_entry) # type logRecord
#                 )
#         except Exception as e:
#             print(f"Failed to log to Elasticsearch: {e}")


# config logger
def get_async_elk_logger(app_name=None) ->logging.Logger:
    """
    Configure the logger to send logs to elk server.
    """
    import queue

    _logger = logging.getLogger(name=app_name if app_name else LOGGER_APP_NAME)
    _logger.setLevel(logging.INFO)
    
    if get_env_var('ENV', compulsory=True) == 'LOCAL':
        log_dir = None # get_env_var("PATH_LOG_DIR", default_value=None, compulsory=False)
        # if log_dir is not None:
        #     os.makedirs(log_dir, exist_ok=True)
        #     log_file = os.path.join(log_dir, f"run_{get_today_date()}.log")
        #     _backup_handler = logging.FileHandler(log_file)
        # else:
        _backup_handler = logging.StreamHandler(sys.stdout)
    else:
        #_backup_handler = AsyncElasticSearchHandler(index=ELASTICSEARCH_INDEX)
        _backup_handler = logging.StreamHandler(sys.stdout)
    
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _backup_handler.setFormatter(_formatter)

    # create a queue for log records
    _log_queue = queue.Queue(-1) # infinite size for queue
    _queue_handler = logging.handlers.QueueHandler(_log_queue)
    _logger.addHandler(_queue_handler)

    # Start a listener with the elastic handler
    listener = logging.handlers.QueueListener(_log_queue, _backup_handler)
    listener.start()

    _logger.propagate = False  # Prevents the log messages from being propagated to the root logger
    # _logger.info(f"Logger initialized for {LOGGER_APP_NAME} with Elasticsearch")
    return _logger


# --- logger pour les fonctions 
def decorator_logger(func, logger=get_async_elk_logger()):
    @wraps(func)
    def wrapper(*args, **kwargs):
        s = datetime.datetime.now()
        log_entry = get_custom_logger_dict().copy()
        log_entry["function_name"] = func.__name__
        log_entry["timestamp"] = s.isoformat()
        try:
            result = func(*args, **kwargs)
            log_entry["status"] = "success"
        except Exception as e:
            log_entry["status"] = "fail"
            log_entry["details"]["message"] = str(e)
        finally:
            log_entry["duration_ms"] = round((datetime.datetime.now() - s).total_seconds() * 1_000, 3)
            log_entry["correlation_id"] = BATCH_CORRELATION_ID
            log_entry["app_name"] = LOGGER_APP_NAME
            if log_entry["status"] == "fail":
                log_entry["severity"] = "CRITICAL"
                logger.critical(log_entry)
                raise Exception(f"Error in function {func.__name__}: {log_entry['details']['message']}")
            else:
                # logger.info(log_entry)
                log_entry["severity"] = "INFO"
                # print(log_entry)
                return result
    return wrapper

logger = get_async_elk_logger()

# # Example usage
# logger.info("This is an info message.")
# logger.error("This is an error message.")
# logger.warning("{'message': 'this is a warning message', 'function_name': 'test_Warning'}")

# @decorator_logger
# def some_fonction_ts(a, b):
#     time.sleep(10)
#     return a/b

# @decorator_logger
# def some_fonction(a, b):
#     time.sleep(10)
#     return a/b

# some_fonction_ts(100, 2)
# some_fonction(100, 0)


### legacy code
# import os, logging
# from functools import wraps
# from datetime import datetime

# from ..utils.fonctions import get_today_date, set_config_as_env_var

# set_config = set_config_as_env_var
# # set_config()

# # config logger 
# # DEBUG: Detailed information, typically of interest only when diagnosing problems.
# # INFO: Confirmation that things are working as expected.
# # WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g., ‘disk space low’). The software is still working as expected.
# # ERROR: Due to a more serious problem, the software has not been able to perform some function.
# # CRITICAL: A very serious error, indicating that the program itself may be unable to continue running.

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# logger = logging.getLogger(os.getenv("logs-app-name"))
# # local_paths = eval(os.getenv("local-paths"))
# # log_dir = local_paths.get("path-logs-dir")
# # os.makedirs(log_dir, exist_ok=True)
# # log_file = os.path.join(log_dir, f"run_{get_today_date()}.log")

# # file_handler = logging.FileHandler(log_file)
# # file_handler.setLevel(logging.INFO)
# # file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# # logger.addHandler(file_handler)

# def log_decorator(func):
#     """
#     A decorator to log the start, end, and any exceptions of a function.
#     """
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         logger.info(f"Starting function: {func.__name__}")
#         try:
#             d = datetime.now()
#             result = func(*args, **kwargs)
#             logger.info(f"{func.__name__} - {datetime.now()-d} - Function {func.__name__} completed successfully.")
#             return result
#         except Exception as e:
#             logger.error(f"{func.__name__} - {datetime.now()-d} - Error in function {func.__name__}: {e}", exc_info=True)
#             raise
#     return wrapper
