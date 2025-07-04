import logging
import uuid, math
import datetime, time
from typing import Union
from functools import wraps

from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor

def get_custom_logger_dict():
    """
    Liste des champs :
    app_name, function_name, timestamp, duration (ms),
    ln_duration (ms), correlation_id, status, severity, details[message]
    """
    return {
        "app_name": "", # est le nom du logger 
        "function_name": "",
        "timestamp": "",
        "@timestamp": "",
        "duration (ms)": "",
        "ln_duration (ms)": "",
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

class AsyncElasticSearchHandler(logging.Handler):

    def __init__(self, index='default', max_workers=5):
        """ LogRecords attributes :
        ['args', 'created', 'exc_info', 'exc_text', 'filename', 'funcName', 
        'getMessage', 'levelname', 'levelno', 'lineno', 'module', 'msecs', 
        'msg', 'name', 'pathname', 'process', 'processName', 'relativeCreated', 
        'stack_info', 'taskName', 'thread', 'threadName']
        """
        super().__init__()
        self.es = Elasticsearch("http://localhost:9200")
        self.index = index
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def emit(self, record):
        # override de emit pour envoyer sur le serveur elastic
        # on fait dans des threads différents à chaque nouveau log qui arrive
        self.executor.submit(self.log_to_elasticsearch, record)
    
    def parse_log(self, _log_entry: logging.LogRecord) -> dict[str, Union[str, float, int, datetime.datetime]]:

        parsed_log = get_custom_logger_dict().copy()
        parsed_log["timestamp"] = datetime.datetime.now()
        parsed_log["details"]["module"] = _log_entry.module
        parsed_log["details"]["source"] = _log_entry.funcName # renvoie wrapper quand c'est un log
        parsed_log["details"]["logger_name"] = _log_entry.name
        parsed_log["severity"] = _log_entry.levelname
        
        try:
            _log_msg = eval(_log_entry.getMessage()) 
            # si c'est un str dict, retournera a dict sinon leve excep
            parsed_log["status"] = _log_msg["status"]
            parsed_log["app_name"] = _log_msg["app_name"]
            parsed_log["timestamp"] = _log_msg["timestamp"]
            parsed_log["function_name"] = _log_msg["function_name"]
            parsed_log["duration (ms)"] = _log_msg["duration (ms)"]
            parsed_log["correlation_id"] = _log_msg["correlation_id"]
            parsed_log["ln_duration (ms)"] = _log_msg["ln_duration (ms)"] 
            parsed_log["details"]["message"] = _log_msg["details"]["message"]
            return parsed_log
        except:
            try:
                # le getMessage est un str simple dans ce cas
                parsed_log["details"]["message"] = _log_entry.getMessage()
                parsed_log["timestamp"] = datetime.datetime.fromtimestamp(_log_entry.created)
            except:
                raise
        return parsed_log

    def log_to_elasticsearch(self, log_entry):
        try:
            self.es.index(
                index=self.index, 
                id=uuid.uuid4(), 
                document=self.parse_log(log_entry) # type logRecord
                )
        except Exception as e:
            print(f"Failed to log to Elasticsearch: {e}")

# config logger
logger = logging.getLogger("async_logger")
logger.setLevel(logging.DEBUG)

# Create an instance of the custom handler
elastic_handler = AsyncElasticSearchHandler(index="async_logging_test_6")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

elastic_handler.setFormatter(formatter)
logger.addHandler(elastic_handler)


# --- logger pour les fonctions 
def decorator_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        s = datetime.datetime.now()
        log_entry = get_custom_logger_dict().copy()
        log_entry["function_name"] = func.__name__
        log_entry["timestamp"] = s.isoformat()
        try:
            result = func(*args, **kwargs)
            log_entry["status"] = "success"
            return result
        except Exception as e:
            log_entry["status"] = "fail"
            log_entry["details"]["message"] = str(e)
            raise
        finally:
            log_entry["duration (ms)"] = round((datetime.datetime.now() - s).total_seconds() * 1_000, 3)
            log_entry["ln_duration (ms)"] = round(math.log(log_entry["duration (ms)"]), 3)
            if log_entry["status"] == "fail":
                logger.critical(log_entry)
            else:
                logger.info(log_entry)
    return wrapper



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