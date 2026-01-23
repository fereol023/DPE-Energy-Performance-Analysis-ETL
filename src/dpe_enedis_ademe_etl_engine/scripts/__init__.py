import os
import json
import requests
import pandas as pd
from io import BytesIO

# use s3fs with boto3 client later
from minio import Minio 
from pyarrow import Table, parquet as pq

try:
    from ..utils import async_logger, decorator_logger
    from ..utils.fonctions import (
        get_env_var,
        get_today_date, 
    )
    from ..scripts.envs_helper import Envs
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    sys.path.append(str(parent_dir))
    from utils import async_logger, decorator_logger
    from utils.fonctions import (
        get_env_var,
        get_today_date, 
    )
    from scripts.envs_helper import Envs

class Paths:
    """
    Class to manage paths based on environment variables.
    """
    def __init__(self):
        self.env = get_env_var('ENV', compulsory=True)
        assert self.env in [Envs.LOCAL, Envs.PROD, Envs.ISOLATED], f"Invalid ENV value: {self.env}"
        # self.PATH_LOG_DIR = get_env_var('PATH_LOG_DIR', compulsory=True)
        self.PATH_ARCHIVE_DIR = get_env_var('PATH_ARCHIVE_DIR', compulsory=True)
        self.PATH_DATA_BRONZE = get_env_var('PATH_DATA_BRONZE', compulsory=True)
        self.PATH_DATA_SILVER = get_env_var('PATH_DATA_SILVER', compulsory=True)
        self.PATH_DATA_GOLD = get_env_var('PATH_DATA_GOLD', compulsory=True)
        async_logger.info(f"Environment: {self.env}")

__all__ = [
    "Paths",
    "Envs"
    ]