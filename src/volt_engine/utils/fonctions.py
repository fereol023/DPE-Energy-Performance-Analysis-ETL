import re
import os
import json
import yaml
import pickle
import logging
import warnings
import numpy as np
import pandas as pd

from datetime import datetime
from unidecode import unidecode
from functools import lru_cache, wraps


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def deprecation_warning(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"Function {func.__name__} is deprecated and will be removed in future versions.",
            DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return wrapper

def load_yaml(fpath, default_value=None):
    if not os.path.exists(fpath):
        logging.warning(f"File {fpath} does not exists !")
        return default_value
    return yaml.safe_load(open(fpath, 'r'))

def load_json(fpath, default_value=None):
    if not os.path.exists(fpath):
        logging.warning(f"File {fpath} does not exists !")
        return default_value
    return json.load(open(fpath, 'rb'))

def load_pickle(fpath, is_optional=False):
    if (not os.path.exists(fpath) and (not is_optional)):
        raise Exception(f"File {fpath} does not exist and is not optional !")
    with open(fpath, 'rb') as f:
        res = pickle.load(f)
    return res

def save_pickle(obj, fpath):
    """obj : serialisable obj"""
    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))
    with open(fpath, 'wb') as f:
        pickle.dump(obj, f)
    print(f"Sauvegarde ok at : {fpath}")

@lru_cache(maxsize=1024)
def normalize_name(colname):
    pat1, pat2 = re.compile('[^0-9a-zA-Z]+'), re.compile('_+')
    return pat1.sub('_', pat2.sub('_', colname))

def normalize_colnames_list(list_colnames=[]):
    if list_colnames:
        return list(map(lambda c: normalize_name(unidecode(c)).lower(), list_colnames))
    return []

def sort_colnames(df):
    return df[sorted(df.columns)]

def normalize_df_colnames(df):
    return sort_colnames(df.rename(columns={c: normalize_name(unidecode(c)).lower() for c in df.columns}))

def get_today_date():
    return datetime.today().strftime('%Y_%m_%d')

def get_yesterday_date():
    return (datetime.today() - pd.Timedelta(days=1)).strftime('%Y_%m_%d')

def load_parquet_dataframe(_PATH):
    """TODO"""
    print(f"Loading parquet data from : {_PATH}..")
    try:
        return normalize_df_colnames(pd.read_parquet(_PATH))
    except Exception as e:
        print(f"Error while loading : {e}")

def load_json_config(path):
    return load_json(path, default_value={})

def load_yaml_config(fpath):
    return load_yaml(fpath, default_value={})

def get_env_var(var_name, default_value=None, compulsory=False, cast_to_type=None):
    """
    Get an environment variable.
    Returns default_value if not set and value is not compulsory.
    Returns default_value if not set and value is compulsory, but logs a warning.
    Raises ValueError if compulsory and not set, without default_value.
    :param var_name: Name of the environment variable.
    :param default_value: Default value to return if the variable is not set.
    :param compulsory: If True, raises an error if the variable is not set and no default_value is provided.
    :param cast_to_type: Optional type to cast the value to (e.g., int, float, str).
    :return: The value of the environment variable or the default_value.
    :raises ValueError: If the variable is compulsory and not set without a default_value. 
    """
    value = os.getenv(var_name)
    if not value:
        if compulsory:
            if not default_value:
                raise ValueError(f"Environment variable {var_name} is not set and is compulsory.")
            else:
                logging.warning(f"Environment variable {var_name} is not set, using default value: {default_value}")
                value = default_value

    try:
        return cast_to_type(value) if cast_to_type is not None else value
    except ValueError as e:
        raise ValueError(f"Cannot cast environment variable {var_name} to {cast_to_type}: {e}")

def set_config_as_env_var(dirpath=None):
    """
    Provides an unified way to set environment 
    variables from a config directory including 
    json, yaml and .env files.
    TODO: add .env files support
    """
    if os.getenv('ENV') == None:
        config = {}
        for f in os.listdir(dirpath):
            if f.endswith('.yaml'):
                config.update(load_yaml_config(os.path.join(dirpath, f)))
            if f.endswith('.json'):
                config.update(load_json_config(os.path.join(dirpath, f)))
        appname, env = config.get('app-name'), config.get('ENV')
        assert env in ['LOCAL', 'NOLOCAL'], f"Config error : ENV ({env}) is not valid. Choose between ['LOCAL', 'NOLOCAL']"
        logging.info(f"Application {appname} is running on env {env}")
        for key, value in config.items():
            os.environ[key] = str(value)
