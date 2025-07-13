import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import httpx
import pytest
import pandas as pd

def set_config(config_folder, data_folder):
    """util to setup config"""
    dict_config = json.load(open(os.path.join(config_folder, "config.json"), "r"))
    dict_config.update({'PATH_ARCHIVE_DIR': os.path.join(data_folder, 'tmp', 'archive')})
    dict_config.update({'PATH_DATA_BRONZE': os.path.join(data_folder, 'tmp', 'bronze')})
    dict_config.update({'PATH_DATA_SILVER': os.path.join(data_folder, 'tmp', 'silver')})
    dict_config.update({'PATH_DATA_GOLD': os.path.join(data_folder, 'tmp', 'gold')})
    dict_config.update({'PATH_FILE_INPUT_ENEDIS_CSV': os.path.join(data_folder, 'example_extract_input.csv')})

    for key, value in dict_config.items():
        print(f"{key}={value}")
        os.environ[key] = value
    

@pytest.fixture(scope="session")
def test_config_folder():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")


@pytest.fixture(scope="session")
def test_data_folder():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

@pytest.fixture(scope="session")
def test_schemas_folder():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "ressources", "schemas")

@pytest.fixture(scope="session")
def example_extract_output(test_data_folder):
    return pd.read_parquet(os.path.join(test_data_folder, "example_extract_output.parquet"))

@pytest.fixture(scope="session")
def extraction_pip(test_config_folder, test_data_folder):
    set_config(test_config_folder, test_data_folder)
    from src.dpe_enedis_ademe_etl_engine.pipelines import DataEnedisAdemeExtractor
    return DataEnedisAdemeExtractor(debug=True)

@pytest.fixture(scope="session")
def transformation_pip(test_config_folder, test_data_folder, example_extract_output, test_schemas_folder):
    set_config(test_config_folder, test_data_folder)
    from src.dpe_enedis_ademe_etl_engine.pipelines import DataEnedisAdemeTransformer
    return DataEnedisAdemeTransformer(
        example_extract_output, 
        inplace=False,
        golden_data_config_fpath=os.path.join(test_schemas_folder, "schema_golden_data.json")
    )

@pytest.fixture(scope="session")
def fs_conn(test_config_folder, test_data_folder):
    set_config(test_config_folder, test_data_folder)
    from src.dpe_enedis_ademe_etl_engine.scripts.filestorage_helper import FileStorageConnexion
    return FileStorageConnexion

@pytest.fixture(scope="session")
def input_data_schema_cols(test_schemas_folder):
    f=open(os.path.join(test_schemas_folder, "schema_input_data.json"), "r")
    return json.load(f)