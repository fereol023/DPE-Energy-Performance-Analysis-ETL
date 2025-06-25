import sys, os, httpx
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ['ENV'] = 'LOCAL'

import pandas as pd, copy
from src.scripts import extract

obj_extractor = extract.DataEnedisAdemeExtractor()

def test_abc():
    pass


def fetch_api_enedis_data(obj_extractor):
    '''
    Test Enedis's API response. 
    Rule : should return status code 200, even if data empty.
    Why : assert that API is still on.
    '''
    test_year, nb_rows = 2022, 3
    test_url = obj_extractor.get_url_enedis_year_rows(annee=test_year, rows=nb_rows)
    api_res = httpx.get(test_url,)
    exp = f"""
    ENEDIS API returned non 200 response for \
    call : test_year={test_year} with {nb_rows} rows : {api_res.json()}
    """
    assert api_res.status_code == 200, exp
    enedis_data = obj_extractor.load_get_data_pandas(test_url)
    # this should return a dataframe obj (empty or not)
    assert isinstance(enedis_data.T, pd.DataFrame) 
    # validate schema (required cols not empty)


def load_enedis_input_data(obj_extractor, test_data_folder):
    '''
    Test that extractor can load batch inputs 
        (local mode only, nolocal would require S3 connexion in the VM's CI machine)
    '''
    test_data_fpath = os.path.join(test_data_folder, "inputs_etl", "batchfile.json/parquet")
    res = obj_extractor.load_batch_input(test_data_fpath)
    assert not res.empty

# add more 
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ['ENV'] = 'LOCAL'

import pandas as pd
from src.scripts.extract import DataEnedisAdemeExtractor
from src.scripts.transform import TransformDataEnedisAdeme

def aest_extract():
    extractor = DataEnedisAdemeExtractor()
    extractor.extract(annee=2022, rows=100)
    print(extractor.output.T)

def aest_transform():
    example_extract_output = pd.read_parquet("tests/example_extract_output.parquet")
    print(example_extract_output.shape)
    pipeline = TransformDataEnedisAdeme(example_extract_output, inplace=False)
    pipeline.run()
    print(pipeline.df_adresses.shape)
    print(pipeline.df_logements.shape)
    print(pipeline.df_consommations.shape)

aest_transform()
