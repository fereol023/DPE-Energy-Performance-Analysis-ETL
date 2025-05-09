import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd, copy
from src.scripts import extract

os.environ['ENV'] = 'LOCAL'
extractor = extract.DataEnedisAdemeExtractor()

def test_fetch_api_enedis_data():
    exple_url = extractor.get_url_enedis_year_rows(annee=2022, rows=3)
    res = extractor.load_get_data_pandas(exple_url)
    assert not res.empty
    print(res.T)


def test_load_enedis_input_data():
    res = extractor.load_batch_input()
    assert not res.empty
    print(res.T)

if __name__=='__main__':
    test_fetch_api_enedis_data()