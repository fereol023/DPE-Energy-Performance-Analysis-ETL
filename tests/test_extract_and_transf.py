import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from src.scripts.extract import DataEnedisAdemeExtractor
from src.scripts.transform import TransformDataEnedisAdeme

def test_extract():
    extractor = DataEnedisAdemeExtractor()
    extractor.extract(annee=2022, rows=100)
    print(extractor.output.T)

def test_transform():
    example_extract_output = pd.read_parquet("tests/example_extract_output.parquet")
    print(example_extract_output.shape)
    pipeline = TransformDataEnedisAdeme(example_extract_output, inplace=False)
    pipeline.run()
    print(pipeline.df_adresses.shape)
    print(pipeline.df_logements.shape)
    print(pipeline.df_consommations.shape)

test_transform()
