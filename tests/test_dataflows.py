import copy
import httpx
import pandas as pd
from conftest import *


def test_fetch_api_enedis(extraction_pip, input_data_schema_cols):
    """TDD 
    Test Enedis's API response. 
    Rule : should return status code 200, even if data empty.
    Why : assert that API is still on.
    """
    test_year, nb_rows = 2023, 3
    test_url = extraction_pip.get_url_enedis_year_rows(annee=test_year, rows=nb_rows)
    api_res = httpx.get(test_url,)
    excp = f"""
    ENEDIS API returned non 200 response for \
    call : test_year={test_year} with {nb_rows} rows : {api_res.json()}
    """
    assert api_res.status_code == 200, excp
    enedis_data = extraction_pip.get_dataframe_from_url(test_url)
    # this should return a dataframe obj (empty or not)
    assert isinstance(enedis_data, pd.DataFrame), "extraction did not return pandas type" 
    assert not enedis_data.empty, "extraction empty"

    # validate schema (required cols not empty)
    schema_req = input_data_schema_cols.get("required-cols", [])
    assert all([c in enedis_data.columns for c in schema_req]), "missing cols from extraction"


def test_load_enedis_batch_input_data(extraction_pip, test_data_folder, input_data_schema_cols):
    """
    Test that extractor can load batch inputs 
        (local mode only, nolocal would require S3 connexion in the VM's CI machine)
    """
    # load from file
    # uses filestorageconn methods and paths to deduct location 
    extraction_pip.load_batch_input()
    assert not extraction_pip.input.empty, "endeis data inputs file loaded empty"
    
    # validate schema (required cols not empty)
    schema_req = input_data_schema_cols.get("required-cols", [])
    assert all([c in extraction_pip.input.columns for c in schema_req]), "missing cols from extraction"


def test_fetch_api_ademe():
    pass

def test_fetch_api_ban():
    pass

def test_run_extract(
        extraction_pip, 
    ):
    extraction_pip.extract(
        from_input=False, 
        input_csv_path="",
        code_departement=75, 
        annee=2023, 
        rows=10, 
        n_threads_for_querying=10,
        save_schema=False
        )
    assert not extraction_pip.output.empty

def test_run_transform(
        transformation_pip,
        test_schemas_folder
    ):
    # utilse en entrée l'exemple extract output (cf. l'init dans conftest)
    transformation_pip.run(
        types_schema_fpath=os.path.join(test_schemas_folder, 'schema_silver_data.json')
    )
    assert not transformation_pip.df_adresses.empty
    assert not transformation_pip.df_logements.empty

def test_load():
    pass