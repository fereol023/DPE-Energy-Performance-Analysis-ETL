from ..scripts import extract, transform, load
from ..utils.fonctions import get_env_var
from ..utils import decorator_logger

from prefect import flow, task
from sqlalchemy import create_engine

@decorator_logger
@task
def extract_data_task():
    """call the extract function from extract module"""
    extract_pipeline = extract.DataEnedisAdemeExtractor(debug=False)
    extract_pipeline.extract(annee=2022, rows=100)
    return extract_pipeline.output

@decorator_logger
@task
def transform_data_task(data):
    """call the transform function from your transform module"""
    transf_pipeline = transform\
        .DataEnedisAdemeTransformer(
            data, 
            inplace=False, 
            golden_data_config_fpath=get_env_var('SCHEMA_GOLDEN_DATA_FILEPATH', compulsory=True))
    transf_pipeline.run(
        types_schema_fpath=None, # schema de la data silver en input (pour le cast), si none est inféré
        keep_only_required=False
    )
    return transf_pipeline

@decorator_logger
@task
def load_data_task():
    """call the load pipeline from your load module"""   
    USERNAME = get_env_var('POSTGRES_ADMIN_USERNAME', 'username')
    PASSWORD = get_env_var('POSTGRES_ADMIN_PASSWORD', 'password')
    HOST = get_env_var('POSTGRES_HOST', 'localhost')
    PORT = get_env_var('POSTGRES_PORT', '5432')
    DATABASE = get_env_var('POSTGRES_DB_NAME', 'mydatabase')

    load_pipeline = load.DataEnedisAdemeLoader(
        engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        )
    load_pipeline.run()

@decorator_logger
@flow(name="ETL-DPE-Analysis", log_prints=False)
def dpe_enedis_ademe_etl_flow():
    data_bronze = extract_data_task()
    transform_data_task(data_bronze)
    load_data_task()

if __name__=="__main__":
    # orchestration
    dpe_enedis_ademe_etl_flow()
