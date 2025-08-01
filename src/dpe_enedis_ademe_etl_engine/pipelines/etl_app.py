from ..scripts import extract, transform, load
from ..utils.fonctions import get_env_var
from ..utils import decorator_logger

from prefect import flow, task
from sqlalchemy import create_engine

@decorator_logger
def extract_data_task(
    annee,
    code_departement,
    rows
):
    """call the extract function from extract module"""
    extract_pipeline = extract.DataEnedisAdemeExtractor(debug=False)
    extract_pipeline.extract(annee=annee, rows=rows, code_departement=code_departement)
    return extract_pipeline.output

@decorator_logger
def transform_data_task(data):
    """call the transform function from your transform module"""
    transf_pipeline = transform\
        .DataEnedisAdemeTransformer(
            data, 
            inplace=False, 
            golden_data_config_fpath=get_env_var('SCHEMA_GOLDEN_DATA_FILEPATH', compulsory=True))
    transf_pipeline.run(
        types_schema_fpath="", # schema de la data silver en input (pour le cast), si vide ("") est inféré depuis les env variables
        keep_only_required=False
    )
    return transf_pipeline

@decorator_logger
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
@flow(name="ETL Enedis Ademe", log_prints=False)
def dpe_enedis_ademe_etl_flow(annee, code_departement, batch_size):
    data_silver = extract_data_task(annee=annee, code_departement=code_departement, rows=batch_size)
    transform_data_task(data_silver)
    load_data_task()

if __name__=="__main__":
    # orchestration
    dpe_enedis_ademe_etl_flow.serve(
        name="deployment-etl-enedis-ademe",
        tags=["rncp", "dpe", "enedis", "ademe"],
        parameters={"annee": 2023, "code_departement": 95, "batch_size": 90},
        cron="0 9 * * WED"
    ) 


