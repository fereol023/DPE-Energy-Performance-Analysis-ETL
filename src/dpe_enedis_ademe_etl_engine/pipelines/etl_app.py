dict_config = {
    "ENV": "NOLOCAL",
    "APP_NAME": "DPE-API",
    "APP_VERSION": "0.0.1",
    "APP_DESCRIPTION": "API backend for DPE Energy Performance Analysis",
    "LOGGER_APP_NAME": "ETL-Logger",

    "API_HOST": "localhost",
    "API_PORT": "8000",
    "API_SECRET_KEY": "your-secret-key",

    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB_NAME": "dpedb_v2",
    "POSTGRES_ADMIN_USERNAME": "postgres",
    "POSTGRES_ADMIN_PASSWORD": "password",
    "POSTGRES_READER_USERNAME": "reader",
    "POSTGRES_READER_PASSWORD": "reader_password",
    "POSTGRES_WRITER_USERNAME": "writer",
    "POSTGRES_WRITER_PASSWORD": "writer_password",

    "S3_ACCESS_KEY": "eGKqqYvxAC7Np4CCFl6s",
    "S3_SECRET_KEY": "FAzihfatYwjWGlPIpr9GaHMJv0b6JGfASmpliyMR",
    "S3_BUCKET_NAME": "dpe-storage-v1",
    "S3_REGION": "eu-west",
    "S3_ENDPOINT_URL": "localhost:9000",

    "PATH_LOG_DIR" : "etl/logs/",
    "PATH_ARCHIVE_DIR" : "etl/data/archive/",
    "PATH_DATA_BRONZE" : "etl/data/1_bronze/",
    "PATH_DATA_SILVER" : "etl/data/2_silver/",
    "PATH_DATA_GOLD" : "etl/data/3_gold/",
    "PATH_FILE_INPUT_ENEDIS_CSV": "etl/data/1_bronze/consommation-annuelle-residentielle-par-adresse-2024.csv", 
    
    "SCHEMA_ETL_INPUT_FILEPATH": "etl/ressources/schemas/schema_input_data.json",
    "SCHEMA_SILVER_DATA_FILEPATH": "etl/ressources/schemas/schema_silver_data.json",
    "SCHEMA_GOLDEN_DATA_FILEPATH": "etl/ressources/schemas/schema_golden_data.json",

    "ELASTICSEARCH_HOST": "localhost",
    "ELASTICSEARCH_PORT": "9200",
    "ELASTICSEARCH_INDEX": "test_dpe_async_idx", #"async_logging_test_6"
    "BATCH_CORRELATION_ID": "000000000"

}

import os
for key, value in dict_config.items():
    os.environ[key] = value


try:
    from ..scripts import extract, transform, load
    from ..utils.fonctions import get_env_var
    from ..utils import decorator_logger
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    sys.path.append(str(parent_dir))
    from scripts import extract, transform, load
    from utils.fonctions import get_env_var
    from utils import decorator_logger

from prefect import flow, task
from sqlalchemy import create_engine

@decorator_logger
def extract_data_task(
    annee,
    code_departement,
    rows,
    debug
):
    """call the extract function from extract module"""
    extract_pipeline = extract.DataEnedisAdemeExtractor(debug=debug)
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
def load_data_task(debug):
    """call the load pipeline from your load module"""   
    USERNAME = get_env_var('POSTGRES_ADMIN_USERNAME', 'username')
    PASSWORD = get_env_var('POSTGRES_ADMIN_PASSWORD', 'password')
    HOST = get_env_var('POSTGRES_HOST', 'localhost')
    PORT = get_env_var('POSTGRES_PORT', '5432')
    DATABASE = get_env_var('POSTGRES_DB_NAME', 'mydatabase')

    load_pipeline = load.DataEnedisAdemeLoader(
        engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"),
        debug=debug
        )
    load_pipeline.run()

@decorator_logger
# @flow(name="ETL Enedis Ademe", log_prints=False)
def dpe_enedis_ademe_etl_flow(annee, code_departement, batch_size, debug=False):
    data_silver = extract_data_task(annee=annee, code_departement=code_departement, rows=batch_size, debug=debug)
    transform_data_task(data_silver)
    load_data_task(debug=debug)

dpe_enedis_ademe_etl_flow = flow(
    dpe_enedis_ademe_etl_flow,
    name="ETL Enedis Ademe",
    log_prints=False
)

if __name__=="__main__":
    # orchestration
    dpe_enedis_ademe_etl_flow.serve(
        name="deployment-etl-enedis-ademe-v01",
        tags=["rncp", "dpe", "enedis", "ademe"],
        cron="0 * * * *", 
        parameters={"annee": 2023, "code_departement": 95, "batch_size": 2},
        pause_on_shutdown=True,
    )