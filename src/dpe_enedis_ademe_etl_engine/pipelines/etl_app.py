api_config = None
try:
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    api_config = os.path.join(Path(__file__).resolve().parent.parent.parent.parent.parent, "config", ".env")
    print(f"Loading environment variables from {api_config} ...")
    load_dotenv(api_config)
except:
    print(f"No .env file found, using default environment variables into {api_config}.")
    pass

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
        name="deployment-etl-enedis-ademe-v03-local",
        tags=["rncp", "dpe", "enedis", "ademe"],
        cron="0 * * * *",  # every monday at 16:00
        parameters={"annee": 2023, "code_departement": 95, "batch_size": 2},
        pause_on_shutdown=True,
    )