import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.scripts import extract, transform, load
from utils.fonctions import get_today_date

from prefect import flow, task
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine

@task
def extract_data():
    # Call the extract function from your extract module
    extract_pipeline = extract.DataEnedisAdemeExtractor()
    extract_pipeline.extract(annee=2022, rows=100)
    data_bronze = extract_pipeline.output
    return data_bronze

@task
def transform_data(data):
    # call the transform function from your transform module
    pipeline = transform\
        .TransformDataEnedisAdeme(
            data, 
            inplace=False, 
            cols_config_fpath=os.getenv('Transform_Step_Cols_Config_Path'))
    pipeline.run()
    return pipeline

@task
def load_data():
    # call the load pipeline from your load module    
    USERNAME = os.getenv('POSTGRES_ADMIN_USERNAME', 'username')
    PASSWORD = os.getenv('POSTGRES_ADMIN_PASSWORD', 'password')
    HOST = os.getenv('POSTGRES_HOST', 'localhost')
    PORT = os.getenv('POSTGRES_PORT', '5432')
    DATABASE = os.getenv('POSTGRES_DB_NAME', 'mydatabase')
    load_pipeline = load.LoadToDB(
        engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        )
    load_pipeline.run()

@flow(name="ETL-DPE-Analysis", log_prints=False)
def etl_flow():
    data_bronze = extract_data()
    data_silver = transform_data(data_bronze)
    print(data_silver.df_adresses.shape)
    print(data_silver.df_logements.shape)
    load_data()

if __name__=="__main__":
    # orchestration
    etl_flow()
