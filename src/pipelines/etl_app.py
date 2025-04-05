import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.scripts import extract, transform, load
from utils.fonctions import get_today_date

from prefect import flow, task
from concurrent.futures import ThreadPoolExecutor

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
    pipeline = transform.TransformDataEnedisAdeme(data, inplace=False)
    pipeline.run()
    return pipeline

@task
def load_data(transformed_data, target_table):
    # call the load function from your load module in multithreading
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(lambda args: load.push_to_api(*args), [
            (transformed_data['table1'], 'table1', API_CONFIG),
            (transformed_data['table2'], 'table2', API_CONFIG),
            (transformed_data['table3'], 'table3', API_CONFIG)
        ])

@flow(name="ETL-DPE-Analysis", log_prints=False)
def etl_flow():
    data_bronze = extract_data()
    data_silver = transform_data(data_bronze)
    print(data_silver.df_adresses.shape)
    print(data_silver.df_logements.shape)
    print(data_silver.df_consommations.shape)
    # load_data(obj_transformed_data, "table1")

if __name__=="__main__":
    # orchestration
    etl_flow()
