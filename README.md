# DPE-Energy-Performance-Analysis-ETL
ETL module repos for DPE-Energy-Performance-Analysis-ETL package.


[![CI - Test](https://img.shields.io/badge/pytests-passed-green)](https://github.com/fereol023/DPE-Energy-Performance-Analysis-ETL/blob/main/.github/workflows/github-volt-engine-ci.yml) [![PyPI Latest Release](https://img.shields.io/badge/dpe_enedis_ademe_etl_engine-v2.2.2-blue)](https://test.pypi.org/project/volt-etl-engine/)

### 📃 Description

This ETL module is responsible for extracting, transforming, and loading energy performance data for the DPE-Energy-Performance-Analysis project. It supports both local and containerized (Docker) execution modes, enabling flexible data processing workflows. The ETL pipeline handles data ingestion from CSV files or API sources, applies schema-based validation and transformation, and loads the processed data into a PostgreSQL database or remote storage (S3). For the input, you are free to provide a csv input file (which must meet the input schema) (see [here](input_schema.placeholder)) or not. If not provided, the ETL will fetch directly from Enedis API source (see [API sources](#API-sources)). **Providing a csv input file is recommended for batch extraction** exemple when targetting specifc locations. Logging and profiling are integrated for monitoring and debugging purposes.

- The flow is made of 3 steps : Extraction - Transformation - Loading.

- Depending on whether you have input file (a csv file which specifies locations on which you want to focus your extraction) or not there are two ways to use the extract module.  

- **Transformation step** will require that you define dataframe schemas. There are some function used in this project (based on our red-line project). But you can also go through the code and implement your own transformation functions.

- **Loading step** send the output golden data to the RDMS (postgres connection is implemented but one more time you are free to customize the code to fit your needs).


### ⚙️ How to set up 

#### Set up for - local running
  To run the ETL pipeline locally, follow these steps :
1. Get the codebase on your local machine with the package **pip install from pypi index** or **clone this repository**. In the second case, you can pip install -e or use the code as is.
  ````bash
  pip install dpe_enedis_ademe_etl_engine
  ````
2. **Set up environment variables** as described in the [Environment variables](#environment-variables) section. In the environment variables, you will have to define the paths.
3. **Install dependencies**:
  If you choosed to clone the repos, do not forgot to install dependencies. If not, just skip this step.
  ```
  pip install -r requirements.txt
  ```
#### Set up for - remote running
  Remote running refers to when one runs the pipeline in non-local environment (not on your computer). Then, the ETL requires a filesytem to save files. That's why a S3-like storage is required. The switch is transparent to the final users. They just have to specify paths in the bucket storage and the bucket name (see [Environment variables](#environment-variables)). Plus, you may need to load results somewhere for persistance (RDMS). Postgres is used here but you are free to implement any other solution.  

1. Start by installing the package on your remote machine :
  ````bash
  pip install dpe_enedis_ademe_etl_engine
  ````
2. Define the environment variables.

### ✅ How to use

#### ➡️ Extraction 
1. If you are not extracting (in batch) from input file, skip this step ; else follow the steps to **prepare your input data**. Input datafile must be a csv-file. See the [here](input_schema.placeholder). You should put it in the locations specified in environment variables for *PATH_FILE_INPUT_ENEDIS_CSV*. Please respect this if not your extraction will raise exceptions.
2. Then import the extraction module. The following code snippet is an exemple of how to, but you have some parameters not defined here such as `input_csv_path` or `save_schema` and `n_threads_for_querying` available in the full documentation of the method `DataEnedisAdemeExtractor.extract()`.
  ```python
  from dpe_enedis_ademe_etl_engine.pipelines import DataEnedisAdemeExtractor

  pipeline = DataEnedisAdemeExtractor()
  pipeline.extract(
    from_input=False, # set as True if input file is provided
    code_departement=95, 
    annee=2023, 
    rows=3
  )
  ```

3. (Not implemented yet) running flow from CLI :
  ```bash
  python src/pipelines/etl_app.py  < -year=2022 > < -nrows=100 >
  ```

4. **Outputs files** will be saved in the paths directory defined as environment variables (bronze, silver and golden zones). **Logs/profiling stats** will be available in given locations as well. You may understand why these env variables are compulsory 🧏.

#### ➡️ Transformation
The transformation step is responsible for cleaning, validating, and enriching the extracted data. You must define the input and output schemas (see the `SCHEMA_ETL_INPUT_FILEPATH` and `SCHEMA_ETL_OUTPUT_FILEPATH` environment variables). The transformation logic can be customized by editing or extending the transformation functions in the codebase.

Example usage:
```python
from dpe_enedis_ademe_etl_engine.pipelines import DataEnedisAdemeTransformer

transformer = DataEnedisAdemeTransformer()
transformer.transform(
  input_path="etl/data/1_bronze/conso_enedis.csv",
  output_path="etl/data/2_silver/transformed_data.csv",
  input_schema_path="etl_engine/ressources/schemas/schema_input.json",
  output_schema_path="etl_engine/ressources/schemas/schema_output.json"
)
```
The transformed data will be saved to the path specified in your environment variables. You can implement your own transformation logic by modifying the transformation functions in the pipeline.

#### ➡️ Loading
The loading step is responsible for persisting the transformed data into the target storage, typically a PostgreSQL database or an S3 bucket. You can use the provided loader class to handle this process.

Example usage:
```python
from dpe_enedis_ademe_etl_engine.pipelines import DataEnedisAdemeLoader
from sqlalchemy import create_engine

engine = create_engine(f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
# these informations are defined in env vars
loader = DataEnedisAdemeLoader(engine)
loader.run()
```
This will load the data from the specified files from **the gold data zone** into the configured PostgreSQL table. Make sure your environment variables for the database connection are set correctly. You can customize the loading logic or implement additional loaders for other storage backends as needed.

### Environment variables

This package requires some environment variables to be set. These are :

````python
config = {
  "ENV": "LOCAL", # or NOLOCAL
  "APP_NAME": "DPE-API",
  "APP_DESCRIPTION": "ETL for DPE Energy Performance Analysis",
  "LOGGER_APP_NAME": "ETL-Logger",
  # compulsory if you aim to run the ETL flow until the load step
  "POSTGRES_HOST": "******",
  "POSTGRES_PORT": "5432",
  "POSTGRES_DB_NAME": "******",
  "POSTGRES_ADMIN_USERNAME": "postgres",
  "POSTGRES_ADMIN_PASSWORD": "******",
  "POSTGRES_READER_USERNAME": "reader",
  "POSTGRES_READER_PASSWORD": "******",
  "POSTGRES_WRITER_USERNAME": "writer",
  "POSTGRES_WRITER_PASSWORD": "******",
  # compulsory if you aim to run the pipeline with remote storage for files
  "S3_ACCESS_KEY": "<YOUR_S3_ACCESS_KEY>",
  "S3_SECRET_KEY": "<YOUR_S3_SECRET_KEY>",
  "S3_BUCKET_NAME": "dpe-storage-v1",
  "S3_REGION": "eu-west",
  "S3_ENDPOINT_URL": "<HOST>:<PORT>",
  # compulsory
  "PATH_LOG_DIR" : "etl/logs/",
  "PATH_ARCHIVE_DIR" : "etl/data/archive/",
  "PATH_DATA_BRONZE" : "etl/data/1_bronze/",
  "PATH_DATA_SILVER" : "etl/data/2_silver/",
  "PATH_DATA_GOLD" : "etl/data/3_gold/",
  "PATH_FILE_INPUT_ENEDIS_CSV": "etl/data/1_bronze/conso_enedis.csv",
  # compulsory if you aim to run the transformation pipeline
  "SCHEMA_ETL_INPUT_FILEPATH": "etl/ressources/schemas/schema_input.json",
  "SCHEMA_ETL_OUTPUT_FILEPATH": "etl/ressources/schemas/schema_output.json",
  "SCHEMA_GOLDEN_DATA_FILEPATH": "etl/ressources/schemas/schema_golden_data.json",
  # orchestration tool, compulsory
  "PREFECT_API_URL": "http://host:port/api",
}
````

### API-sources
1. [Enedis](https://data.enedis.fr/explore/dataset/consommation-annuelle-residentielle-par-adresse/analyze/?refine.annee=2023&dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7ImFsaWduTW9udGgiOnRydWUsInR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQ09VTlQiLCJ5QXhpcyI6Im5vbWJyZV9kZV9sb2dlbWVudHMiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiIjMTQyM0RDIn1dLCJ4QXhpcyI6Im5vbV9jb21tdW5lIiwibWF4cG9pbnRzIjoyMCwidGltZXNjYWxlIjoiIiwic29ydCI6InNlcmllMS0xIiwiY29uZmlnIjp7ImRhdGFzZXQiOiJjb25zb21tYXRpb24tYW5udWVsbGUtcmVzaWRlbnRpZWxsZS1wYXItYWRyZXNzZSIsIm9wdGlvbnMiOnsicmVmaW5lLmFubmVlIjoiMjAyMyJ9fSwic2VyaWVzQnJlYWtkb3duVGltZXNjYWxlIjpudWxsfV0sImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWUsInRpbWVzY2FsZSI6IiJ9)
2. [BAN](https://adresse.data.gouv.fr/outils/api-doc/adresse)
3. [ADEME](https://data.ademe.fr/datasets/dpe03existant)

### Schemas

### Authors 
- Fereol Gbenou - *feel free to reach me here for any contribution*
<p align="center">
  <a href="https://www.linkedin.com/in/fereol-gbenou/" target="_blank">
    <img align="center" alt="LinkedIn" height="20" src="https://raw.githubusercontent.com/rahuldkjain/github-profile-readme-generator/master/src/images/icons/Social/linked-in-alt.svg" width="20"/>
  </a>
</p>