# DPE-Energy-Performance-Analysis-ETL
ETL module repos for DPE-Energy-Performance-Analysis-ETL

## 2 modes 

1 - local mode : clone this repo to use the datapipeline locally

Add these env variables in config/paths.yaml :
```yaml
local-paths:
  path-logs-dir : "ressources/logs/"
  path-data-archive : "ressources/data/0_archive/"
  path-data-bronze : "ressources/data/1_bronze/"
  path-data-silver : "ressources/data/2_silver/"
  path-data-gold : "ressources/data/3_gold/"
  batch-input-enedis-csv-fpath : "ressources/0_archive/conso_enedis_2022.csv"
```

And add these in config/secrets.json :
```json
{
    "ENV": "LOCAL",
    "app-name": "DPE-Energy-Performance-Analysis-ETL",
    "logs-app-name": "ETL-Logger"
}
```

Run 
```
pip install -r requirements.txt
```

- batch mode

Put the batch input csv file into the filepath specified in **local-paths** env var  
*(see required columns in ressources/schema_tables.json)*

- streaming mode (api call directly) : nothing to do 

Then run for both cases (options are for streaming mode api fetching):
```
python src/pipelines/etl_app.py  < -year=2022 > < -nrows=100 > 
```

Outputs are in folder `ressources/data/` and logs and profiling stats are in `ressources/logs/`


2 - nolocal mode (docker image) : works with microservices (S3 as filesystem and RDMS)

You will need some S3 credentials values as env var. Use :

````
docker run DPE-Energy-Performance-Analysis-ETL:<tag> -e <env_var> = <value>
```
