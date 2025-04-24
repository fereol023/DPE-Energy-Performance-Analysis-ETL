import requests, os, json
import pandas as pd
import s3fs # built on top of boto3
from minio import Minio
from pyarrow import Table, parquet as pq
from io import BytesIO
import polars as pl

from utils.mylogging import logger, log_decorator
from utils.fonctions import (
    get_today_date, 
    get_yesterday_date, 
    set_config_as_env_var as set_config
)


class App:
    def __init__(self):
        set_config()
        self.env = os.getenv('ENV', 'LOCAL')
        self.PATHS = eval(os.getenv('local-paths')) if self.env == 'LOCAL' else eval(os.getenv('s3-paths'))            


class S3Connexion(App):
    def __init__(self):
        super().__init__()
        self.__set_client()

    def __set_client(self):
        try:
            if self.env == "LOCAL":
                self.client = None
            else:
                s3_config = eval(os.getenv('s3-secrets'))

                # client minio juste utilsé pour créer le bucket
                self.client = Minio(
                    s3_config.get('s3-endpoint'),
                    access_key=s3_config.get('s3-accessKey'),
                    secret_key=s3_config.get('s3-secretKey'),
                    secure=False
                )

                bucket_name = self.PATHS.get('s3-bucket-name')
                if not self.client.bucket_exists(bucket_name):
                    self.client.make_bucket(bucket_name)

                # self.s3fs = s3fs.S3FileSystem(
                #     anon=False,
                #     use_ssl=False,
                #     client_kwargs={
                #         "region_name": "us-east-1",
                #         "endpoint_url": s3_config.get('s3-endpoint'),
                #         "aws_access_key_id": s3_config.get('s3-accessKey'),
                #         "aws_secret_access_key": s3_config.get('s3-secretKey'),
                #         "verify": False,
                #     }
                # )   
                # # create bucket if it does not exist
                # bucket_name = self.PATHS.get('s3-bucket-name')
                # if not self.s3fs.exists(bucket_name):
                #     self.s3fs.mkdir(bucket_name)
        except Exception as e:
            raise

    @log_decorator
    def purge_archive_dir(self):
        """Purge the archive directory."""

        archive_dir = self.PATHS.get('path-data-archive')

        def purge_local_archive_dir():
            os.rmdir(archive_dir)
            os.makedirs(archive_dir, exist_ok=False)
        
        def purge_s3_archive_dir():
            bucket_name = self.PATHS.get('s3-bucket-name')
            # minio
            self.client.remove_objects(bucket_name, prefix=archive_dir)    
            
            # # s3fs
            # files = self.s3fs.ls(f"{bucket_name}/{archive_dir}")
            # for file in files:
            #     self.s3fs.rm(file)
        try:
            if self.env == "LOCAL":
                purge_local_archive_dir()
            else:
                purge_s3_archive_dir()
        except Exception as e:
            logger.warning(f"Erreur purge archive dir : {self.PATHS.get('path-data-archive')} pour la raison {e}")


    @log_decorator
    def save_parquet_file(self, df, dir, fname):
        """Save a DataFrame to a parquet file."""
        
        def save_parquet_file_to_local():
            if not os.path.exists(dir):
                os.makedirs(dir)
            df.to_parquet(f"{dir}/{fname}", compression="gzip")

        def save_parquet_file_to_s3():
            # push data parquet to s3
            bucket_name = self.PATHS.get('s3-bucket-name')
            # path_to_s3_object = f"s3://{bucket_name}/{dir}{fname}"
            # # print(f"Saving {fname} to {path_to_s3_object}")
            # pq.write_to_dataset(
            #     Table.from_pandas(df),
            #     path_to_s3_object,
            #     filesystem=self.s3fs,
            #     use_dictionary=True,
            #     compression="snappy",
            #     version="2.6",
            # )

            # JSON
            json_data = df.to_json(orient="records", lines=True)
            json_bytes = BytesIO(json_data.encode("utf-8"))
            self.client.put_object(
                bucket_name,
                f"{dir}{fname.replace('.parquet', '.json')}",
                data=json_bytes,
                length=len(json_data),
                content_type="application/json"
            )
            logger.info(f"Uploaded {fname} to bucket {bucket_name}.")

        try:
            if self.env=="LOCAL":
                save_parquet_file_to_local()
            else:
                save_parquet_file_to_s3()
        except Exception as e:
            logger.error(f"Erreur sauvegarde data parquet file {fname} to {dir}: {e}")


    @log_decorator
    def load_parquet_file(self, dir, fname):
        """Load a parquet file into a DataFrame."""

        def load_parquet_file_from_local():
            return pd.read_parquet(f"{dir}/{fname}")
        
        def load_parquet_file_from_s3():
            # load data parquet from s3
            bucket_name = self.PATHS.get('s3-bucket-name')

            # path_to_s3_object = f"s3://{bucket_name}/{dir}{fname}"
            # with self.s3fs.open(path_to_s3_object, 'rb') as f:
            #     return pd.read_parquet(f)

            # JSON
            json_object = self.client.get_object(bucket_name, f"{dir}{fname.replace('.parquet', '.json')}")
            json_data = json_object.read().decode("utf-8")
            return pd.read_json(BytesIO(json_data.encode("utf-8")), orient="records", lines=True)
         
        try:
            if self.env=="LOCAL":
                return load_parquet_file_from_local()
            else:
                return load_parquet_file_from_s3()
        except Exception as e:
            logger.error(f"Erreur chargement data parquet file {fname} from {dir}: {e}")
            raise

    
    def _save_df_schema(self, df, fname):
        """Save the schema of a DataFrame to a JSON file."""        
        try:
            schema = df.dtypes.apply(lambda x: x.name).to_dict()
            with open(f"ressources/schemas/{fname}", "w") as f:
                json.dump(schema, f, separators=(',', ': '), indent=4)
        except Exception as e:
            logger.error(f"Erreur sauvegarde schema data parquet file {fname} to {dir}: {e}")
            raise

    def _load_df_schema(self, fname):
        """Load the schema of a DataFrame from a JSON file."""
        try:
            with open(f"ressources/schemas/{fname}", "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur chargement schema data parquet file {fname} from {dir}: {e}")
            raise

def make_logs_backup(bucket_name=eval(os.getenv('s3-paths')).get('s3-bucket-name')):

    # en attendant de logger directement sur le bucket/voire le serveur ELK
    # on peut migrer les logs en batch à la fin de chaque jour
    # et suppr les logs du jour d'avant

    minio_client = S3Connexion().client

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # les fichiers de log aujd
    log_dir = eval(os.getenv('local-paths')).get('path-logs-dir')
    logs_to_upload = [f for f in os.listdir(log_dir) if get_today_date() in f]
    if logs_to_upload:
        for log_file in logs_to_upload:
            log_path = os.path.join(log_dir, log_file)
            try:
                # upload log file to MinIO bucket
                s3_logs_dir = eval(os.getenv("s3-paths")).get("s3-logs-dir")
                minio_client.fput_object(bucket_name, f"{s3_logs_dir}/{log_file}", log_path)
                logger.info(f"Uploaded {log_file} to bucket {bucket_name}.")
            except Exception as e:
                logger.error(f"Failed to upload {log_file} to bucket {bucket_name}: {e}")

    # suppr les logs de la veille 
    yesterday = get_yesterday_date()
    logs_to_delete = [f for f in os.listdir(log_dir) if yesterday in f]
    if logs_to_delete:
        for log_file in logs_to_delete:
            log_path = os.path.join(log_dir, log_file)
            try:
                os.remove(log_path)
                logger.info(f"Deleted old log file: {log_file}.")
            except OSError as e:
                logger.error(f"Failed to delete log file {log_file}: {e}")