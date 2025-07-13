import os
import json
import requests
import pandas as pd
from io import BytesIO

# use s3fs with boto3 client later
from minio import Minio 
from pyarrow import Table, parquet as pq

from ..scripts import Paths
from ..utils import logger, decorator_logger
from ..utils.fonctions import (
    get_env_var,
    get_today_date, 
)


class FileStorageConnexion(Paths):
    """
    Class to manage S3 connections and operations or Local filestorage.
    Inherits from Paths to access environment-specific paths.
    Uses Minio client for S3 operations if env is not local.
    """
    def __init__(self):
        super().__init__()
        self.__set_client()
        self.get_today_date = get_today_date

    def __set_client(self):
        try:
            if self.env == "LOCAL":
                self.client = None
            else:
                # client minio is just used to init the bucket
                self.client = Minio(
                    get_env_var('S3_ENDPOINT_URL', compulsory=True),
                    access_key=get_env_var('S3_ACCESS_KEY', compulsory=True),
                    secret_key=get_env_var('S3_SECRET_KEY', compulsory=True),
                    region=get_env_var('S3_REGION', compulsory=True),
                    secure=False # set to True if using https
                )
                self.BUCKET_NAME = get_env_var('S3_BUCKET_NAME', compulsory=True)
                if not self.client.bucket_exists(self.BUCKET_NAME):
                    self.client.make_bucket(self.BUCKET_NAME)
        except Exception as e:
            raise

    @decorator_logger
    def purge_archive_dir(self):
        """
        Purge the archive directory.
        Depending on the environment, it will either 
        remove the local directory or delete objects 
        from the S3 bucket.
        """
        def purge_local_archive_dir():
            os.rmdir(self.PATH_ARCHIVE_DIR)
            os.makedirs(self.PATH_ARCHIVE_DIR, exist_ok=False)
        
        def purge_s3_archive_dir():
            self.client.remove_objects(self.BUCKET_NAME, prefix=self.PATH_ARCHIVE_DIR)    
 
        if self.env == "LOCAL":
            purge_local_archive_dir()
        else:
            purge_s3_archive_dir()

    @decorator_logger
    def save_parquet_file(self, df, dir, fname):
        """
        Save a DataFrame to a parquet file.
        Depending on the environment, it will either
        save the file locally or upload it to an S3 bucket.
        :param df: DataFrame to save.
        :param dir: Directory where the file will be saved.
        :param fname: Name of the file to save.
        :return: None
        :raises Exception: If there is an error during the save operation.
        """
        
        def save_parquet_file_to_local():
            if not os.path.exists(dir):
                os.makedirs(dir)
            df.to_parquet(f"{os.path.join(dir, fname)}", compression="gzip")

        def save_parquet_file_to_s3():
            # push data parquet to s3
            # path_to_s3_object = f"s3://{bucket_name}/{dir}{fname}"
            # pq.write_to_dataset(
            #     Table.from_pandas(df),
            #     path_to_s3_object,
            #     filesystem=self.s3fs,
            #     use_dictionary=True,
            #     compression="snappy",
            #     version="2.6",
            # )

            # JSON instead of parquet
            json_data = df.to_json(orient="records", lines=True)
            json_bytes = BytesIO(json_data.encode("utf-8"))
            self.client.put_object(
                self.BUCKET_NAME,
                f"{dir}{fname.replace('.parquet', '.json')}",
                data=json_bytes,
                length=len(json_data),
                content_type="application/json"
            )
            logger.info(f"Uploaded {fname} to bucket {self.BUCKET_NAME}.")

        if self.env=="LOCAL":
            save_parquet_file_to_local()
        else:
            save_parquet_file_to_s3()

    @decorator_logger
    def load_parquet_file(self, dir, fname):
        """
        Load a parquet file into a DataFrame.
        """
        def load_parquet_file_from_local():
            return pd.read_parquet(os.path.join(dir, fname))
        
        def load_parquet_file_from_s3():
            # path_to_s3_object = f"s3://{bucket_name}/{dir}{fname}"
            # with self.s3fs.open(path_to_s3_object, 'rb') as f:
            #     return pd.read_parquet(f)
            json_object = self.client.get_object(
                self.BUCKET_NAME, 
                f"{dir}{fname.replace('.parquet', '.json')}"
            )
            json_data = json_object.read().decode("utf-8")
            return pd.read_json(
                BytesIO(json_data.encode("utf-8")), 
                orient="records", 
                lines=True
            )
         
        if self.env=="LOCAL":
            return load_parquet_file_from_local()
        else:
            return load_parquet_file_from_s3()

    
    def _save_df_schema(self, df, fpath):
        """Save the schema of a DataFrame to a JSON file."""        
        try:
            schema = df.dtypes.apply(lambda x: x.name).to_dict()
            with open(fpath, "w") as f:
                json.dump(schema, f, separators=(',', ': '), indent=4)
        except Exception as e:
            logger.error(f"Erreur sauvegarde schema data parquet file {fpath}: {e}")
            raise

    def _load_df_schema(self, fpath):
        """Load the schema of a DataFrame from a JSON file."""
        try:
            with open(fpath, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur chargement schema data parquet file {fpath}: {e}")
            raise
