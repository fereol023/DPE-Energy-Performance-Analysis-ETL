import os
import uuid
AUTOM_BATCH_CORRELATION_ID = str(uuid.uuid4()).replace('-', '')
if not os.getenv("BATCH_CORRELATION_ID"):
    # Set the environment variable for batch correlation ID
    # This is used to track the batch processing in logs and other systems
    os.environ["BATCH_CORRELATION_ID"] = AUTOM_BATCH_CORRELATION_ID

from .etl_app import dpe_enedis_ademe_etl_flow as DataEnedisAdemeETL
from ..scripts.extract import DataEnedisAdemeExtractor
from ..scripts.transform import DataEnedisAdemeTransformer
from ..scripts.load import DataEnedisAdemeLoader

# DataEnedisAdemeETL = etl_flow

__all__ = [
    "DataEnedisAdemeETL",
    "DataEnedisAdemeExtractor",
    "DataEnedisAdemeTransformer",
    "DataEnedisAdemeLoader"
    ]