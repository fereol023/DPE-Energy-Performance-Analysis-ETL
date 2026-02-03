import os
import uuid
import pathlib
from ..utils.fonctions import set_config_as_env_var as set_config

set_config(
    dirpath=pathlib.Path(__file__).parent.parent / "config",
    filename="paths.yml",
    debug=True,
    bypass_env=True,
    override_existing=False, # this avoid to override paths if they are already set for exemple (spe. useful in tests)
)

AUTOM_BATCH_CORRELATION_ID = str(uuid.uuid4()).replace('-', '')
if not os.getenv("BATCH_CORRELATION_ID"):
    os.environ["BATCH_CORRELATION_ID"] = AUTOM_BATCH_CORRELATION_ID # this is required to track the batch processing in logs and other systems

from .etl_app import dpe_enedis_ademe_etl_flow as DataEnedisAdemeETL
from ..scripts.extract import DataEnedisAdemeExtractor
from ..scripts.transform import DataEnedisAdemeTransformer
from ..scripts.load import DataEnedisAdemeLoader

__all__ = [
    "DataEnedisAdemeETL",
    "DataEnedisAdemeExtractor",
    "DataEnedisAdemeTransformer",
    "DataEnedisAdemeLoader"
    ]