import os, sys, requests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.mylogging import log_decorator, logger


@log_decorator
def push_to_api(self, df=None, table_name="", config_api_server={}):
    """
    Envoie le DataFrame à l'endpoint de l'API.
    :param df: Le DataFrame pandas à envoyer.
    """
    api_endpoint = f"{config_api_server['API_SERVER']}/{table_name}/insert"
    data = df.to_dict(orient='records')
    response = requests.post(api_endpoint, json=data) # must credentials + timeout
    response.raise_for_status()
    logger.info(f"Données envoyées avec succès à {api_endpoint}. Code de statut : {response.status_code}")
