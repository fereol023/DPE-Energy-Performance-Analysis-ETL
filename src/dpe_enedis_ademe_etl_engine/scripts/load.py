import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pandas as pd

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import Secret
from prefect.artifacts import create_markdown_artifact
from prefect.server.schemas.schedules import CronSchedule
from prefect.cache_policies import NO_CACHE

from ..utils import decorator_logger, logger
from ..scripts.filestorage_helper import FileStorageConnexion
from ..utils.fonctions import get_env_var


class DataEnedisAdemeLoader(FileStorageConnexion):
    """
    Classe pour charger les données dans la base de données.
    Hérite de la classe FileStorageConnexion pour la connexion S3.
    """

    def __init__(self, engine=None, db_connection=None):
        """
        Initialise la classe DataEnedisAdemeLoader.
        :param db_connection: Connexion à la base de données envoyé au job depuis le serveur API (by design).
        autre solution : faire une connexion à la base de données ici ou une classe dediée.
        anyway : la db connection doit avoir les droits d'écriture sur la base de données / ou admin.
        """
        # la connexion S3 va lire depuis les variables d'environnement
        super().__init__()

        self.engine = engine
        self.db_connection = db_connection
        self.bdd_pk_mapping = {
            "adresses": ["id_ban"],
            "logements": ["_id_ademe"],
        }
        self.df_adresses = self.load_parquet_file(
            dir=get_env_var('PATH_DATA_GOLD', compulsory=True),
            fname=f"adresses_{self.get_today_date()}.parquet"
        )
        self.df_logements = self.load_parquet_file(
            dir=get_env_var('PATH_DATA_GOLD', compulsory=True),
            fname=f"logements_{self.get_today_date()}.parquet"
        )
        if self.df_adresses.empty or self.df_logements.empty:
            raise ValueError("Les DataFrames chargés sont vides. Vérifiez les fichiers dans la gold zone.")


    @decorator_logger
    @task(name="load-save-tables-to-db", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def save_one_table(self, df, table_name=""):
        """
        Envoie un DataFrame à une table spécifique dans la base de données.
        :param df: Le DataFrame pandas à envoyer.
        :param table_name: Le nom de la table dans laquelle envoyer les données.
        :raises ValueError: Si la connexion à la base de données ou le DataFrame est vide.
        
        Pour le connecteur, si on utilise pandas il y a un connecteur sqlalchemy pour la bdd.
        sauf que la bdd doit être compatible avec sqlalchemy. ce qui n'est pas le cas de postgres.
        pd.dataframe.to_sql() ne fonctionne pas avec postgres.
        on utilise un engine sqlalchemy pour se connecter à la bdd.
        """
        logger = get_run_logger()
        # ------- Vérification des paramètres
        if (self.db_connection is None) and (self.engine is None):
            raise ValueError("La connexion à la base de données est requise/engine est requis.")
        # if not isinstance(self.db_connection, type):
        #    raise TypeError("La connexion à la base de données doit être une instance de la classe de connexion appropriée.")
        if df is None or df.empty:
            raise ValueError("Le DataFrame à envoyer est requis et ne doit pas être vide.")
        if not table_name:
            raise ValueError("Le nom de la table est requis.")
        
        # ------- Préparation des données
        # idempotence : on ne veut pas insérer des doublons dans la table
        # lire les la table depuis la bdd pour vérifier si la ligne existe déjà
        # si la ligne existe dejà dans la table, on ne l'insère pas
        # Lire les données existantes de la table pour éviter les doublons
        try:
            existing_df = pd.read_sql_table(table_name, con=self.engine)
        except Exception as e:
            logger.warning(f"Impossible de lire la table {table_name} pour vérifier les doublons : {e}")
            existing_df = pd.DataFrame()

        if not existing_df.empty:
            
            pk_cols = self.bdd_pk_mapping.get(table_name, None)
            if not pk_cols: raise ValueError(f"Aucune clé primaire définie pour la table {table_name}.")
            
            key_cols = [col for col in pk_cols if col in df.columns and col in existing_df.columns]
            if key_cols:
                # récuperer les clés déjà existantes dans la table
                exiting_keys = existing_df[key_cols[0]].unique()
                # supprimer les lignes du DataFrame qui existent déjà dans la table
                logger.info(f"Suppression des doublons dans le DataFrame pour la table {table_name} en utilisant la colonne clé {key_cols[0]}.")
                df = df[~df[key_cols[0]].isin(exiting_keys)]
                # df = df[~df[key_cols[0]].isin(existing_df[key_cols[0]])]
            else:
                logger.warning(f"Aucune colonne clé primaire trouvée pour la déduplication dans la table {table_name}.")
        if df.empty:
            logger.info(f"Aucune nouvelle donnée à insérer dans la table {table_name}.")
            return

        # ------- Envoi des données
        try:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            logger.info(f"Données envoyées avec succès à la table {table_name}.")
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi des données à la table {table_name}: {e}")
            raise

    @decorator_logger
    @flow(name="ETL data loading pipeline", 
      description="Pipeline de chargement orchestré avec Prefect")
    def run(self):
        """
        Envoie les données dans la bdd
        Ordre upload, car les tables sont liées entre elles par des clés étrangères.
        """
        logger = get_run_logger()
        ## Ordre 
        self.save_one_table(
            df=self.df_logements.drop_duplicates(subset=self.bdd_pk_mapping.get("logements", []), keep='first'), 
            table_name="logements"
        )
        self.save_one_table(
            df=self.df_adresses.drop_duplicates(subset=self.bdd_pk_mapping.get("adresses", []), keep='first'), 
            table_name="adresses"
        )
        logger.info("Toutes les tables ont été envoyées avec succès à la base de données.")
