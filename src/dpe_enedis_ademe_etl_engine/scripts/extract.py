import os
import requests
import functools 
import threading
import numpy as np 
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from ..scripts.filestorage_helper import FileStorageConnexion
from ..utils import logger, decorator_logger
from ..utils.fonctions import (
    get_env_var,
    get_today_date, 
    normalize_df_colnames
)


class DataEnedisAdemeExtractor(FileStorageConnexion):
    """
    This class is responsible for extracting data from Enedis and Ademe APIs.
    - It handles the extraction of Enedis data, BAN data, and Ademe data.
    - It also merges the data and saves the output in a parquet file.
    - It is designed to be used in a pipeline for ETL operations.
    - It can extract data either from a CSV file 
    or from the Enedis API (directly from a city name).
    __good to know__ :
    - enedis records endpoint has limitations ~ default 10 if not set  
    - combiner limit et offset pour imiter la 
    - pagination limite à 10_000 lignes 
    - while export endpoint has no limitations of rows
    - maybe more efficient ot query in batch
    """
    def __init__(self, debug=False):
        super().__init__()
        self.input = pd.DataFrame()
        self.output = pd.DataFrame()
        self.ban_data = pd.DataFrame()
        self.ademe_data = pd.DataFrame()
        self.PATH_FILE_INPUT_ENEDIS_CSV = get_env_var('PATH_FILE_INPUT_ENEDIS_CSV', compulsory=True, default_value="")
        # --- objet debugger ---
        self.debug = debug
        if self.debug: self.debugger = {} 
        # --- fonctions urls ---
        # generer une url pour requeter l'api enedis avec restriction sur l'année et le nombre de lignes
        self.get_url_enedis_year_rows = lambda annee, rows: f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?where=annee%20%3D%20date'{annee}'&limit={rows}"
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ademe_filter_on_ban = lambda key: f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=1000&format=json&qs=Identifiant__BAN%3A{key}"
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ban_filter_on_adresse = lambda key: f"https://api-adresse.data.gouv.fr/search/?q={key}&limit=1"

    def load_batch_input(self):
        """
        Load csv data from conso input file. 
        Only input format allowed is csv.
        If the environment is LOCAL, it will load from a local CSV file.
        If the environment is not LOCAL, it will load from an S3 bucket.
        The input CSV file must contain the following columns:
        - Adresse
        - Nom Commune
        - Code Commune
        - Code Département (optional, used for filtering)
        If the input CSV file is not valid or does not contain the required columns,
        it will raise an AssertionError.
        """
        def load_enedis_input_from_local_csv():
            self.input = pd.read_csv(self.PATH_FILE_INPUT_ENEDIS_CSV, sep=';')
                     
        def load_enedis_input_from_s3_csv():
            self.input = pd.DataFrame(
                self.client.get_object(self.BUCKET_NAME, self.PATH_FILE_INPUT_ENEDIS_CSV)            
                )
        
        try:
            if self.env=='LOCAL':
                load_enedis_input_from_local_csv()
            else:
                load_enedis_input_from_s3_csv()
            assert self.input.shape[0] > 0, f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV}"
            self.input = self.input.rename(
                            columns={
                                'Adresse': 'adresse', 
                                'Nom Commune': 'nom_commune', 
                                'Code Commune': 'code_commune'
                            })
            # validate schama with input required cols
            #assert all(col in self.input.columns for col in ['adresse', 'nom_commune', 'code_commune']),\
            #       f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV} - "
        except:
            logger.critical(f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV}")
            raise

    @decorator_logger
    def get_dataframe_from_url(self, url):
        """Extract pandas dataframe from any valid url."""
        logger.info(f"Fetching data from : {url}")
        res = requests.get(url).json().get('results')
        return pd.DataFrame(res)

    @functools.lru_cache(maxsize=128)
    def call_ban_api_individually(self, addr):
        """ 
        Call the BAN API individually for a given address.
        :param addr: The address to query the BAN API.
        :return: A dictionary with the BAN data for the given address.
        """
        res = requests.get(self.get_url_ban_filter_on_adresse(addr))
        if res.status_code == 200:
            j = res.json()
            if len(j.get('features')) > 0:
                first_result = j.get('features')[0]
                lon, lat = first_result.get('geometry').get('coordinates')
                first_result_all_infos = { **first_result.get('properties'), **{"lon": lon, "lat": lat}, **{'full_adress': addr}}
                first_result_all_infos = { **first_result_all_infos, **{"thread_name": threading.current_thread().name}}
                return first_result_all_infos
            else:
                return
        else:
            return
        
    @functools.lru_cache(maxsize=128)
    def call_ademe_api_individually(self, id_ban):
        """
        Call the Ademe API individually for a given id_ban.
        :param id_ban: The id_ban to query the Ademe API.
        :return: A dictionary with the Ademe data for the given id_ban.
        """
        res = requests.get(self.get_url_ademe_filter_on_ban(id_ban))
        if res.status_code == 200:
            j = res.json()
            if j.get('results'):
                return j.get('results')[0]
            else:
                logger.warning(f"No results found for id_ban: {id_ban}")
                return None
        else:
            logger.error(f"Error fetching data for id_ban: {id_ban}. Status code: {res.status_code}")
            return None
    
    def request_ban_from_adress_list(self, adress_list, n_threads):
        workers = ThreadPoolExecutor(max_workers=n_threads)
        res = workers.map(self.call_ban_api_individually, adress_list)
        workers.shutdown()
        res = list(filter(lambda x: x is not None, res))
        return list(res)

    def request_api_multithreaded(self, api_call_func, n_threads, obj_list=[]):
        """
        Request the API using multithreading.
        :param api_call_func: The function to call the API.
        :param id_ban_list: The list of id_ban to query the API.
        :param n_threads: The number of threads to use for querying the API.
        :return: A list of results from the API.
        __bechmark call api ban__
        - benchmark 100 lignes 
        - 9 secondes 1 thread
        - 1 seconde 10 threads, instantané (0.8s avec 10 threads + cache)
        """
        workers = ThreadPoolExecutor(max_workers=n_threads)
        res = workers.map(api_call_func, obj_list)
        workers.shutdown()
        return res

    # TACHE EXTRACTION 1
    @decorator_logger
    def get_enedis_data(
        self, 
        from_input:bool=False, 
        code_departement:int=75, 
        annee:int=2022, 
        rows:int=10
    ):
        """
        Extraire le dataframe enedis soit à partir d'un fichier csv 
        soit à partir d'une url.
        :param from_input: If True, use the input CSV file. If False, use the Enedis API.
        :param code_departement: Code of the department to filter the data.
        :param annee: Year to filter the data.
        :param rows: Number of rows to extract from the Enedis API.
        :return: self, with self.input containing the Enedis data.
        """
        def add_full_adress_col(df):
            df['full_adress'] = df['adresse'] + ' ' + df['code_commune'] + ' ' + df['nom_commune']
            return df

        if from_input:
            self.load_batch_input()
            if self.debug: self.debugger.update({'source_enedis': "input csv"})
            if code_departement: # filter sur le code département dans le df input
                logger.info(f"Filtering input data on code département : {code_departement}")
                self.input = self.input[self.input['Code Département']==code_departement]
        else:
            requete_url_enedis = self.get_url_enedis_year_rows(annee, rows)
            if code_departement: # filter sur le code département dans l'url
                requete_url_enedis += f"&where=code_departement%20%3D%20{code_departement}"
            self.input = self.get_dataframe_from_url(requete_url_enedis)
            logger.info(f"Extract input from url enedis :\n {requete_url_enedis}")
            if self.debug: self.debugger.update({'source_enedis': requete_url_enedis})

        # enfin, reconstituer les adresses complètes
        self.input = add_full_adress_col(self.input)
        return self

    # TACHE EXTRACTION 2
    @decorator_logger
    def get_ban_data(self, n_threads=10):    
        """
        Extraire le dataframe de la BAN à partir d'une liste d'adresses.
        :param n_threads: Number of threads to use for querying the BAN API.
        :return: self, with self.ban_data containing the BAN data.
        :raises ValueError: If the input dataframe is empty.
        """
        # tache 1 - prendre input enedis
        if self.input.empty:
            logger.critical("Erreur dans le chargement du fichier CSV input : pas de données")
            raise ValueError("Pas de données dans le dataframe")

        # tache 2 - constituer les adresses enedis
        enedis_adresses_list = self.input.full_adress.values.tolist()

        # tache 3 - requeter l'api de la BAN sur les adresses enedis avec n_threads
        self.ban_data = self.request_api_multithreaded(
                api_call_func=self.call_ban_api_individually,
                obj_list=enedis_adresses_list,
                n_threads=n_threads
            )
        # tache 4 - filtrer les adresses valides
        self.ban_data = list(filter(lambda x: x is not None, self.ban_data))
        if not self.ban_data:
            logger.critical("Erreur dans le chargement des données BAN : pas de données")
            raise ValueError("Pas de données dans le dataframe BAN")
        # tache 5 - convertir en dataframe pandas
        self.ban_data = pd.DataFrame(self.ban_data)

        vectorized_upper = np.vectorize(str.upper, cache=True) # est une optimisation
        self.ban_data['label'] = vectorized_upper(self.ban_data['label'].values) 
        # on remet en upper car on en a besoin pour le merge avec enedis
        if self.debug: self.debugger.update({'sample_ban_data': self.ban_data.tail(5)})
        logger.info(f"Valid data BAN : {len(self.ban_data)} addresses founded over {len(enedis_adresses_list)} requested.")
        return self

    # TACHE EXTRACTION 3
    @decorator_logger
    def get_ademe_data(self, n_threads):
        """
        Extraire la data de l'ademe au complet en utilisant 
        la liste des id_ban.
        
        - recup le df ademe data sur la base des id_ban
        - sur la base des Identifiants BAN de enedis, aller chercher les logements mappés sur ces codes BAN
        - 1 id_ban = * adresses (entre 10 et 1_000) - en effet, les données enedis sont agrégées
        """
        ademe_data = []
        # ? multithreading -> limite les requetes en parallele - renvoie 0 resultats si trop de requetes en parallele
        ademe_data_res = [requests.get(self.get_url_ademe_filter_on_ban(_id)).json().get('results')\
                          for _id in self.id_BAN_list] 
        # ademe_data_res = self.request_api_multithreaded(
        #     api_call_func=self.call_ademe_api_individually,
        #     obj_list=self.id_BAN_list,
        #     n_threads=n_threads
        # )
        # suppr les None
        ademe_data_res = list(filter(lambda x: x is not None, ademe_data_res))
        if not ademe_data_res:
            logger.critical("Erreur dans le chargement des données Ademe : pas de données")
            raise ValueError("Pas de données dans le dataframe Ademe")
        # on a une liste de listes, chaque liste correspond à un id_ban
        # on obtient une liste à 2 niveaux pour chaque Id_BAN 
        # on a plusieurs lignes ademe  
        for _ in ademe_data_res:
            ademe_data.extend(_)
        del ademe_data_res
        ademe_data = pd.DataFrame(ademe_data)
        ademe_data = ademe_data.add_suffix('_ademe')
        
        self.save_parquet_file(
            df=ademe_data,
            dir=self.PATH_DATA_BRONZE,
            fname="ademe_data_tmp.parquet"
        )
        self.ademe_data = ademe_data.copy()
        del ademe_data
        return self

    # TACHE MERGE 1 
    @decorator_logger
    def merge_and_save_enedis_with_ban_as_output(self): 
        """obtenir le df pandas des données enedis (requete) + les données de la BAN pour les adresses trouvées."""
        # note : si une adresse est pas  trouvée on tej la data enedis (cf. inner join)
        # merge enedis avec ban
        # ? - free memory
        self.input = self.input.add_suffix('_enedis')
        self.ban_data = self.ban_data.add_suffix('_ban')
        self.output = pd.merge(
                        self.input, 
                        self.ban_data, 
                        how='inner', 
                        left_on='full_adress_enedis', 
                        right_on='full_adress_ban')\
                        .rename(columns={'id_ban': 'id_BAN'})
        self.id_BAN_list = self.output.id_BAN.values.tolist()
        if self.debug: self.debugger.update({'sample_output_enedis_with_ban_tmp': self.output.tail(5)})
        if self.debug: self.debugger.update({'id_BAN_list': self.id_BAN_list})
        self.save_parquet_file(
            df=self.output,
            dir=self.PATH_DATA_BRONZE,
            fname=f"enedis_with_ban_data_tmp_{get_today_date()}.parquet"
        )
        self.output = pd.DataFrame() # free memory
        self.ban_data = pd.DataFrame() # free memory
        return self

    # TACHE MERGE 2 (final)
    @decorator_logger
    def merge_all_as_output(self):
        """
        Merge all dataframes to get the final dataframe.
        """
        # reconstituer le dataframe complet
        enedis_with_ban_data = self.load_parquet_file(
            dir=self.PATH_DATA_BRONZE,
            fname=f"enedis_with_ban_data_tmp_{get_today_date()}.parquet"
        )
        # enedis_with_ban_data = enedis_with_ban_data.add_suffix('_enedis_with_ban')
        logger.info(f"Enedis with BAN data loaded : {enedis_with_ban_data.shape[0]} rows, {enedis_with_ban_data.shape[1]} columns.")
        logger.info(f"Ademe data loaded : {self.ademe_data.shape[0]} rows, {self.ademe_data.shape[1]} columns.")
        assert 'Identifiant__BAN_ademe' in self.ademe_data.columns, \
            "Identifiant__BAN_ademe column not found in Ademe data. Check the schema or the data extraction process."
        assert 'id_BAN' in enedis_with_ban_data.columns, \
            "id_BAN column not found in Enedis with BAN data. Check the schema or the data extraction process."
        # merge enedis with ban data and ademe data
        self.ademe_data['Identifiant__BAN_ademe'] = self.ademe_data['Identifiant__BAN_ademe'].astype('string')
        enedis_with_ban_data['id_BAN'] = enedis_with_ban_data['id_BAN'].astype('string')
        self.output = pd.merge(self.ademe_data,
                            enedis_with_ban_data,
                            how='left',
                            left_on='Identifiant__BAN_ademe',
                            right_on='id_BAN').drop_duplicates().reset_index(drop=True)
        # normaliser les noms de colonnes et trier les colonnes
        self.output = normalize_df_colnames(self.output)
        self.save_parquet_file(
            df=self.output,
            dir=self.PATH_DATA_SILVER,
            fname=f"extract_output_data_{get_today_date()}.parquet"
        )
        if self.debug: self.debugger.update({'sample_output': self.output.tail(5)})

    @decorator_logger
    def extract(self, 
        from_input:bool=False, 
        input_csv_path:str=None,
        code_departement:int=75, 
        annee:int=2022, 
        rows:int=10, 
        n_threads_for_querying:int=10,
        save_schema:bool=True
        )-> None:
        """
        Run the extraction process.
        
        :param from_input: If True, use the input CSV file. If False, use the Enedis API.
        :param input_csv_path: Path to the input CSV file if from_input is True.
        :param code_departement: Code of the department to filter the data.
        :param annee: Year to filter the data.
        :param rows: Number of rows to extract from the Enedis API.
        :param n_threads_for_querying: Number of threads to use for querying the BAN API.
        :param save_schema: If True, save the schema of the output dataframe.
        
        :raises ValueError: If compulsory environment variables are not set.
        :raises Exception: If there is an error during the extraction process.
        :raises AssertionError: If the input CSV file is not valid or does not contain the required columns.
        :raises KeyError: If the schema file does not contain the required key.
        :raises TypeError: If the input data is not a pandas DataFrame.
        
        :return: None
        
        1. If from_input is True, it will load the input CSV file.
        2. If from_input is False, it will extract data from the Enedis API.
        3. It will then extract BAN data based on the input data.
        4. Finally, it will extract Ademe data based on the BAN data and merge all dataframes.
        5. The final output will be saved in a parquet file and the
        """
        if from_input:
            self.PATH_FILE_INPUT_ENEDIS_CSV = get_env_var(
                'PATH_FILE_INPUT_ENEDIS_CSV',
                default_value=input_csv_path, 
                compulsory=True
            )
        self.get_enedis_data(from_input, code_departement, annee, rows)\
            .get_ban_data(n_threads_for_querying)\
            .merge_and_save_enedis_with_ban_as_output()\
            .get_ademe_data(n_threads_for_querying)\
            .merge_all_as_output()
        logger.info(f"Extraction results : {self.output.shape[0]} rows, {self.output.shape[1]} columns.")
        # save schema
        if save_schema:
            fpath = get_env_var('SCHEMA_SILVER_DATA_FILEPATH', compulsory=True)
            fdir = os.path.dirname(fpath)
            if not os.path.exists(fpath): 
                os.makedirs(fdir, exist_ok=True)
                self._save_df_schema(self.output, fpath)
                logger.info(f"Extraction schema saved in : {fpath}")
        if self.debug: 
            import pprint
            pprint.pprint(self.debugger)