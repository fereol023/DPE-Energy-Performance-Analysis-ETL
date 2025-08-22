import os
import time
import httpx
import requests
import functools 
import threading
import numpy as np 
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, Optional

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import Secret
from prefect.artifacts import create_markdown_artifact
from prefect.server.schemas.schedules import CronSchedule
from prefect.cache_policies import NO_CACHE

try:
    from ..scripts.filestorage_helper import FileStorageConnexion
    from ..utils import logger, decorator_logger
    from ..utils.fonctions import (
        get_env_var,
        get_today_date, 
        normalize_df_colnames
    )
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    sys.path.append(str(parent_dir))
    from scripts.filestorage_helper import FileStorageConnexion
    from utils import logger, decorator_logger
    from utils.fonctions import (
        get_env_var,
        get_today_date, 
        normalize_df_colnames
    )

class RateLimiter:
    """Thread-safe rate limiter"""
    def __init__(self, rate_limit: int):
        self.rate_limit = rate_limit
        self.requests_made = 0
        self.last_reset = time.time()
        self.lock = threading.Lock()
    
    def acquire(self):
        """Acquire permission to make a request, blocking if rate limit exceeded"""
        with self.lock:
            current_time = time.time()
            
            # Reset counter if more than 1 second has passed
            if current_time - self.last_reset >= 1.0:
                self.requests_made = 0
                self.last_reset = current_time
            
            # If we've hit the rate limit, wait until next second
            if self.requests_made >= self.rate_limit:
                sleep_time = 1.0 - (current_time - self.last_reset)
                if sleep_time > 0:
                    time.sleep(sleep_time + 1)
                # Reset after waiting
                self.requests_made = 0
                self.last_reset = time.time()
            
            self.requests_made += 1


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
        self.PATH_FILE_INPUT_ENEDIS_CSV = get_env_var('PATH_FILE_INPUT_ENEDIS_CSV', compulsory=True)
        # --- objet debugger ---
        self.debug = debug
        if self.debug: self.debugger = {} 
        # --- fonctions urls ---
        # generer une url pour requeter l'api enedis avec restriction sur l'année et le nombre de lignes
        self.get_url_enedis_year_rows = lambda annee, rows: f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?where=annee%20%3D%20date'{annee}'&limit={rows}"
        self.get_url_enedis=lambda annee,  code_departement, limit, offset: f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?where=annee%3Ddate%27{annee}%27%20and%20code_departement%3D%27{code_departement}%27&order_by=tri_des_adresses&limit={limit}&offset={offset}"
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ademe_filter_on_ban = lambda key: f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=1000&format=json&qs=Identifiant__BAN%3A{key}"
        self.get_url_ademe_filter_on_ban = lambda key: f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines?q_fields=identifiant_ban&q={key}" # update 19 juillet 2025
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ban_filter_on_adresse = lambda key: f"https://api-adresse.data.gouv.fr/search/?q={key}&limit=1"
        self.get_url_ban_filter_on_adresse = lambda addr: f"https://data.geopf.fr/geocodage/search?q={addr}&limit=1" # adresse est complete car obtenue par concat dans enedis (update 23 juillet 2025)
        self.meta = "" # suffix files 

    @decorator_logger
    @task(name="extract-input-df-from-PATH_FILE_INPUT_ENEDIS_CSV", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
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
        logger = get_run_logger()

        def load_enedis_input_from_local_csv():
            self.input = pd.read_csv(self.PATH_FILE_INPUT_ENEDIS_CSV, sep=';')
                     
        def load_enedis_input_from_s3_csv():
            self.input = pd.read_csv(
                self.client.get_object(self.BUCKET_NAME, self.PATH_FILE_INPUT_ENEDIS_CSV), sep=';'            
                )
        
        try:
            if self.env=='LOCAL':
                load_enedis_input_from_local_csv()
            else:
                load_enedis_input_from_s3_csv()
        except:
            logger.critical(f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV}")
            raise

    @decorator_logger
    def get_dataframe_from_url(self, url):
        """Extract pandas dataframe from any valid url."""
        logger.info(f"Fetching data from : {url}")
        res = requests.get(url)
        if res.status_code != 200:
            logger.critical(f"Error fetching data from {url} - Status code: {res.status_code} - Status message: {res.text}")
            raise ValueError(f"Error fetching data from {url} - Status code: {res.status_code} - Status message: {res.text}")
        res = res.json().get('results')
        return pd.DataFrame(res)

    @functools.lru_cache(maxsize=128)
    def call_ban_api_individually(self, addr):
        """ 
        Call the BAN API individually for a given address.
        :param addr: The address to query the BAN API.
        :return: A dictionary with the BAN data for the given address.
        """
        res = requests.get(self.get_url_ban_filter_on_adresse(addr), timeout=60)
        if res.status_code == 200:
            j = res.json()
            if len(j.get('features')) > 0:
                first_result = j.get('features')[0]
                lon, lat = first_result.get('geometry').get('coordinates')
                first_result_all_infos = { **first_result.get('properties'), **{"lon": lon, "lat": lat}, **{'full_adress': addr}}
                first_result_all_infos = { **first_result_all_infos, **{"thread_name": threading.current_thread().name}}
                time.sleep(1) # limite 50 appels/sec - stratégie : 1 thread attend 1 sec
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
        res = requests.get(self.get_url_ademe_filter_on_ban(id_ban), timeout=90)
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

    def multithreaded_api_request(
        self,
        num_threads: int,
        api_call_func: Callable[[Any], Any],
        obj_list: List[Any],
        rate_limit: int,
        timeout: Optional[float] = None
    ) -> List[Any]:
        """
        Make multithreaded API requests with rate limiting.
        Args:
            num_threads: Number of threads to use
            api_call_func: Function that takes an object from obj_list and returns API response
            obj_list: List of objects to process
            rate_limit: Maximum number of requests per second
            timeout: Optional timeout for each request in seconds
        
        Returns:
            List of results from API calls (same order as input list)
        
        Example:
            def my_api_call(item):
                response = requests.get(f"https://api.example.com/data/{item['id']}")
                return response.json()
            
            items = [{'id': 1}, {'id': 2}, {'id': 3}]
            results = multithreaded_api_request(
                num_threads=3,
                api_call_func=my_api_call,
                obj_list=items,
                rate_limit=10
            )
        """
        if not obj_list:
            return []
        
        # Initialize rate limiter and results storage
        rate_limiter = RateLimiter(rate_limit)
        results = [None] * len(obj_list)
        errors = []
        
        def worker(index: int, obj: Any) -> tuple:
            """Worker function for each thread"""
            try:
                # Acquire rate limit permission
                rate_limiter.acquire()
                # Make the API call
                if timeout:
                    # You might want to implement timeout handling in your api_call_func
                    result = api_call_func(obj)
                else:
                    result = api_call_func(obj)    
                return index, result, None
            except Exception as e:
                if "Max retries exceeded" in str(e):
                    time.sleep(30) # nombre de secondes bloquées non communiquées
                    try:
                        rate_limiter.acquire()
                        result = api_call_func(obj)
                        return index, result, None
                    except Exception as e:
                        return index, None, e
                else:
                    return index, result, None

        
        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(worker, i, obj): i 
                for i, obj in enumerate(obj_list)
            }
            # Collect results as they complete
            for future in as_completed(future_to_index):
                index, result, error = future.result()
                if error:
                    errors.append((index, error))
                    if self.debug: print(f"Error processing item {index}: {error}")
                else:
                    results[index] = result
        
        # Print summary
        successful = len([r for r in results if r is not None])
        if self.debug: print(f"Completed {successful}/{len(obj_list)} requests successfully")
        
        if errors and self.debug:
            print(f"Encountered {len(errors)} errors")
            for index, error in errors[:5]:  # Show first 5 errors
                print(f"  Item {index}: {type(error).__name__}: {error}")
            if len(errors) > 5:
                print(f"  ... and {len(errors) - 5} more errors")
        
        return results

    # TACHE VALIDER SCHEMA INPUT
    @decorator_logger
    @task(name="validate-enedis-input-df-schema", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def validate_schema_input(self):
        logger = get_run_logger()
        try:
            cols = self.input.columns
            assert not self.input.empty, f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV}"
            assert ('Adresse' in cols) or ('adresse' in cols), f"'Adresse' not in input columns : {cols}"
            assert ('Nom Commune' in cols) or ('nom_commune' in cols), f"'Nom Commune' not in input columns : {cols}"
            assert ('Code Commune' in cols) or ('code_commune' in cols), f"'Code Commune' not in input columns : {cols}"
            assert ('Code IRIS' in cols) or ('code_iris' in cols), f"'Code IRIS' not in input columns : {cols}"
            assert ('Code Département' in cols) or ('code_departement' in cols), f"'Code Département' not in input columns : {cols}"
        except Exception as e:
            logger.critical(f"Enedis schema validation failed with exception : {e}")

    # TACHE AJOUTER LES COLONNES A ENEDIS
    @decorator_logger
    @task(name="compute-adress-columns-and-format", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def add_enedis_columns(self):
        self.input = self.input.rename(
                        columns={
                            'Adresse': 'adresse', 
                            'Nom Commune': 'nom_commune', 
                            'Code Commune': 'code_commune',
                            'Code IRIS': 'code_iris',
                            'Code Département': 'code_departement'
                        })
        # validate schama with input required cols
        #assert all(col in self.input.columns for col in ['adresse', 'nom_commune', 'code_commune']),\
        #       f"Erreur dans le chargement du fichier CSV input : {self.PATH_FILE_INPUT_ENEDIS_CSV} - "
        self.input['code_departement'] = self.input['code_iris'].apply(lambda r: int(r[:2]))
        self.input['code_commune'] = self.input['code_commune'].astype('str')
        self.input['nom_commune'] = self.input['nom_commune'].astype('str')
        self.input['full_adress'] = self.input['adresse'] + ' ' + self.input['code_commune'] + ' ' + self.input['nom_commune']

    def call_enedis_api_single_thread(self, annee, code_departement, limit, offset):
        u = self.get_url_enedis(annee, code_departement, limit, offset)
        resp = requests.get(u)
        if resp.status_code == 200:
            return resp.json().get('results', [])
        return []

    def call_enedis_api_mutlithreads(self, annee, code_departement):
        params = [{"annee":annee, "code_departement":code_departement, "limit":100, "offset":i*100} for i in range(100)]
        results = []
        for p in params:
            results.extend(self.call_enedis_api_single_thread(**p))
        return results

    # TACHE EXTRACTION 1
    @decorator_logger
    @task(name="extract-data-from-enedis-api", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def get_enedis_data(
        self, 
        from_input:bool=False, 
        code_departement:int=-1, 
        annee:int=2023, 
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
        logger = get_run_logger()

        if self.debug: print("-> get_enedis_data")
        if from_input:
            self.load_batch_input()
            if self.debug: self.debugger.update({'source_enedis': "input csv"})
        else:
            if rows == -1:
                self.input = pd.DataFrame(self.call_enedis_api_mutlithreads(annee=annee, code_departement=code_departement)).drop_duplicates().reset_index(drop=True)
            else:
                requete_url_enedis = self.get_url_enedis_year_rows(annee, rows)
                if code_departement>0: # filter sur le code département dans l'url
                    # requete_url_enedis += f"&where=code_departement%20%3D%20{code_departement}"
                    requete_url_enedis += f"&code_departement%3D{code_departement}"
                self.input = self.get_dataframe_from_url(requete_url_enedis)
                logger.info(f"Extract input from url enedis :\n {requete_url_enedis}")
                if self.debug: self.debugger.update({'source_enedis': requete_url_enedis})
        
        logger.info(f"Shape of raw loaded dataframe : {self.input.shape} with cols {list(self.input.columns)}")

        # valider le schema
        self.validate_schema_input()
        # enfin, reconstituer les adresses complètes et les autres champs
        self.add_enedis_columns()
        if (from_input) and (code_departement>0): # filter sur le code département dans le df input
            self.input = self.input[self.input['code_departement']==code_departement]
            if rows > 0: self.input = self.input.head(rows)
            logger.info(f"Filtering input data on code département : {code_departement} ({self.input.shape[0]} rows, {self.input.shape[1]} columns)")
        
        logger.info(f"Shape of loaded dataframe : {self.input.shape} with cols {list(self.input.columns)}")
        return self

    # TACHE EXTRACTION 2
    @decorator_logger
    @task(name="extract-data-from-ban-api", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def get_ban_data(self, n_threads):    
        """
        Extraire le dataframe de la BAN à partir d'une liste d'adresses.
        :param n_threads: Number of threads to use for querying the BAN API.
        :return: self, with self.ban_data containing the BAN data.
        :raises ValueError: If the input dataframe is empty.
        """
        logger = get_run_logger()
        # tache 1 - prendre input enedis
        if self.input.empty:
            logger.critical("Erreur dans le chargement du fichier CSV input : pas de données")
            raise ValueError("Pas de données dans le dataframe")

        # tache 2 - constituer les adresses enedis
        enedis_adresses_list = list(set(self.input.full_adress.values.tolist()))
        if self.debug: print(f"-> get_ban_data : {len(enedis_adresses_list)}")

        # tache 3 - requeter l'api de la BAN sur les adresses enedis avec n_threads
        # self.ban_data = self.request_api_multithreaded(
        #         api_call_func=self.call_ban_api_individually,
        #         obj_list=enedis_adresses_list,
        #         n_threads=n_threads
        #     )
        self.ban_data = self.multithreaded_api_request(
            num_threads=n_threads,
            api_call_func=self.call_ban_api_individually,
            obj_list=enedis_adresses_list,
            rate_limit=30 # 50 en vrai d'après la doc
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
    @task(name="extract-data-from-ademe-api", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def get_ademe_data(self, n_threads):
        """
        Extraire la data de l'ademe au complet en utilisant 
        la liste des id_ban.
        
        - recup le df ademe data sur la base des id_ban
        - sur la base des Identifiants BAN de enedis, aller chercher les logements mappés sur ces codes BAN
        - 1 id_ban = * adresses (entre 10 et 1_000) - en effet, les données enedis sont agrégées
        """
        logger = get_run_logger()
        if self.debug: print("-> get_ademe_data")
        ademe_data = []
        # ? multithreading -> limite les requetes en parallele - renvoie 0 resultats si trop de requetes en parallele
        ademe_data_res = []
        k = 0
        for _id in self.id_BAN_list:
            try:
                resp = requests.get(self.get_url_ademe_filter_on_ban(_id), timeout=60)
                if resp.status_code==200:
                    ademe_data_res.append(resp.json().get('results'))
            except:
                pass
            time.sleep(1) # 600 req/secondes = 0,001s pour 1 req => on y va 2000 fois plus lentement que le rate limiteur
            k+=1
            if k % 100 == 0:
                logger.info(f"Ademe data extraction progress : {k}/{len(self.id_BAN_list)}")
                time.sleep(10) # on attend 60 secondes toutes les 100 requetes pour ne pas dépasser le rate limit
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
    @task(name="join-enedis-data-with-ban-data", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def merge_and_save_enedis_with_ban_as_output(self): 
        """obtenir le df pandas des données enedis (requete) + les données de la BAN pour les adresses trouvées."""
        # note : si une adresse est pas  trouvée on tej la data enedis (cf. inner join)
        # merge enedis avec ban
        # ? - free memory
        logger = get_run_logger()
        if self.debug: print("-> merge_and_save_enedis_with_ban_as_output")
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
    @task(name="join-all-and-backup", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def merge_all_as_output(self):
        """
        Merge all dataframes to get the final dataframe.
        """
        logger = get_run_logger()
        if self.debug: print("-> get_ademe_data")
        # reconstituer le dataframe complet
        enedis_with_ban_data = self.load_parquet_file(
            dir=self.PATH_DATA_BRONZE,
            fname=f"enedis_with_ban_data_tmp_{get_today_date()}.parquet"
        )
        # enedis_with_ban_data = enedis_with_ban_data.add_suffix('_enedis_with_ban')
        logger.info(f"Enedis with BAN data loaded : {enedis_with_ban_data.shape[0]} rows, {enedis_with_ban_data.shape[1]} columns.")
        logger.info(f"Ademe data loaded : {self.ademe_data.shape[0]} rows, {self.ademe_data.shape[1]} columns.")
        assert 'identifiant_ban_ademe' in self.ademe_data.columns, \
            "identifiant_ban_ademe column not found in Ademe data. Check the schema or the data extraction process. (Identifiant__BAN or identifiant_ban)"
        assert 'id_BAN' in enedis_with_ban_data.columns, \
            "id_BAN column not found in Enedis with BAN data. Check the schema or the data extraction process."
        # merge enedis with ban data and ademe data
        self.ademe_data['identifiant_ban_ademe'] = self.ademe_data['identifiant_ban_ademe'].astype('string')
        enedis_with_ban_data['id_BAN'] = enedis_with_ban_data['id_BAN'].astype('string')
        self.output = pd.merge(self.ademe_data,
                            enedis_with_ban_data,
                            how='left',
                            left_on='identifiant_ban_ademe',
                            right_on='id_BAN').drop_duplicates().reset_index(drop=True)
        # normaliser les noms de colonnes et trier les colonnes
        self.output = normalize_df_colnames(self.output)
        self.output = self.output.assign(batch_id=self.batch_id)
        self.save_parquet_file(
            df=self.output,
            dir=self.PATH_DATA_SILVER,
            fname=f"extraction_{get_today_date()}_{self.meta}.parquet"
        )
        if self.debug: self.debugger.update({'sample_output': self.output.tail(5)})

    @decorator_logger
    @flow(name="ETL data extraction pipeline", 
      description="Pipeline de collecte orchestré avec Prefect")
    def extract(self, 
        from_input:bool=False, 
        input_csv_path:str="",
        code_departement:int=-1, 
        annee:int=2023, 
        rows:int=10, 
        n_threads_for_querying:int=10,
        save_schema:bool=True
        )-> None:
        """
        Run the extraction process.
        
        1. If from_input is True, it will load the input CSV file.
        2. If from_input is False, it will extract data from the Enedis API.
        3. It will then extract BAN data based on the input data.
        4. Finally, it will extract Ademe data based on the BAN data and merge all dataframes.
        5. The final output will be saved in a parquet file and the schema will be saved if save_schema is True.

        :param from_input: If True, use the input CSV file. If False, use the Enedis API.
        :param input_csv_path: Path to the input CSV file if from_input is True.
        :param code_departement: Code of the department to filter the data.
        :param annee: Year to filter the data.
        :param rows: Number of rows to extract from the Enedis API.
        :param n_threads_for_querying: Number of threads to use for querying the BAN API.
        :param save_schema: If True, save the schema of the output dataframe.
        
        :return: None
        """
        if from_input:
            self.PATH_FILE_INPUT_ENEDIS_CSV = get_env_var(
                'PATH_FILE_INPUT_ENEDIS_CSV',
                default_value=input_csv_path, 
                compulsory=True
            )
        self.meta = f"from_input_{str(from_input)}_dept_{str(code_departement)}_year_{str(annee)}_{self.batch_id}"
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