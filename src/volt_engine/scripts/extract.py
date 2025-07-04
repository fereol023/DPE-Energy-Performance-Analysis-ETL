import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ..scripts import S3Connexion as Connexion
from ..utils.mylogging import logger, log_decorator
from ..utils.fonctions import get_today_date, normalize_df_colnames

import pandas as pd
import numpy as np 
import requests
import functools 
import threading
from concurrent.futures import ThreadPoolExecutor


class DataEnedisAdemeExtractor(Connexion):

    # enedis records endpoint has limitations ~ default 10 if not set  
    # combiner limit et offset pour imiter la pagination limite à 10_000 lignes 
    # while export endpoint has no limitations of rows

    def __init__(self):
        super().__init__()
        self.debugger = {}
        self.output = pd.DataFrame()
        self.input = pd.DataFrame()
        self.ban_data = pd.DataFrame()
        self.ademe_data = pd.DataFrame()
        self.batch_input_enedis_fpath = self.PATHS.get('batch-input-enedis-csv', None) # peut override mais pas souhaitable

        # fonctions urls
        # generer une url pour requeter l'api enedis avec restriction sur l'année et le nombre de lignes
        self.get_url_enedis_year_rows = lambda annee, rows: f"https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records?where=annee%20%3D%20date'{annee}'&limit={rows}"
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ademe_filter_on_ban = lambda key: f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=1000&format=json&qs=Identifiant__BAN%3A{key}"
        # generer une url pour requeter l'api de la ban à partir d'une adresse
        self.get_url_ban_filter_on_adresse = lambda key: f"https://api-adresse.data.gouv.fr/search/?q={key}&limit=1"


    def __load_batch_input(self):
        """Load csv data from conso input file. Only input is csv"""
        input_path = self.batch_input_enedis

        def load_enedis_input_from_local_csv():
            self.input_df = pd.read_csv(input_path, sep=';')
                     
        def load_enedis_input_from_s3_csv():
            bucket_name = self.PATHS.get('s3-bucket-name')
            data = self.client.get_object(bucket_name, input_path)            
            self.input_df = pd.DataFrame(data)
        
        try:
            if self.env=='LOCAL':
                load_enedis_input_from_local_csv()
            else:
                load_enedis_input_from_s3_csv()
            assert self.input_df.shape[0] > 0, f"Erreur dans le chargement du fichier CSV input : {input_path}"
            assert all(col in self.input_df.columns for col in ['Adresse', 'Nom Commune', 'Code Commune']),\
                    f"Erreur dans le chargement du fichier CSV input : {input_path}"
            self.input = self.input.rename(
                            columns={
                                'Adresse': 'adresse', 
                                'Nom Commune': 'nom_commune', 
                                'Code Commune': 'code_commune'
                            })
        except:
            logger.critical(f"Erreur dans le chargement du fichier CSV input : {input_path}")
            raise

    def get_dataframe_from_url(self, url):
        """Extract pandas dataframe from any valid url."""
        print(f"Fetching data from : {url}")
        try:
            res = requests.get(url).json().get('results')
            return pd.DataFrame(res)
        except Exception as e:
            logger.critical(f"Erreur dans le chargement du fichier JSON : {url} - {e}")
            raise

    @functools.lru_cache(maxsize=128)
    def call_ban_api_individually(self, addr):
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
    

    def request_ban_from_adress_list(self, adress_list, n_threads):
        # benchmark 100 lignes 
        # 9 secondes 1 thread
        # 1 seconde 10 threads, instantané (0.8s avec 10 threads + cache) 
        workers = ThreadPoolExecutor(max_workers=n_threads)
        res = workers.map(self.call_ban_api_individually, adress_list)
        workers.shutdown()
        res = list(filter(lambda x: x is not None, res))
        return list(res)

    # TACHE EXTRACTION 1
    @log_decorator
    def get_enedis_data(self, from_input:bool=False, code_departement:int=75, annee:int=2022, rows:int=10):
        """
        Extraire le dataframe enedis soit à partir d'un fichier csv soit à partir d'une url.
        """
        def add_full_adress_col(df):
            df['full_adress'] = df['adresse'] + ' ' + df['code_commune'] + ' ' + df['nom_commune']
            return df

        if from_input:
            self.__load_batch_input() # set input df soit à partir d'un csv soit en local soit sur s3
            self.debugger.update({'source_enedis': "input csv"})
            if code_departement:
                self.input = self.input[self.input['Code Département']==code_departement]
        else:
            requete_url_enedis = self.get_url_enedis_year_rows(annee, rows)
            self.input = self.get_dataframe_from_url(requete_url_enedis)
            logger.info(f"Extract input from url enedis :\n {requete_url_enedis}")
            self.debugger.update({'source_enedis': requete_url_enedis})

        # reconstituer les adresses complètes
        self.input = add_full_adress_col(self.input)
        return self

    # TACHE EXTRACT 2
    @log_decorator
    def get_ban_data(self, n_threads=10):    
        """
        Extraire le dataframe de la BAN à partir d'une liste d'adresses.
        """
        # tache 1 - prendre input enedis
        if self.input.empty:
            logger.critical("Erreur dans le chargement du fichier CSV input : pas de données")
            raise ValueError("Pas de données dans le dataframe")

        # tache 2- constituer les adresses enedis
        enedis_adresses_list = self.input.full_adress.values.tolist()

        # tache 3- requeter l'api de la BAN sur les adresses enedis
        self.ban_data = self.request_ban_from_adress_list(enedis_adresses_list, n_threads)
        self.ban_data = pd.DataFrame(self.ban_data)
        vectorized_upper = np.vectorize(str.upper, cache=True) # est une optimisation
        self.ban_data['label'] = vectorized_upper(self.ban_data['label'].values) # on remet en upper car on en a besoin pour le merge avec enedis
        self.debugger.update({'sample_ban_data': self.ban_data.tail(5)})
        logger.info(f"Valid data BAN : {len(self.ban_data)}/{len(enedis_adresses_list)}")
        return self

    # TACHE MERGE 1 
    @log_decorator
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
        self.debugger.update({'sample_output_enedis_with_ban_tmp': self.output.tail(5)})
        self.debugger.update({'id_BAN_list': self.id_BAN_list})
        self.save_parquet_file(
            df=self.output,
            dir=self.PATHS.get("path-data-archive"),
            fname=f"enedis_with_ban_data_tmp_{get_today_date()}.parquet"
        )
        self.output = pd.DataFrame() # free memory
        self.ban_data = pd.DataFrame() # free memory
        return self

    # TACHE EXTRACTION 3
    @log_decorator
    def get_ademe_data(self, n_threads):
        """
        Extraire la data de l'ademe au complet en utilisant la liste des id_ban.
        """
        # - recup le df ademe data sur la base des id_ban
        # sur la base des Identifiants BAN de enedis, aller chercher les logements mappés sur ces codes BAN
        # 1 id_ban = * adresses (entre 10 et 1_000) - en effet les données enedis sont agrégées
        ademe_data = []
        # ? threading
        ademe_data_res = [requests.get(self.get_url_ademe_filter_on_ban(_id)).json().get('results')\
                          for _id in self.id_BAN_list] 
        # on obtient une liste à 2 niveaux pour chaque Id_BAN on a plusieurs lignes ademe        
        for _ in ademe_data_res:
            ademe_data.extend(_)
        del ademe_data_res
        ademe_data = pd.DataFrame(ademe_data)
        ademe_data = ademe_data.add_suffix('_ademe')
        self.save_parquet_file(
            df=ademe_data,
            dir=self.PATHS.get("path-data-archive"),
            fname="ademe_data_tmp.parquet"
        )
        self.ademe_data = ademe_data.copy()
        del ademe_data
        return self

    def merge_all_as_output(self):
        """
        Merge all dataframes to get the final dataframe.
        """
        # reconstituer le dataframe complet
        enedis_with_ban_data = self.load_parquet_file(
            dir=self.PATHS.get("path-data-archive"),
            fname=f"enedis_with_ban_data_tmp_{get_today_date()}.parquet"
        )
        # enedis_with_ban_data = enedis_with_ban_data.add_suffix('_enedis_with_ban')

        self.output = pd.merge(self.ademe_data,
                            enedis_with_ban_data,
                            how='left',
                            left_on='Identifiant__BAN_ademe',
                            right_on='id_BAN').drop_duplicates().reset_index(drop=True)
        # normaliser les noms de colonnes et trier les colonnes
        self.output = normalize_df_colnames(self.output)
        self.save_parquet_file(
            df=self.output,
            dir=self.PATHS.get("path-data-bronze"),
            fname=f"extract_output_data_{get_today_date()}.parquet"
        )
        self.debugger.update({'sample_output': self.output.tail(5)})

    @log_decorator
    def extract_year_rows(self, year:int=2018, rows:int=20):
        try: # improve to handle pagination
            res = self.get_enedis_with_ban_with_ademe(self.get_url_enedis_year_rows(year,rows), from_input=False)
            logger.info(f"Extraction results : {res.shape[0]} rows, {res.shape[1]} columns.")
            self.result = res
            self.purge_archive_dir()
        except Exception as e:
            logger.critical(f"Erreur dans l'extraction : {e}")
    
    @log_decorator
    def extract_batch(self):
        try:
            res = self.get_enedis_with_ban_with_ademe(None, from_input=True)
            logger.info(f"Extraction results : {res.shape[0]} rows, {res.shape[1]} columns.")
            self.result = res
            self.purge_archive_dir()
        except Exception as e:
            logger.critical(f"Erreur dans l'extraction : {e}")

    @log_decorator
    def extract(self, 
                from_input:bool=False, 
                code_departement:int=75, 
                annee:int=2022, 
                rows:int=10, 
                output_schema_fname:str="schema_extraction_output.json"
                ):
        """
        Run the extraction process.
        """
        self.get_enedis_data(from_input=from_input, code_departement=code_departement, annee=annee, rows=rows)\
            .get_ban_data(n_threads=10)\
            .merge_and_save_enedis_with_ban_as_output()\
            .get_ademe_data(n_threads=10)\
            .merge_all_as_output()
        logger.info(f"Extraction results : {self.output.shape[0]} rows, {self.output.shape[1]} columns.")
        # save schema
        if not os.path.exists(f"ressources/schemas/{output_schema_fname}"):
            os.makedirs(f"ressources/schemas", exist_ok=True)
            self._save_df_schema(self.output, output_schema_fname)
            logger.info(f"Extraction schema saved in : {output_schema_fname}")
