import os
import re
import warnings
import datetime
import numpy as np
import pandas as pd

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import Secret
from prefect.artifacts import create_markdown_artifact
from prefect.server.schemas.schedules import CronSchedule
from prefect.cache_policies import NO_CACHE

try:
    from ..utils.fonctions import (
        normalize_colnames_list, 
        normalize_df_colnames, 
        get_today_date,
        load_json
        )
    from ..utils import (
        logger, 
        decorator_logger
        )
    from ..scripts.filestorage_helper import FileStorageConnexion
    from ..utils.fonctions import get_env_var
except ImportError:
    import sys
    from pathlib import Path
    current_dir = Path(__file__).resolve().parent
    parent_dir = current_dir.parent
    sys.path.append(str(parent_dir))
    from utils.fonctions import (
        normalize_colnames_list, 
        normalize_df_colnames, 
        get_today_date,
        load_json
        )
    from utils import (
        logger, 
        decorator_logger
        )
    from scripts.filestorage_helper import FileStorageConnexion
    from utils.fonctions import get_env_var

from scipy.stats import ttest_rel, wilcoxon


class DataEnedisAdemeTransformer(FileStorageConnexion):
    """
    Classe principale qui gère le nettoyage d'un df.
        -> Logique basée sur notre étude.
    1 - cast/autocast des colonnes
    2 - selection des colonnes => 3 entités : adresse(id_ban), logement(id_ban), consommation(id_ban)
      - ttes les colonnes adresses sont mutualisés sur une seule table (geo representation)
      - 1 consommation => * logements ; lien avec id_ban
    3 - fillage des NaN
    """

    def __init__(self, df, inplace=False, golden_data_config_fpath=None):
        # normalisation des noms de colonne
        super().__init__()
        self.df = df if inplace else df.copy()
        self.df = normalize_df_colnames(self.df) # deja normalise en principe
        # init des df vides
        self.df_adresses = pd.DataFrame()
        self.df_logements = pd.DataFrame()
        self.df_villes = pd.DataFrame()
        self.df_donnees_geocodage = pd.DataFrame()
        self.df_donnees_climatiques = pd.DataFrame()
        self.df_tests_statistiques_dpe = pd.DataFrame()
        # update ces valeurs plus tard
        self.cols_adresses = [] 
        self.cols_logements = []
        self.golden_data_config_fpath = get_env_var(
            'SCHEMA_GOLDEN_DATA_FILEPATH', 
            default_value=golden_data_config_fpath,
            compulsory=True
        )
        self.cols_filled = {"mean": [], "median": []} # cols ou les nan auront été remplis

    @decorator_logger
    @task(name="transform-auto-cast-object-variables", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def auto_cast_object_columns(self):
        """
        Automatic casting for object columns.
        -----------------------------------
        Technique :
        On teste le cast en numeric, si ca fail on teste 
        le cast en datetime, si ca fail on laisse en str.
        """
        cols_obj = self.df.select_dtypes(include='O').columns
        for c in cols_obj:
            try:
                self.df[c] = pd.to_numeric(self.df[c].str.replace(',','.'), errors='raise')
            except Exception as e:
                try:
                    self.df[c] = pd.to_datetime(self.df[c])
                except Exception as e:
                    self.df[c] = self.df[c].astype('string')
        return self
    
    @decorator_logger
    @task(name="transform-imputation-with-float-variables", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def fillnan_float_dtypes(self):
        """
        Il est conseillé de faire un fillna par la médiane 
        si on a une variable avec des outliers et
        de faire une imputation par la moyenne sinon.

        Technique ===========
        on calcule les bornes de l'IQR et on vérifie si on des 
        obs superieurs ou inferieures à bsup et binf.
        si oui, on fait une imputatin par la médiane, si non 
        on fait une imputation par la moyenne. 
        """
        col_fill_median, col_fill_mean = [], []
        for col in self.df.select_dtypes(include ='float').columns:
            if self.df[col].isna().any():
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                born_inf = (self.df[col]<(Q1-1.5*IQR)).value_counts()
                born_sup = (self.df[col]>(Q3+1.5*IQR)).value_counts()
                try:
                    born_inf[1]
                    self.df[col].fillna(self.df[col].median(), inplace=True)
                    col_fill_median.append(col)
                    self.cols_filled["median"].append(col)
                except Exception:
                    try:
                        born_sup[1]
                        self.df[col].fillna(self.df[col].median(), inplace=True)
                        col_fill_median.append(col)
                    except Exception:
                        self.df[col] = self.df[col].fillna(self.df[col].mean())
                        col_fill_mean.append(col)
                    self.cols_filled["mean"].append(col)
        return self

    def extract_digit(self, x):
        return re.sub(r'\D', '', str(x))

    @decorator_logger
    @task(name="transform-compute-arrondissement", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def compute_arrondissement(self):
        try:
            if "district_enedis_with_ban" not in self.df.columns:
                self.df["arrondissement"] = "N/A"
                return self
            self.df["arrondissement"] = self.df["district_enedis_with_ban"].apply(self.extract_digit).astype('string')
            self.df = self.df.drop('district_enedis_with_ban', axis=1)
            return self
        except: 
            return self
    
    @decorator_logger
    @task(name="transform-compute-conso-per-kwh", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def compute_conso_kwh(self):
        target = normalize_colnames_list(["Consommation annuelle moyenne par logement de l'adresse (MWh)_enedis_with_ban"])[0]
        new_target = target.replace('mwh', 'kwh')
        if target not in self.df.columns:
            self.df[target] = 0
        self.df[new_target] = 1_000*self.df[target]
        return self
    
    @decorator_logger
    @task(name="transform-compute-conso-per-kwh-per-m2", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def compute_conso_kwh_m2(self):
        target = normalize_colnames_list(["Consommation annuelle moyenne par logement de l'adresse (MWh)_enedis_with_ban"])[0]
        target = target.replace('mwh', 'kwh')
        surface = "surface_habitable_logement_ademe"
        to_compute = "conso_kwh_m2"
        self.df[surface] = self.df[surface].replace(0, np.nan)  # Avoid division by zero
        self.df[to_compute] = self.df[target] / self.df[surface]
        return self

    @decorator_logger
    @task(name="transform-compute-absolute-diff-cols", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def compute_absolute_diff_consos(self):
        to_compute1 = "absolute_diff_conso_prim_fin"
        to_compute2 = "absolute_diff_conso_fin_act"
        to_compute3 = "consumption_difference"
        conso_prim = "conso_5_usages_par_m2_ep_ademe"
        conso_fin = "conso_5_usages_par_m2_ef_ademe"
        conso_act = "conso_kwh_m2"
        conso_cols = [c for c in self.df.columns if 'conso' in c.lower()]
        assert conso_prim in self.df.columns, f"Column {conso_prim} not found in DataFrame. {conso_cols}"
        assert conso_fin in self.df.columns, f"Column {conso_fin} not found in DataFrame. {conso_cols}"
        assert conso_act in self.df.columns, f"Column {conso_act} not found in DataFrame. {conso_cols}"
        self.df[to_compute1] = (self.df[conso_prim] - self.df[conso_fin]).abs()
        self.df[to_compute2] = (self.df[conso_act] - self.df[conso_fin]).abs()
        self.df[to_compute3] = (self.df[conso_prim] - self.df[conso_act])
        return self

    def get_cols(self, key: str, only_required: bool=False) -> list:
        """
        Récupère les colonnes à partir du fichier de configuration.
        :param key: La clé du schéma dans le fichier de configuration.
        :param
        :param only_required: Si True, ne récupère que les colonnes requises.
        :return: Une liste de colonnes.
        """
        cols_config = load_json(self.golden_data_config_fpath, default_value={})
        if key not in cols_config:
            raise KeyError(f"Key {key} not found in schema file.")
        col_config = cols_config[key] # dict
        if only_required:
            return col_config.get("required", [])
        else:
            return list(col_config.get("cols", {}).keys())
    
    def get_default_value_from_golden_colname(self, key, colname):
        cols_config = load_json(self.golden_data_config_fpath, default_value={})
        if key not in cols_config:
            raise KeyError(f"Key {key} not found in schema file.")
        return cols_config.get(key).get("cols", {}).get(colname, {}).get("default", "N/C")

    @decorator_logger
    @task(name="transform-select-and-split-per-entities", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def select_and_split(self, only_required_columns: bool=False):
        """Selection des colonnes et split en 3 tables : adresses, logements, consommations"""

        # load cols from config
        self.cols_adresses = list(set(self.get_cols("schema-adresses", only_required_columns)))
        self.cols_logements = list(set(self.get_cols("schema-logements", only_required_columns)))
        self.cols_villes = list(set(self.get_cols("schema-villes", only_required_columns)))
        self.cols_donnees_geocodage = list(set(self.get_cols("schema-donnees_geocodage", only_required_columns)))
        self.cols_donnees_climatiques = list(set(self.get_cols("schema-donnees_climatiques", only_required_columns)))
        self.cols_tests_statistiques_dpe = list(set(self.get_cols("schema-tests_statistiques_dpe", only_required_columns)))

        # adapt dataframe when some columns are missing
        all_cols = self.cols_adresses + self.cols_logements + self.cols_villes + \
            self.cols_donnees_geocodage + self.cols_donnees_climatiques # + self.cols_tests_statistiques_dpe
        missing_cols = list(set(all_cols) - set(self.df.columns))
        if missing_cols:
            for c in missing_cols:
                if c in self.cols_adresses:
                    self.df[c]=self.get_default_value_from_golden_colname(key="schema-adresses", colname=c) #default value
                if c in self.cols_logements:
                    self.df[c]=self.get_default_value_from_golden_colname(key="schema-logements", colname=c) #default value

        # split df
        self.df_adresses = self.df[self.cols_adresses].drop_duplicates()
        self.df_logements = self.df[self.cols_logements].drop_duplicates()
        self.df_villes = self.df[self.cols_villes].drop_duplicates()
        self.df_donnees_geocodage = self.df[self.cols_donnees_geocodage].drop_duplicates()
        self.df_donnees_climatiques = self.df[self.cols_donnees_climatiques].drop_duplicates()
        return self
    
    @decorator_logger
    @task(name="transform-cast-variables-with-types", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def apply_schema_to_df(self, data_schema: dict) -> pd.DataFrame:
        """
        Applique le schéma de données à un DataFrame.
        :param data_schema: Le schéma de données à appliquer.
        :return: Le DataFrame avec le schéma appliqué.
        """
        for col, dtype in data_schema.items():
            if col in self.df.columns:
                if dtype == 'datetime64[ns]':
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                elif dtype == 'float64':
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                elif dtype == 'int64':
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
                else:
                    self.df[col] = self.df[col].astype(dtype)
        return self
    
    @decorator_logger
    @task(name="transform-save-tables-files", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def save_all(self):
        """Save the transformed data to parquet files in gold zone."""
        for n,d in [
            ("adresses", self.df_adresses), 
            ("logements", self.df_logements),
            ("villes", self.df_villes),
            ("donnees_geocodage", self.df_donnees_geocodage),
            ("donnees_climatiques", self.df_donnees_climatiques),
            ("tests_statistiques_dpe", self.df_tests_statistiques_dpe) # TODO compute this separately
            ]:
            self.save_parquet_file(
                df=d,
                dir=self.PATH_DATA_GOLD, # ? add le run id dans dir path
                fname=f"{n}_{get_today_date()}_{self.batch_id}.parquet"
            )

    @decorator_logger
    @task(name="transform-make-statistical-metrics", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
    def make_statistical_metrics(self):
        """
        Compute statistical metrics on current batch
        """
        logger = get_run_logger()
        # Create a new column for the difference between real and estimated consumption
        if self.df_logements.empty:
            logger.warning("DataFrame 'df_logements' is empty. Skipping statistical metrics computation.")
            return self
        # Ensure the necessary columns are present
        required_columns = [
            'conso_kwh_m2',
            'conso_5_usages_par_m2_ef_ademe',
            'etiquette_dpe_ademe'
        ]
        for col in required_columns:
            if col not in self.df_logements.columns: 
                raise Exception(f"Column {col} not found in DataFrame and is missing for stat analysis step.")

        # Perform statistical tests for each DPE group and store results
        df = self.df_logements[required_columns].copy()
        dpe_groups = df.groupby('etiquette_dpe_ademe')
        results_list = []

        for dpe_label, group_data in dpe_groups:
            # Remove rows with NaN values in either consumption column for the paired tests
            cleaned_group_data = group_data.dropna(subset=['conso_5_usages_par_m2_ef_ademe', 'conso_kwh_m2'])

            n_samples = len(cleaned_group_data)
            result_row = {'etiquette_dpe_ademe': dpe_label, 'sample_size': n_samples}

            if n_samples > 1: # Need at least 2 samples for tests
                # Paired t-test
                t_stat, p_ttest = ttest_rel(cleaned_group_data['conso_kwh_m2'], cleaned_group_data['conso_5_usages_par_m2_ef_ademe'])
                result_row['paired_t_test_t_statistic'] = t_stat
                result_row['paired_t_test_p_value'] = p_ttest

                # Wilcoxon signed-rank test
                try:
                    wilcoxon_stat, p_wilcoxon = wilcoxon(cleaned_group_data['conso_kwh_m2'], cleaned_group_data['conso_5_usages_par_m2_ef_ademe'])
                    result_row['wilcoxon_statistic'] = wilcoxon_stat
                    result_row['wilcoxon_p_value'] = p_wilcoxon
                except ValueError as e:
                    result_row['wilcoxon_statistic'] = -99999
                    result_row['wilcoxon_p_value'] = -99999 # f"Could not perform: {e}"

            else:
                result_row['paired_t_test_t_statistic'] = -99999 # None
                result_row['paired_t_test_p_value'] = -99999 # "Not enough data"
                result_row['wilcoxon_statistic'] = -99999 # None
                result_row['wilcoxon_p_value'] = -99999 # "Not enough data"

            results_list.append(result_row)

        # Create a DataFrame from the results list
        results_df = pd.DataFrame(results_list)
        results_df = results_df.assign(batch_id=self.batch_id)
        logger.info(results_list)
        logger.info(f"Statistical metrics computed for {len(results_df)} DPE groups.")
        self.df_tests_statistiques_dpe = results_df.copy()
        del results_df, df, dpe_groups, group_data, cleaned_group_data
        return self
        

    @decorator_logger
    @flow(name="ETL data transformation pipeline", 
      description="Pipeline de nettoyage orchestré avec Prefect")
    def run(
        self, 
        types_schema_fpath: str="", 
        keep_only_required: bool=False,
    ):
        warnings.filterwarnings("ignore")
        # étapes de transformation 
        # 1 - casting 
        if not types_schema_fpath:
            # si le schema n'existe pas ou n'est pas fourni on le créé
            # a partir de la sauvegarde à l'extract
            self.auto_cast_object_columns()            
            self._save_df_schema(
                self.df, 
                fpath=get_env_var('SCHEMA_SILVER_DATA_FILEPATH', compulsory=False)
            )
        else:
            data_schema=self._load_df_schema(types_schema_fpath)
            self.apply_schema_to_df(data_schema)
        # 2 - transfo
        self.fillnan_float_dtypes()\
            .compute_conso_kwh()\
            .compute_arrondissement()\
            .compute_conso_kwh_m2()\
            .compute_absolute_diff_consos()\
            .select_and_split(keep_only_required)\
            .make_statistical_metrics()\
            .save_all()
