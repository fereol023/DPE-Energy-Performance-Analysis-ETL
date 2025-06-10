import os
import numpy as np
import pandas as pd

import re, datetime
from utils.fonctions import (
    normalize_colnames_list, 
    normalize_df_colnames, 
    get_today_date,
    load_json
    )
from utils.mylogging import (
    logger, 
    log_decorator
    )
from src.scripts import S3Connexion as ConnexionMinio


class TransformDataEnedisAdeme(ConnexionMinio):
    """Classe principale qui gère le nettoyage d'un df.
    1 - cast/autocast des colonnes
    2 - selection des colonnes => 3 entités : adresse(id_ban), logement(id_ban), consommation(id_ban)
      - ttes les colonnes adresses sont mutualisés sur une seule table (geo representation)
      - 1 consommation => * logements ; lien avec id_ban
    3 - fillage des NaN
    """

    def __init__(self, df, inplace=False, cols_config_fpath=None):
        # normalisation des noms de colonne
        super().__init__()
        self.df = df if inplace else df.copy()
        self.df = normalize_df_colnames(self.df) # deja normalise en principe
        
        self.df_adresses = pd.DataFrame()
        self.df_logements = pd.DataFrame()

        # update ces valeurs plus tard
        self.cols_adresses = [] 
        self.cols_logements = []
        self.cols_config_fpath = "ressources/schemas/schema_tables.json" if \
            not cols_config_fpath else cols_config_fpath
        self.cols_filled = {"mean": [], "median": []} # cols ou les nan auront été remplis

    @log_decorator
    def auto_cast_object_columns(self):
        """Automatic casting for object columns.
        -----------------------------------
        Technique :
        On teste le cast en numeric, si ca fail on teste le cast en datetime, si ca fail on laisse en str.
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
        self._save_df_schema(self.df, "schema_exctraction_output_real_types.json")
        return self
    
    @log_decorator
    def fillnan_float_dtypes(self):
        """
        Il est conseillé de faire un fillna par la médiane si on a une variable avec des outliers et
          de faire une imputation par la moyenne sinon.

        Technique ===========
        on calcule les bornes de l'IQR et on vérifie si on des obs superieurs ou inferieures à bsup et binf.
        si oui, on fait une imputatin par la médiane, si non on fait une imputation par la moyenne. 
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
    
    def compute_target(self):
        target = normalize_colnames_list(["Consommation annuelle moyenne par logement de l'adresse (MWh)_enedis_with_ban"])[0]
        if target not in self.df.columns:
            self.df[target] = 0
        new_target = target.replace('mwh', 'kwh')
        if target in self.df.columns:
            self.df[new_target] = 1_000*self.df[target]
            self.df = self.df.drop(target, axis=1)
        return self
    
    def get_cols(self, key: str, only_required: bool=False) -> list:
        """
        Récupère les colonnes à partir du fichier de configuration.
        :param key: La clé du schéma dans le fichier de configuration.
        :param
        :param only_required: Si True, ne récupère que les colonnes requises.
        :return: Une liste de colonnes.
        """
        cols_config = load_json(self.cols_config_fpath, default_value={})
        if key not in cols_config:
            raise KeyError(f"Key {key} not found in schema file.")
        col_config = cols_config[key] # dict
        if only_required:
            return col_config.get("required", [])
        else:
            return list(col_config.get("cols", {}).keys())

    @log_decorator
    def select_and_split(self, only_required_columns=False):
        """Selection des colonnes et split en 3 tables : adresses, logements, consommations"""

        # load cols from config
        self.cols_adresses = list(set(self.get_cols("schema-adresses", only_required_columns)))
        self.cols_logements = list(set(self.get_cols("schema-logements", only_required_columns)))

        # split df
        self.df_adresses = self.df[self.cols_adresses].drop_duplicates()
        self.df_logements = self.df[self.cols_logements].drop_duplicates()
        return self
    
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

    def save_all(self):
        """Save the transformed data to parquet files."""
        for n,d in [
            ("adresses", self.df_adresses), 
            ("logements", self.df_logements), 
            ]:
            self.save_parquet_file(
                df=d,
                dir=self.PATHS.get("path-data-silver"), # ? add le run id dans dir path
                fname=f"{n}_{get_today_date()}.parquet"
            )

    @log_decorator
    def run(self, types_schema_fpath: str=None, keep_only_required: bool=False):
        # étapes de transformation
        if not types_schema_fpath:
            # si le schema n'existe pas ou n'est pas fourni on le créé
            types_schema_fpath = "ressources/schemas/schema_extraction_output_realtypes.json"
            self.auto_cast_object_columns() # is saved
        else:
            data_schema = self._load_df_schema(types_schema_fpath)
            self.df = self.apply_schema_to_df(data_schema)

        self.fillnan_float_dtypes()\
            .compute_target()\
            .compute_arrondissement()\
            .select_and_split(keep_only_required).save_all()
