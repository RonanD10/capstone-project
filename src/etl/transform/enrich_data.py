import pandas as pd
from src.utils.file_utils import save_dataframe_to_csv

OUTPUT_DIR = "data/processed"
FILE_NAME = "transformed_data.csv"


def create_country_columns(olympic_data: pd.DataFrame, noc_data: pd.DataFrame) -> pd.DataFrame:
    """
    """
    country_map = dict(zip(noc_data["NOC"], noc_data["region"]))
    country_map["SGP"] = "Singapore" # Not in NOC dataset
    olympic_data["country"] = olympic_data["noc"].map(country_map)

    save_dataframe_to_csv(olympic_data, OUTPUT_DIR, FILE_NAME)
    
    return olympic_data 
