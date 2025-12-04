import pandas as pd
from src.utils.file_utils import save_dataframe_to_csv


OUTPUT_DIR = "data/processed"
FILE_NAME = "cleaned_noc_data.csv"


def clean_noc_data(noc_data: pd.DataFrame) -> pd.DataFrame:
    region_map = {
        "ROT": "Refugee Olympic Team",
        "TUV": "Tuvalu",
        "UNK": "Unknown"
    }
    noc_data["region"] = noc_data["region"].fillna(
        noc_data["NOC"].map(region_map)
    )

    # Save the dataframe as a CSV for logging purposes
    # Ensure the directory exists
    save_dataframe_to_csv(noc_data, OUTPUT_DIR, FILE_NAME)

    return noc_data
