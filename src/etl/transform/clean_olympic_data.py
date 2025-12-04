import pandas as pd
from src.utils.file_utils import save_dataframe_to_csv

OUTPUT_DIR = "data/processed"
FILE_NAME = "cleaned_data.csv"


def clean_olympic_data(data: pd.DataFrame) -> pd.DataFrame:
    data = drop_duplicates(data)
    data = standardise_column_names(data)
    data = standardise_object_columns(data)
    data = fill_missing_values(data)

    save_dataframe_to_csv(data, OUTPUT_DIR, FILE_NAME)

    return data


def standardise_column_names(data: pd.DataFrame) -> pd.DataFrame:
    # Ensure consistent column names, including units
    cols = data.columns
    cols_lower = [c.lower() for c in cols]
    data = data.rename(columns=dict(zip(cols, cols_lower)))
    data = data.rename(columns={"weight": "weight_kg", "height": "height_cm"})
    return data


def standardise_object_columns(data: pd.DataFrame) -> pd.DataFrame:
    object_cols = data.select_dtypes(include=["object"])
    for col in object_cols:
        data[col] = data[col].str.title()
    data["event"] = data["event"].str.capitalize()
    data["noc"] = data["noc"].str.upper()
    return data


def drop_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    data = data.drop_duplicates()
    data.reset_index(drop=True, inplace=True)
    return data


def fill_missing_values(data: pd.DataFrame) -> pd.DataFrame:
    # -- Issue, needs resolving --
    # Use sport groups to impute missing age, height, weight
    data["age"] = data.groupby("sport")["age"].transform(
         lambda x: x.fillna(x.mean())
    )
    data["height_cm"] = data.groupby("sport")["height_cm"].transform(
        lambda x: x.fillna(x.mean())
    )
    data["weight_kg"] = data.groupby("sport")["weight_kg"].transform(
        lambda x: x.fillna(x.mean())
    )
    data["medal"] = data["medal"].fillna("No Medal")

    # For sports with entirely missing height, weight
    # fill using overall averages
    data["height_cm"] = data["height_cm"].fillna(data["height_cm"].mean())
    data["weight_kg"] = data["weight_kg"].fillna(data["weight_kg"].mean())
    return data
