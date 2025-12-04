import pandas as pd
from src.utils.logging_utils import setup_logger
from src.etl.transform.clean_olympic_data import clean_olympic_data
from src.etl.transform.clean_noc_data import clean_noc_data
from src.etl.transform.enrich_data import create_country_columns


logger = setup_logger("transform_data", "transform_data.log")


def transform_data(
        olympic_data: pd.DataFrame,
        noc_data: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting data transformation process...")
        logger.info("Cleaning data...")
        cleaned_olympic_data = clean_olympic_data(olympic_data)
        logger.info("Data cleaned successfully.")
        logger.info("Cleaning NOC data...")
        cleaned_noc_data = clean_noc_data(noc_data)
        logger.info("NOC data cleaned successfully.")
        transformed_data = create_country_columns(
            cleaned_olympic_data,
            cleaned_noc_data
        )
        logger.info("Data cleaned successfully.")
        return transformed_data
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise
