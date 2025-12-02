import pandas as pd
from src.utils.logging_utils import setup_logger
from src.etl.transform.clean_data import clean_data


logger = setup_logger("transform_data", "transform_data.log")


def transform_data(data) -> pd.DataFrame:
    try:
        logger.info("Starting data transformation process...")
        logger.info("Cleaning data...")
        clean_data(data)
        # enrich_data(data)
        # aggregate_data(data)
        logger.info("Data cleaned successfully.")
        return data
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise








