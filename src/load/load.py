import pandas as pd
from typing import Optional
from src.load.create_transactions_by_customers import (
    create_transactions_by_customers,
)
from src.utils.logging_utils import setup_logger

logger = setup_logger("load_data", "load_data.log")


def load_data(transformed_data: Optional[pd.DataFrame]) -> None:
    """
    Load transformed data into the target database.

    Args:
        transformed_data (pd.DataFrame): The cleaned and transformed data to
        load.

    Raises:
        QueryExecutionError: If database operations fail.
    """
    # Validate input data
    if transformed_data is None or transformed_data.empty:
        logger.warning("No data to load - DataFrame is empty or None")
        return
    try:
        logger.info("Starting data load process...")
        create_transactions_by_customers(transformed_data)
        logger.info("Data load process completed successfully.")
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}")
        raise e
