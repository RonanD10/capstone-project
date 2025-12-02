import logging
import os 
import timeit
import pandas as pd
from src.utils.logging_utils import setup_logger, log_extract_success
from src.etl.transform.clean_data import (
    OUTPUT_DIR, 
    FILE_NAME,
    )

FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..", 
    f"{OUTPUT_DIR}",
    f"{FILE_NAME}",
)

# Configure the logger
logger = setup_logger(__name__, "extract_data.log", level=logging.DEBUG)

EXPECTED_PERFORMANCE = 0.0001

TYPE = "DATA from CSV"

logger = setup_logger("load_data", "load_data.log")


def load_data() -> pd.DataFrame:
    """
    Load data from CSV file with performance logging.

    Returns:
        DataFrame containing records from CSV file.

    Raises:
        Exception: If CSV file cannot be loaded.
    """
    logger.info("Starting data load process...")

    start_time = timeit.default_timer()

    try:
        loaded_data = pd.read_csv(FILE_PATH)
        loaded_data_execution_time = timeit.default_timer() - start_time
        log_extract_success(
            logger,
            TYPE,
            loaded_data.shape,
            loaded_data_execution_time,
            EXPECTED_PERFORMANCE,
        )

        logger.info("Data load process completed successfully.")

        return loaded_data
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}")
        raise e
