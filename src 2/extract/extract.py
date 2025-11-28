import os
import logging
import pandas as pd
import timeit
from src.utils.logging_utils import setup_logger, log_extract_success

# Define the file path for the CSV file
FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "data",
    "raw",
    "unclean_data.csv",
)

# Configure the logger
logger = setup_logger(__name__, "extract_data.log", level=logging.DEBUG)

EXPECTED_PERFORMANCE = 0.0001

TYPE = "DATA from CSV"


def extract_data() -> pd.DataFrame:
    """
    Extract data from CSV file with performance logging.

    Returns:
        DataFrame containing records from CSV file.

    Raises:
        Exception: If CSV file cannot be loaded.
    """
    logger.info("Starting data extraction process")

    start_time = timeit.default_timer()

    try:
        extracted_data = pd.read_csv(FILE_PATH)
        extract_data_execution_time = timeit.default_timer() - start_time
        log_extract_success(
            logger,
            TYPE,
            extracted_data.shape,
            extract_data_execution_time,
            EXPECTED_PERFORMANCE,
        )

        logger.info(
            f"Data extraction completed successfully "
        )

        return extracted_data
    except Exception as e:
        logger.error(f"Error loading {FILE_PATH}: {e}")
        raise Exception(f"Failed to load CSV file: {FILE_PATH}")
    
extract_data()