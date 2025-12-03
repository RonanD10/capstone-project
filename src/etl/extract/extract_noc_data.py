import os
import logging
import pandas as pd
import timeit
from src.utils.logging_utils import setup_logger, log_extract_success

# Define the file path for the customers CSV file
FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "..",
    "data",
    "raw",
    "noc_regions.csv",
)

# Configure the logger
logger = setup_logger(__name__, "extract_data.log", level=logging.DEBUG)

EXPECTED_PERFORMANCE = 0.0001

TYPE = "NOC data from CSV"


def extract_noc_data() -> pd.DataFrame:
    """
    Extract noc data from CSV file with performance logging.

    Returns:
        DataFrame containing records from CSV file.

    Raises:
        Exception: If CSV file cannot be loaded.
    """
    start_time = timeit.default_timer()

    try:
        noc_data = pd.read_csv(FILE_PATH)
        extract_noc_data_execution_time = timeit.default_timer() - start_time
        log_extract_success(
            logger,
            TYPE,
            noc_data.shape,
            extract_noc_data_execution_time,
            EXPECTED_PERFORMANCE,
        )
        return noc_data
    except Exception as e:
        logger.error(f"Error loading {FILE_PATH}: {e}")
        raise Exception(f"Failed to load CSV file: {FILE_PATH}")
