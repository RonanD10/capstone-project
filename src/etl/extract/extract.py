import pandas as pd
from src.etl.extract.extract_olympic_data import extract_olympic_data
from src.etl.extract.extract_noc_data import extract_noc_data
from src.utils.logging_utils import setup_logger


logger = setup_logger("extract_data", "extract_data.log")


def extract_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        logger.info("Starting data extraction process")

        olympic_data = extract_olympic_data()
        noc_data = extract_noc_data()

        logger.info(
            f"Data extraction completed successfully - "
            f"Data: {olympic_data.shape}, NOC data: {noc_data.shape}"
        )

        return (olympic_data, noc_data)

    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise
