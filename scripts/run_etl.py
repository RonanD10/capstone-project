import os
import sys
from pathlib import Path
from config.env_config import setup_env
from src.etl.extract.extract import extract_data
from src.etl.load.load import load_data
from src.etl.transform.transform import transform_data
from src.utils.logging_utils import setup_logger


def main():
    
    # Setup ETL pipeline logger
    logger = setup_logger("etl_pipeline", "etl_pipeline.log")

    try:
        logger.info("Starting ETL pipeline")

        # Extract phase
        logger.info("Beginning data extraction phase")
        extracted_data = extract_data()
        logger.info("Data extraction phase completed")

        # Transformation phase
        logger.info("Beginning data transformation phase")
        transform_data(extracted_data)
        logger.info("Data transformation phase completed")

        # Load phase
        logger.info("Beginning data load phase")
        loaded_data = load_data()
        logger.info("Data load phase completed")

        logger.info("ETL pipeline completed successfully")
        # print(
        #     f"ETL pipeline run successfully in "
        #     f"{os.getenv('ENV', 'error')} environment!"
        # )
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
