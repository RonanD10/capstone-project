import pandas as pd

from sqlalchemy import Connection, text
from config.db_config import load_db_config, DatabaseConfigError
from src.utils.db_utils import (
    get_db_connection,
    DatabaseConnectionError,
    QueryExecutionError,
)
from src.utils.logging_utils import setup_logger


logger = setup_logger("load_data", "load_data.log")

TABLE_NAME = "transactions_by_customers"


def log_table_action(
    connection: Connection, table_name: str, schema: str = "public"
) -> bool:
    """
    Check if table exists and log the appropriate action.

    Args:
        connection (Connection): Database connection object.
        table_name (str): Name of the table to check.
        schema (str): Schema name to check in.

    Returns:
        bool: True if table exists, False otherwise.
    """
    result = connection.execute(
        text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE"
            " table_name = :table_name AND table_schema = :schema)"
        ),
        {"table_name": table_name, "schema": schema},
    ).scalar()

    table_exists = bool(result) if result is not None else False

    if table_exists:
        logger.info(f"Appending data to existing {table_name} table...")
    else:
        logger.info(f"Creating new {table_name} table...")

    return table_exists


def create_transactions_by_customers(transformed_data: pd.DataFrame) -> None:
    """
    Load the transformed data into the target database.
    Creates new table if it doesn't exist, or appends to existing table.

    Args:
        transformed_data (pd.DataFrame):
            The DataFrame containing the transformed data.

    Raises:
        QueryExecutionError: If any database operation fails, including:
            - Database configuration errors
            - Connection failures
            - SQL execution errors
    """
    # Validate input data - return early if empty
    if transformed_data.empty:
        logger.warning("No data to load - DataFrame is empty")
        return

    connection: Connection | None = None
    try:
        # Get a connection to the target database
        connection_details = load_db_config()["target_database"]
        connection = get_db_connection(connection_details)

        # Check if table exists and log action
        schema = connection_details.get("schema", "public")
        table_exists = log_table_action(connection, TABLE_NAME, schema)

        # Load the transformed data into the target database
        if_exists_mode = "append" if table_exists else "replace"
        transformed_data.to_sql(
            TABLE_NAME,
            con=connection,
            if_exists=if_exists_mode,
            index=False,
        )
        # Explicitly commit the transaction
        connection.commit()

        action = "appended to" if table_exists else "created and loaded into"
        logger.info(f"Data successfully {action} {TABLE_NAME} table.")

    except DatabaseConfigError as e:
        logger.error(f"Target database not configured correctly: {e}")
        raise QueryExecutionError(f"Database configuration error: {e}")
    except DatabaseConnectionError as e:
        logger.error(
            "Failed to connect to the database when creating merged table:"
            f" {e}"
        )
        raise QueryExecutionError(f"Database connection failed: {e}")
    except pd.errors.DatabaseError as e:
        logger.error(f"Failed to create merged data table: {e}")
        raise QueryExecutionError(f"Failed to execute query: {e}")
    finally:
        if connection and hasattr(connection, "close"):
            connection.close()
