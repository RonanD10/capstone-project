import pytest
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from config.db_config import load_db_config
import logging

# Disable SQLAlchemy logging to reduce noise
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)

# Load test environment variables
project_root = Path(__file__).parent.parent.parent
test_env_path = project_root / ".env.test"
load_dotenv(test_env_path)


@pytest.fixture(scope="session", autouse=True)
def setup_test_transactions():
    """
    Set up the transactions table before integration tests in this module.
    This fixture runs once per test module for better performance.
    """
    config = load_db_config()
    source_db_config = config["source_database"]

    connection_string = (
        f"postgresql+psycopg2://{source_db_config['user']}"
        f":{source_db_config['password']}@{source_db_config['host']}"
        f":{source_db_config['port']}/{source_db_config['dbname']}"
    )

    engine = create_engine(
        connection_string,
        connect_args={
            "options": f"-csearch_path="
            f"{source_db_config.get('schema', 'public')}"
        },
    )
    sql_file_path = project_root / "data" / "raw" / "unclean_transactions.sql"

    try:
        with open(sql_file_path, "r") as file:
            sql_content = file.read()

        with engine.connect() as connection:
            # Drop table if exists and recreate
            connection.execute(text("DROP TABLE IF EXISTS transactions;"))
            connection.execute(text(sql_content))
            connection.commit()

    except Exception as e:
        # If there's an error, ensure we clean up properly
        with engine.connect() as connection:
            connection.rollback()
        raise RuntimeError(f"Failed to setup test transactions table: {e}")

    # Yield control to tests
    yield


@pytest.fixture(scope="function")
def clean_target_table():
    """
    Clean the target transactions_by_customers table before and after each test.
    This ensures test isolation by starting with a clean state.
    """
    from src.utils.db_utils import get_db_connection

    def cleanup_target_table():
        """Helper function to clean up target table"""
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)
        try:
            connection.execute(
                text("DROP TABLE IF EXISTS transactions_by_customers")
            )
            connection.commit()
        except Exception:
            # Ignore errors if table doesn't exist
            try:
                connection.rollback()
            except:
                pass
        finally:
            connection.close()

    def setup_source_data():
        """Helper function to ensure source data is available"""
        source_config = load_db_config()["source_database"]
        source_connection = get_db_connection(source_config)
        
        try:
            # Check if tables exist, if not create them
            tables_exist = source_connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'transactions') AND "
                    "EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'customers')"
                )
            ).scalar()
            
            if not tables_exist:
                # Drop and recreate with test data
                source_connection.execute(text("DROP TABLE IF EXISTS transactions CASCADE"))
                source_connection.execute(text("DROP TABLE IF EXISTS customers CASCADE"))
                
                sql_file_path = project_root / "data" / "raw" / "unclean_transactions.sql"
                with open(sql_file_path, "r") as file:
                    sql_content = file.read()
                source_connection.execute(text(sql_content))
                
                customers_sql_path = project_root / "data" / "raw" / "unclean_customers.sql"
                with open(customers_sql_path, "r") as file:
                    customers_sql = file.read()
                source_connection.execute(text(customers_sql))
                
                source_connection.commit()
        except Exception as e:
            print(f"Warning: Could not setup source data: {e}")
            try:
                source_connection.rollback()
            except:
                pass
        finally:
            source_connection.close()

    # Clean up before test
    cleanup_target_table()
    setup_source_data()
    
    yield
    
    # Clean up after test
    cleanup_target_table()


@pytest.fixture(scope="function")
def setup_target_table():
    """
    Set up the target transactions_by_customers table with test data.
    Used by tests that need an existing table to work with.
    """
    from src.utils.db_utils import get_db_connection
    import pandas as pd
    
    # Create test data and load it directly
    test_data = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "total_spent": [600.0, 700.0, 800.0],
            "avg_transaction_value": [200.0, 233.33, 266.67],
        }
    )
    
    config = load_db_config()["target_database"]
    connection = get_db_connection(config)
    try:
        # Create table directly using pandas to_sql
        test_data.to_sql(
            "transactions_by_customers",
            con=connection,
            if_exists="replace",
            index=False,
        )
        if hasattr(connection, 'commit'):
            connection.commit()
    except Exception as e:
        if hasattr(connection, 'rollback'):
            try:
                connection.rollback()
            except:
                pass
        raise RuntimeError(f"Failed to setup target table: {e}")
    finally:
        if hasattr(connection, 'close'):
            connection.close()
    
    yield
    
    # Clean up after test
    connection = get_db_connection(config)
    try:
        connection.execute(
            text("DROP TABLE IF EXISTS transactions_by_customers")
        )
        if hasattr(connection, 'commit'):
            connection.commit()
    except Exception:
        if hasattr(connection, 'rollback'):
            try:
                connection.rollback()
            except:
                pass
    finally:
        if hasattr(connection, 'close'):
            connection.close()
