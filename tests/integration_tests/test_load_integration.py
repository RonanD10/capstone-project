import pandas as pd
from src.extract.extract import extract_data
from src.transform.transform import transform_data
from src.load.load import load_data
from src.utils.db_utils import get_db_connection
from config.db_config import load_db_config
from sqlalchemy import text
from src.load.create_transactions_by_customers import TABLE_NAME

"""
Integration Tests for Load Process

These tests validate the load process with actual database operations,
testing real database interactions and data persistence.

Test Coverage:
1. Database Table Creation: Verifies actual table creation in database
2. Data Persistence: Tests that data is actually written and retrievable
3. Upsert Operations: Validates append behaviour with existing tables
4. Performance: Tests load performance with realistic data volumes
5. Data Integrity: Ensures data types and constraints are maintained

Integration Tests focus on:
- Real database operations (not mocked)
- Data persistence validation
- Table schema verification
- Performance with actual database I/O
- Cross-transaction consistency
"""


class TestLoadIntegration:
    """Integration tests for load process with actual database"""

    def test_load_creates_table_in_database(self, clean_target_table):
        """Test that load creates table with real ETL data"""
        # Run full ETL process
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)

        # If no real data, create test data to ensure integration test can run
        if transformed_data.empty:
            transformed_data = pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "total_spent": [600.0, 700.0, 800.0],
                    "avg_transaction_value": [200.0, 233.33, 266.67],
                }
            )

        load_data(transformed_data)

        # Verify expected behavior based on data
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            schema = config.get("schema", "public")
            table_exists = connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = :table_name AND table_schema = :schema)"
                ),
                {"table_name": TABLE_NAME, "schema": schema},
            ).scalar()

            # Should have created table with data
            assert table_exists, f"Table '{TABLE_NAME}' was not created"
            count_result = connection.execute(
                text(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            ).scalar()
            assert count_result == len(
                transformed_data
            ), f"Expected {len(transformed_data)} rows, got {count_result}"

        finally:
            connection.close()

    def test_load_appends_to_existing_table(self, clean_target_table):
        """Test that subsequent loads append data"""
        # First ETL run
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)
        if transformed_data.empty:
            transformed_data = pd.DataFrame(
                {
                    "customer_id": [1, 2],
                    "total_spent": [600.0, 700.0],
                    "avg_transaction_value": [200.0, 233.33],
                }
            )
        load_data(transformed_data)

        # Get initial count
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            initial_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert (
                initial_count is not None
            ), "Initial count is None, table may not exist or be accessible"

            # Second load with same data (simulating incremental load)
            load_data(transformed_data)

            # Verify data was appended
            final_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert (
                final_count is not None
            ), "Final count is None, table may not exist or be accessible"
            assert (
                final_count == initial_count * 2
            ), f"Expected {initial_count * 2}, got {final_count}"
        finally:
            connection.close()

    def test_load_preserves_data_types(self, setup_target_table):
        """Test that data types are preserved in database"""
        # Table already exists with data from fixture

        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            # Query data back and verify types for filtered data
            result = connection.execute(
                text(
                    (
                        "SELECT customer_id, total_spent, "
                        "avg_transaction_value FROM transactions_by_customers "
                        "LIMIT 1"
                    )
                )
            ).fetchone()

            assert result is not None and isinstance(
                result[0], int
            )  # customer_id
            assert result is not None and isinstance(
                result[1], float
            )  # total_spent
            assert result is not None and isinstance(
                result[2], float
            )  # avg_transaction_value
        finally:
            connection.close()

    def test_load_performance_with_real_dataset(self, clean_target_table):
        """Test load performance with real dataset"""
        import time

        # Run ETL process and measure performance
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)

        if transformed_data.empty:
            transformed_data = pd.DataFrame(
                {
                    "customer_id": list(range(1, 101)),
                    "total_spent": [600.0 + i for i in range(100)],
                    "avg_transaction_value": [200.0 + i for i in range(100)],
                }
            )

        start_time = time.time()
        load_data(transformed_data)
        execution_time = time.time() - start_time

        # Should complete within 5 seconds
        assert (
            execution_time < 5.0
        ), f"Load took {execution_time:.2f}s, expected <5s"

        # Verify all filtered data was loaded
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            count_result = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            # Should match the filtered data count (high-value customers)
            assert count_result == len(
                transformed_data
            ), f"Expected {len(transformed_data)}, got {count_result}"
            assert count_result is not None and count_result > 0
        finally:
            connection.close()

    def test_load_handles_empty_data(self, clean_target_table):
        """Test load behaviour with empty DataFrame"""
        empty_df = pd.DataFrame()

        # Should handle gracefully
        load_data(empty_df)

        # Verify no table was created
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            table_exists = connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'transactions_by_customers')"
                )
            ).scalar()
            assert table_exists is False
        finally:
            connection.close()
