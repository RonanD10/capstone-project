import pandas as pd
import pytest
import os

# from pathlib import Path
from src.transform.transform import transform_data
from src.load.load import load_data

# from src.extract.extract import extract_data
from src.utils.db_utils import get_db_connection
from config.db_config import load_db_config
from sqlalchemy import text


"""
Integration Tests for Transform-Load Pipeline

These tests validate the complete transform-load workflow,
testing how transformed data flows into the database correctly.

Test Coverage:
1. Transform-Load Pipeline: Validates transform → load produces correct
database state
2. Data Consistency: Ensures transformed data matches loaded data
3. Schema Validation: Tests that transform output matches load expectations
4. Performance: Validates end-to-end transform-load performance
5. Error Handling: Tests pipeline resilience across transform-load boundary

Integration Tests focus on:
- End-to-end transform-load workflow validation
- Data consistency between transform output and database state
- Schema compatibility validation
- Performance benchmarking with controlled data
- Error propagation across phase boundaries
"""


class TestTransformLoadIntegration:
    """Integration tests for transform-load pipeline"""

    @pytest.fixture
    def sample_extracted_data(self):
        """Provide controlled test data"""
        transactions = pd.DataFrame(
            {
                "transaction_id": [101, 102, 103, 104, 105, 106, 107],
                "customer_id": [1, 1, 2, 3, 4, 4, 5],
                "amount": [600.0, 400.0, 800.0, 300.0, 700.0, 200.0, 1000.0],
                "transaction_date": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-04",
                        "2023-01-05",
                        "2023-01-06",
                        "2023-01-07",
                    ]
                ),
            }
        )
        customers = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "customer_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "country": ["US", "UK", "CA", "US", "UK"],
                "is_active": [True, True, False, True, True],
                "age": [25, 30, 35, 40, 45],
            }
        )
        return [transactions, customers]

    @pytest.fixture
    def empty_extracted_data(self):
        """Provide empty test data"""
        transactions = pd.DataFrame(
            columns=[
                "transaction_id",
                "customer_id",
                "amount",
                "transaction_date",
            ]
        )
        transactions["transaction_date"] = pd.to_datetime(
            transactions["transaction_date"]
        )
        customers = pd.DataFrame(
            columns=[
                "customer_id",
                "customer_name",
                "country",
                "is_active",
                "age",
            ]
        )
        return [transactions, customers]

    @pytest.fixture
    def performance_timeout(self):
        """Get performance timeout from environment or default"""
        return float(os.environ.get("TEST_PERFORMANCE_TIMEOUT", "30.0"))

    @pytest.fixture
    def high_value_threshold(self):
        """Get high-value threshold from environment or default"""
        return float(os.environ.get("HIGH_VALUE_THRESHOLD", "500.0"))

    def test_transform_load_integration_full_pipeline(
        self, clean_target_table, sample_extracted_data
    ):
        """Test complete transform-load pipeline with controlled data"""
        # Transform data (returns filtered high-value customers)
        transformed_data = transform_data(sample_extracted_data)

        # Skip test if no high-value customers in sample data
        if transformed_data.empty:
            pytest.skip("No high-value customers in test data")

        # Load data
        load_data(transformed_data)

        # Verify data in database matches transformed results
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            # Check record count matches transformed data
            db_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert db_count == len(
                transformed_data
            ), f"Expected {len(transformed_data)} records, got {db_count}"

            # Verify schema matches filtered data structure
            db_columns = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'transactions_by_customers' "
                    "ORDER BY column_name"
                )
            ).fetchall()

            db_column_names = {col[0] for col in db_columns}
            expected_columns = set(transformed_data.columns)
            assert db_column_names == expected_columns, (
                f"Schema mismatch. Expected: {expected_columns},"
                f"Got: {db_column_names}"
            )
        finally:
            connection.close()

    def test_transform_load_data_consistency(
        self, clean_target_table, sample_extracted_data
    ):
        """Test that loaded data contains transformed data"""

        # Transform sample data
        transformed_data = transform_data(sample_extracted_data)

        # Skip test if no data to work with
        if transformed_data.empty:
            pytest.skip("No data after transformation")

        # Load data
        load_data(transformed_data)

        # Read back from database
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            # Get available columns
            columns_query = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'transactions_by_customers'"
                )
            ).fetchall()
            available_columns = [col[0] for col in columns_query]

            # Verify data was loaded correctly
            db_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert db_count == len(
                transformed_data
            ), f"Expected {len(transformed_data)} records, got {db_count}"

            # Check that key columns exist and have data
            if "customer_id" in available_columns:
                unique_customers = connection.execute(
                    text(
                        (
                            "SELECT COUNT(DISTINCT customer_id) FROM "
                            "transactions_by_customers"
                        )
                    )
                ).scalar()
                assert (
                    unique_customers is not None and unique_customers > 0
                ), "Should have customer data"

        finally:
            connection.close()

    def test_transform_load_performance_benchmark(
        self, clean_target_table, sample_extracted_data, performance_timeout
    ):
        """Test transform-load pipeline performance"""
        import time

        start_time = time.time()

        # Transform and load
        transformed_data = transform_data(sample_extracted_data)

        # Skip if no data to process
        if transformed_data.empty:
            pytest.skip("No data to benchmark")

        load_data(transformed_data)

        total_time = time.time() - start_time

        # Use configurable timeout
        assert total_time < performance_timeout, (
            f"Transform-load took {total_time:.2f}s, "
            f"expected <{performance_timeout}s"
        )

        # Verify data was processed
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert count is not None and count > 0
        finally:
            connection.close()

    def test_transform_load_handles_empty_transform_output(
        self, clean_target_table, empty_extracted_data
    ):
        """Test load behaviour when transform returns empty DataFrame"""

        # Transform empty data (should return empty DataFrame)
        empty_transformed = transform_data(empty_extracted_data)

        # Verify it's empty
        assert (
            empty_transformed.empty
        ), "Transform of empty data should return empty DataFrame"

        # Should handle gracefully
        load_data(empty_transformed)

        # Verify table state after loading empty data
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            table_exists = connection.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'transactions_by_customers')"
                )
            ).scalar()

            # If table exists, it should be empty
            if table_exists:
                count = connection.execute(
                    text("SELECT COUNT(*) FROM transactions_by_customers")
                ).scalar()
                assert (
                    count == 0
                ), "Table should be empty when loading empty data"
        finally:
            connection.close()

    def test_transform_load_data_quality_validation(
        self, clean_target_table, sample_extracted_data, high_value_threshold
    ):
        """Test that transform-load pipeline produces high-quality data"""

        # Run transform-load pipeline
        transformed_data = transform_data(sample_extracted_data)

        # Skip if no data to validate
        if transformed_data.empty:
            pytest.skip("No data to validate")

        load_data(transformed_data)

        # Validate data quality in database
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            # Check that all records have required fields
            null_check = connection.execute(
                text(
                    "SELECT COUNT(*) FROM transactions_by_customers "
                    "WHERE customer_id IS NULL"
                )
            ).scalar()
            assert null_check == 0, "Should not have null customer_id values"

            # Check for reasonable data ranges in filtered data
            total_spent_check = connection.execute(
                text(
                    "SELECT COUNT(*) FROM transactions_by_customers "
                    "WHERE total_spent <= 0"
                )
            ).scalar()
            assert (
                total_spent_check == 0
            ), "Should not have negative or zero total_spent values"

            # Check that all customers have spent more than the threshold
            # since this is filtered high-value customer data
            high_value_check = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM transactions_by_customers "
                    f"WHERE total_spent > {high_value_threshold}"
                )
            ).scalar()
            total_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()
            assert high_value_check == total_count, (
                f"All customers should be high-value "
                f"(>${high_value_threshold} total spent)"
            )

        finally:
            connection.close()

    def test_transform_load_incremental_processing(
        self, clean_target_table, sample_extracted_data
    ):
        """
        Test multiple transform-load cycles with different data
        """

        # First batch
        transformed_data = transform_data(sample_extracted_data)

        # Skip if no data
        if transformed_data.empty:
            pytest.skip("No data for incremental test")

        load_data(transformed_data)

        # Get initial count
        config = load_db_config()["target_database"]
        connection = get_db_connection(config)

        try:
            initial_count = connection.execute(
                text("SELECT COUNT(*) FROM transactions_by_customers")
            ).scalar()

            # Second batch with modified data (simulate new customers)
            incremental_transactions = sample_extracted_data[0].copy()
            incremental_customers = sample_extracted_data[1].copy()
            incremental_customers["customer_id"] = (
                incremental_customers["customer_id"] + 10
            )
            incremental_transactions["customer_id"] = (
                incremental_transactions["customer_id"] + 10
            )
            incremental_transactions["transaction_id"] = (
                incremental_transactions["transaction_id"] + 100
            )
            incremental_data = [
                incremental_transactions,
                incremental_customers,
            ]

            incremental_transformed = transform_data(incremental_data)

            if not incremental_transformed.empty:
                load_data(incremental_transformed)

                # Verify data was appended
                final_count = connection.execute(
                    text("SELECT COUNT(*) FROM transactions_by_customers")
                ).scalar()

                expected_count = (initial_count or 0) + len(incremental_transformed)
                assert (
                    final_count == expected_count
                ), f"Expected {expected_count}, got {final_count}"
        finally:
            connection.close()

    def test_transform_load_error_handling(self, clean_target_table):
        """Test error handling in transform-load pipeline"""

        # Test with malformed data
        malformed_data = [
            pd.DataFrame({"wrong_column": [1, 2, 3]}),
            pd.DataFrame({"wrong_column": [1, 2, 3]}),
        ]

        # Should raise appropriate error
        with pytest.raises((KeyError, AttributeError)):
            transform_data(malformed_data)

    def test_transform_load_with_missing_data(self, clean_target_table):
        """Test handling of missing required data"""

        # Test with missing customers data (only transactions, no customers)
        incomplete_data = [
            pd.DataFrame(
                {
                    "transaction_id": [101, 102],
                    "customer_id": [1, 2],
                    "amount": [600.0, 400.0],
                    "transaction_date": pd.to_datetime(
                        ["2023-01-01", "2023-01-02"]
                    ),
                }
            ),
            # Missing customers DataFrame
        ]

        # Should raise appropriate error
        with pytest.raises((KeyError, IndexError)):
            transform_data(incomplete_data)
