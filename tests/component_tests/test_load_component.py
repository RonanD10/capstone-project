import pytest
import pandas as pd
from unittest.mock import patch
from src.load.load import load_data
from src.utils.db_utils import QueryExecutionError
import time

"""
Component Tests for load_data() Function

These tests validate the load_data() function as a component,
testing how it coordinates with the database loading functionality.

Test Coverage:
1. Input Validation: Verifies proper handling of None and empty DataFrames
2. Component Coordination: Tests integration with
   create_transactions_by_customers
3. Error Propagation: Ensures database errors are properly propagated
4. Logging Behaviour: Validates appropriate logging at component level

Component Tests focus on:
- Load coordinator behaviour and validation
- Integration with database loading components
- Error handling and propagation from database operations
- Logging coordination across load operations
"""


class TestLoadData:
    """Component tests for load_data function"""

    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "amount": [100.0, 200.0, 300.0],
                "transaction_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            }
        )

    @pytest.fixture
    def empty_dataframe(self):
        """Empty DataFrame for testing"""
        return pd.DataFrame()

    def test_load_data_success(self, sample_dataframe):
        """Test successful data loading with valid DataFrame"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            with patch("src.load.load.logger") as mock_logger:
                load_data(sample_dataframe)

        # Verify component coordination
        mock_create.assert_called_once_with(sample_dataframe)

        # Verify logging
        mock_logger.info.assert_any_call("Starting data load process...")
        mock_logger.info.assert_any_call(
            "Data load process completed successfully."
        )

    def test_load_data_empty_dataframe(self, empty_dataframe):
        """Test handling of empty DataFrame"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            with patch("src.load.load.logger") as mock_logger:
                load_data(empty_dataframe)

        # Verify early return behaviour
        mock_create.assert_not_called()
        mock_logger.warning.assert_called_once_with(
            "No data to load - DataFrame is empty or None"
        )

    def test_load_data_none_input(self):
        """Test handling of None input"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            with patch("src.load.load.logger") as mock_logger:
                load_data(None)

        # Verify early return behaviour
        mock_create.assert_not_called()
        mock_logger.warning.assert_called_once_with(
            "No data to load - DataFrame is empty or None"
        )

    def test_load_data_propagates_query_execution_error(
        self, sample_dataframe
    ):
        """Test that QueryExecutionError is properly propagated"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            mock_create.side_effect = QueryExecutionError(
                "Database operation failed"
            )

            with pytest.raises(
                QueryExecutionError, match="Database operation failed"
            ):
                load_data(sample_dataframe)

        # Verify the component was called
        mock_create.assert_called_once_with(sample_dataframe)

    def test_load_data_propagates_generic_exceptions(self, sample_dataframe):
        """Test that other exceptions are properly propagated"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            mock_create.side_effect = ValueError("Invalid data format")

            with pytest.raises(ValueError, match="Invalid data format"):
                load_data(sample_dataframe)

        # Verify the component was called
        mock_create.assert_called_once_with(sample_dataframe)

    def test_load_data_logs_start_before_component_call(
        self, sample_dataframe
    ):
        """Test that start logging occurs before component call"""
        call_order = []

        def mock_create_side_effect(data):
            call_order.append("create_called")

        def mock_log_side_effect(message):
            if "Starting data load process" in message:
                call_order.append("start_logged")

        with patch(
            "src.load.load.create_transactions_by_customers",
            side_effect=mock_create_side_effect,
        ):
            with patch("src.load.load.logger") as mock_logger:
                mock_logger.info.side_effect = mock_log_side_effect
                load_data(sample_dataframe)

        # Verify logging happens before component call
        assert call_order == ["start_logged", "create_called"]

    def test_load_data_with_dataframe_containing_nulls(self):
        """Test handling of DataFrame with null values"""
        df_with_nulls = pd.DataFrame(
            {
                "customer_id": [1, 2, None],
                "amount": [100.0, None, 300.0],
                "transaction_date": ["2023-01-01", "2023-01-02", None],
            }
        )

        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            with patch("src.load.load.logger") as mock_logger:
                load_data(df_with_nulls)

        # Should still process (not considered empty)
        mock_create.assert_called_once_with(df_with_nulls)
        mock_logger.info.assert_any_call("Starting data load process...")

    def test_load_data_component_coordination(self, sample_dataframe):
        """Test that load_data properly coordinates with database component"""
        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            # Simulate successful database operation
            mock_create.return_value = None

            with patch("src.load.load.logger") as mock_logger:
                result = load_data(sample_dataframe)

        # Verify coordination
        assert result is None  # Function returns None on success
        mock_create.assert_called_once_with(sample_dataframe)

        # Verify complete logging flow
        expected_calls = [
            "Starting data load process...",
            "Data load process completed successfully.",
        ]

        actual_calls = [
            call.args[0] for call in mock_logger.info.call_args_list
        ]
        assert all(expected in actual_calls for expected in expected_calls)

    def test_load_data_handles_database_timeout(self, sample_dataframe):
        """Test handling of database timeout scenarios"""

        def slow_operation(data):
            time.sleep(0.1)  # Simulate slow operation
            raise QueryExecutionError("Database timeout")

        with patch(
            "src.load.load.create_transactions_by_customers",
            side_effect=slow_operation,
        ):
            with pytest.raises(QueryExecutionError, match="Database timeout"):
                load_data(sample_dataframe)

    def test_load_data_validates_dataframe_structure(self):
        """Test handling of DataFrame with unexpected structure"""
        # DataFrame missing expected columns
        invalid_df = pd.DataFrame({"unexpected_column": [1, 2, 3]})

        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            # Should still call the function (validation happens downstream)
            load_data(invalid_df)
            mock_create.assert_called_once_with(invalid_df)

    def test_load_data_memory_efficiency_large_dataframe(self):
        """Test load behaviour with memory-intensive DataFrames"""
        # Create a larger DataFrame to test memory handling
        large_df = pd.DataFrame(
            {
                "customer_id": range(10000),
                "amount": [100.0] * 10000,
                "data": ["x" * 100] * 10000,  # Large string data
            }
        )

        with patch(
            "src.load.load.create_transactions_by_customers"
        ) as mock_create:
            with patch("src.load.load.logger") as mock_logger:
                load_data(large_df)

        # Verify it handles large data without issues
        mock_create.assert_called_once_with(large_df)
        mock_logger.info.assert_any_call("Starting data load process...")
