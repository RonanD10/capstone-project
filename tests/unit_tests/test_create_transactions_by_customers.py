import pytest
import pandas as pd
from unittest.mock import Mock, patch
from src.load.create_transactions_by_customers import (
    log_table_action,
    create_transactions_by_customers,
    TABLE_NAME,
)
from config.db_config import DatabaseConfigError
from src.utils.db_utils import DatabaseConnectionError, QueryExecutionError


class TestLogTableAction:
    """Unit tests for log_table_action function"""

    def test_log_table_action_table_exists(self):
        """Test logging when table exists"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = True
        mock_connection.execute.return_value = mock_result

        with patch(
            "src.load.create_transactions_by_customers.logger"
        ) as mock_logger:
            result = log_table_action(mock_connection, "test_table")

        assert result is True
        mock_logger.info.assert_called_once_with(
            "Appending data to existing test_table table..."
        )

    def test_log_table_action_table_not_exists(self):
        """Test logging when table doesn't exist"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = False
        mock_connection.execute.return_value = mock_result

        with patch(
            "src.load.create_transactions_by_customers.logger"
        ) as mock_logger:
            result = log_table_action(mock_connection, "test_table")

        assert result is False
        mock_logger.info.assert_called_once_with(
            "Creating new test_table table..."
        )

    def test_log_table_action_scalar_returns_none(self):
        """Test handling when scalar returns None"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = None
        mock_connection.execute.return_value = mock_result

        with patch(
            "src.load.create_transactions_by_customers.logger"
        ) as mock_logger:
            result = log_table_action(mock_connection, "test_table")

        assert result is False
        mock_logger.info.assert_called_once_with(
            "Creating new test_table table..."
        )

    def test_log_table_action_executes_correct_query(self):
        """Test that correct SQL query is executed"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = True
        mock_connection.execute.return_value = mock_result

        log_table_action(mock_connection, "test_table")

        # Verify execute was called with correct parameters
        mock_connection.execute.assert_called_once()
        call_args = mock_connection.execute.call_args
        assert "table_name" in str(call_args)
        assert "test_table" in str(call_args)

    def test_log_table_action_with_special_characters(self):
        """Test table name with special characters"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = False
        mock_connection.execute.return_value = mock_result

        with patch(
            "src.load.create_transactions_by_customers.logger"
        ) as mock_logger:
            result = log_table_action(mock_connection, "test_table_123")

        assert result is False
        mock_logger.info.assert_called_once_with(
            "Creating new test_table_123 table..."
        )

    def test_log_table_action_database_error(self):
        """Test handling of database errors during table check"""
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            log_table_action(mock_connection, "test_table")

    def test_log_table_action_scalar_exception(self):
        """Test handling when scalar() raises exception"""
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.scalar.side_effect = Exception("Scalar error")
        mock_connection.execute.return_value = mock_result

        with pytest.raises(Exception, match="Scalar error"):
            log_table_action(mock_connection, "test_table")


class TestCreateTransactionsByCustomers:
    """Unit tests for create_transactions_by_customers function"""

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

    def test_create_transactions_by_customers_empty_dataframe(
        self, empty_dataframe
    ):
        """Test early return with empty DataFrame"""
        with patch(
            "src.load.create_transactions_by_customers.logger"
        ) as mock_logger:
            create_transactions_by_customers(empty_dataframe)

        mock_logger.warning.assert_called_once_with(
            "No data to load - DataFrame is empty"
        )

    @patch("src.load.create_transactions_by_customers.log_table_action")
    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_success_new_table(
        self,
        mock_load_config,
        mock_get_connection,
        mock_log_action,
        sample_dataframe,
    ):
        """Test successful data load to new table"""
        # Setup mocks
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
                "schema": "public",
            }
        }
        mock_connection = Mock()
        mock_connection.close = Mock()
        mock_get_connection.return_value = mock_connection
        mock_log_action.return_value = False  # Table doesn't exist

        with patch.object(sample_dataframe, "to_sql") as mock_to_sql:
            with patch(
                "src.load.create_transactions_by_customers.logger"
            ) as mock_logger:
                create_transactions_by_customers(sample_dataframe)

        # Verify calls
        mock_log_action.assert_called_once_with(
            mock_connection, TABLE_NAME, "public"
        )
        mock_to_sql.assert_called_once_with(
            TABLE_NAME, con=mock_connection, if_exists="replace", index=False
        )
        # Check that success message was logged
        success_calls = [
            call
            for call in mock_logger.info.call_args_list
            if "Data successfully created and loaded into" in str(call)
        ]
        assert len(success_calls) == 1
        mock_connection.close.assert_called_once()

    @patch("src.load.create_transactions_by_customers.log_table_action")
    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_success_existing_table(
        self,
        mock_load_config,
        mock_get_connection,
        mock_log_action,
        sample_dataframe,
    ):
        """Test successful data load to existing table"""
        # Setup mocks
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
            }
        }
        mock_connection = Mock()
        mock_connection.close = Mock()
        mock_get_connection.return_value = mock_connection
        mock_log_action.return_value = True  # Table exists

        with patch.object(sample_dataframe, "to_sql") as mock_to_sql:
            with patch(
                "src.load.create_transactions_by_customers.logger"
            ) as mock_logger:
                create_transactions_by_customers(sample_dataframe)

        # Verify calls
        mock_log_action.assert_called_once_with(
            mock_connection, TABLE_NAME, "public"
        )
        mock_to_sql.assert_called_once_with(
            TABLE_NAME, con=mock_connection, if_exists="append", index=False
        )
        # Check that success message was logged
        success_calls = [
            call
            for call in mock_logger.info.call_args_list
            if "Data successfully appended to" in str(call)
        ]
        assert len(success_calls) == 1
        mock_connection.close.assert_called_once()

    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_database_config_error(
        self, mock_load_config, sample_dataframe
    ):
        """Test handling of DatabaseConfigError"""
        mock_load_config.side_effect = DatabaseConfigError("Config error")

        with pytest.raises(
            QueryExecutionError, match="Database configuration error"
        ):
            create_transactions_by_customers(sample_dataframe)

    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_connection_error(
        self, mock_load_config, mock_get_connection, sample_dataframe
    ):
        """Test handling of DatabaseConnectionError"""
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
            }
        }
        mock_get_connection.side_effect = DatabaseConnectionError(
            "Connection failed"
        )

        with pytest.raises(
            QueryExecutionError, match="Database connection failed"
        ):
            create_transactions_by_customers(sample_dataframe)

    @patch("src.load.create_transactions_by_customers.log_table_action")
    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_database_error(
        self,
        mock_load_config,
        mock_get_connection,
        mock_log_action,
        sample_dataframe,
    ):
        """Test handling of pandas DatabaseError"""
        # Setup mocks
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
            }
        }
        mock_connection = Mock()
        mock_connection.close = Mock()
        mock_get_connection.return_value = mock_connection
        mock_log_action.return_value = False

        # Mock to_sql to raise DatabaseError
        with patch.object(
            sample_dataframe,
            "to_sql",
            side_effect=pd.errors.DatabaseError("SQL error"),
        ):

            with pytest.raises(
                QueryExecutionError, match="Failed to execute query"
            ):
                create_transactions_by_customers(sample_dataframe)

            # Verify connection is still closed
            mock_connection.close.assert_called_once()

    @patch("src.load.create_transactions_by_customers.log_table_action")
    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_connection_clean_no_close_method(
        self,
        mock_load_config,
        mock_get_connection,
        mock_log_action,
        sample_dataframe,
    ):
        """Test connection cleanup when connection has no close method"""
        # Setup mocks
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
            }
        }
        mock_connection = Mock()
        del mock_connection.close  # Remove close method
        mock_get_connection.return_value = mock_connection
        mock_log_action.return_value = False

        with patch.object(sample_dataframe, "to_sql"):
            # Should not raise exception even without close method
            create_transactions_by_customers(sample_dataframe)

    @patch("src.load.create_transactions_by_customers.log_table_action")
    @patch("src.load.create_transactions_by_customers.get_db_connection")
    @patch("src.load.create_transactions_by_customers.load_db_config")
    def test_create_transactions_by_customers_connection_is_none(
        self,
        mock_load_config,
        mock_get_connection,
        mock_log_action,
        sample_dataframe,
    ):
        """Test handling when connection is None"""
        # Setup mocks
        mock_load_config.return_value = {
            "target_database": {
                "dbname": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "host": "localhost",
                "port": "5432",
            }
        }
        mock_get_connection.return_value = None

        with pytest.raises(AttributeError):
            create_transactions_by_customers(sample_dataframe)
