import pandas as pd
from unittest.mock import patch
import pytest
from src.etl.extract.extract import extract_data


class TestExtractData:
    @patch("src.etl.extract.extract.extract_olympic_data")
    @patch("src.etl.extract.extract.extract_noc_data")
    def test_extract_data_success(
        self, mock_extract_noc, mock_extract_olympic
    ):
        df = pd.DataFrame(
            {
                "NOC": ["USA", "GBR"],
                "region": ["United States", "Great Britain"],
            }
        )

        # Mock each extract function to return a DataFrame
        mock_extract_noc.return_value = df
        mock_extract_olympic.return_value = df

        olympic_df, noc_df = extract_data()

        assert isinstance(olympic_df, pd.DataFrame)
        assert isinstance(noc_df, pd.DataFrame)
        assert len(olympic_df) == 2
        assert len(noc_df) == 2

        mock_extract_noc.assert_called_once()
        mock_extract_olympic.assert_called_once()

    def test_extract_data_function_exists(self):
        assert callable(extract_data)
    
    @patch("src.etl.extract.extract.extract_olympic_data")
    @patch("src.etl.extract.extract.logger")
    def test_extract_data_exception(self, mock_logger, mock_extract_olympic):
        mock_extract_olympic.side_effect = Exception("Data extraction failed")
        with pytest.raises(Exception) as e:
            extract_data()
        assert "Data extraction failed" in str(e.value)
        mock_logger.error.assert_called_once()
