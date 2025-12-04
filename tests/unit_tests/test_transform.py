import pandas as pd
from unittest.mock import patch
import pytest
from src.etl.transform.transform import transform_data


class TestTransformData:
    @patch("src.etl.transform.transform.create_country_columns")
    @patch("src.etl.transform.transform.clean_noc_data")
    @patch("src.etl.transform.transform.clean_olympic_data")
    def test_transform_data_success(self, mock_clean_olympic, mock_clean_noc, mock_create_country):
        # Sample input data
        olympic_data = pd.DataFrame({"noc": ["USA", "GBR"]})
        noc_data = pd.DataFrame({"NOC": ["USA", "GBR"], "region": ["United States", "Great Britain"]})

        # Mock return values
        cleaned_olympic = pd.DataFrame({"noc": ["USA", "GBR"], "athlete": ["A", "B"]})
        cleaned_noc = pd.DataFrame({"NOC": ["USA", "GBR"], "region": ["United States", "Great Britain"]})
        final_df = pd.DataFrame({"noc": ["USA", "GBR"], "athlete": ["A", "B"], "country": ["United States", "Great Britain"]})

        mock_clean_olympic.return_value = cleaned_olympic
        mock_clean_noc.return_value = cleaned_noc
        mock_create_country.return_value = final_df

        result = transform_data(olympic_data, noc_data)

        mock_clean_olympic.assert_called_once_with(olympic_data)
        mock_clean_noc.assert_called_once_with(noc_data)
        mock_create_country.assert_called_once_with(cleaned_olympic, cleaned_noc)

        pd.testing.assert_frame_equal(result, final_df)
   
    @patch("src.etl.transform.transform.logger")
    @patch("src.etl.transform.transform.clean_olympic_data")
    def test_transform_data_exception(self, mock_clean_olympic, mock_logger):
        olympic_data = pd.DataFrame({"noc": ["USA"]})
        noc_data = pd.DataFrame({"NOC": ["USA"], "region": ["United States"]})
        mock_clean_olympic.side_effect = Exception("Data transformation failed")
        with pytest.raises(Exception, match="Data transformation failed"):
            transform_data(olympic_data, noc_data)

        mock_logger.error.assert_called_once()