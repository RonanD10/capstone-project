import pandas as pd
from unittest.mock import patch
from src.etl.transform.clean_noc_data import (
    clean_noc_data, 
)


class TestCleanNocData:
    @patch("src.etl.transform.clean_noc_data.save_dataframe_to_csv")
    def test_clean_noc_data(self, mock_save):
        df = pd.DataFrame(
            {
                "NOC": ["ROT"],
                "region": [None]
            }
        )
        result = clean_noc_data(df)
        assert result["region"][0] == "Refugee Olympic Team"
        assert mock_save.called
       
    @patch("src.etl.transform.clean_noc_data.save_dataframe_to_csv")
    def test_clean_noc_data_calls_save_function(self, mock_save):
        df = pd.DataFrame(
            {
                "NOC": ["ROT"],
                "region": [None]
            }
        )

        clean_noc_data(df)

        mock_save.assert_called_once()
        args, kwargs = mock_save.call_args
        assert args[1] == "data/processed"
        assert args[2] == "cleaned_noc_data.csv"
