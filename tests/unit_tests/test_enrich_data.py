import pandas as pd
from unittest.mock import patch
from pandas.testing import assert_frame_equal
from src.etl.transform.enrich_data import create_country_columns


@patch("src.etl.transform.enrich_data.save_dataframe_to_csv")
def test_create_country_columns_maps_country_correctly(mock_save):
    olympic_data = pd.DataFrame({
        "noc": ["USA", "GBR", "FRA"]
    })

    noc_data = pd.DataFrame({
        "NOC": ["USA", "GBR", "FRA"],
        "region": ["United States", "Great Britain", "France"]
    })

    result = create_country_columns(olympic_data, noc_data)

    expected = pd.DataFrame({
        "noc": ["USA", "GBR", "FRA"],
        "country": ["United States", "Great Britain", "France"]
    })

    assert_frame_equal(result, expected)

    mock_save.assert_called_once()