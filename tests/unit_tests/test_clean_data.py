import pandas as pd
from unittest.mock import patch
from src.etl.transform.clean_data import (
    clean_data,
    standardise_column_names,
    standardise_object_columns, 
    drop_duplicates, 
    fill_missing_values,
)


class TestStandardiseColumnNames:
    def test_standardise_column_names(self):
        df = pd.DataFrame(
            {
                "Weight": [60],
                "NAME": ["John Smith"],
            }
        )
        result = standardise_column_names(df)
        assert list(result.columns) == ["weight_kg", "name"]


class TestStandardiseObjectColumns:
    def test_standardise_object_columns(self):
        df = pd.DataFrame(
            {
                "country": ["GREAT BRITAIN"],
            }
        )
        result = standardise_object_columns(df)
        assert result["country"][0] == "Great Britain"


class TestDropDuplicates:
    def test_drop_duplicates(self):
        df = pd.DataFrame(
            {
                "name": ["John Smith", "John Smith"],
            }
        )
        result = drop_duplicates(df)
        assert len(result) == 1


class TestFillMissingValues:
    def test_fill_missing_values(self):
        df = pd.DataFrame(
            {
                "age": [None, 12],
                "height_cm": [None, 180],
                "weight_kg": [67, None],
                "sport": ["Football", "Football"],
                "medal": ["Bronze", None]
            }
        )
        result = fill_missing_values(df)
        correct_result = pd.DataFrame(
            {
                "age": [12.0, 12.0],
                "height_cm": [180.0, 180.0],
                "weight_kg": [67.0, 67.0],
                "sport": ["Football", "Football"],
                "medal": ["Bronze", "No Medal"]
            }
        )
        assert result.equals(correct_result)


class TestCleanData:
    @patch("src.etl.transform.clean_data.save_dataframe_to_csv")
    def test_clean_data_full_pipeline(self, mock_save):
        df = pd.DataFrame(
            {
                "id": [1, 1, 3, 4, 5],
                "name": ["John Smith", "John Smith", "Running Man", "hans rob", "PETER BART"],
                "age": [25, 25, None, 35, 35],
                "country": ["United states", "United states", "Japan", "Canada", "Canada"],
                "weight": [45, 45, 67, 88, 90],
                "height": [180, 180, 122, 201, 178],
                "SPORT": ["Cycling", "Cycling", "Cycling", "Athletics", "Athletics"], 
                "medal": ["Gold", "Gold", None, "Silver", "Bronze"]
            }
        )

        result = clean_data(df)

        # Should impute missing age with average by sport group
        # Should standardise column names
        # Should standardise object columns
        # Should drop duplicate row 
        assert result["age"][1] == 25
        assert list(result.columns) == ["id", "name", "age", "country", "weight_kg", "height_cm", "sport", "medal"]
        assert len(result) == 4
        assert result["country"][0] == "United States"
        assert mock_save.called

    @patch("src.etl.transform.clean_data.save_dataframe_to_csv")
    def test_clean_customers_calls_save_function(self, mock_save):
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["John Smith", "Rob Jones"],
                "age": [25, 24],
                "country": ["France", "spain"],
                "weight_kg": [123, 56],
                "height_cm": [180, 167],
                "sport": ["Swimming", "Athletics"], 
                "medal": ["Gold", "Bronze"]
            }
        )

        clean_data(df)

        mock_save.assert_called_once()
        args, kwargs = mock_save.call_args
        assert args[1] == "data/processed"
        assert args[2] == "cleaned_data.csv"

