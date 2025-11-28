import pandas as pd
import pytest
import timeit
from unittest.mock import patch
from src.extract.extract import (
    extract_data,
    EXPECTED_PERFORMANCE,
)

"""
Component Tests for extract_data() Function

These tests validate the extract_data() function as a complete component,
testing its behaviour with real file system interactions and actual data.

Test Coverage:
1. Data Integrity: Verifies extracted data matches expected raw CSV exactly
2. Performance: Ensures extraction meets <1 second requirement for 5200 rows
3. Error Handling: Tests proper exception handling for missing and corrupt files

Component tests differ from unit tests by:
- Using real file paths and actual CSV data
- Testing the complete function behaviour end-to-end
- Validating integration with file system and pandas library
- Ensuring production-like performance requirements are met
"""


@pytest.fixture
def expected_unclean_data():
    return pd.read_csv("data/raw/unclean_data.csv")


def test_extract_data_returns_correct_dataframe(
    expected_unclean_data,
):
    # Call the function to get the DataFrame
    df = extract_data()

    # Verify the DataFrame is the same as the expected unclean data
    pd.testing.assert_frame_equal(df, expected_unclean_data)


def test_extract_data_performance():
    execution_time = timeit.timeit(
        "extract_data()", globals=globals(), number=1
    )

    # Call the function to get the DataFrame
    df = extract_data()

    # Load time per row
    actual_execution_time_per_row = execution_time / df.shape[0]

    # Assert that the execution time is within an acceptable range
    assert actual_execution_time_per_row <= EXPECTED_PERFORMANCE, (
        f"Expected execution time to be less than or equal to "
        f"{str(EXPECTED_PERFORMANCE)} seconds, but got "
        f"{str(actual_execution_time_per_row)} seconds"
    )


@patch("src.extract.extract.FILE_PATH", "nonexistent_file.csv")
def test_extract_data_file_not_found():
    with pytest.raises(Exception, match="Failed to load CSV file"):
        extract_data()


def test_extract_data_corrupt_csv():
    corrupt_file_path = "tests/test_data/corrupt_csv.csv"

    with patch("src.extract.extract.FILE_PATH", corrupt_file_path):
        with pytest.raises(Exception, match="Failed to load CSV file"):
            extract_data()
