import pytest
from datetime import datetime
from common.date_helper import standardize_date_format


@pytest.mark.parametrize("date_input,expected", [
    ("1 January 2020", datetime(2020, 1, 1, 0, 0)), 
    ("25/05/2020", datetime(2020, 5, 25, 0, 0)), 
    ("2020-05-20", datetime(2020, 5, 20, 0, 0))])
def test_standardize_date_format_should_return_right_format(date_input, expected):
    assert standardize_date_format(date_input) == expected