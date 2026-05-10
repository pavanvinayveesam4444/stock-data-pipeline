"""Tests for extractor.py"""
import pytest
import pandas as pd
from etl.extractor import fetch_stock_data, validate_data


def test_fetch_valid_ticker():
    """Valid ticker should return a DataFrame"""
    data = fetch_stock_data("AAPL", "5d")
    assert data is not None
    assert isinstance(data, pd.DataFrame)
    assert len(data) > 0


def test_fetch_invalid_ticker():
    """Invalid ticker should return None"""
    data = fetch_stock_data("XYZXYZ123", "5d")
    assert data is None


def test_data_has_required_columns():
    """Downloaded data must have OHLCV columns"""
    data = fetch_stock_data("AAPL", "5d")
    assert data is not None
    if isinstance(data.columns, pd.MultiIndex):
        columns = [col[0] for col in data.columns]
    else:
        columns = list(data.columns)
    required = ["Open", "High", "Low", "Close", "Volume"]
    for col in required:
        assert col in columns