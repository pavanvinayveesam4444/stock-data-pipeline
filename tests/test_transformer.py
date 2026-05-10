"""Tests for transformer.py"""
import pytest
import pandas as pd
import numpy as np
from etl.transformer import (
    flatten_columns,
    add_daily_returns,
    add_moving_averages,
    add_volatility,
    add_rsi,
    transform_stock_data
)


def create_sample_data(days=60):
    """Helper: create sample stock data for testing"""
    dates = pd.date_range(start="2026-01-01", periods=days, freq="D")
    data = pd.DataFrame({
        "Close": np.random.uniform(100, 200, days),
        "Open": np.random.uniform(100, 200, days),
        "High": np.random.uniform(150, 250, days),
        "Low": np.random.uniform(80, 150, days),
        "Volume": np.random.randint(1000000, 50000000, days)
    }, index=dates)
    return data


def test_daily_returns_calculated():
    """daily_return column should exist after transformation"""
    data = create_sample_data()
    result = add_daily_returns(data)
    assert "daily_return" in result.columns


def test_moving_averages_calculated():
    """ma_7 and ma_30 columns should exist"""
    data = create_sample_data()
    result = add_moving_averages(data)
    assert "ma_7" in result.columns
    assert "ma_30" in result.columns


def test_rsi_between_0_and_100():
    """RSI must always be between 0 and 100"""
    data = create_sample_data(60)
    result = add_rsi(data)
    rsi_values = result["rsi"].dropna()
    assert (rsi_values >= 0).all()
    assert (rsi_values <= 100).all()


def test_transform_adds_all_columns():
    """transform_stock_data should add all indicator columns"""
    data = create_sample_data(60)
    result = transform_stock_data(data)
    expected_columns = ["daily_return", "ma_7", "ma_30", "volatility", "rsi"]
    for col in expected_columns:
        assert col in result.columns


def test_no_crash_on_minimum_data():
    """Pipeline should not crash with small dataset"""
    data = create_sample_data(days=5)
    try:
        result = transform_stock_data(data)
    except Exception as e:
        pytest.fail(f"transform_stock_data crashed: {e}")