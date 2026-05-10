"""Tests for loader.py"""
import pytest
import pandas as pd
import numpy as np
import os
import sqlite3
from etl.loader import (
    create_connection,
    save_stock_data,
    get_stock_data,
    get_all_tickers,
    count_rows,
    close_connection
)

TEST_DB = "database/test_stocks.db"


def create_sample_data(days=30):
    """Helper: create sample processed stock data"""
    dates = pd.date_range(start="2026-01-01", periods=days, freq="D")
    data = pd.DataFrame({
        "Close": np.random.uniform(100, 200, days),
        "daily_return": np.random.uniform(-0.05, 0.05, days),
        "ma_7": np.random.uniform(100, 200, days),
        "ma_30": np.random.uniform(100, 200, days),
        "volatility": np.random.uniform(0.01, 0.05, days),
        "rsi": np.random.uniform(30, 70, days)
    }, index=dates)
    data.index.name = "Date"    # ← THIS IS THE FIX
    return data


@pytest.fixture
def test_conn():
    """Create a test database connection"""
    os.makedirs("database", exist_ok=True)
    conn = sqlite3.connect(TEST_DB)
    yield conn
    conn.close()
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_database_created(test_conn):
    """Database connection should be created successfully"""
    assert test_conn is not None


def test_save_and_retrieve(test_conn):
    """Save data then retrieve it — should match"""
    data = create_sample_data(30)
    save_stock_data(test_conn, "AAPL", data)
    retrieved = get_stock_data(test_conn, "AAPL")
    assert len(retrieved) == 30


def test_no_duplicates(test_conn):
    """Saving same data twice should not create duplicates"""
    data = create_sample_data(30)
    save_stock_data(test_conn, "AAPL", data)
    save_stock_data(test_conn, "AAPL", data)
    total_rows = count_rows(test_conn, "AAPL")
    assert total_rows == 30


def test_multiple_tickers(test_conn):
    """Multiple stocks should be stored separately"""
    apple_data = create_sample_data(30)
    tesla_data = create_sample_data(25)
    save_stock_data(test_conn, "AAPL", apple_data)
    save_stock_data(test_conn, "TSLA", tesla_data)
    tickers = get_all_tickers(test_conn)
    assert "AAPL" in tickers
    assert "TSLA" in tickers


def test_get_all_tickers_empty(test_conn):
    """Empty database should return empty list"""
    tickers = get_all_tickers(test_conn)
    assert isinstance(tickers, list)