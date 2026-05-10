"""
TRANSFORMER - Step 2 of ETL Pipeline
Job: Take raw stock data and calculate financial indicators.
"""

import pandas as pd
import numpy as np
import logging
from config import MA_SHORT, MA_LONG, RSI_PERIOD, VOLATILITY_WINDOW

logger = logging.getLogger(__name__)


def flatten_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Fix multi-level column names from yfinance.
    
    Converts: ('Close', 'AAPL') → 'Close'
    So we can access columns simply as data["Close"]
    """
    
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]
        logger.info("Flattened multi-level column names")
    
    return data


def add_daily_returns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate how much the price changed each day as a percentage.
    
    Example: Yesterday close = $100, Today close = $105
    Daily return = (105 - 100) / 100 = 0.05 = 5%
    """
    
    data["daily_return"] = data["Close"].pct_change()
    logger.info("Calculated daily returns")
    return data


def add_moving_averages(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate short-term and long-term moving averages.
    
    MA_SHORT (7-day): Shows short-term trend
    MA_LONG (30-day): Shows long-term trend
    
    When short MA crosses ABOVE long MA = bullish signal (price trending up)
    When short MA crosses BELOW long MA = bearish signal (price trending down)
    """
    
    data[f"ma_{MA_SHORT}"] = data["Close"].rolling(window=MA_SHORT).mean()
    data[f"ma_{MA_LONG}"] = data["Close"].rolling(window=MA_LONG).mean()
    logger.info(f"Calculated {MA_SHORT}-day and {MA_LONG}-day moving averages")
    return data


def add_volatility(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate how much the stock price jumps around (volatility).
    
    High volatility = risky, price swings wildly (like Tesla)
    Low volatility = stable, price barely moves (like Coca-Cola)
    
    Calculated as the standard deviation of daily returns over a rolling window.
    """
    
    # Make sure daily returns exist first
    if "daily_return" not in data.columns:
        data = add_daily_returns(data)
    
    data["volatility"] = data["daily_return"].rolling(window=VOLATILITY_WINDOW).std()
    logger.info(f"Calculated {VOLATILITY_WINDOW}-day volatility")
    return data


def add_rsi(data: pd.DataFrame, period: int = RSI_PERIOD) -> pd.DataFrame:
    """
    Calculate RSI (Relative Strength Index).
    
    RSI is a number between 0 and 100:
    - Above 70: stock might be OVERBOUGHT (too expensive, might drop)
    - Below 30: stock might be OVERSOLD (too cheap, might rise)
    - Between 30-70: normal range
    
    Steps:
    1. Calculate daily price changes
    2. Separate gains (positive) and losses (negative)
    3. Calculate average gain and average loss over the period
    4. RS = average gain / average loss
    5. RSI = 100 - (100 / (1 + RS))
    """
    
    # Step 1: Calculate daily price changes (not percentage, just dollar difference)
    delta = data["Close"].diff()
    
    # Step 2: Separate gains and losses
    gains = delta.copy()
    losses = delta.copy()
    gains[gains < 0] = 0      # keep only positive changes (gains)
    losses[losses > 0] = 0    # keep only negative changes (losses)
    losses = abs(losses)       # make losses positive for calculation
    
    # Step 3: Calculate average gain and average loss over the period
    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()
    
    # Step 4: Calculate RS (Relative Strength)
    rs = avg_gain / avg_loss
    
    # Step 5: Calculate RSI
    data["rsi"] = 100 - (100 / (1 + rs))
    
    logger.info(f"Calculated {period}-day RSI")
    return data


def transform_stock_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Master function: applies ALL transformations in order.
    
    Takes raw stock data (Open, High, Low, Close, Volume)
    and adds: daily_return, ma_7, ma_30, volatility, rsi
    """
    
    logger.info("Starting data transformation...")
    
    # First: fix column names (yfinance sometimes returns double-layered names)
    data = flatten_columns(data)
    
    data = add_daily_returns(data)
    data = add_moving_averages(data)
    data = add_volatility(data)
    data = add_rsi(data)
    
    # Drop rows where ALL new columns are NaN
    initial_rows = len(data)
    data = data.dropna(subset=["daily_return", f"ma_{MA_SHORT}", "volatility", "rsi"], how="all")
    dropped = initial_rows - len(data)
    
    if dropped > 0:
        logger.info(f"Dropped {dropped} rows with insufficient data")
    
    logger.info(f"Transformation complete: {len(data)} rows with {len(data.columns)} columns")
    return data