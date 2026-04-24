"""
EXTRACTOR - Step 1 of ETL Pipeline
Job: Connect to Yahoo Finance and download raw stock price data.
"""

import yfinance as yf
import pandas as pd
import logging
import time
from config import MAX_RETRIES, RETRY_DELAY_SECONDS

# Create a logger for this file
logger = logging.getLogger(__name__)


def fetch_stock_data(ticker: str, period: str) -> pd.DataFrame | None:
    """
    Fetch stock data from Yahoo Finance for one stock.
    
    Args:
        ticker: Stock symbol like "AAPL", "TSLA", "JPM"
        period: How far back to fetch like "30d", "60d", "1y"
    
    Returns:
        DataFrame with stock data, or None if all attempts failed
    """
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching {ticker} data (attempt {attempt}/{MAX_RETRIES})")
            
            # Ask Yahoo Finance for the data
            data = yf.download(ticker, period=period, progress=False)
            
            # Check if we got anything back
            if data.empty:
                logger.error(f"No data returned for {ticker}. Is the ticker symbol correct?")
                return None
            
            logger.info(f"Successfully fetched {len(data)} rows for {ticker}")
            return data
            
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {ticker}: {e}")
            
            if attempt < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS} seconds before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(f"All {MAX_RETRIES} attempts failed for {ticker}")
                return None


def fetch_multiple_stocks(tickers: list[str], period: str) -> dict[str, pd.DataFrame]:
    """
    Fetch stock data for multiple stocks.
    
    Args:
        tickers: List of stock symbols like ["AAPL", "TSLA", "JPM"]
        period: How far back to fetch like "60d"
    
    Returns:
        Dictionary mapping ticker to its DataFrame
    """
    
    results = {}
    
    for ticker in tickers:
        data = fetch_stock_data(ticker, period)
        
        if data is not None:
            results[ticker] = data
        else:
            logger.warning(f"Skipping {ticker} — could not fetch data")
    
    logger.info(f"Successfully fetched {len(results)} out of {len(tickers)} stocks")
    return results


def validate_data(data: pd.DataFrame, ticker: str) -> bool:
    """
    Check if the downloaded data looks correct.
    
    Args:
        data: The downloaded stock data
        ticker: Stock symbol (for logging purposes)
    
    Returns:
        True if data is valid, False if something is wrong
    """
    
    # Check 1: Is the data empty?
    if data.empty:
        logger.error(f"Validation failed for {ticker}: data is empty")
        return False
    
    # Check 2: Does it have the required columns?
    required_columns = ["Open", "High", "Low", "Close", "Volume"]
    
    # Handle multi-level columns from yfinance
    if isinstance(data.columns, pd.MultiIndex):
        actual_columns = [col[0] for col in data.columns]
    else:
        actual_columns = list(data.columns)
    
    for col in required_columns:
        if col not in actual_columns:
            logger.error(f"Validation failed for {ticker}: missing column '{col}'")
            return False
    
    # Check 3: Does it have at least 1 row of data?
    if len(data) < 1:
        logger.error(f"Validation failed for {ticker}: no rows of data")
        return False
    
    # Check 4: Are there any completely empty rows?
    null_rows = data.isnull().all(axis=1).sum()
    if null_rows > 0:
        logger.warning(f"{ticker}: found {null_rows} completely empty rows")
    
    logger.info(f"Validation passed for {ticker}: {len(data)} rows, all columns present")
    return True