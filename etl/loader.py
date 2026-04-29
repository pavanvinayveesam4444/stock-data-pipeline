"""
LOADER - Step 3 of ETL Pipeline
Job: Save processed stock data into SQLite database permanently.
"""

import sqlite3
import pandas as pd
import logging
import os
from config import DATABASE_PATH

logger = logging.getLogger(__name__)
def create_connection() -> sqlite3.Connection:
    """
    Open a connection to the SQLite database.
    
    If the database file doesn't exist, SQLite creates it automatically.
    If the database folder doesn't exist, we create it first.
    
    Returns:
        Connection object to the database
    """
    
    # Create the database folder if it doesn't exist
    db_folder = os.path.dirname(DATABASE_PATH)
    if db_folder and not os.path.exists(db_folder):
        os.makedirs(db_folder)
        logger.info(f"Created database folder: {db_folder}")
    
    # Connect to the database (creates the file if it doesn't exist)
    conn = sqlite3.connect(DATABASE_PATH)
    logger.info(f"Connected to database: {DATABASE_PATH}")
    return conn
def save_stock_data(conn: sqlite3.Connection, ticker: str, data: pd.DataFrame) -> int:
    """
    Save processed stock data into the database.
    
    Each stock gets its own table named after the ticker.
    Example: AAPL data → "AAPL" table
             TSLA data → "TSLA" table
    
    Args:
        conn: Database connection
        ticker: Stock symbol like "AAPL"
        data: Processed DataFrame with all indicators
    
    Returns:
        Number of new rows saved
    """
    
    try:
        # Count rows before saving
        rows_before = count_rows(conn, ticker)
        
        # Save data to database
        # if_exists='append' means: add to existing data, don't delete old data
        data.to_sql(ticker, conn, if_exists="append", index=True)
        
        # Count rows after saving
        rows_after = count_rows(conn, ticker)
        new_rows = rows_after - rows_before
        
        logger.info(f"Saved {new_rows} new rows for {ticker} to database")
        return new_rows
        
    except Exception as e:
        logger.error(f"Failed to save data for {ticker}: {e}")
        return 0
def count_rows(conn: sqlite3.Connection, ticker: str) -> int:
    """
    Count how many rows exist in a stock's table.
    
    Used to calculate how many NEW rows were added after saving.
    Returns 0 if the table doesn't exist yet.
    """
    
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM '{ticker}'")
        count = cursor.fetchone()[0]
        return count
    except sqlite3.OperationalError:
        # Table doesn't exist yet - that's fine, return 0
        return 0
def check_duplicates(conn: sqlite3.Connection, ticker: str, data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that already exist in the database.
    
    This prevents saving the same day's data twice if the pipeline
    runs multiple times on the same day.
    
    Example:
        Database already has: Apr 1, Apr 2, Apr 3
        New data has: Apr 3, Apr 4, Apr 5
        After dedup: only Apr 4, Apr 5 get saved
    """
    
    try:
        # Get all dates already in the database for this ticker
        existing_dates = pd.read_sql(
            f"SELECT Date FROM '{ticker}'",
            conn,
            parse_dates=["Date"]
        )
        
        if existing_dates.empty:
            # No existing data — save everything
            return data
        
        # Keep only rows whose dates are NOT already in the database
        existing_date_list = existing_dates["Date"].dt.date.tolist()
        data_dates = data.index.date
        
        new_data = data[~pd.Series(data_dates, index=data.index).isin(existing_date_list)]
        
        duplicates_removed = len(data) - len(new_data)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows for {ticker}")
        
        return new_data
        
    except Exception:
        # If anything goes wrong with duplicate check, just return all data
        return data
def get_stock_data(conn: sqlite3.Connection, ticker: str,
                   start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Retrieve saved stock data from the database.
    
    Args:
        conn: Database connection
        ticker: Stock symbol like "AAPL"
        start_date: Optional start date like "2026-01-01"
        end_date: Optional end date like "2026-04-30"
    
    Returns:
        DataFrame with the requested data, or empty DataFrame if none found
    
    Example:
        get_stock_data(conn, "AAPL")                          → all AAPL data
        get_stock_data(conn, "AAPL", start_date="2026-01-01") → AAPL from Jan 1
    """
    
    try:
        # Build the SQL query
        query = f"SELECT * FROM '{ticker}'"
        
        # Add date filters if provided
        if start_date and end_date:
            query += f" WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            query += f" WHERE Date >= '{start_date}'"
        elif end_date:
            query += f" WHERE Date <= '{end_date}'"
        
        # Add ordering
        query += " ORDER BY Date ASC"
        
        # Execute query and return as DataFrame
        data = pd.read_sql(query, conn, parse_dates=["Date"], index_col="Date")
        logger.info(f"Retrieved {len(data)} rows for {ticker}")
        return data
        
    except Exception as e:
        logger.error(f"Could not retrieve data for {ticker}: {e}")
        return pd.DataFrame()
def get_all_tickers(conn: sqlite3.Connection) -> list[str]:
    """
    Get a list of all stocks currently stored in the database.
    
    Returns:
        List of ticker symbols like ["AAPL", "TSLA", "JPM"]
    """
    
    try:
        # SQLite stores table names in a special system table called sqlite_master
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tickers = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(tickers)} tickers in database: {tickers}")
        return tickers
        
    except Exception as e:
        logger.error(f"Could not get ticker list: {e}")
        return []
def close_connection(conn: sqlite3.Connection) -> None:
    """
    Safely close the database connection.
    
    Always close the connection when you're done.
    Leaving it open wastes memory and can cause issues.
    """
    
    conn.close()
    logger.info("Database connection closed")