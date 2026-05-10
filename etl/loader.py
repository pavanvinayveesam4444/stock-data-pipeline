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
        # Table doesn't exist yet — that's fine, return 0
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
        # Check if table exists first
        cursor = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{ticker}'"
        )
        if cursor.fetchone() is None:
            # Table doesn't exist yet — no duplicates possible
            return data
        
        # Get all dates already in the database
        # Try both "Date" and "index" column names
        # SQLite stores index differently depending on how data was saved
        try:
            existing_dates_df = pd.read_sql(
                f"SELECT Date FROM '{ticker}'",
                conn,
                parse_dates=["Date"]
            )
            date_col = "Date"
        except Exception:
            try:
                existing_dates_df = pd.read_sql(
                    f'SELECT "index" FROM \'{ticker}\'',
                    conn,
                    parse_dates=["index"]
                )
                date_col = "index"
            except Exception:
                # Can't determine dates — return all data
                return data
        
        if existing_dates_df.empty:
            return data
        
        # Convert existing dates to a simple list
        existing_date_list = pd.to_datetime(
            existing_dates_df[date_col]
        ).dt.date.tolist()
        
        # Get dates from new data
        data_dates = pd.to_datetime(data.index).date
        
        # Keep only rows whose dates are NOT already in the database
        mask = ~pd.Series(data_dates, index=data.index).isin(existing_date_list)
        new_data = data[mask]
        
        duplicates_removed = len(data) - len(new_data)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows for {ticker}")
        
        return new_data
        
    except Exception as e:
        logger.warning(f"Duplicate check failed for {ticker}: {e}. Saving all data.")
        return data


def save_stock_data(conn: sqlite3.Connection, ticker: str, data: pd.DataFrame) -> int:
    """
    Save processed stock data into the database.
    Automatically checks for duplicates before saving.
    
    Args:
        conn: Database connection
        ticker: Stock symbol like "AAPL"
        data: Processed DataFrame with all indicators
    
    Returns:
        Number of new rows saved
    """
    
    try:
        # Step 1: CHECK DUPLICATES FIRST (remove already existing rows)
        clean_data = check_duplicates(conn, ticker, data)
        
        # Step 2: If nothing new to save, return 0
        if clean_data.empty:
            logger.info(f"No new data to save for {ticker} — all rows already exist")
            return 0
        
        # Step 3: Count rows before saving
        rows_before = count_rows(conn, ticker)
        
        # Step 4: Save the clean data (duplicates already removed)
        clean_data.to_sql(ticker, conn, if_exists="append", index=True)
        
        # Step 5: Count rows after saving
        rows_after = count_rows(conn, ticker)
        new_rows = rows_after - rows_before
        
        logger.info(f"Saved {new_rows} new rows for {ticker} to database")
        return new_rows
        
    except Exception as e:
        logger.error(f"Failed to save data for {ticker}: {e}")
        return 0


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
        get_stock_data(conn, "AAPL")                           → all AAPL data
        get_stock_data(conn, "AAPL", start_date="2026-01-01")  → AAPL from Jan 1
    """
    
    try:
        # First check what the date column is named in this table
        # It could be "Date" or "index" depending on how data was saved
        cursor = conn.execute(f"PRAGMA table_info('{ticker}')")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # Determine the date column name
        if "Date" in column_names:
            date_col = "Date"
        elif "index" in column_names:
            date_col = "index"
        else:
            date_col = column_names[0] if column_names else "Date"
        
        # Build the SQL query
        query = f"SELECT * FROM '{ticker}'"
        
        # Add date filters if provided
        if start_date and end_date:
            query += f" WHERE \"{date_col}\" BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            query += f" WHERE \"{date_col}\" >= '{start_date}'"
        elif end_date:
            query += f" WHERE \"{date_col}\" <= '{end_date}'"
        
        # Add ordering
        query += f" ORDER BY \"{date_col}\" ASC"
        
        # Execute query and return as DataFrame
        data = pd.read_sql(
            query,
            conn,
            parse_dates=[date_col],
            index_col=date_col
        )
        
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