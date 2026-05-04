"""
PIPELINE - Main Coordinator
Job: Runs the complete ETL process automatically every day.

Flow:
1. Read stock list from config
2. For each stock: Extract → Transform → Load
3. Generate summary report
4. Schedule to run daily at 6 PM
"""

import logging
import schedule
import time
from datetime import datetime
from etl.extractor import fetch_stock_data, fetch_multiple_stocks
from etl.transformer import transform_stock_data
from etl.loader import (
    create_connection,
    save_stock_data,
    get_all_tickers,
    close_connection
)
from config import STOCK_TICKERS, FETCH_PERIOD, RUN_TIME

logger = logging.getLogger(__name__)
def setup_logging() -> None:
    """
    Configure the logging system.
    
    Logs go to TWO places simultaneously:
    1. Console (terminal) — so you can see what's happening in real time
    2. Log file (logs/pipeline.log) — permanent record of everything
    """
    
    import os
    
    # Create logs folder if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Configure logging format
    log_format = "[%(asctime)s] %(levelname)-5s %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Set up logging to BOTH console and file
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Handler 1: Write to log file
            logging.FileHandler("logs/pipeline.log"),
            # Handler 2: Write to console (terminal)
            logging.StreamHandler()
        ]
    )
    
    logger.info("Logging system initialized")
def process_single_stock(conn, ticker: str) -> dict:
    """
    Run the complete ETL process for ONE stock.
    
    Returns a result dictionary:
    {
        "ticker": "AAPL",
        "status": "success" or "failed",
        "rows_saved": 30,
        "error": None or error message
    }
    """
    
    result = {
        "ticker": ticker,
        "status": "failed",
        "rows_saved": 0,
        "error": None
    }
    
    try:
        # ===== STEP 1: EXTRACT =====
        logger.info(f"EXTRACT: Fetching {ticker} data...")
        raw_data = fetch_stock_data(ticker, FETCH_PERIOD)
        
        if raw_data is None:
            result["error"] = "Failed to fetch data from Yahoo Finance"
            logger.error(f"EXTRACT failed for {ticker}")
            return result
        
        logger.info(f"EXTRACT complete: {len(raw_data)} rows fetched for {ticker}")
        
        # ===== STEP 2: TRANSFORM =====
        logger.info(f"TRANSFORM: Calculating indicators for {ticker}...")
        processed_data = transform_stock_data(raw_data)
        
        if processed_data.empty:
            result["error"] = "Transformation produced empty data"
            logger.error(f"TRANSFORM failed for {ticker}")
            return result
        
        logger.info(f"TRANSFORM complete: {len(processed_data)} rows processed for {ticker}")
        
        # ===== STEP 3: LOAD =====
        logger.info(f"LOAD: Saving {ticker} data to database...")
        rows_saved = save_stock_data(conn, ticker, processed_data)
        
        logger.info(f"LOAD complete: {rows_saved} new rows saved for {ticker}")
        
        # Mark as success
        result["status"] = "success"
        result["rows_saved"] = rows_saved
        return result
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Unexpected error processing {ticker}: {e}")
        return result
def run_pipeline() -> None:
    """
    Main pipeline function.
    Processes ALL stocks from config one by one.
    Generates a summary report at the end.
    """
    
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("PIPELINE STARTED")
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Stocks to process: {STOCK_TICKERS}")
    logger.info("=" * 50)
    
    # Open database connection
    conn = create_connection()
    
    # Track results for each stock
    results = []
    
    # Process each stock one by one
    for ticker in STOCK_TICKERS:
        logger.info(f"\nProcessing {ticker}...")
        result = process_single_stock(conn, ticker)
        results.append(result)
    
    # Close database connection
    close_connection(conn)
    
    # Generate summary report
    generate_summary(results, start_time)
def generate_summary(results: list, start_time: datetime) -> None:
    """
    Print a summary report after the pipeline finishes.
    
    Shows:
    - How many stocks succeeded vs failed
    - Total rows saved
    - How long the pipeline took
    - Details for each stock
    """
    
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    # Count successes and failures
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    total_rows = sum(r["rows_saved"] for r in results)
    
    logger.info("\n" + "=" * 50)
    logger.info("PIPELINE COMPLETE — SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total stocks processed: {len(results)}")
    logger.info(f"Successful:             {len(successful)}")
    logger.info(f"Failed:                 {len(failed)}")
    logger.info(f"Total rows saved:       {total_rows}")
    logger.info(f"Time taken:             {duration} seconds")
    logger.info("-" * 50)
    
    # Print result for each stock
    for result in results:
        if result["status"] == "success":
            logger.info(f"✓ {result['ticker']}: {result['rows_saved']} rows saved")
        else:
            logger.info(f"✗ {result['ticker']}: FAILED — {result['error']}")
    
    logger.info("=" * 50)
    
    # Warn if any stocks failed
    if failed:
        logger.warning(f"{len(failed)} stocks failed: {[r['ticker'] for r in failed]}")
def schedule_pipeline() -> None:
    """
    Schedule the pipeline to run automatically every day.
    
    Uses the 'schedule' library.
    RUN_TIME from config.py controls when it runs (default: 18:00 = 6 PM)
    """
    
    logger.info(f"Scheduling pipeline to run daily at {RUN_TIME}")
    
    # Schedule the job
    schedule.every().day.at(RUN_TIME).do(run_pipeline)
    
    logger.info("Scheduler started. Pipeline will run automatically.")
    logger.info("Press Ctrl+C to stop.")
    
    # Keep the program running forever
    # Every 60 seconds, check if it's time to run
    while True:
        schedule.run_pending()
        time.sleep(60)


def main() -> None:
    """
    Entry point of the entire application.
    
    1. Set up logging
    2. Run pipeline ONCE immediately
    3. Then schedule it to run daily
    """
    
    # Step 1: Set up logging first
    setup_logging()
    
    logger.info("Stock Data Pipeline starting up...")
    
    # Step 2: Run the pipeline ONCE immediately when you start the program
    run_pipeline()
    
    # Step 3: Then schedule it to run automatically every day
    schedule_pipeline()


# This is the entry point — only runs when you execute: python pipeline.py
if __name__ == "__main__":
    main()