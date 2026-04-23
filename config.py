"""
Configuration settings for the Stock Data Pipeline.
Change settings here — no need to touch any other file.
"""

# ========== STOCKS TO TRACK ==========
# Add or remove tickers from this list
STOCK_TICKERS = ["AAPL", "TSLA", "JPM", "MSFT", "GOOGL"]

# ========== DATA SETTINGS ==========
# How many days of historical data to fetch each time
FETCH_PERIOD = "60d"

# ========== DATABASE ==========
# Where to save the processed data
DATABASE_PATH = "database/stocks.db"

# ========== LOGGING ==========
# Where to save log files
LOG_PATH = "logs/pipeline.log"

# ========== MOVING AVERAGES ==========
# Short-term trend (7 days)
MA_SHORT = 7
# Long-term trend (30 days)
MA_LONG = 30

# ========== RSI ==========
# Relative Strength Index period
RSI_PERIOD = 14

# ========== VOLATILITY ==========
# Number of days to calculate volatility over
VOLATILITY_WINDOW = 20

# ========== API RETRY SETTINGS ==========
# If Yahoo Finance doesn't respond, how many times to retry
MAX_RETRIES = 3
# How many seconds to wait between retries
RETRY_DELAY_SECONDS = 5

# ========== SCHEDULE ==========
# What time to run the pipeline daily (24-hour format)
RUN_TIME = "18:00"