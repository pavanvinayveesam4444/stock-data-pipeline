# Automated Stock Data Pipeline

An automated ETL pipeline that extracts stock price data
from Yahoo Finance, calculates financial indicators,
and loads everything into a SQLite database daily.

## What It Does

Extracts daily stock data for 5 stocks:
AAPL, TSLA, JPM, MSFT, GOOGL

Transforms raw prices into financial indicators:
- Daily Returns
- 7-day and 30-day Moving Averages
- 20-day Volatility
- 14-day RSI (Relative Strength Index)

Loads processed data into SQLite database.
Schedules automatically at 6 PM daily.
Logs every step to logs/pipeline.log.
Handles errors with automatic retries.

## Project Structure

stock-data-pipeline/
├── pipeline.py          → Main coordinator
├── config.py            → All settings
├── etl/
│   ├── extractor.py     → Fetches from Yahoo Finance
│   ├── transformer.py   → Calculates indicators
│   └── loader.py        → Saves to SQLite
├── tests/               → 13 pytest tests
├── logs/                → Log files
└── database/            → SQLite database

## How To Run

Install dependencies:
pip install -r requirements.txt

Run the pipeline:
python pipeline.py

Run tests:
pytest tests/ -v

## Skills Demonstrated

- ETL Pipeline Design (Extract, Transform, Load)
- Python Engineering (OOP, error handling, logging)
- Financial Data Analysis (RSI, Moving Averages, Volatility)
- SQLite Database Operations
- Automated Scheduling
- Data Validation
- Unit Testing with pytest (13 tests)
- Git Version Control
