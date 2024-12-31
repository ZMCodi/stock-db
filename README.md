# Financial Data ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for collecting and storing financial market data from various sources. The project automates the collection of stock data from FTSE 100 and S&P 100 companies, as well as forex data, storing them in a PostgreSQL database with TimescaleDB for efficient time-series data management.

## Features

- Automated data collection from multiple sources:
  - FTSE 100 stocks
  - S&P 100 stocks
  - Forex currency pairs
- Multiple timeframe support:
  - Daily OHLCV data
  - 5-minute OHLCV data
  - Daily forex rates
- Data validation and cleaning:
  - OHLC price integrity checks
  - Automated data correction
  - Comprehensive error logging
- Batch processing with error handling and logging
- Automated updates through scheduled execution
- TimescaleDB integration for efficient time-series data storage

## Technologies

- PostgreSQL 16 with TimescaleDB extension
- Python (see environment.yml for specific version)
- Key dependencies:
  - yfinance: Financial data retrieval
  - psycopg: PostgreSQL database connection
  - pandas: Data manipulation
  - pandas_market_calendars: Market schedule management

## Installation

1. Clone the repository:
```bash
git clone https://github.com/ZMCodi/stock-db.git
cd stock-db
```

2. Create and activate the Python environment:
```bash
conda env create -f environment.yml
conda activate [environment-name]
```

3. Install and configure PostgreSQL 16 with TimescaleDB extension

4. Initialize the database using the provided schema:
```bash
psql -U [username] -d [database] -f db.sql
```

## Configuration

Create a `config.py` file in the same directory as `insert.py` with your database credentials:

```python
DB_CONFIG = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
}
```

## Project Structure

- `db.sql`: Database schema definition and TimescaleDB configuration
- `environment.yml`: Python environment specification
- `daily_insert/`
  - `insert.py`: Main ETL pipeline script for batch processing
  - `config.py`: Database configuration (needs to be created)
- `initial_setup/`:
  - `tickers.ipynb`: Initial setup for populating the tickers table
  - `daily.ipynb`: Initial daily data population
  - `5min.ipynb`: Initial 5-minute data population
  - `forex.ipynb`: Initial forex data population

## Usage

### Initial Setup

1. First, run the ticker population notebook to set up the universe of tradable assets:
```bash
jupyter notebook initial_setup/tickers.ipynb
```

2. Then run the initial data population notebooks if you need historical data:
```bash
jupyter notebook initial_setup/daily.ipynb
jupyter notebook initial_setup/5min.ipynb
jupyter notebook initial_setup/forex.ipynb
```

### Automated Updates

The main ETL pipeline is handled by `insert.py`, which can be scheduled to run periodically. The script:
- Checks for market open/close status
- Downloads new data since the last update
- Validates and cleans the data
- Inserts new records into the database
- Provides comprehensive logging

Example of running the script:
```bash
python insert.py
```

You can schedule this script using your preferred scheduling tool (cron, Windows Task Scheduler, etc.).

### Logging

Logs are stored in the `logs/` directory with the format `stock_insertion_YYYYMMDD.log`. Monitor these logs for:
- Successful data insertions
- Download failures
- Data validation issues
- Database insertion errors

## Notes

- The notebooks are for initial setup only. Regular updates should use `insert.py`
- Data is validated to ensure OHLC integrity (High ≥ Open, Close, Low and Low ≤ Open, Close)
- The project uses YFinance API for data collection
- TimescaleDB retention policy is set to 60 days for 5-minute data