import yfinance as yf
import psycopg as pg
import pandas as pd
import logging
import pandas_market_calendars as mcal
from datetime import datetime
import pytz
from config import DB_CONFIG

logging.basicConfig(
    filename=f'/Users/ZMCodi/git/personal/stock-db/daily_insert/logs/stock_insertion_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

gbp = ['HIWS.L', 'V3AB.L', 'VFEG.L', 'VUSA.L']

def insert_data(table):
    logging.info(f"Starting {table} data insertion")
    try:
        with pg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                tickers = get_tickers(cur, table)
                    
                logging.info(f"Found {len(tickers)} tickers to process")

                if not tickers:
                    return

                df_list = []
                failed_downloads = []
                for ticker in tickers:

                    data = get_data(cur, table, ticker)
                    
                    if data.empty:
                        failed_downloads.append(ticker)
                        logging.error(f"Failed to download {ticker} for {table} table")
                        continue

                    data = data.droplevel(1, axis=1)
                    if table == 'daily_forex':
                        data['currency_pair'] = f'{ticker[:3]}/{ticker[3:6]}'
                    else:
                        data['ticker'] = ticker

                    if ticker not in gbp:
                        data[['Open', 'High', 'Low', 'Close']] /= 100
                        try:
                            data[['Adj Close']] /= 100
                        except Exception as e:
                            pass
                    df_list.append(data)
                    logging.info(f"Successfully downloaded data for {ticker}")

                if not df_list:
                    logging.info("No new data to insert")
                    return
                
                df = pd.concat(df_list)
                logging.info(f"Total rows before cleaning: {len(df)}")

                mask = (df['High'] < df['Open']) | (df['High'] < df['Close']) | (df['Low'] > df['Open']) | (df['Low'] > df['Close'])
                clean = df[~mask].copy()
                temp = df[mask].copy()

                temp['High'] = temp[['Open', 'Close', 'High']].max(axis=1)
                temp['Low'] = temp[['Open', 'Close', 'Low']].min(axis=1)
                clean = pd.concat([clean, temp], axis=0)
                clean = clean.reset_index()
                logging.info(f"Total rows after cleaning: {len(clean)}")

                if table == 'daily_forex':
                    clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'})
                elif table == 'five_minute':
                    clean = clean.rename(columns={'Datetime': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                    if 'Adj Close' not in clean.columns:
                        logging.warning("Adj Close column not found in data, using Close price")
                        clean['adj_close'] = clean['close']
                    else:
                        clean = clean.rename(columns={'Adj Close': 'adj_close'})
                else:
                    clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                    if 'Adj Close' not in clean.columns:
                        logging.warning("Adj Close column not found in data, using Close price")
                        clean['adj_close'] = clean['close']
                    else:
                        clean = clean.rename(columns={'Adj Close': 'adj_close'})

                rows_inserted = 0
                failed_inserts = []
                for _, row in clean.iterrows():
                    try:
                        if table == 'daily_forex':
                            cur.execute("INSERT INTO daily_forex (currency_pair, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)", (row['currency_pair'], row['date'], row['open'], row['high'], row['low'], row['close']))
                        else:
                            cur.execute(f"INSERT INTO {table} (ticker, date, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row['ticker'], row['date'], row['open'], row['high'], row['low'], row['close'], row['adj_close'], row['volume']))
                        rows_inserted += 1
                    except Exception as e:
                        if table == 'daily_forex':
                            failed_inserts.append((row['currency_pair'], row['date']))
                            logging.error(f"Failed to insert {row['currency_pair']}: {str(e)}")
                        else:
                            failed_inserts.append((row['ticker'], row['date']))
                            logging.error(f"Failed to insert {row['ticker']}: {str(e)}")
                        continue

                conn.commit()
                logging.info(f"Successfully inserted {rows_inserted} rows")

                if failed_downloads:
                    logging.warning(f"Failed downloads: {failed_downloads}")
                if failed_inserts:
                    logging.warning(f"Failed inserts: {failed_inserts}")
    
    except Exception as e:
        logging.critical(f"Critical error in insert_{table}_data: {str(e)}")
        raise

def get_tickers(cur, table):
    try:
        if table == 'daily_forex':
            cur.execute("SELECT DISTINCT(currency_pair) FROM daily_forex")
            tickers = [ticker[0] for ticker in cur.fetchall()]
            tickers = [f'{ticker[:3]}{ticker[4:7]}=X' for ticker in tickers]
        else:
            tickers = get_open_exchange(cur)

        return tickers
    
    except Exception as e:
        logging.error(f"Fetching tickers error: {str(e)}")
        raise

def get_open_exchange(cur):
    try:
        cur.execute("SELECT DISTINCT(exchange) FROM tickers WHERE exchange != 'CCC'")
        exchanges = [exc[0] for exc in cur.fetchall()]

        closed_exchanges = []
        for exchange in exchanges:
            exc = mcal.get_calendar(exchange)
            if exc.valid_days(start_date=datetime.today(), end_date=datetime.today()).empty:
                closed_exchanges.append(exchange)

        if closed_exchanges:
            logging.info(f"Closed exchanges: {closed_exchanges}")

            placeholder =  ', '.join(['%s'] * len(closed_exchanges))

            cur.execute(f'SELECT ticker FROM tickers WHERE exchange NOT IN ({placeholder})', tuple(closed_exchanges))

        else:
            cur.execute('SELECT ticker FROM tickers')

        tickers = [ticker[0] for ticker in cur.fetchall()]

        return tickers
    
    except Exception as e:
        logging.error(f"Error fetching market calendar: {str(e)}")
        raise

def get_data(cur, table, ticker):
    try:
        if table == 'daily_forex':
            cur.execute(f"SELECT MAX(date) FROM {table} WHERE currency_pair = '{ticker[:3]}/{ticker[3:6]}'")
        else:
            cur.execute(f"SELECT MAX(date) FROM {table} WHERE ticker = '{ticker}'")

        if table == 'five_minute':
            last_ts = cur.fetchone()[0].replace(tzinfo=pytz.UTC)
            if last_ts is None:
                logging.info("No existing data found, using default start date")
                last_date = datetime.now().date() - pd.Timedelta(days=61)
            else:
                last_date = last_ts.date()

            data = yf.download(ticker, start=last_date, interval='5m', auto_adjust=False)
            data = data[data.index > pd.to_datetime(last_ts)]

        else:
            last_date = cur.fetchone()[0]
            if last_date is None:
                logging.info("No existing data found, using default start date")
                last_date = datetime(2019, 12, 31)
            
            data = yf.download(ticker, start=last_date + pd.Timedelta(days=1), auto_adjust=False)
            data = data[data.index > pd.to_datetime(last_date)]


        logging.info(f"Last date for {ticker}: {last_date}")
        return data
    
    except Exception as e:
        logging.error(f"yfinance API error: {str(e)}")
        raise


if __name__ == '__main__':
    logging.info("Starting insertion process")
    try:
        insert_data('daily')
        insert_data('five_minute')
        insert_data('daily_forex')
        logging.info("Finished insertion process successfully")
    except Exception as e:
        logging.critical(f"Script failed: {str(e)}")