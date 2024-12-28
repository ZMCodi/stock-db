import yfinance as yf
import psycopg as pg
import pandas as pd
import logging
import pandas_market_calendars as mcal
from datetime import datetime

logging.basicConfig(
    filename=f'/Users/ZMCodi/git/personal/stock-db/daily_insert/logs/stock_insertion_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def insert_data(table):
    logging.info(f"Starting {table} data insertion")
    try:
        with pg.connect(dbname='Stocks', user='postgres', password='420691') as conn:
            with conn.cursor() as cur:
                tickers = get_tickers(cur, table)
                logging.info(f"Found {len(tickers)} tickers to process")

                if not tickers:
                    return
                
                last_date = get_last_date(cur, table)

                df_list = []
                failed_downloads = []
                for ticker in tickers:
                    try:
                        if table == 'five_minute':
                            data = yf.download(ticker, start=last_date + pd.Timedelta(days=1), interval='5m')
                        else:
                            data = yf.download(ticker, start=last_date + pd.Timedelta(days=1))
                        data = data.droplevel(1, axis=1)
                        if table == 'daily_forex':
                            data['currency_pair'] = f'{ticker[:3]}/{ticker[3:6]}'
                        else:
                            data['ticker'] = ticker
                        df_list.append(data)
                        logging.info(f"Successfully downloaded data for {ticker}")

                    except Exception as e:
                        failed_downloads.append(ticker)
                        logging.error(f"Failed to download {ticker}: {str(e)}")
                        continue

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
                    clean.drop(columns=['Adj Close', 'Volume'], inplace=True)
                    clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'})
                elif table == 'five_minute':
                    clean = clean.rename(columns={'Datetime': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'})
                else:
                    clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'})

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
            cur.execute('SELECT currency_pair FROM daily_forex')
            tickers = [ticker[0] for ticker in cur.fetchall()]
            tickers = list(set(tickers))
            tickers = [f'{ticker[:3]}{ticker[4:7]}=X' for ticker in tickers]
        else:
            tickers = get_open_exchange(cur)

        return tickers
    
    except Exception as e:
        logging.error(f"Database error: {str(e)}")
        raise

def get_open_exchange(cur):
    try:
        cur.execute('SELECT DISTINCT(exchange) FROM tickers')
        exchanges = [exc[0] for exc in cur.fetchall()]

        # Mapping dictionary
        exchange_mapping = {
            'NYQ': 'NYSE',
            'NMS': 'NASDAQ'
        }

        # Mapped list
        exchanges = [exchange_mapping.get(exchange, exchange) for exchange in exchanges]

        closed_exchanges = []
        for exchange in exchanges:
            exc = mcal.get_calendar(exchange)
            if exc.valid_days(start_date=datetime.today(), end_date=datetime.today()).empty:
                closed_exchanges.append(exchange)

        if closed_exchanges:
            logging.info(f"Closed exchanges: {closed_exchanges}")

            db_exchanges = [
                next((k for k, v in exchange_mapping.items() if v == exchange), exchange)
                for exchange in closed_exchanges
            ]

            placeholder =  ', '.join(['%s'] * len(db_exchanges))

            cur.execute(f'SELECT ticker FROM tickers WHERE exchange NOT IN ({placeholder})', tuple(db_exchanges))

        else:
            cur.execute('SELECT ticker FROM tickers')

        tickers = [ticker[0] for ticker in cur.fetchall()]

        return tickers
    
    except Exception as e:
        logging.error(f"Database error: {str(e)}")
        raise

def get_last_date(cur, table):
    try:
        cur.execute(f'SELECT MAX(date) FROM {table}')
        if table == 'five_minute':
            last_ts = cur.fetchone()[0]
            if last_ts is None:
                logging.info("No existing data found, using default start date")
                last_date = datetime.now().date() - pd.Timedelta(days=60)
            else:
                last_date = last_ts.date()
        else:
            last_date = cur.fetchone()[0]
            if last_date is None:
                logging.info("No existing data found, using default start date")
                last_date = datetime(2020, 1, 1)
        logging.info(f"Last date in database: {last_date}")
        return last_date
    
    except Exception as e:
        logging.error(f"Database error: {str(e)}")
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