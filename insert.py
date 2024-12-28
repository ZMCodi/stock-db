import yfinance as yf
import psycopg as pg
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    filename=f'stock_insertion_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def insert_daily_data():
    with pg.connect(dbname='Stocks', user='postgres', password='420691') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT ticker FROM tickers')
            tickers = [ticker[0] for ticker in cur.fetchall()]

            cur.execute(f"SELECT MAX(date) FROM daily")
            last_date = cur.fetchone()[0]

            df_list = []
            for ticker in tickers:
                data = yf.download(ticker, start=last_date + pd.Timedelta(days=1))
                data = data.droplevel(1, axis=1)
                data['ticker'] = ticker
                df_list.append(data)

            df = pd.concat(df_list)
            
            mask = (df['High'] < df['Open']) | (df['High'] < df['Close']) | (df['Low'] > df['Open']) | (df['Low'] > df['Close'])
            clean = df[~mask].copy()
            temp = df[mask].copy()

            temp['High'] = temp[['Open', 'Close', 'High']].max(axis=1)
            temp['Low'] = temp[['Open', 'Close', 'Low']].min(axis=1)
            clean = pd.concat([clean, temp], axis=0)
            clean = clean.reset_index()

            clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'})

            for _, row in clean.iterrows():
                cur.execute("INSERT INTO daily (ticker, date, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row['ticker'], row['date'], row['open'], row['high'], row['low'], row['close'], row['adj_close'], row['volume']))

            conn.commit()

def insert_5min_data():
    with pg.connect(dbname='Stocks', user='postgres', password='420691') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT ticker FROM tickers')
            tickers = [ticker[0] for ticker in cur.fetchall()]

            cur.execute(f"SELECT MAX(date) FROM five_minute")
            last_ts = cur.fetchone()[0]
            last_date = last_ts.date()

            df_list = []
            for ticker in tickers:
                data = yf.download(ticker, start=last_date + pd.Timedelta(days=1), interval='5m')
                data = data.droplevel(1, axis=1)
                data['ticker'] = ticker
                df_list.append(data)

            df = pd.concat(df_list)
            
            mask = (df['High'] < df['Open']) | (df['High'] < df['Close']) | (df['Low'] > df['Open']) | (df['Low'] > df['Close'])
            clean = df[~mask].copy()
            temp = df[mask].copy()

            temp['High'] = temp[['Open', 'Close', 'High']].max(axis=1)
            temp['Low'] = temp[['Open', 'Close', 'Low']].min(axis=1)
            clean = pd.concat([clean, temp], axis=0)
            clean = clean.reset_index()

            clean = clean.rename(columns={'Datetime': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'})

            for _, row in clean.iterrows():
                cur.execute("INSERT INTO five_minute (ticker, date, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (row['ticker'], row['date'], row['open'], row['high'], row['low'], row['close'], row['adj_close'], row['volume']))

            conn.commit()

def insert_forex():
    with pg.connect(dbname='Stocks', user='postgres', password='420691') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT currency_pair FROM daily_forex')
            tickers = [ticker[0] for ticker in cur.fetchall()]
            tickers = list(set(tickers))
            tickers = [f'{ticker[:3]}{ticker[4:7]}=X' for ticker in tickers]

            cur.execute(f"SELECT MAX(date) FROM daily_forex")
            last_date = cur.fetchone()[0]

            df_list = []
            for ticker in tickers:
                data = yf.download(ticker, start=last_date + pd.Timedelta(days=1))
                data = data.droplevel(1, axis=1)
                data['currency_pair'] = f'{ticker[:3]}/{ticker[3:6]}'
                df_list.append(data)

            df = pd.concat(df_list)
            
            mask = (df['High'] < df['Open']) | (df['High'] < df['Close']) | (df['Low'] > df['Open']) | (df['Low'] > df['Close'])
            clean = df[~mask].copy()
            temp = df[mask].copy()

            temp['High'] = temp[['Open', 'Close', 'High']].max(axis=1)
            temp['Low'] = temp[['Open', 'Close', 'Low']].min(axis=1)
            clean = pd.concat([clean, temp], axis=0)
            clean = clean.reset_index()
            clean.drop(columns=['Adj Close', 'Volume'], inplace=True)

            clean = clean.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close'})

            for _, row in clean.iterrows():
                cur.execute("INSERT INTO daily_forex (currency_pair, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)", (row['currency_pair'], row['date'], row['open'], row['high'], row['low'], row['close']))

            conn.commit()

if __name__ == '__main__':
    insert_daily_data()
    insert_5min_data()
    insert_forex()