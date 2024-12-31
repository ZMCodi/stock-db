CREATE TABLE tickers (
    ticker VARCHAR(25) PRIMARY KEY,
    comp_name VARCHAR(100) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    sector VARCHAR(100),
    market_cap BIGINT,
    start_date DATE NOT NULL,
  	currency VARCHAR(3) NOT NULL
);

CREATE TABLE daily (
    ticker VARCHAR(25) NOT NULL REFERENCES tickers (ticker),
    date DATE NOT NULL,
    open NUMERIC(16,5) NOT NULL,
    high NUMERIC(16,5) NOT NULL,
    low NUMERIC(16,5) NOT NULL,
    close NUMERIC(16,5) NOT NULL,
    adj_close NUMERIC(16,5) NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    CONSTRAINT price_check CHECK (high >= open AND high >= low AND high >= close AND
                                low <= open AND low <= high AND low <= close),
    PRIMARY KEY (ticker, date)
);

CREATE INDEX daily_ticker_time_idx ON daily(ticker, date DESC);

CREATE TABLE five_minute (
    ticker VARCHAR(25) NOT NULL REFERENCES tickers (ticker),
    date TIMESTAMP NOT NULL,
    open NUMERIC(16,5) NOT NULL,
    high NUMERIC(16,5) NOT NULL,
    low NUMERIC(16,5) NOT NULL,
    close NUMERIC(16,5) NOT NULL,
    adj_close NUMERIC(16,5) NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    CONSTRAINT price_check CHECK (high >= open AND high >= low AND high >= close AND
                                low <= open AND low <= high AND low <= close),
    PRIMARY KEY (ticker, date)
);

CREATE INDEX five_minute_ticker_time_idx ON five_minute(ticker, date DESC);

CREATE TABLE daily_forex (
    currency_pair VARCHAR(7) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(16,5) NOT NULL,
    high NUMERIC(16,5) NOT NULL,
    low NUMERIC(16,5) NOT NULL,
    close NUMERIC(16,5) NOT NULL,
    volume BIGINT,
    CONSTRAINT price_check CHECK (high >= open AND high >= low AND high >= close AND
                                low <= open AND low <= high AND low <= close),
    PRIMARY KEY (currency_pair, date)
);

CREATE INDEX forex_time_idx ON daily_forex(currency_pair, date DESC);

SELECT create_hypertable('daily', by_range('date'));
SELECT create_hypertable('five_minute', by_range('date', INTERVAL '24 hours'));
SELECT create_hypertable('daily_forex', by_range('date'));
SELECT add_retention_policy('five_minute', INTERVAL '60 days');