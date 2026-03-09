

import yfinance as yf
import pandas_datareader.data as pdr
import sqlite3
import pandas as pd
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os





class MarketDataUpdater:
    """
    Fetches stock and index data from yfinance and stooq,
    and stores it in a local SQLite database.
    """
    def __init__(self, db_path):
        """
        Parameters
        ----------
        db_path : str
            Full path to the SQLite database file.
        """
        self.db_path = db_path
        self._create_db()
      
        
    def _create_db(self):
        """Create the market_data table & the db file if they don't exist."""
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)

        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    symbol   TEXT NOT NULL,
                    date     TEXT NOT NULL,
                    open     REAL,
                    high     REAL,
                    low      REAL,
                    close    REAL NOT NULL,
                    volume   INTEGER,
                    source   TEXT NOT NULL,
                    PRIMARY KEY (symbol, date)
                ) STRICT
            """)
            """
            symbol: ticker symbol (stock or index)
            date: ISO format (YYYY-MM-DD)
            open:
            high:
            low:
            close:
            volume: NULL for indices without volume
            source: helps you track where data came from ('yfinance' or 'stooq')
            """
            conn.commit()


    def _parse_date_range(self, period = None, days_back = None, start = None, end = None):
        """
        Convert various input formats into (start_date, end_date) tuples.
        At least one of period, days_back, or (start and end) must be provided.
        Returns (start_date, end_date) as date objects.
        """
        # If explicit start and end are given, use them directly
        if start is not None and end is not None:
            # Convert to date if datetime
            if hasattr(start, 'date'):
                start = start.date()

            if hasattr(end, 'date'):
                end = end.date()

            return start, end
    
    
        # Use end = today if not provided
        if end is None:
            end = datetime.now().date()
        else:
            if hasattr(end, 'date'):
                end = end.date()
    
    
        # Case 1: period string
        if period is not None:
            match = re.match(r'(\d+)([dmy])', period.lower())
        
            
            if not match:
                raise ValueError(f"Invalid period: {period}. Use e.g., '1d', '1mo', '1y'")
            
            
            value = int(match.group(1))
            unit = match.group(2)
            
            
            if unit == 'd':
                start = end - timedelta(days=value)
            
            elif unit == 'm':
                start = end - relativedelta(months=value)
            
            elif unit == 'y':
                start = end - relativedelta(years=value)
            
            return start, end
    
    
        # Case 2: days_back integer
        if days_back is not None:
            start = end - timedelta(days=days_back)
            
            return start, end


        raise ValueError("Must provide period, days_back, or start+end")


    def fetch_stock_data(self, symbol, period = None, days_back = None, start = None, end = None):
        """
        Fetch OHLCV data for a stock from yfinance.

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g., 'AAPL').
        period : str
            Valid yfinance period (e.g., '1d', '5d', '1mo', '3mo', '1y').

        Returns
        -------
        pd.DataFrame
            Columns: Date, Open, High, Low, Close, Volume
        """
        tkr = yf.Ticker(symbol)
        
        
        if period is not None:
            df = tkr.history(period = period).reset_index()
        else:
            s, e = self._parse_date_range(period, days_back, start, end)
            
            # yfinance end is exclusive, so add one day
            df = tkr.history(start = s, end = e + timedelta(days = 1)).reset_index()
        

        # Remove timezone info from Datetime column
        df['Date'] = df['Date'].dt.tz_localize(None)
        
        # Keep only needed columns        
        return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]


    def fetch_index_data(self, symbol, period = None, days_back = None, start = None, end = None):
        """
        Fetch OHLCV data for an index from stooq (via pandas_datareader).

        Parameters
        ----------
        symbol : str
            Index symbol (e.g., '^SPX' for S&P 500).
        start_date : datetime or str
            Start date (inclusive).
        end_date : datetime or str
            End date (inclusive).

        Returns
        -------
        pd.DataFrame
            Columns: Date, Open, High, Low, Close, Volume
            (Volume may be NaN for indices without volume).
        """
        s, e = self._parse_date_range(period, days_back, start, end)

        df = pdr.get_data_stooq(symbol, start = s, end = e)

        df = df.reset_index().sort_values('Date', ascending = True)

        df = df.reset_index(drop = True)

        # Ensure column names match yfinance format
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

        return df


    def store_data(self, df, symbol, source):
        """
        Insert DataFrame rows into the market_data table.
        Duplicate (symbol, date) rows are ignored.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain columns: Date, Open, High, Low, Close, Volume.
        symbol : str
            Symbol to associate with all rows.
        source : str
            Data source ('yfinance' or 'stooq').

        Returns
        -------
        int
            Number of rows inserted.
        """
        # Prepare rows for insertion
        rows = []
        for _, row in df.iterrows():
            rows.append((
                symbol,
                row['Date'].strftime('%Y-%m-%d'),
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                int(row['Volume']) if pd.notna(row['Volume']) else None,
                source
            ))

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.executemany("""
                INSERT OR IGNORE INTO market_data
                (symbol, date, open, high, low, close, volume, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
            
            return cursor.rowcount


    def update_stocks(self, symbols, period = None, days_back = None, start = None, end = None):
        """
        Fetch and store data for a list of stock symbols.
    
        Parameters
        ----------
        symbols : list of str
        period : str, optional
            e.g., '1d', '5d', '1mo', '3mo', '1y', '2y', '5y', '10y', 'ytd', 'max'
        days_back : int, optional
            Number of days back from today.
        start, end : date or datetime, optional
            Explicit date range.
        """
        for sym in symbols:
            print(f"Fetching stock: {sym}")
            df = self.fetch_stock_data(sym, period, days_back, start, end)

            inserted = self.store_data(df, sym, source="yfinance")
            print(f"  Inserted {inserted} new rows.")

    
    def update_index(self, symbol, period = None, days_back = None, start = None, end = None):
        """
        Convenience: fetch and store an index.
        """
        print(f"Fetching index: {symbol}")
        df = self.fetch_index_data(symbol, period, days_back, start, end)

        inserted = self.store_data(df, symbol, source="stooq")
        print(f"  Inserted {inserted} new rows.")







