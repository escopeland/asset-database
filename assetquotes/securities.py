# securities.py

'''
--------------------------------------------------------------------------------
Class Securities
 
This class manages a database of asset information assembled using tiingo-python
(https://github.com/hydrosquall/tiingo-python)

Initialiation reads a metadata file at location specified:
    path, file

'''
import os
import pickle as pk
import pandas as pd
import datetime as dt

from constants import DATE_MAX, DATE_MIN
from tiingo import TiingoClient

TIINGO_API_KEY = 'TIINGO_API_KEY'
DATA_PATH = '.\\data\\pkl'


class Securities:
    def __init__(self, key=None):
        self._api_key = os.getenv('TIINGO_API_KEY') if key is None else key
        self._datapath = DATA_PATH

        self._client = self._client_init(self._api_key)
        self._quote_db = self._quote_db_init(self._datapath)

        # self._init_quote_db()
        # self._update_quote_db()

    # Metadata attributes:
    # def ticker(self, ticker):
    #     return ticker in self._quote_db.index

    # def listed(self, ticker):
    #     return self._quote_db['listed'][ticker]

    # def quoted(self, ticker):
    #     return self._quote_db['quoted'][ticker]

    def name(self, ticker):
        return self._quote_db[ticker]['metadata']['name'].title()

    def description(self, ticker):
        return self._quote_db[ticker]['metadata']['description']

    def close(self, ticker, date):
        return self._quote_db[ticker]['quotes']['close'].asof(
            pd.to_datetime(date, utc=True)
        )

    # def asset_class(self, ticker):
    #     return self._quote_db['assetClass'][ticker]

    # def inception(self, ticker):
    #     return self._quote_db['inception'][ticker].date()

    # def delisting(self, ticker):
    #     return self._quote_db['delisting'][ticker].date()

    # def last_update(self, ticker):
    #     return self._quote_db['lastupdate'][ticker].date()

    # Quote attributes:
    # def quote(self, ticker, date=None):
    #     if date is None:
    #         date = dt.date.today() - dt.timedelta(days=1)
    #     return self._quote_db[ticker]['close'][date]

    # def split_factor(self, ticker, date=None):
    #     if date is None:
    #         date = dt.date.today() - dt.timedelta(days=1)
    #     return self._quote_db[ticker]['splitFactor'][date]   # def _get_quote_db(self, ticker, tag=None):
    #     if tag is None:
    #         tag = 'ticker'
    #     return self._quote_db[ticker][tag]

    # Methods
    def add(self, ticker):
        if not self.exists(ticker):
            print(f'Ticker {ticker} does not exist: adding')
            ticker_db = self._client_get(ticker)
            self._db_file_write(ticker, ticker_db)
            self._quote_db.update({ticker: ticker_db})
        else:
            print(f'Ticker {ticker} exists: skipping add')

    def exists(self, ticker):
        return ticker in self._quote_db

    # Tiingo interface:
    def _client_init(self, api_key):
        config = {}
        config['session'] = True
        config['api_key'] = self._api_key
        return TiingoClient(config)

    def _client_get(self, ticker):
        get_db = {}
        metadata = self._client.get_ticker_metadata(ticker)
        quotes = self._client.get_dataframe(
            ticker,
            startDate=metadata['startDate'],
            endDate=metadata['endDate'],
            frequency='daily',
        )
        get_db.update(
            {'metadata': metadata, 'quotes': quotes,}
        )
        return get_db

    def _client_update(self, ticker):
        # # I don't need to pass the API key because it's saved as an environment variable.
        # df1 = pdr.get_data_tiingo('VGSLX', '12-01-2019', '12-03-2019')
        # df2 = pdr.get_data_tiingo('VGSLX', '12-05-2019', '12-07-2019')
        # df  = pd.concat([df1, df2])
        # df  = df.xs('VGSLX')
        # df['close'].iloc[df.index.get_loc(dt.datetime(2019, 12, 4), method='pad')]
        # 130.76
        # df['close'][df.index.asof(dt.datetime(2019, 12, 4))]
        # 130.76
        pass

    def _quote_db_init(self, path):
        quote_db = {}
        [
            quote_db.update({f.split('.')[0]: self._db_file_read(f.split('.')[0])})
            for f in os.listdir(path)
            if f.endswith(".pkl")
        ]
        return quote_db

    # File I/O utilities:
    def _db_file_read(self, ticker):
        with open(os.path.join(self._datapath, '.'.join([ticker, 'pkl'])), 'rb') as f:
            try:
                return pk.load(f)
            except:
                print(self.__class__, f': Bad database read ({f.name})')
                return {}

    def _db_file_write(self, ticker, quote_db):
        with open(os.path.join(self._datapath, '.'.join([ticker, 'pkl'])), 'wb') as f:
            try:
                pk.dump(quote_db, f)
            except:
                print(self.__class__, f': Bad database write ({f.name})')
        pass


Portfolio = Securities()
# print(Portfolio._quote_db)
ticker = 'VGCIX'
# Portfolio.add(ticker)

print(Portfolio.name(ticker))
import time

start = time.time()
for t in range(0, 999):
    price = Portfolio.close(ticker, '2020-12-31')

print(price)
print(1000 * (time.time() - start), 'us elapsed')
