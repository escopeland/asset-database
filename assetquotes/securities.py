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
from tiingo.restclient import RestClientError
from tiingo.exceptions import (
    InstallPandasException,
    APIColumnNameError,
    InvalidFrequencyError,
    MissingRequiredArgumentError,
)

TIINGO_API_KEY = 'TIINGO_API_KEY'
DATA_PATH = '.\\data\\pkl'
NAME_DB = '_name_db'


class Securities:
    def __init__(self, key=None):
        self._api_key = os.getenv('TIINGO_API_KEY') if key is None else key
        self._datapath = DATA_PATH

        self._client = self._client_init()
        self._name_db = self._name_db_init()
        self._quote_db = self._quote_db_init()

        # self._init_quote_db()
        # self._update_quote_db()

    def ticker(self, ticker):
        return self._get_metadata(ticker, 'ticker')

    def name(self, ticker):
        if response := self._get_metadata(ticker, 'name').title():
            response = response.title()
        return response

    def start(self, ticker):
        return self._get_metadata(ticker, 'startDate')

    def end(self, ticker):
        return self._get_metadata(ticker, 'endDate')

    def last(self, ticker):
        return self._get_metadata(ticker, 'lastUpdate')

    def description(self, ticker):
        return self._get_metadata(ticker, 'description')

    def close(self, ticker, date):
        return self._quote_db[ticker]['quotes']['close'].asof(
            pd.to_datetime(date, utc=True)
        )

    def _get_metadata(self, ticker, field):
        try:
            return self._quote_db[ticker]['metadata'][field]
        except ValueError:
            print(
                self.__class__, f"({ticker}): Ticker does not exist in database.",
            )
            return False

    # Methods
    def add(self, ticker):
        if not self.exists(ticker):
            print(f'Ticker {ticker} does not exist: adding')
            # ticker_db = self._client_get(ticker)
            # self._db_write(ticker, ticker_db)
            # self._quote_db.update({ticker: ticker_db})
        else:
            print(f'Ticker {ticker} exists: skipping add')

    def exists(self, ticker):
        return ticker in self._quote_db

    # Initialization support
    def _client_init(self):
        config = {}
        config['session'] = True
        config['api_key'] = self._api_key
        return TiingoClient(config)

    # _name_db is a dictionary of all tickers supported by the quote client.
    #    All other tickers must be manually (or otherwise) supported
    def _name_db_init(self):
        return self._db_read(NAME_DB)

    def _name_db_update(self):
        # client.list_tickers() returns a list of dictionarys. The name database
        # of listed names is a dictionary by ticker of the corresponding meta-
        # data
        name_db = {}
        [name_db.update({d['ticker']: d}) for d in self._client.list_tickers()]
        self._db_write(NAME_DB, name_db)

    # _quote_db is a dictionary of all tickers set up by this library
    def _quote_db_init(self):
        quote_db = {}
        [
            quote_db.update({f: self._db_read(f)})
            for f in os.listdir(DATA_PATH)
            if f != NAME_DB
        ]
        return quote_db

    def _quote_db_update(self, ticker):
        pass
        # for ticker in self._quote_db:
        # if ticker hasn't yet been updated today
        #     update its last update
        #
        #     if ticker is listed in REST client database
        #         update the ticker's endDate
        #
        #         if ticker's endDate > its last update date
        #             update the ticker's quotes
        #
        # yesterday = (dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")

        # d = dict()
        # d = self._quote_db['ticker']['metadata'] # d IS the metadata dictionary, not a oopy
        # q = pd.DataFrame()
        # q = self._quote_db['ticker']['quotes']   # q IS the quotes dataframe, not a oopy

        # last_update = d['lastUpdate']
        # today = dt.date.today().strftime("%Y-%m-%d")

        # if last_update < today:
        #     if ticker in self._name_db:
        #         if end_date := self._client.get_ticker_metadata(ticker)['endDate'] is not None:
        #             if today < d['endDate']:
        #                 q = pd.concat(q, self._client.get_dataframe(
        #                     ticker,
        #                     startDate=d['endDate'],
        #                     endDate=end_date,
        #                     frequency='daily',
        #                     )
        #                 )
        #                 d['endDate'] = end_date
        #     d['lastUpdate'] = today

    def _client_exists(self, ticker):
        return ticker in self._client.list_tickers()

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

        # File I/O utilities:

    def _db_read(self, filename):
        with open(os.path.join(self._datapath, filename), 'rb') as f:
            try:
                return pk.load(f)
            except:
                print(self.__class__, f': Bad database read ({f.name})')
                return {}

    def _db_write(self, filename, db):
        with open(os.path.join(self._datapath, filename), 'wb') as f:
            try:
                pk.dump(db, f)
            except:
                print(self.__class__, f': Bad database write ({f.name})')

    def update_fix(self):
        yesterday = (dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")
        for ticker in self._quote_db:
            d = {}
            d = self._quote_db[ticker]
            d['metadata'].update({'lastUpdate': yesterday})
