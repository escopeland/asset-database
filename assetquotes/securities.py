# securities.py

'''
--------------------------------------------------------------------------------
Class Securities
 
This class manages a database of asset information assembled using tiingo-python
(https://github.com/hydrosquall/tiingo-python)

Initialiation reads the database files in t
    path, file

The asset information API returns as follows:
    _client.list_tickers() returns a list of OrderedDict objects.
        To instead work with the list of OrderedDict objects, first sort it:
            from operator import itemgetter
            db.sort(key=itemgetter('ticker'))

        Then, to search the sorted list:
            from bisect import bisect_left
            key_index = bisect_left(list(db.keys())), search_key)
            found_dict = db[db.keys()[key_index]]

        I later need to determine which approach is faster.
'''
import os
import pickle as pk
import pandas as pd
import datetime as dt

from collections import OrderedDict
from constants import DATE_MAX, DATE_MIN
from tiingo import TiingoClient
from tiingo.restclient import RestClientError

TIINGO_API_KEY = 'TIINGO_API_KEY'
DATA_PATH = '.\\data\\pkl'
NAME_DB = '_name_db'


class Securities:
    ''' Initialize data structures from locally stored databases and update them via the
        asset data API if they are out of date.

        Arguments:
        key: The asset data API key. If not passed, it is set equal to
            environment variable 'TIINGO_API_KEY'.

        path: The location of the asset databases.
    '''

    ########################################################################################
    def __init__(self, key=None, path=None):
        '''Initialize data structures from locally stored databases; update each database
        via the asset data API if it is out of date.

        Arguments:
        key: The asset data API key. If not passed, it is set equal to
            environment variable 'TIINGO_API_KEY'.

        path: The location of the asset databases.
        '''
        self._key = os.getenv('TIINGO_API_KEY') if key is None else key
        self._path = DATA_PATH if path is None else path

        self._client = self._client_init(self._key)

        self._name_db = self._name_db_init(self._path, NAME_DB)
        self._quote_db = self._quote_db_init(self._path)

    def ticker(self, ticker):
        return self._get_metadata(ticker, 'ticker')

    def name(self, ticker):
        return self._get_metadata(ticker, 'name')

    def start(self, ticker):
        return self._get_metadata(ticker, 'startDate')

    def end(self, ticker):
        return self._get_metadata(ticker, 'endDate')

    def close(self, ticker, date):
        return self._quote_db[ticker]['quotes']['close'].asof(
            pd.to_datetime(date, utc=True)
        )

    # Methods
    def delete(self, ticker):
        if ticker in self._quote_db:
            os.remove(os.path.join(self._path, ticker))
            del self._quote_db[ticker]

    def add(self, ticker):
        if ticker in self._quote_db:
            print(f'Ticker {ticker} already loaded: skipping add')

        elif ticker not in self._name_db:
            print(f'Ticker {ticker} not loaded and not available: manual add')

        else:  # ticker in self._name_db:
            print(f'Ticker {ticker} not loaded but available: adding...')
            t = {}
            t = self._name_db[ticker]
            q = pd.DataFrame()
            q = self._client_get_quotes(
                ticker, start_date=t['startDate'], end_date=t['endDate']
            )
            m = {}
            m = self._client_get_metadata(ticker)
            db = {}
            db = {'metadata': m, 'quotes': q}

            db['metadata']['assetType'] = t['assetType']
            db['metadata']['priceCurrency'] = t['priceCurrency']

            self._db_write(self._path, ticker, db)
            self._quote_db.update({ticker: db})

    # Client API access consolidatd here so that it's easier to control format parameters
    # throughout this class.
    def _client_init(self, key):
        config = {}
        config['session'] = True
        config['api_key'] = key
        return TiingoClient(config)

    def _client_get_supported(self):
        return self._client.list_tickers()

    def _client_get_metadata(self, ticker):
        return self._client.get_ticker_metadata(ticker)

    def _client_get_quotes(self, ticker, start_date=None, end_date=None):
        start_date = (
            self._name_db['ticker']['startDate'] if start_date is None else start_date
        )
        end_date = self._name_db['ticker']['end_date'] if end_date is None else end_date
        return self._client.get_dataframe(
            ticker, startDate=start_date, endDate=end_date, frequency='daily'
        )

    # _name_db holds the list of supported tickers provided by the API.
    def _name_db_init(self, path, filename):
        if self._db_stale(path, filename):
            self._name_db_update(path, filename)
        return self._db_read(path, filename)

    def _name_db_update(self, path, filename):
        ''' Recast the list of supported tickers as OrderedDict objects returned by the 
            quote API  an 
            OrderedDict of the original OrderedDict objects, using the  tickers in the
            listed OrderedDict objects as keys. Save the result as a pickle file.
        '''
        db = OrderedDict()
        [db.update({d['ticker']: d}) for d in self._client_get_supported()]
        self._db_write(path, filename, db)

    # _quote_db holds a ticker's metadata and price quotes
    def _quote_db_init(self, path):
        tickers = [f for f in os.listdir(path) if f != NAME_DB]
        [
            self._quote_db_update(path, f)
            for f in tickers
            if self._db_stale(path, f) and f in self._name_db
        ]

        db = OrderedDict()
        [db.update({f: self._db_read(path, f)}) for f in tickers]

        return db

    def _quote_db_update(self, path, filename):
        ''' Update and re-write the ticker quote databases if they're out of 
            date. Quotes are read from the quote API as a pandas dataframe.
        '''
        if (end_date := self._name_db[filename]['endDate']) is None:
            # do nothing if the supported tickers database indicates that the API does not
            # provide ticker price quotes.
            pass

        elif self._file_date(path, filename) < self.strpdate(end_date):
            # update ticker price quotes and 'endDate' via the API.
            db = OrderedDict()
            db = self._db_read(path, filename)

            db['quotes'] = pd.concat(
                [
                    db['quotes'],
                    self._client_get_quotes(
                        filename,
                        start_date=self.incrdate(db['metadata']['endDate']),
                        end_date=self._today_date(fmt='str'),
                    ),
                ]
            )
            db['metadata']['endDate'] = end_date

            self._db_write(path, filename, db)

    # General Utilities
    def _get_metadata(self, ticker, field):
        try:
            return self._quote_db[ticker]['metadata'][field]

        except ValueError:
            print(
                self.__class__, f"({ticker}): Ticker does not exist in database.",
            )
            return False

    #   Time-related utility methods for dealing with dataframe datetimes and dictionary
    #   string times.
    def _db_stale(self, path, filename=''):
        return self._today_date() > self._file_date(path, filename)

    def incrdate(self, date, offset=1, fmt='%Y-%m-%d'):
        return (self.strpdate(date) + dt.timedelta(offset)).strftime(fmt)

    @staticmethod
    def strpdate(date, fmt='%Y-%m-%d'):
        return dt.datetime.strptime(date, fmt).date()

    @staticmethod
    def _today_date(fmt=None):
        returnval = dt.datetime.now().date()
        if fmt == 'str':
            returnval = returnval.strftime('%Y-%m-%d')
        return returnval

    @staticmethod
    def _file_date(path, filename='', fmt=None):
        returnval = dt.datetime.fromtimestamp(
            os.path.getmtime(os.path.join(path, filename))
        ).date()
        if fmt == 'str':
            returnval = returnval.strftime('%Y-%m-%d')
        return returnval

    # File I/O utilities:
    def _db_read(self, path, filename):
        with open(os.path.join(path, filename), 'rb') as f:
            try:
                return pk.load(f)

            except:
                print(self.__class__, f': Bad database read ({f.name})')
                return {}

    def _db_write(self, path, filename, db):
        with open(os.path.join(path, filename), 'wb') as f:
            try:
                pk.dump(db, f)

            except:
                print(self.__class__, f': Bad database write ({f.name})')


portfolio = Securities()
