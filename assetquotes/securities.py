# securities.py

import os
import pickle
import pandas as pd
import datetime as dt

from tiingo import TiingoClient
from tiingo.restclient import RestClientError

TIINGO_API_KEY = 'TIINGO_API_KEY'
DATA_PATH = '.\\data\\pkl'
NAME_DB = '_local_db'


def strpdate(date, fmt='%Y-%m-%d'):
    return dt.datetime.strptime(date, fmt).date()


def incrdate(date, offset=1, fmt='%Y-%m-%d'):
    return (strpdate(date) + dt.timedelta(offset)).strftime(fmt)


def todaydate(fmt='%Y-%m-%d'):
    return dt.datetime.now().date().strftime(fmt)


def filedate(path, filename='', fmt='%Y-%m-%d'):
    return (
        dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, filename)))
        .date()
        .strftime(fmt)
    )


def filestale(path, filename=''):
    return todaydate() > filedate(path, filename)


def db_read(path, filename):
    with open(os.path.join(path, filename), 'rb') as f:
        return pickle.load(f)


def db_write(path, filename, db):
    with open(os.path.join(path, filename), 'wb') as f:
        pickle.dump(db, f)


class Securities:
    ''' Manages access to a locally stored database of asset information.
        
        Add securities to the local database using the add method, which either: 
            (1) retrieves data from tiingo (https://www.tiingo.com) REST API using the 
                tiingo-python PyPI package; or, 
            (2) uses a user-supplied ticker name, in which case the ticker metadata and
                quotes must be added with the named metadata and quote methods.

        The metadata attribute methods return the value requested if available or None
        if not, along with a status message of either 'OK' or 'Ticker Error' if the 
        ticker is not in the local database. In the metadata methods that accept the 
        'let' parameter below, the parameter acts like an attribute set.

        The quote attribute methods return the value requested and an 'as of' date if
        available or None and the passed date if not, along with a status message of 
        either 'OK' 'Ticker Error' if the ticker is not in the local database. The 'let'
        paramater acts like an attribute set, returning 'Overwrite Error' status for
        any ticker quoted by tiingo; that is, 'let' only operates on manually created
        tickers.
        
        metadata attribute methods:
            ticker(ticker)
            name(ticker, let=None)
            description(ticker, let=None)
            asset_class(ticker, let=None)
            asset_type(ticker, let=None)
            exchange(ticker, let=None)
            currency(ticker, let=None)
            start(ticker)
            end(ticker)

        quote attribute methods
            close(ticker, date, let=None)
            open(ticker, date, let=None)
            high(ticker, date)
            low(ticker, date)
            adj_close(ticker, date)
            adj_open(ticker, date)
            adj_high(ticker, date)
            adj_low(ticker, date)
            volume(ticker, date, let=None)
            dividend(ticker, date, let=None)
            split(ticker, date, let=None)

        other methods
            delete(ticker)
            add(ticker)
    '''

    def __init__(self, key=None, path=None):
        ''' Initialize data structures from locally stored databases; update each 
            database via the Tiingo REST API if it is out of date.

            args
                key(str=None): The Tiingp API key, equal by default to environment 
                    variable 'TIINGO_API_KEY'.

                path(str='.\\data\\pkl\\'): The location of the local database. 
        '''
        self._key = self._key_init(key)
        self._path = self._path_init(path)

        self._client = self._client_init(self._key)

        self._tiingo_db = self._tiingo_db_init(self._path, NAME_DB)
        self._local_db = self._local_db_init(self._path)

    def __repr__(self):
        return f'{self.__class__.__name__}(' f'key={self._key!r}, path={self._path!r})'

    def _str__(self):
        return f'class Securites pointing to data in {self._path!r}'

    def _key_init(self, key):
        return key if key is not None else os.getenv(TIINGO_API_KEY)

    def _path_init(self, path):
        if (path := DATA_PATH if path is None else path) == DATA_PATH:
            if not os.path.exists(path):
                os.mkdir(path)
        return path

    def _client_init(self, key):
        self._key = os.getenv('TIINGO_API_KEY') if key is None else key
        return TiingoClient(dict([('session', True), ('api_key', self._key)]))

    def _tiingo_db_init(self, path, filename):
        if (
            True
            if not os.path.exists(os.path.join(path, filename))
            else filestale(path, filename)
        ):
            db = {}
            [db.update({d['ticker']: d}) for d in self._client.list_tickers()]
            db_write(path, filename, db)

        return db_read(path, filename)

    def _local_db_init(self, path):
        quote_db = {}
        [
            quote_db.update({f: self._ticker_db_init(path, f)})
            for f in os.listdir(path)
            if f != NAME_DB
        ]

        return quote_db

    def _ticker_db_init(self, path, ticker):
        ticker_db = db_read(path, ticker)

        if (
            ticker in self._tiingo_db
            and self._tiingo_db[ticker]['endDate'] is not None
            and filestale(path, ticker)
        ):
            ticker_db['quotes'] = pd.concat(
                [
                    ticker_db['quotes'],
                    self._client.get_dataframe(
                        ticker,
                        startDate=incrdate(ticker_db['metadata']['endDate']),
                        endDate=self._tiingo_db[ticker]['endDate'],
                        frequency='daily',
                    ),
                ]
            )
            ticker_db['metadata']['endDate'] = self._tiingo_db[ticker]['endDate']
            db_write(path, ticker, ticker_db)

        return ticker_db

    def _metadata_update(self, ticker, field, value):
        if ticker not in self._local_db:
            return (None, 'Ticker Error')

        metadata_db = self._local_db[ticker]['metadata']
        if value is not None:
            metadata_db[field] = value
            db_write(self._path, ticker, self._local_db[ticker])

        try:
            return (metadata_db[field], 'OK')

        except KeyError:
            return (None, 'OK')

    def _quote_update(self, ticker, field, date, value, last=False):
        if ticker not in self._local_db:
            return (None, 'Ticker Error')

        date_dt = pd.to_datetime(date, utc=True)
        quote_db = self._local_db[ticker]['quotes']

        if value is not None:
            if ticker in self._tiingo_db:
                if (
                    self._tiingo_db['ticker']['startDate'] is None
                    and self._tiingo_db['ticker']['endDate'] is None
                ):
                    return (None, "Overwrite Error", date)

            if date_dt in quote_db.index:
                quote_db.at[date_dt, field] = value

            else:
                quote_db = quote_db.append(
                    pd.DataFrame({'date': [date_dt], 'close': [value],}).set_index(
                        'date'
                    ),
                    sort='False',
                ).sort()

                metadata_db = self._local_db[ticker]['metadata']
                metadata_db['startDate'] = min(metadata_db['startDate'], date)
                metadata_db['endDate'] = max(metadata_db['endDate'], date)

            db_write(self._path, ticker, self._local_db[ticker])

        try:
            if isinstance(last, bool) and last == False:
                date_asof = quote_db.index.asof(date_dt)

            else:
                df = quote_db[quote_db.index <= date_dt]
                date_asof = df[df[field] != last].last('1d').index[0]

                # date_asof = quote_db[quote_db[field] != last].last('1d').index[0]

            return (quote_db.at[date_asof, field], date_asof.strftime('%Y-%m-%d'), 'OK')

        except:
            return (None, date, 'OK')

    # public metadata attribute methods
    def ticker(self, ticker):
        return self._metadata_update(ticker, 'ticker', None)

    def name(self, ticker, let=None):
        return self._metadata_update(ticker, 'name', let)

    def description(self, ticker, let=None):
        return self._metadata_update(ticker, 'description', let)

    def asset_class(self, ticker, let=None):
        return self._metadata_update(ticker, 'assetClass', let)

    def asset_type(self, ticker, let=None):
        return self._metadata_update(ticker, 'assetType', let)

    def exchange(self, ticker, let=None):
        return self._metadata_update(ticker, 'exchange', let)

    def currency(self, ticker, let=None):
        return self._metadata_update(ticker, 'priceCurrency', let)

    def start(self, ticker):
        return self._metadata_update(ticker, 'startDate', None)

    def end(self, ticker):
        return self._metadata_update(ticker, 'endDate', None)

    # public quote attribute methods
    def close(self, ticker, date, let=None):
        return self._quote_update(ticker, 'close', date, let)

    def open(self, ticker, date, let=None):
        return self._quote_update(ticker, 'open', date, let)

    def high(self, ticker, date, let=None):
        return self._quote_update(ticker, 'high', date, let)

    def low(self, ticker, date, let=None):
        return self._quote_update(ticker, 'low', date, let)

    def volume(self, ticker, date, let=None):
        return self._quote_update(ticker, 'volume', date, let)

    def dividend(self, ticker, date, let=None):
        return self._quote_update(ticker, 'divCash', date, let, last=0)

    def split(self, ticker, date, let=None):
        return self._quote_update(ticker, 'splitFactor', date, let, last=1)

    # other methods
    def delete(self, ticker):
        ''' Delete a security from the local database.

            args
                ticker (str): the ticker of the security to delete.

            returns
                (ticker, status): status 'OK' or 'Ticker Error'.
        '''
        if ticker not in self._local_db:
            return (ticker, 'Ticker Error')

        os.remove(os.path.join(self._path, ticker))
        del self._local_db[ticker]
        return (ticker, 'OK')

    def add(self, ticker, asset_class=None):
        ''' Add a new security to the local database.

            If the security is supported by Tiingo, add all available metadata and quote data
            available from Tiingo's REST API to the local database.

            Otherwise, create the ticker but add no metadata or quote data here; use the public
            metadata and quote data access methods for that purpose.

            args
                ticker (str): the ticker of the security to add.

            returns
                (ticker, status): status 'OK' or 'Ticker Error'.
        '''
        if ticker in self._local_db:
            return (ticker, 'Ticker Error')

        if ticker not in self._tiingo_db:
            ticker_db = {ticker: {'metadata': {}, 'quotes': pd.DataFrame()}}

        else:
            meta_db = self._client.get_ticker_metadata(ticker)
            meta_db.update(self._tiingo_db[ticker])
            meta_db['assetClass'] = asset_class
            meta_db.pop('exchangeCode')

            quote_df = self._client.get_dataframe(
                ticker,
                startDate=meta_db['startDate'],
                endDate=meta_db['endDate'],
                frequency='daily',
            )

            ticker_db = {'metadata': meta_db, 'quotes': quote_df}

        db_write(self._path, ticker, ticker_db)
        self._local_db.update({ticker: ticker_db})

        return (ticker, 'OK')
