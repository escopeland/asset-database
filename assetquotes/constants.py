import pandas as pd
import datetime as dt

# Class Timestamp max and min values normalized to date-only and 'UTC' time.
DATE_MAX = pd.Timestamp(pd.Timestamp.max, tz='UTC').normalize() # datetime.date(2262, 4, 11)
DATE_MIN = pd.Timestamp(pd.Timestamp.min + pd.Timedelta(1, unit='day'), tz='UTC').normalize() # datetime.date(1677, 9, 22)
