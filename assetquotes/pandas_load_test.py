import pandas as pd
import pandas_datareader as pdr

# This section reads quotes with pandas-datareader using Tiingo.
start = datetime.datetime(2019, 12, 1).date()
end = datetime.datetime(2019, 12, 15).date()
df = pdr.get_data_tiingo(
    ['GOOG'], start=start, end=end, api_key=os.environ["TIINGO_KEY"]
)
df.head()
df['close'].head()
df.index
goog_df = df.xs('GOOG', level='symbol')  # Extract just the second level index, 'date'
goog_df
goog_df.index
goog_df.index[0]
type(goog_df.index[0])  # Dataframe Timestamp type

# This section reads quotes from my existing Stock Quotes.xlam file.
vwo = pd.read_excel(
    'C:\OneDrive\Applications\Excel\Addins\MIH LLC\Source\Security Data\Security Data v2.0.xlam',
    sheet_name='VWO',
    index_col=0,
    header=None,
    names=['date', 'close', 'split factor'],
)
vwo.index
type(vwo.index[0])
type(vwo['split_factor'][0])  # Floating point

# Write the dataframe back out to a CSV, then read it again.
vwo.to_csv('Stock Quotes\VWO.csv')
vwo = pd.read_csv('Stock Quotes\\VWO.csv', index_col=0, parse_dates=True)
type(vwo.index[0])  # Dataframe Timestamp type is preserved.

# To pull out all the data from my Excel.xlam file, then store to CSV, just
# constuct a generator of tickers using a list, then iterate through them using
#     os.path.join()
# to construct the filename.


# To convert dateframe index to date-only format:
df = vwo
dti = pd.to_datetime(df.index).date
df.index = dti
