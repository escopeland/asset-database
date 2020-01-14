from constants import DATE_MAX
from constants import DATE_MIN

class Quote:
    # class attributes:
    _DATE = 'date'
    _CLOSE = 'close'
    _HIGH = 'high'
    _LOW = 'low'
    _OPEN = 'open'
    _VOLUME = 'volume'
    _ADJ_CLOSE = 'adjClose'
    _ADJ_HIGH = 'adjHigh'
    _ADJ_LOW = 'adjLow'
    _ADJ_VOLUME = 'adjVolume'
    _DIV_CASH = 'divCash'
    _SPLIT_FACTOR = 'splitFactor'
    
    def __init__(self,
                 date=DATE_MIN, close=1.00, high=1.00, low=1.00, open_=1.00, volume=1,
                 adj_close=1.00, adj_high=1.00, adj_low=1.00, adj_open=1.00, adj_volume=1,
                 div_cash=0.00, split_factor=1.00):
        pass

    @property # property.date
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._date = value

    @property # property.close
    def close(self):
        return self._close

    @close.setter
    def close(self, value):
        self._close = value
        
    @property # property.high
    def high(self):
        return self._high
        
    @high.setter
    def high(self, value):
        self._high = value

    @property # property.low
    def low(self):
        return self._low
        
    @low.setter
    def low(self, value):
        self._low = value
        
    @property # property.adj_close
    def adj_close(self):
        return self._adj_close
        
    @adj_close.setter
    def adj_close(self, value):
        self._adj_close = value
        
    # property.volume
    @property
    def volume(self):
        return self._volume
        
    @volume.setter
    def volume(self, value):
        self._volume = value
