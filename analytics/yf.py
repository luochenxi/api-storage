from datetime import date
from pprint import pprint as pt
import yfinance as yf
import pandas as pd
from dateutil.relativedelta import relativedelta
# from pandas_datareader import data as pdr
import pandas_datareader as pdr

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

yf.pdr_override()

def day_range():
    start = date.today() - relativedelta(days=2)
    end = date.today() + relativedelta(days=0)
    return start.isoformat(), end.isoformat()

if __name__ == '__main__':
    start, end = day_range()
    # etd_data = pdr.get_data_yahoo("NQ=F", start=start, end=end,interval = "d")
    etd_data = yf.Ticker("NQ=F")
    # print(etd_data)
    pt(etd_data.history(period="3d", interval="1h").shift())
    pt(etd_data.history(period="3d", interval="1h").to_dict(orient='records'))
    print(type(etd_data.history(period="3d", interval="1h")))
    for i in etd_data.history(period="3d", interval="1h").to_dict(orient='records'):
        print(i)
    # pt(etd_data.to_dict(orient='index'))
    #
    # actions = pdr.DataReader('QQQ', 'yahoo-actions', start, end)
    # pt(actions.head())
    #
    # d = yf.Ticker("ITA")
    # pt(d.splits)
    #
    #
    # d = yf.Ticker("QQQ")
    # pt(d.splits)
