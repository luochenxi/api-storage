from datetime import date
from pprint import pprint as pt
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
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
    etd_data = yf.Ticker("^GSPC")
    print(etd_data)
    data = etd_data.history(period="1mo", interval="30m")
    pt(data)

    # pt(data.to_dict(orient='index'))

    # pt(data.index)
    first = data.resample("D")['Close'].first().dropna(axis=0,how='any')
    last = data.resample("D")['Close'].last().dropna(axis=0,how='any')
    print(type(first))
    print(last)
    l = list(last - first)
    pt(l)
    t = 1000
    t2 = 1000
    ret = []
    aa = []
    first['val'] = l
    print(type(first))
    print(first.index)
    for i in first.index:
        print(type(i), str(i).split()[0])
    # for i in l :
    #     t2 += i
    #     ret.append(t + i)
    #
    #     aa.append(t2)
    # print(aa)
    # print(ret)
    # plt.plot(ret)
    #
    # plt.show()
    # plt.plot(aa)
    # plt.show()
    # pt(data.resample("D")['Close'].first())
    # pt(data.groupby([data.index, pd.Grouper(freq='D')]))
    # pt(list(data.groupby([data.index, pd.Grouper(freq='D')])))
    # print(type(etd_data.history(period="3d", interval="1h")))
    # for i in etd_data.history(period="3d", interval="1h").to_dict(orient='records'):
    #     print(i)
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
