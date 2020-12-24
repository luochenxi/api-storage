import ssl
import os
import logging
import urllib3
import time
import csv
import io
import yfinance as yf
from datetime import date
from datetime import datetime
import pandas as pd
from retrying import retry
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from pandas_datareader import data as pdr

import config as cf
from lib import utils

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)


logger = logging.getLogger(__name__)


def strfdate(str_date):
    return datetime.strptime(str_date, '%d-%b-%Y').strftime('%Y-%m-%d')

def run():
    cl = ['Date',' GLD Close', ' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT']

    origin = csv.DictReader(open(cf.US_GLD_SRC_DATA, 'r'))
    resp = utils.get(cf.GLD_URL)
    # header 在第6行
    df = pd.read_csv(io.StringIO(resp.text), header=6)
    df = df[~df[' GLD Close'].str.contains('HOLIDAY')]
    df = df[cl] # 取指定列

    if len(list(origin)) < len(df): # 有新的数据
        # 转换时间格式
        df['Date'] = df.apply(lambda item: strfdate(item['Date'], ), axis = 1)
        df = df.rename(columns={' GLD Close': 'GLD Close', ' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT': 'TNAVT'})
        df.to_csv(cf.US_GLD_SRC_DATA)
        split_output(df.to_dict(orient='records'))



def split_output(data, y=[5,]):
    years = []
    for i in y:
        r = utils.today() - relativedelta(years=i)
        years.append(r)
    for i in years:
        c = []
        for item in data:
            _t = datetime.strptime(item['Date'], "%Y-%m-%d")
            _t = date(year=_t.year, month=_t.month, day=_t.day)
            if i <= _t:
                c.append(item)
        utils.save_ouput(c, cf.GLD_OUTPUT)

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS GLD TIME. RUN...')
        run()
        return
    logger.info('NOT GLD TIME.')


if __name__ == '__main__':
    run()
