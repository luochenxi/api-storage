import io
import urllib3
import pandas as pd
import quandl
import logging
import awswrangler as wr
from decimal import Decimal
from datetime import datetime

import config as cf
from lib import utils

urllib3.disable_warnings()
# 设置pd列数
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

column_dict = {
    "Date": 'date',
    "Preliminary Estimate": 'preliminary_estimate',
    "First Revision": 'first_revision',
    "Second Revision": 'second_revision',
    "Final": 'final',
}

'''
    # df['preliminary_estimate'] = df['preliminary_estimate'].apply(lambda x: Decimal('%.3f' %  x))
    # df['first_revision'] = df['first_revision'].apply(lambda x: Decimal('%.3f' %  x))
    # df['second_revision'] = df['second_revision'].apply(lambda x: Decimal('%.3f' %  x))
    # df['final'] = df['final'].apply(lambda x: Decimal('%.3f' %  x))
'''
@utils.extract_context_info
def wei():
    """Weekly Economic Index (WEI) https://www.newyorkfed.org/research/policy/weekly-economic-index"""
    resp = utils.get(cf.NEWYORKFED_WEI_URL)
    column = ['date', 'preliminary_estimate', 'first_revision', 'second_revision', 'final']
    df = pd.read_csv(io.StringIO(resp.text), names=column, header=0)
    # 把 Nan 替换为NULL
    df = df.fillna("NULL")
    df['hash_key'] = "US_ECONOMIC_WEI"
    df['i18n'] = "economic.chart.US_ECONOMIC_WEI"
    df['date'] = pd.to_datetime(df['date'],infer_datetime_format=True)
    # 1/2/2008 转换为 2008-01-02
    df['date'] = df['date'].apply(lambda x: f(x))
    # float 转换为 字符串
    df['preliminary_estimate'] = df['preliminary_estimate'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df['first_revision'] = df['first_revision'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df['second_revision'] = df['second_revision'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df['final'] = df['final'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    # 只更新最后5条数据
    df = df.tail(5)
    # 写入到 aws dynamodb
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

'''
https://www.quandl.com/api/v3/datasets/FRED/DFEDTARU.csv?api_key=DUdvbzr6ytDBqdR6zxxU
Federal Funds Target Range - Upper Limit
'''
@utils.extract_context_info
def fftr():
    '''Federal Funds Target Range - Upper Limit // 联储目标利益'''
    url = 'https://www.quandl.com/api/v3/datasets/FRED/DFEDTARU.csv?api_key=DUdvbzr6ytDBqdR6zxxU'
    resp = utils.get(url)
    column = ['date', 'value']
    df = pd.read_csv(io.StringIO(resp.text),names=column, header=0)
    df['hash_key'] = "US_ECONOMIC_FFTRUL"
    df['i18n'] = "economic.chart.US_ECONOMIC_FFTRUL"
    df['value'] = df['value'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    # 只更新最后5条数据
    df = df.tail(5)
    # 写入到 aws dynamodb
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

@utils.extract_context_info
def effr():
    '''Effective Federal Funds Rate 美联储有效基金利率'''
    url = 'https://www.quandl.com/api/v3/datasets/FRED/FEDFUNDS.csv?api_key=DUdvbzr6ytDBqdR6zxxU'
    resp = utils.get(url)
    column = ['date', 'value']
    df = pd.read_csv(io.StringIO(resp.text),names=column, header=0)
    df['hash_key'] = "US_ECONOMIC_FEDFUNDS"
    df['i18n'] = "economic.chart.US_ECONOMIC_FEDFUNDS"
    df['value'] = df['value'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    # 只更新最后5条数据
    df = df.tail(3)
    # 写入到 aws dynamodb
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

@utils.extract_context_info
def INFLATION_USA():
    '''CPI RATEINF/INFLATION_USA  Inflation Rates'''
    url = 'https://www.quandl.com/api/v3/datasets/RATEINF/INFLATION_USA.csv?api_key=DUdvbzr6ytDBqdR6zxxU'
    resp = utils.get(url)
    column = ['date', 'value']
    df = pd.read_csv(io.StringIO(resp.text),names=column, header=0)
    df['hash_key'] = "US_ECONOMIC_INFLATION"
    df['i18n'] = "economic.chart.US_ECONOMIC_INFLATION"
    df['value'] = df['value'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    # 只更新最后5条数据
    df = df.tail(3)
    # print(df)
    # 写入到 aws dynamodb
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

@utils.extract_context_info
def USTREASURY_REALYIELD():
    column = ['5YR', '7YR','10YR', '20YR', '30YR','date']
    df = quandl.get("USTREASURY/REALYIELD", authtoken="DUdvbzr6ytDBqdR6zxxU")
    df = df.fillna("NULL")
    df['date'] = list(df.index)
    df.index = range(0,len(df))
    df = df.rename(columns={'Date': 'date'})
    df.columns = column
    df['hash_key'] = 'ECONOMIC_USTREASURY_REALYIELD'
    df['i18n'] = 'economic.chart.ECONOMIC_USTREASURY_REALYIELD'
    for i in column:
        if i == 'date':
            df['date'] = df['date'].apply(lambda x: f(x))
            continue
        df[i] = df[i].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df = df.tail(3)
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

@utils.extract_context_info
def gold():
    '''
    http://www.spdrgoldshares.com
    https://www.spdrgoldshares.com/usa/historical-data/
    :return:黄金 和黄金公吨
    '''
    resp = utils.get(cf.GLD_URL)
    # header 在第6行
    df = pd.read_csv(io.StringIO(resp.text), header=6)
    cl = ['Date',' GLD Close', ' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT']
    df = df[cl] # 取指定列
    # header 重命名
    df = df.rename(columns={'Date':'date', ' GLD Close': 'gold', ' Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT': 'tnavt'})

    # 去除文字行
    df = df[~df['gold'].str.contains('HOLIDAY')]
    df = df[~df['gold'].str.contains('AWAITED')]
    df = df[~df['gold'].str.contains('NYSE Closed')]
    df = df[~df['tnavt'].str.contains('AWAITED')]

    # 转换时间格式
    df['date'] = df.apply(lambda item: strfdate(item['date'], ), axis = 1)
    df['gold'] = df['gold'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df['tnavt'] = df['tnavt'].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df['hash_key'] = 'US_SPDR_GOLD'
    df['i18n'] = 'economic.chart.US_SPDR_GOLD'
    df = df.tail(3)
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

@utils.extract_context_info
def market_some_hold(url=cf.NEWYORKFED_SOMA_HOLD_URL):
    '''
    https://www.newyorkfed.org/markets/soma-holdings
    :param url:
    :return:
    '''
    start,end = utils.day_range() # 只看最近一周
    url = url.format(start, end) # url 拼接

    resp = utils.get(url)
    logger.info("NEWYORKFED: code:{} data: {}".format(resp.status_code, resp.text))
    if resp.status_code != 200: return
    resp_dict = resp.json()
    summary = resp_dict['soma']['summary']
    # 没有数据
    if len(summary) == 0: return

    column = ['date', 'mbs', 'cmbs', 'tips', 'frn', 'tipsInflationCompensation',
              'notesbonds', 'bills', 'agencies', 'total']
    li = []
    for v in summary:
        li.append([
            v['asOfDate'], v['mbs'],v['cmbs'],v['tips'],v['frn'],
            v['tipsInflationCompensation'],v['notesbonds'],v['bills'],v['agencies'],v['total'],
        ])

    df = pd.DataFrame(columns=column, data=li)
    df['hash_key'] = 'US_SYSTEM_SOMA_HOLDINGS' # 设置hash_key
    df['i18n'] = 'economic.chart.US_SYSTEM_SOMA_HOLDINGS'
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

def f(s):
    return str(s).split()[0]

def d(s):
    return datetime.strptime('%m/%d/%Y', s).strftime("%Y-%m-%d")
def strfdate(str_date):
    return datetime.strptime(str_date, '%d-%b-%Y').strftime('%Y-%m-%d')

if __name__ == '__main__':
    market_some_hold()
    # gold()
    # USTREASURY_REALYIELD()
    # INFLATION_USA()
    # effr()
    # fftr()
    # wei()
