import json
import logging
import datetime

import pandas as pd
from tornado import ioloop, gen, httpclient, queues
from tornado.httpclient import HTTPRequest
import awswrangler as wr
from decimal import Decimal
import numpy as np

import config as cf
from lib import utils
from lib import xq
from .oper_dynamodb import update_dynamodb

logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)

'''
https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
https://www.slickcharts.com/sp500
'''

# 设置pd列数
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

headers = {
    'Content-Type': 'application/json',
    'Cookie': xq.get_cookies()
}
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
concurrency = 50  # 并发数
GICS = {
    'Communication Services': {'code': 'COM'},
    'Consumer Discretionary': {'code': 'CND'},
    'Consumer Staples': {'code': 'CNS'},
    'Energy': {'code': 'ENE'},
    'Financials': {'code': 'FIN'},
    'Health Care': {'code': 'HLT'},
    'Industrials': {'code': 'IND'},
    'Materials': {'code': 'MAT'},
    'Real Estate': {'code': 'REL'},
    'Information Technology': {'code': 'TEC'},
    'Utilities': {'code': 'UTL'},
}

SYMBOL = dict()
industry = dict()
ALL = dict() # 在SP500里的股票


def gspc_industry(url=url):
    '''结构化每个行业和行业内的股票'''
    resp = utils.get(url)
    global industry
    df_data = pd.read_html(resp.text)
    data = df_data[0].to_dict(orient='records')
    for i in data:
        SYMBOL[i['Symbol']] = {'name': i['GICS Sector']}
        if i['GICS Sector'] in industry:
            industry[i['GICS Sector']]['item'].append(i['Symbol'])
        else:
            industry[i['GICS Sector']] = dict()
            industry[i['GICS Sector']]['item'] = [i['Symbol'], ]
            industry[i['GICS Sector']]['sma20rc'] = []
            industry[i['GICS Sector']]['sma20ro'] = []
    return data


async def get_symbol(url):
    '''
    构造请求并发送  get kline
    :param url:
    :return:
    '''
    global headers
    request_kwargs = {
        'validate_cert': False,
        'headers': headers,
    }
    request = HTTPRequest(url=url, method='GET', **request_kwargs)
    client = httpclient.AsyncHTTPClient()
    response = await client.fetch(request)
    return json.loads(response.body)

cl = 'timestamp volume    open      high       low   close   chg  percent  turnoverrate        amount'.split()
re_dict = dict(
    timestamp='date',
    volume='v',
    open='o',
    high='h',
    low='l',
    close='c',
    chg='chg1d',
    percent='chgp1d',
)

def item_sma(data):
    global SYMBOL, industry, ALL
    column = data['data']['column']
    item = data['data']['item']
    symbol = data['data']['symbol']
    df = pd.DataFrame(columns=column, data=item)
    ALL[symbol] = df[cl] # 取制指定列
    df['sma20close'] = df["close"].rolling(window=20).mean()
    df['sma20open'] = df["open"].rolling(window=20).mean()
    df['sma20rc'] = df["sma20close"] < df['close']
    df['sma20ro'] = df["sma20open"] < df['open']
    df['date'] = df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000))
    item_industry = SYMBOL[symbol]['name']
    d = -1
    last_rc = df['sma20rc'].iloc[d]
    last_ro = df['sma20ro'].iloc[d]
    SYMBOL[symbol]['sma20ro'] = int(last_ro)
    SYMBOL[symbol]['last_rc'] = int(last_rc)
    industry[item_industry]['date'] = df['date'].iloc[d].strftime("%Y-%m-%d")
    industry[item_industry]['timestamp'] = df['timestamp'].iloc[d]
    if last_rc:
        industry[item_industry]['sma20rc'].append(int(last_rc))
    if last_ro:
        industry[item_industry]['sma20ro'].append(int(last_ro))


def breadth_total():
    '''
    所有子行业的数据整理，结构化
    :return:
    '''
    global SYMBOL, industry, GICS
    ret = []
    spx_sma20rc = 0  # rc  收盘  r 是结果  c 收盘
    spx_sma20ro = 0  # ro  开盘  r 是结果  o 开盘
    breadth_close = 0
    breadth_open = 0
    update_at = int(utils.now().timestamp() * 1000)
    for k, v in industry.items():
        industry_total = len(v['item'])
        spx_sma20rc += len(v['sma20rc'])
        spx_sma20ro += len(v['sma20ro'])
        sma_rc = len(v['sma20rc']) / industry_total * 100
        sma_ro = len(v['sma20ro']) / industry_total * 100
        GICS[k]['sma_rc'] = sma_rc
        GICS[k]['sma_rc_round'] = round(sma_rc)
        GICS[k]['sma_ro_round'] = round(sma_ro)
        GICS[k]['sma_ro'] = sma_ro
        industry[k]['sma_rc'] = sma_rc
        industry[k]['sma_rc_round'] = round(sma_rc)
        industry[k]['sma_ro_round'] = round(sma_ro)
        industry[k]['sma_ro'] = sma_ro
        breadth_close += sma_rc
        breadth_open += sma_ro
        # timestamp = industry[k]['timestamp']
        date = industry[k]['date']
        name = k
        symbol = GICS[k]['code']
        open = sma_ro
        close = sma_rc
        update_dynamodb(**dict(
            symbol=symbol,
            date=date,
            name=name,
            o=Decimal(str(open)),
            c=Decimal(str(close)),
            i18n=utils.i18n(symbol),
            data_type="BREADTH_US",
            updatedAt=update_at,
        ))
        # df 的数据部分
        # test demo
        # item = ['US_{}'.format(symbol),symbol, name, Decimal(str(open)), Decimal(str(close)), date, i18n(symbol), 'us', update_at]
        # ret.append(item)

    # 计算spx的宽度
    spx_rc = spx_sma20rc / len(SYMBOL) * 100
    spx_ro = spx_sma20ro / len(SYMBOL) * 100
    industry['SPX'] = dict(sma_rc=breadth_close, sma_ro=breadth_open)
    logging.info(industry)
    for k, v in GICS.items():
        logging.info(
            f"{v['code']}  \t sma_rc_round: {v['sma_rc_round']} \t sma_ro_round: {v['sma_ro_round']}  \t sma_rc: {v['sma_rc']} \t  sma_ro: {v['sma_ro']} ")
    logging.info("SPX open: {}; SPX close: {}".format(spx_ro, spx_rc))
    logging.info(f"Total open: {breadth_open}; SPX close: {breadth_close}")
    spx_data = dict(
        symbol='SPX',
        name='S&P 500',
        date=date,
        o=Decimal(str(spx_ro)),
        c=Decimal(str(spx_rc)),
        i18n=utils.i18n('SPX'),
        data_type="BREADTH_US",
        updatedAt=update_at,
    )
    total_data = dict(
        symbol="TOTAL",
        name="Total",
        date=date,
        o=Decimal(str(breadth_open)),
        c=Decimal(str(breadth_close)),
        i18n=utils.i18n('TOTAL'),
        data_type="BREADTH_US",
        updatedAt=update_at,
    )
    update_dynamodb(**spx_data)
    update_dynamodb(**total_data)
    # test demo      # 写入 Dynamodb
    # ret.append(['US_SPX', 'SPX','S&P 500', Decimal(str(spx_ro)), Decimal(str(spx_rc)), date, i18n('SPX'), 'us', update_at])
    # ret.append(['US_TOTAL','TOTAL', 'Total', Decimal(str(breadth_open)), Decimal(str(breadth_close)), date,i18n('TOTAL'), 'us', update_at])
    # column = ['symbol_country','symbol', 'name', 'o', 'c', 'date', 'i18n','country', 'updatedAt']
    # df = pd.DataFrame(columns=column, data=ret)
    # wr.dynamodb.put_df(df=df, table_name="Breadth-pro")


async def main():
    seen_set = set()
    q = queues.Queue()
    industry_data = gspc_industry()

    async def fetch_url(symbol):
        '''get data'''
        if symbol in seen_set:
            return
        logging.info(f"get：{symbol}")
        seen_set.add(symbol)
        symbol_url = xq.format_url(symbol, -130)
        data = await get_symbol(symbol_url)
        item_sma(data)

        return data

    async def worker():
        async for url in q:
            if url is None:
                return
            try:
                await fetch_url(url)
            except Exception as e:
                logging.error(f"exception:{e}")
            finally:
                # 计数器，每进入一个就加1，所以我们调用完了之后，要减去1
                q.task_done()

    # 放入初始url到队列
    for idx, i in enumerate(industry_data):
        await q.put(i['Symbol'])

    # 启动协程，同时开启消费者
    workers = gen.multi([worker() for _ in range(concurrency)])

    # 会阻塞，直到队列里面没有数据为止
    await q.join()

    for _ in range(concurrency):
        await q.put(None)

    # 等待所有协程执行完毕
    await workers

    # 处理 SPX 和 Total 的宽度计算
    breadth_total()
    # 写入到 aws
    update_at = int(utils.now().timestamp() * 1000)
    df = ALL.pop('AAPL')
    df = df_format('AAPL', df)
    for k,v in ALL.items():
        # 格式化每个v
        v = df_format(k,v)
        df = pd.concat([df, v]) # 合并 v
    df['updatedAt'] = update_at
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

def df_format(k, v):
    v = v.rename(columns=re_dict) # 重命名列
    # 格式化时间
    v['date'] = v['date'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    # 计算一段时间的百分比 天
    v['chgp5d'] = v['c'].pct_change(5) *100
    v['chgp20d'] = v['c'].pct_change(20)*100
    # 计算sma
    v['sma20'] = v["c"].rolling(window=20).mean()
    v['sma60'] = v["c"].rolling(window=60).mean()
    v['sma120'] = v["c"].rolling(window=120).mean()
    # 计算cs sm ml
    v['cs'] = (v['c'] - v['sma20']) / v['sma20'] * 100
    v['sm'] = (v['sma20'] - v['sma60']) / v['sma60'] * 100
    v['ml'] = (v['sma60'] - v['sma120']) / v['sma120'] * 100

    cc = 'o sma20 sma60 sma120 cs sm ml h l c chg1d chgp1d  turnoverrate amount chgp20d chgp5d '.split()
    for i in cc:
        v[i] = v[i].apply(lambda x: 'NULL' if type(x) is np.nan else x)
        # 格式化float：dynamodb 不支持float
        v[i] = v[i].apply(lambda x: '{:.4f}'.format(x) if type(x) is int or type(x) is float else x)
    v['hash_key'] = 'US_STOCK_' + k
    v['n'] =  k
    v['symbol'] = k
    v['i18n'] = 'us.stock.' + k
    return v.tail(2) #这里控制数量
    # return v

if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
