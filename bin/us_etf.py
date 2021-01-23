import json
import logging
import datetime

import pandas as pd
from tornado import ioloop, gen, httpclient, queues
from tornado.httpclient import HTTPRequest
import awswrangler as wr

import config as cf
from lib import utils
from lib import xq

logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)

# 设置pd列数
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

headers = {
    'Content-Type': 'application/json',
    'Cookie': xq.get_cookies()
}

ALL = dict()
FANGMAN = dict()
concurrency = 10  # 并发数

def dy_float(df):
    for f in ['h','l','c','o','chg1d', 'chgp1d', 'chgp5d', 'chgp20d', 'turnoverrate', 'sma20', 'amount',
              'sma60', 'sma120' ,'cs','sm','ml']:
        df[f] = utils.df_float(df, f, 4)
    return df


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
    # TODO 设置当前ETF所在全景图里面的板块
    column = data['data']['column']
    item = data['data']['item']
    symbol = data['data']['symbol']
    df = pd.DataFrame(columns=column, data=item)
    df = df[cl] # 取制指定列
    df['symbol'] = symbol
    df['n'] = cf.US_ETF_MAP[symbol]
    df['hash_key'] = 'US_ETF_' + symbol
    df['chgp5d'] = df["close"].pct_change(5) * 100
    df['chgp20d'] = df["close"].pct_change(20) * 100
    # 计算移动平均线
    df['i18n'] = 'us.etf.' + symbol
    df['sma20'] = df["close"].rolling(window=20).mean()
    df['sma60'] = df["close"].rolling(window=60).mean()
    df['sma120'] = df["close"].rolling(window=120).mean()
    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    # 计算cs sm ml
    df['cs'] = (df['close'] - df['sma20']) / df['sma20'] * 100
    df['sm'] = (df['sma20'] - df['sma60']) / df['sma60'] * 100
    df['ml'] = (df['sma60'] - df['sma120']) / df['sma120'] * 100
    # 重命名列
    df = df.rename(columns=re_dict)
    df = df.fillna('NULL')
    logging.info(f'write: {symbol}')
    # 处理FANGMAN
    if symbol in cf.FANGMAN:
        FANGMAN[symbol] = df
    else:
        df = dy_float(df)
        ALL[symbol] = df
        # ALL[symbol] = df.tail(3)
        # wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

def fangman():
    df = FANGMAN.pop('AAPL')
    for k,v in FANGMAN.items():
        df['c'] = v['c'] + df['c']
    df = df['date c n hash_key i18n chgp1d chgp5d chgp20d'.split()]
    df['chgp1d'] = df["c"].pct_change(1) *100
    df['chgp5d'] = df["c"].pct_change(5)*100
    df['chgp20d'] = df["c"].pct_change(20)*100
    df['i18n'] = 'us.etf.FANGMAN'
    df['hash_key'] = 'US_ETF_FANGMAN'
    df= df.fillna('NULL')
    df = utils.target_float(df, 'c chgp1d chgp5d chgp20d'.split(), 6)
    # df = df.tail(2)
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

async def main():
    seen_set = set()
    q = queues.Queue()

    async def fetch_url(symbol):
        '''get data'''
        if symbol in seen_set:
            return
        logging.info(f"get：{symbol}")
        seen_set.add(symbol)
        symbol_url = xq.format_url(symbol, -125)
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
                logging.exception(e, url)
            finally:
                # 计数器，每进入一个就加1，所以我们调用完了之后，要减去1
                q.task_done()

    # 放入初始url到队列
    for idx, i in enumerate(cf.ETF_LIST):
    # for idx, i in enumerate(cf.FANGMAN):
        await q.put(i)
        # break

    # 启动协程，同时开启消费者
    workers = gen.multi([worker() for _ in range(concurrency)])

    # 会阻塞，直到队列里面没有数据为止
    await q.join()

    for _ in range(concurrency):
        await q.put(None)

    # 等待所有协程执行完毕
    await workers
    # FANGMAN
    fangman()

    # 写入到 aws
    df = ALL.pop('VTI')
    for k,v in ALL.items():
        df = pd.concat([df, v]) # 合并 v
    df['updatedAt'] = int(utils.now().timestamp() * 1000)
    logging.info(f'write: ALL')
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
