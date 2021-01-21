import json
import logging
import datetime

import uuid
import pandas as pd
from tornado import ioloop, gen, httpclient, queues
from tornado.httpclient import HTTPRequest
from pprint import pprint as pt
from decimal import Decimal
import awswrangler as wr
import pandas_datareader as pdr

import config as cf
from lib import utils
from lib import xq

# 设置pd列数
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

concurrency = 50  # 并发数


def ss(s):
    return s.strip('^').replace('=', '_')

async def get_symbol(symbol):
    '''
    获取数据
    '''
    df = await pdr.get_data_yahoo(symbol, interval = "d")
    return df


def sync(symbol, df):
    df = df.rename(columns={'High':'h','Low':'l','Open':'o','Close':'c', 'Volume':'v', 'Adj Close':'ac'})
    df['date'] = list(df.index)
    df['hash_key'] = 'US_{}'.format(ss(symbol))
    df['date'] = df['date'].apply(lambda x: str(x).split()[0])
    for i in ['h','l','c','o','v','ac']:
        if i == 'v': continue
        if i == 'date':
            df['date'] = df['date'].apply(lambda x: str(x).split()[0])
            continue
        df[i] = df[i].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    df.index = range(0,len(df))
    # df = df.tail(3)
    print(df.tail(3))
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)


async def main():
    seen_set = set()
    q = queues.Queue()

    async def work(symbol):
        '''get data'''
        if symbol in seen_set:
            return
        print('ddd')
        logging.info(f"get：{symbol}")
        seen_set.add(symbol)
        data = await get_symbol(symbol)
        sync(symbol, data)

        return data

    async def worker():
        async for symbol in q:
            if symbol is None:
                return
            try:
                await work(symbol)
            except Exception as e:
                print(f"exception:{e}")
            finally:
                # 计数器，每进入一个就加1，所以我们调用完了之后，要减去1
                q.task_done()

    # 放入初始url到队列
    for idx, i in enumerate(cf.LEFT):
        await q.put(i)

    # 启动协程，同时开启三个消费者
    workers = gen.multi([worker() for _ in range(concurrency)])

    # 会阻塞，直到队列里面没有数据为止
    await q.join()

    for _ in range(concurrency):
        await q.put(None)

    # 等待所有协程执行完毕
    await workers

    # 处理 SPX 和 Total 的宽度计算
    # breadth_total()


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
