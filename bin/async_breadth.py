'''
https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
https://www.slickcharts.com/sp500
'''
import logging
import time
import os
import json
import datetime
import pandas as pd
from tornado import ioloop, gen, httpclient, queues
from tornado.httputil import HTTPHeaders
from tornado.httpclient import HTTPRequest
from pprint import pprint as pt

import config as cf
from lib import utils
from lib import xq

logger = logging.getLogger(__name__)

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
GICS_RESULT = dict()

SYMBOL = dict()
industry = dict()


def gspc_industry(url=url):
    '''结构每个行业和行业内的股票'''
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
    构造请求并发送
    :param url:
    :return:
    '''
    global headers
    request_kwargs = {
        'validate_cert': False,
        'headers': headers,
    }
    request = HTTPRequest(url=url, method='GET', **request_kwargs)
    # client = httpclient.AsyncHTTPClient(force_instance=True,max_clients=200)
    client = httpclient.AsyncHTTPClient()
    response = await client.fetch(request)
    return json.loads(response.body)


def item_sma(data):
    global SYMBOL, industry
    column = data['data']['column']
    item = data['data']['item']
    symbol = data['data']['symbol']
    df = pd.DataFrame(columns=column, data=item)
    df['sma20close'] = df["close"].rolling(window=20).mean()
    df['sma20open'] = df["open"].rolling(window=20).mean()
    df['sma20rc'] = df["sma20close"] < df['close']
    df['sma20ro'] = df["sma20open"] < df['open']
    df['date'] = df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    item_industry = SYMBOL[symbol]['name']
    print(df)
    day = -1
    last_rc = df['sma20rc'].iloc[day]
    last_ro = df['sma20ro'].iloc[day]
    industry[item_industry]['date'] = df['date'].iloc[day]
    SYMBOL[symbol]['sma20ro'] = int(last_ro)
    SYMBOL[symbol]['last_rc'] = int(last_rc)
    if last_rc:
        industry[item_industry]['sma20ro'].append(int(last_rc))
    if last_ro:
        industry[item_industry]['sma20rc'].append(int(last_ro))


def breadth_total():
    '''
    所有子行业的数据整理，结构化
    :return:
    '''
    global SYMBOL, industry, GICS
    spx_sma20rc = 0  # rc  收盘  r 是结果  c 收盘
    spx_sma20ro = 0  # ro  开盘  r 是结果  o 开盘
    breadth_close = 0
    breadth_open = 0
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
        GICS[k]['date'] = v['date']
        industry[k]['sma_rc'] = sma_rc
        industry[k]['sma_rc_round'] = round(sma_rc)
        industry[k]['sma_ro_round'] = round(sma_ro)
        industry[k]['sma_ro'] = sma_ro
        breadth_close += sma_rc
        breadth_open += sma_ro

    spx_rc = spx_sma20rc / len(SYMBOL) * 100
    spx_ro = spx_sma20ro / len(SYMBOL) * 100
    industry['SPX'] = dict(sma_rc=breadth_close, sma_ro=breadth_open)
    save_data()  # 数据落盘
    # utils.write('./sma.json', GICS)
    # utils.write('./gics.json', industry)
    for k, v in GICS.items():
        print(
            f"{v['code']}  \t sma_rc_round: {v['sma_rc_round']} \t sma_ro_round: {v['sma_ro_round']}  \t sma_rc: {v['sma_rc']} \t  sma_ro: {v['sma_ro']} ")
    print("SPX open: {}; SPX close: {}".format(spx_ro, spx_rc))
    print(f"Total open: {breadth_open}; SPX close: {breadth_close}")


def save_data():
    tmp = dict()
    last_day = None
    global GICS
    for k, v in GICS.items():
        code = v['code']
        last_day = v['date']
        tmp[code] = {
            "close": v['sma_rc'],
            "open": v['sma_ro'],
            "pre": v['sma_rc'],
            "low": v['sma_ro'],
            "high": v['sma_ro'],
            "other": 'xq',
            'day': v['date'],
        }
    path = os.path.join(cf.DATA_US_DIR, 'sp500_all.json')
    all = utils.read_json_file(path)
    all[last_day] = tmp
    utils.save_to_json(path, all)
    # breadth.split_total(all, cf.OUTPUT)
    # 保存所有数据
    output = os.path.join(cf.OUTPUT, 'sp500_all.json')
    utils.save_to_json(output, all)
    # utils.day_trading_save(all)
    utils.split_save_json(all, cf.OUTPUT, 'sp500_{}.json')


async def main():
    seen_set = set()
    q = queues.Queue()
    industry_data = gspc_industry()

    async def fetch_url(symbol):

        if symbol in seen_set:
            return

        logger.info(f"get：{symbol}")
        seen_set.add(symbol)

        symbol_url = xq.format_url(symbol)
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
                print(f"exception:{e}")
            finally:
                # 计数器，每进入一个就加1，所以我们调用完了之后，要减去1
                q.task_done()

    # 放入初始url到队列
    for idx, i in enumerate(industry_data):
        await q.put(i['Symbol'])

    # 启动协程，同时开启三个消费者
    workers = gen.multi([worker() for _ in range(concurrency)])

    # 会阻塞，直到队列里面没有数据为止
    await q.join()

    for _ in range(concurrency):
        await q.put(None)

    # 等待所有协程执行完毕
    await workers

    breadth_total()


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
