import os
import logging
import time
import yfinance as yf
from datetime import date
from retrying import retry
from decimal import Decimal
from dateutil.relativedelta import relativedelta
from pandas_datareader import data as pdr

import config
from lib import utils
from lib import xq


logger = logging.getLogger(__name__)
yf.pdr_override()

def day_range():
    start = date.today() - relativedelta(days=2)
    end = date.today() + relativedelta(days=1)
    return start.isoformat(), end.isoformat()

@retry(stop_max_attempt_number=20, wait_fixed=3)
def get_data():
    start, end = day_range()
    CU_ALL = {}
    for etf in config.ETF_LIST:
        logger.info("GET DATA: {}".format(etf))
        etd_data = pdr.get_data_yahoo(etf, start=start, end=end)
        data_records = etd_data.to_dict(orient='records')
        data_index = etd_data.to_dict(orient='index')
        # 格式化数据
        ret = dict()
        for idx, i in enumerate(data_index):
            d = str(i).split(" ")[0]
            ret[d] = data_records[idx]
            if not CU_ALL.get(d, None):
                CU_ALL[d] = {etf: data_records[idx]}
            else:
                CU_ALL[d][etf] = data_records[idx]
        # 比对数据差异
        path = os.path.join(config.DATA_US_ETF_DIR, "{}.json".format(etf))
        src_data = utils.read(path)
        for k,v in ret.items():
            if k in src_data: continue
            logger.info("NEW {} DATA: {}".format(etf, k))
            src_data[k] = v
        # save data
        utils.write(path, src_data)
        etf_output(src_data, etf)
    return CU_ALL



def get_xq():
    CU_ALL = {}
    for etf in config.ETF_LIST:
        logger.info("GET XQ DATA: {}".format(etf))
        time.sleep(0.3)
        etd_data = xq.get_kline(etf)
        symbol_data  =  etd_data['data']['item']
        symbol_dict = dict()
        for i in symbol_data:
            d = time.strftime("%Y-%m-%d",time.localtime(i[0]/1000))
            tmp = {
                "Open": i[2],
                "High": i[3],
                "Low": i[4],
                "Close": i[5],
                "Adj Close": i[5],
                "Volume": i[1]
            }
            symbol_dict[d] = tmp
            if not CU_ALL.get(d, None):
                CU_ALL[d] = {etf: tmp}
            else:
                CU_ALL[d][etf] = tmp
                # save data
        path = os.path.join(config.DATA_US_ETF_DIR, "{}.json".format(etf))
        utils.write(path, symbol_dict)
        etf_output(symbol_dict, etf)
    return CU_ALL

def etf_output(data, etf, day=22):
    _sort = sorted(data.items(),key=lambda x:x[0])
    res = []
    for item in _sort[-day:]:
        d = dict(Date=item[0])
        val = item[1]['Adj Close']
        d['Close'] = val
        res.append(d)
    logger.info("Write ETF: {}".format(etf))
    path = os.path.join(config.US_ETF_OUTPUT, "{}.json".format(etf))
    utils.save_ouput(res, path)

def fangman(data)-> dict:
    ret = dict()
    for k,v in data.items():
        FANGMAN = {
            'Open':0,
            'High':0,
            'Low':0,
            'Close':0,
            'Adj Close':0,
        }
        for i in config.FANGMAN:
            item = v.pop(i)
            FANGMAN['Adj Close'] += item['Adj Close']
            FANGMAN['Open'] += item['Open']
            FANGMAN['High'] += item['High']
            FANGMAN['Low'] += item['Low']
            FANGMAN['Close'] += item['Close']
        v['FANGMAN'] = FANGMAN
        ret[k] = v
    return ret

def all_output(data, etf, day=22):
    data = fangman(data)
    _sort = sorted(data.items(),key=lambda x:x[0])
    res = []
    for i in _sort[-day:]:
        d = dict(Date=i[0])
        for etf,val in i[1].items():
            adj_close = Decimal(val['Adj Close']).quantize(Decimal("0.00"))
            d[etf] = float(adj_close)
        res.append(d)
    utils.save_ouput(res, config.ALL_ETF_OUTPUT)

@retry(stop_max_attempt_number=20, wait_fixed=3)
def sp500smi():
    etd_data = yf.Ticker("^GSPC")
    data = etd_data.history(period="1mo", interval="30m")
    first = data.resample("D")['Close'].first().dropna(axis=0,how='any')
    last = data.resample("D")['Close'].last().dropna(axis=0,how='any')
    l = list(last - first)
    l = [i+2000 for i in l]
    ret = []
    for idx, i in enumerate(first.index):
        ret.append({
            "Date": str(i).split()[0],
            "val": l[idx]
        })
    utils.save_ouput(ret, config.SP500_SMI_OUTPUT)


def run():
    new_data = get_xq()
    # src_data = utils.read(config.ALL_ETF_DIR)
    # for k,v in new_data.items():
    #     if k in src_data: continue
    #     logger.info("NEW DATA: {}".format(k))
    #     src_data[k]=v
    logger.info("SAVE ETF  DATA...")
    utils.write(config.ALL_ETF_DIR, new_data)
    all_output(new_data, "ALL")
    sp500smi()

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS ETF TIME. RUN...')
        run()
        logger.info('IS ETF TIME. END.')
        return
    logger.info('NOT ETF TIME.')

if __name__ == '__main__':
    # run()
    sp500smi()
    # test()
    #
    # df = pro.us_daily(ts_code='VTI', start_date='20201001', end_date='20201130')
    # print(df)
    # print(df.to_dict())
    #
    # print(len(ETF_LIST))
    # print('sdf')
    # print(len(ETF_LIST))
    # ALL = {}
    # for j in ETF_LIST:
    #     print("start...")
    #     data = pdr.get_data_yahoo(j, start="2020-10-15", end="2020-12-1", )
    #     print(type(data), )
    #     print(data)
    #     print(data.to_dict(orient='index'))
    #     print(data.to_dict(orient='records'))
    #     data_records = data.to_dict(orient='records')
    #     data = data.to_dict(orient='index')
    #     ret = {}
    #     for idx, i in enumerate(data):
    #         d = str(i).split(" ")[0]
    #         ret[d] = data_records[idx]
    #         print(type(str(i)), str(i), data_records[idx])
    #         if not ALL.get(d):
    #             tmp = {j: data_records[idx]}
    #             ALL[d] = tmp
    #         else:
    #             ALL[d][j] = data_records[idx]
    #     path = os.path.join(config.DATA_US_ETF_DIR, "{}.json".format(j))
    #     utils.write(path, ret)
    #     print(ret)
    #     print("end.")
    #     # time.sleep(60)
    # all_path = os.path.join(config.DATA_US_ETF_DIR, "ALL.json")
    # utils.write(all_path, ALL)
    # ret = []
    # for i in ETF_LIST:
    #     path = os.path.join(config.DATA_US_ETF_DIR, "{}.json".format(i))
    #     name = path
    #     data = utils.read(name)
    #     if len(data) < 10:
    #         ret.append(name)
    # print(ret)

