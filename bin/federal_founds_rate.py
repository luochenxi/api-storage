import json
import urllib3
import os
import csv
import time
import requests
import logging
import base64
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from retrying import retry
from collections import namedtuple
import pandas as pd
from tempfile import TemporaryFile  # , NamedTemporaryFile

import config as cf
from lib import utils

urllib3.disable_warnings()
logger = logging.getLogger(__name__)


def cpi_format(data):
    res = {}
    for i in data:
        _t, val = i
        t = time.strftime("%Y-%m-%d",time.localtime(_t/1000))
        tmp = dict(day=t, value=val)
        res[t] = tmp
    return res

def split_data(ys=[5,]):
    '''
    分割数据
    :param ys: 年 list
    :return:
    '''
    data = utils.read(cf.FEDERAL_FUNDS_RATE_SRC_DATA)
    years = []
    ret = []
    for i in ys:
        r = utils.today() - relativedelta(years=i)
        years.append(r)
    for y in years:
        for k,v in data.items():
            _t = datetime.datetime.strptime(k, "%Y-%m-%d")
            _t = date(year=_t.year, month=_t.month, day=_t.day)
            if y <= _t:
                ret.append(v)
        idx = years.index(y)
        path = os.path.join(cf.OUTPUT, cf.FEDERAL_FUNDS_RATE_NAME.format(ys[idx]))
        utils.save_ouput(ret, path)

def merge(ys=[5,]):
    tmp_ret = dict()
    years = []
    cpi_data = utils.read(cf.CPI_SRC_DATA)
    ffr_data = utils.read(cf.FEDERAL_FUNDS_RATE_SRC_DATA)
    data_dict = dict(
        cpi=sorted(cpi_data.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y-%m-%d"), reverse=False),
        ffr=sorted(ffr_data.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y-%m-%d"), reverse=False),
    )
    for i in ys:
        r = utils.today() - relativedelta(years=i)
        years.append(r)

    for y in years:
        for k,v in data_dict.items():

            for i in v:
                tk, tv = i[0], i[1]
                # split year
                _t = datetime.datetime.strptime(tk, "%Y-%m-%d")
                _t = date(year=_t.year, month=_t.month, day=_t.day)
                if y > _t:
                    continue
                # in year
                item = {
                    'day': tv['day'],
                    k: tv['value']
                }
                if tmp_ret.get(tk, None):
                    tmp_ret[tk].update(item)
                else:
                    tmp_ret[tk] = item

    ret = []
    for item in sorted(tmp_ret.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y-%m-%d"), reverse=False):
        ret.append(item[1])
    utils.save_ouput(ret, cf.CPI_FFR_OUTPUT)

def run():
    rep = utils.get(cf.FEDERAL_FUNDS_RATE_URL)
    raw_data = rep.json()['chart_data'][0][0]['raw_data']
    ret = cpi_format(raw_data)
    src_data = utils.read(cf.FEDERAL_FUNDS_RATE_SRC_DATA)
    for k,v in ret.items():
        if k in src_data:
            continue
        logger.info('Federal Founds Rate New Data: {}'.format(v))
        src_data[k] = v
    utils.write(cf.FEDERAL_FUNDS_RATE_SRC_DATA, src_data)
    split_data()
    merge()
    logger.info('run end.')

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS Federal Funds Rate TIME. RUN...')
        run()
        return
    logger.info('NOT Federal Funds Rate TIME.')

if __name__ == '__main__':
    # run()
    merge()
    # rep = utils.get(cf.FEDERAL_FUNDS_RATE_URL)
    # print(rep.text)
    # print(rep.json()['observations'][0])
    # a = rep.json()['observations'][0]
    # for i in a:
    #     print(i)