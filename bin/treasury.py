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

def run():
    src_data = utils.read(cf.TREASURY_REAL_RATES_SRC_DATA)
    url = cf.TREASURY_REAL_RATES_URL.format(date.today().year)
    logger.info(url)
    rep = utils.get(url)
    df_list = pd.read_html(rep.text)
    df = df_list[1]
    year_item = df.to_dict(orient='records')
    for item in year_item:
        src_data[item['DATE']] = item

    utils.write(cf.TREASURY_REAL_RATES_SRC_DATA, src_data)
    split_data()

def split_data(ys=[5,]):
    '''
    分割数据
    :param ys: 年 list
    :return:
    '''
    data = utils.read(cf.TREASURY_REAL_RATES_SRC_DATA)
    years = []
    ret = []
    for i in ys:
        r = utils.today() - relativedelta(years=i)
        years.append(r)
    for y in years:
        for k,v in data.items():
            _t = datetime.datetime.strptime(k, "%m/%d/%y")
            _t = date(year=_t.year, month=_t.month, day=_t.day)
            if y <= _t:
                ret.append(v)
        idx = years.index(y)
        path = os.path.join(cf.OUTPUT, cf.TREASURY_REAL_RATES_NAME.format(ys[idx]))
        utils.save_ouput(ret, path)


def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS TREASURY TIME. RUN...')
        run()
        logger.info('IS TREASURY TIME. END.')
        return
    logger.info('NOT TREASURY TIME.')

if __name__ == '__main__':
    # ret = {}
    # split_data()
    run()
    # years = [2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020,]
    # for i in years:
    #     print(i)
    # ret = utils.read(cf.TREASURY_REAL_RATES_SRC_DATA)
    # url = URL.format(date.today().year)
    # rep = get(url)
    # df_list = pd.read_html(rep.text)
    # # print(len(df_list))
    # df = df_list[1]
    # year_item = df.to_dict(orient='records')
    # for item in year_item:
    #     ret[item['DATE']] = item
    #
    # utils.write(cf.TREASURY_REAL_RATES_SRC_DATA, ret)