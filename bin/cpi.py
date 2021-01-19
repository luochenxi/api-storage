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
    data = utils.read(cf.CPI_SRC_DATA)
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
        path = os.path.join(cf.OUTPUT, 'cpi_{}.json'.format(ys[idx]))
        utils.save_ouput(ret, path)


def run():
    rep = utils.get(cf.CPI_URL)
    raw_data = rep.json()['chart_data'][0][0]['raw_data']
    ret = cpi_format(raw_data)
    src_data = utils.read(cf.CPI_SRC_DATA)
    for k,v in ret.items():
        if k in src_data:
            continue
        logger.info('CPI New Data: {}'.format(v))
        src_data[k] = v
    utils.write(cf.CPI_SRC_DATA, src_data)
    split_data()
    logger.info('IS CPI TIME. END')

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS CPI TIME. RUN...')
        run()
        return
    logger.info('NOT CPI TIME.')


if __name__ == '__main__':
    # bin()
    # run()
    # c = {"chart_data": [
    #     [
    #         {"annotation_line_types": [],
    #          "axis_type": "percent__indicator",
    #          "can_annualize": "true",
    #          "currency_code": None, "estimates_start_date": None, "event_types": [], "format": "real",
    #          "frequency": "monthly", "last_value": 1.1821, "long_label": "US Consumer Price Index YoY",
    #          "object_calc": None, "object_id": "I:USCPIYY", "object_type": "indicator",
    #          "raw_data": [[1448841600000, 0.5018], [1451520000000, 0.7295], [1454198400000, 1.3731],
    #                       [1456704000000, 1.0178], [1459382400000, 0.8525], [1461974400000, 1.1251],
    #                       [1464652800000, 1.0193], [1467244800000, 0.9973], [1469923200000, 0.8271],
    #                       [1472601600000, 1.0629], [1475193600000, 1.4638], [1477872000000, 1.6360],
    #                       [1480464000000, 1.6925], [1483142400000, 2.0746], [1485820800000, 2.5000],
    #                       [1488240000000, 2.7380], [1490918400000, 2.3806], [1493510400000, 2.1997],
    #                       [1496188800000, 1.8749], [1498780800000, 1.6335], [1501459200000, 1.7280],
    #                       [1504137600000, 1.9390], [1506729600000, 2.2330], [1509408000000, 2.0411],
    #                       [1512000000000, 2.2026], [1514678400000, 2.1091], [1517356800000, 2.0705],
    #                       [1519776000000, 2.2118], [1522454400000, 2.3597], [1525046400000, 2.4627],
    #                       [1527724800000, 2.8010], [1530316800000, 2.8715], [1532995200000, 2.9495],
    #                       [1535673600000, 2.6992], [1538265600000, 2.2770], [1540944000000, 2.5225],
    #                       [1543536000000, 2.1766], [1546214400000, 1.9102], [1548892800000, 1.5512],
    #                       [1551312000000, 1.5201], [1553990400000, 1.8625], [1556582400000, 1.9964],
    #                       [1559260800000, 1.7902], [1561852800000, 1.6485], [1564531200000, 1.8115],
    #                       [1567209600000, 1.7498], [1569801600000, 1.7113], [1572480000000, 1.7640],
    #                       [1575072000000, 2.0513], [1577750400000, 2.2851], [1580428800000, 2.4866],
    #                       [1582934400000, 2.3349], [1585612800000, 1.5393], [1588204800000, 0.3291],
    #                       [1590883200000, 0.1179], [1593475200000, 0.6457], [1596153600000, 0.9861],
    #                       [1598832000000, 1.3096], [1601424000000, 1.3713], [1604102400000, 1.1821]],
    #          "securitylist_name": None, "short_label": "I:USCPIYY", "source": "BLS", "type": "percent"}]],
    #     "chart_quarterly_date_format": "quarter",
    #     "chart_url": "https://ycharts.com/indicators/us_consumer_price_index_yoy/chart/",
    #     "chart_url_params": "annotations=&annualizedReturns=false&calcs=&chartType=interactive&colors=&correlations=&dateSelection=range&displayDateRange=false&displayTicker=false&endDate=&format=real&legendOnChart=false&maxPoints=891&note=&partner=basic_2000&quoteLegend=false&quotes=&recessions=false&scaleType=linear&securities=id:I:USCPIYY,include:true,,&securityGroup=&securitylistName=&securitylistSecurityId=&source=false&splitType=single&startDate=&title=&units=false&useCustomColors=false&useEstimates=false&zoom=5",
    #     "embed_host": "https://ycharts.com", "embed_linkback_name": "US Consumer Price Index YoY",
    #     "embed_linkback_url": "https://ycharts.com/indicators/us_consumer_price_index_yoy", "end_date": None,
    #     "quotes": None, "start_date": "11/27/2015"}
    # import time
    a = time.localtime(1448841600000/1000)
    b = time.strftime("%Y-%m-%d", a)
    print(b)
    # print(datetime.datetime.)
    # print(c['chart_data'][0][0]['raw_data'])

    # rep = get(cf.CPI_URL)
    # print(rep)
    # print(rep.text)
