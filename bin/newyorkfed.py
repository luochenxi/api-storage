import json
import urllib3
import os
import csv
import logging
from datetime import date
from dateutil.relativedelta import relativedelta
from retrying import retry
from tempfile import TemporaryFile  # , NamedTemporaryFile

import config as cf
from lib import utils
logger = logging.getLogger(__name__)


urllib3.disable_warnings()


def day_range():
    start = date.today() - relativedelta(days=9)
    end = date.today() + relativedelta(days=1)
    return start, end

def wei(url):
    logger.info("start wei index data ...")
    # ['Date', 'Preliminary Estimate', 'First Revision', 'Second Revision', 'Final']
    _tmp = "tmp.csv"
    rep = utils.get(url)
    f = open(_tmp, 'w')
    f.write(rep.text)
    f.flush()
    f.close()
    f = open(_tmp, 'r')
    d = csv.DictReader(f)

    res = dict()
    for i in d:
        key = i['Date']
        if i['Final']:
            value = i['Final']
        else:
            if i['Second Revision']:
                value = i['Second Revision']
            else:
                if i['First Revision']:
                    value = i['First Revision']
                else:
                    value = i['Preliminary Estimate']
        res[key] = float(value)
    ff = open(cf.NEWYORKFED_WEI_SRC_DATA, 'w')
    json.dump(res, ff)

    newyorkfed_file = os.path.join(cf.OUTPUT, cf.NEWYORKFED_WEI_NAME)
    utils.save_ouput(res, newyorkfed_file)
    logger.info("end wei index data .")
    return res



def market_some_hold(url):
    start,end = day_range()
    url = url.format(start, end)

    rep = utils.get(url)
    logger.info("NEWYORKFED: code:{} data: {}".format(rep.status_code, rep.text))
    if rep.status_code != 200: return
    rep_data = rep.json()
    summary = rep_data['soma']['summary']
    # 没有数据
    if len(summary) == 0:
        return
    # 读取以前的数据
    tf = open(cf.NEWYORKFED_SOMA_SRC_DATA, 'r')
    ALL_DICT = json.load(tf)
    tf.close()

    for i in summary:

        as_of_date = i['asOfDate']
        if as_of_date in ALL_DICT:
            logger.warning("NEWYORKFED: {} is alert.".format(as_of_date))
            continue
        ALL_DICT[as_of_date] = i
    # save src data
    fb = open(cf.NEWYORKFED_SOMA_SRC_DATA, 'w')
    json.dump(ALL_DICT, fb)
    fb.flush()
    fb.close()
    # save output data
    newyorkfed_file = os.path.join(cf.OUTPUT, cf.NEWYORKFED_HOLD_OUTPUT_NAME)
    utils.save_ouput(ALL_DICT, newyorkfed_file)
    return ALL_DICT

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS NEWYORKFED TIME. RUN...')
        market_some_hold(cf.NEWYORKFED_SOMA_HOLD_URL)
        wei(cf.NEWYORKFED_WEI_URL)
    logger.info('NOT NEWYORKFED TIME.')

if __name__ == '__main__':
    # bin()
    market_some_hold(cf.NEWYORKFED_SOMA_HOLD_URL)

#
# r = {}
#
# for i in range(906):
#     with open("../summary/{}.json".format(i), 'r') as f:
#         data = json.load(f)
#         # print(type(data), len(data), data)
#         # print(len(data['soma']['summary']), data['soma'])
#         # print(data['soma']['summary'])
#         if len(data['soma']['summary']) > 1:
#             print(i)
#         summary = data['soma']['summary']
#         item = summary[0]
#         time = item.get('asOfDate')
#         r[time] = item
#
# from pprint import pprint as pt
#
# pt(r)
#
# with open('../data/us/newyorkfed.json', 'w') as ff:
#     json.dump(r, ff)