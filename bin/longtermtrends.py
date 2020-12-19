import json
import urllib3
import os
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

TASK = namedtuple("task", ["url", "file", "output"])

def tasks():
    _t = {
        cf.OLI_GOLD_RATIO_URL: [cf.OLI_GOLD_RATIO_SRC_DATA, cf.OLI_GOLD_RATIO_OUTPUT_NAME],
        cf.OLI_URL: [cf.OLI_SRC_DATA, cf.OLI_OUTPUT_NAME],
        cf.GOLD_URL: [cf.GOLD_SRC_DATA, cf.GOLD_OUTPUT_NAME],
        cf.COPPER_GOLD_RATIO_URL: [cf.COPPER_GOLD_RATIO_SRC_DATA, cf.COPPER_GOLD_RATIO_NAME],
        cf.INTEREST_RATES_URL: [cf.INTEREST_RATES_SRC_DATA, cf.INTEREST_RATES_NAME],
        cf.COPPER_URL: [cf.COPPER_SRC_DATA, cf.COPPER_NAME],
        cf.GOLD_SINCE_URL: [cf.GOLD_SINCE_SRC_DATA, cf.GOLD_SINCE_NAME],
    }
    t = []
    for k,v in _t.items():
        task = TASK(url=k, file=v[0], output=v[1])
        t.append(task)
    return t


def bs64(data):
    res = []
    for i in data:
        _key, _val = i[0], i[1]
        key, val = base64.b64decode(_key),base64.b64decode(_val)
        if b"T" in key:
            key, _ = key.split(b"T")
        res.append({'t':str(key, encoding='utf8'),'v': float(val) })
    return res

def run():
    t = tasks()
    for i in t:
        path, file_name = os.path.split(i.file)
        name, suffix = file_name.split('.')
        output_name_format = name + "_{}." + suffix
        rep = utils.get(i.url)
        logger.info(i)
        logger.info(rep.text[:100])
        new_data = bs64(rep.json())
        src_data = utils.read(i.file)
        if len(new_data) > len(src_data):
            logger.info('write data: {}'.format(i))
            utils.write(i.file, new_data)
            split_output(new_data, output_name_format)
        merge()

def merge(ys=[5,]):
    merge_dict = dict(
        copper_gold_ratio=cf.COPPER_GOLD_RATIO_SRC_DATA,
        interest_rates=cf.INTEREST_RATES_SRC_DATA,
        oil_gold_ratio=cf.OLI_GOLD_RATIO_SRC_DATA,
    )
    years = []
    tmp = {}
    for i in ys:
        r = utils.today() - relativedelta(years=i)
        years.append(r)
    for y in years:
        for k,v in merge_dict.items():
            data = utils.read(v)
            for item in data:
                _t = datetime.datetime.strptime(item['t'], "%Y-%m-%d")
                _t = date(year=_t.year, month=_t.month, day=_t.day)
                t = item['t']
                if y <= _t:
                    item_dict =  tmp.get(t, {})
                    item_dict[k] = item['v']
                    tmp[t]= item_dict
        _sort = sorted(tmp.items(),key=lambda x:x[0])
        ret = []
        for i in _sort:
            val = i[1]
            val['t'] = i[0]
            ret.append(val)
        idx = years.index(y)
        path = os.path.join(cf.OUTPUT, 'oli_copper_gold_ratio_{}.json'.format(ys[idx]))
        utils.save_ouput(ret, path)


def split_output(data, fname, y=[5,]):
    # def split_output(data, fname, y=[6]):
    if not fname: return
    years = []
    for i in y:
        r = utils.today() - relativedelta(years=i)
        years.append(r)
    for i in years:
        c = []
        for item in data:
            _t = datetime.datetime.strptime(item['t'], "%Y-%m-%d")
            _t = date(year=_t.year, month=_t.month, day=_t.day)
            if i <= _t:
                c.append(item)
        idx = years.index(i)
        file_name = fname.format(y[idx])
        output_path = os.path.join(cf.OUTPUT, file_name)
        utils.save_ouput(c, output_path)



def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS OLI GOLD TIME. RUN...')
        run()
        return
    logger.info('NOT OLI GOLD TIME.')

if __name__ == '__main__':
    # bin()
    # print(utils.now().date())
    # split_output('d')
    # run()
    merge()
    # print(datetime.date.fromisoformat("2020-01-23"))
    # aa= datetime.datetime.fromisoformat("2020-01-23")

