import urllib3
import time
import logging
import base64

import datetime
from copy import deepcopy
from retrying import retry
from tempfile import TemporaryFile  # , NamedTemporaryFile
from retrying import retry

import config as cf
from lib import utils

urllib3.disable_warnings()
logger = logging.getLogger(__name__)

def run():
    tasks = {
        cf.US_INITIAL_JOBLESS_URL: [cf.US_INITIAL_JOBLESS_SRC_DATA, cf.INITIAL_JOBLESS_OUTPUT, "initial"],
        cf.US_CONTINUING_JOBLESS_URL: [cf.US_CONTINUING_JOBLESS_SRC_DATA, cf.CONTINUING_JOBLESS_OUTPUT, "continuing"]
    }

    for k ,v in tasks.items():
        get_jobless(k, v[0], v[1], v[2])

@retry(stop_max_attempt_number=5, wait_fixed=3)
def get_jobless(url, src_dir, output_dir, field):
    resp = utils.get(url)
    if resp.status_code != 200:
        raise TypeError
    logger.info(resp.text[-100:])
    src_data = utils.read(src_dir)
    ret = deepcopy(src_data)
    tmp = resp.json()['attr']
    for item in tmp:
        _t = time.strftime("%Y/%m/%d",time.localtime(item.pop('timestamp')/1000))
        item['time'] = _t
        if _t not in src_data:
            ret[_t] = item
            logger.info("New {} Data: {}".format(url, item))
    logger.info("{} Data: Old: {}; New: {}".format(url, len(src_data), len(ret)))
    if len(ret) > len(src_data):
        utils.write(src_dir, ret)
        output(ret, output_dir)
        merge()

def output(data, output_dir):
    ret = list()
    for k,v in data.items():
        ret.append(v)
    utils.save_ouput(ret, output_dir)

def merge():
    logger.info("Jobless Merge...")
    tmp_ret = dict()
    initial_src_data = utils.read(cf.US_INITIAL_JOBLESS_SRC_DATA)
    continuing_src_data = utils.read(cf.US_CONTINUING_JOBLESS_SRC_DATA)
    item_dict = {
        "initial": sorted(initial_src_data.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y/%m/%d"), reverse=False),
        "continuing": sorted(continuing_src_data.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y/%m/%d"), reverse=False),
    }
    # merge: initial continuing
    for k,v in  item_dict.items():
        for i in v:
            tk, tv=i[0], i[1]
            item = {
                "time": tv["time"],
                "{}_actual".format(k): tv["actual"],
                "{}_forecast".format(k): tv["forecast"],
                "{}_revised".format(k): tv["revised"] or tv['actual'],
            }
            if tmp_ret.get(tk, None):
                tmp_ret[tk].update(item)
            else:
                tmp_ret[tk] = item
    # format output
    ret = []
    for item in sorted(tmp_ret.items(), key=lambda i: datetime.datetime.strptime(i[0], "%Y/%m/%d"), reverse=False):
        ret.append(item[1])
    utils.save_ouput(ret, cf.US_JOBLESS_OUTPUT)

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        logger.info('IS Jobless Time. RUN...')
        run()
    logger.info('NOT Jobless TIME.')


if __name__ == '__main__':
    # bin()
    # run()
    merge()
    # from pprint import pprint as pt
    # data = utils.read(cf.US_INITIAL_JOBLESS_SRC_DATA)
    # t = sorted(data.items(),key=lambda x:x)
    # pt(t)
    # resp = utils.get(cf.US_INITIAL_JOBLESS_URL)
    # ret = dict()
    # print(resp.json()['attr'])