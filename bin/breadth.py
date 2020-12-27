import logging
import json
import sys
import copy
import traceback
from collections import namedtuple

import config as cf
from lib import utils
from lib.browser import *

logger = logging.getLogger(__name__)

IndexInfo = namedtuple("index_info", ["code", "url", "tv_index"])
IndexInfoHandle = namedtuple("index_info_handle", ["code", "url", "handle", "tv_index"])


INDEX_DICT = [
    {"tv_index": "INDEX:SLTW", "code": "COM"},
    {"tv_index": "INDEX:SYTW", "code": "CND"},
    {"tv_index": "INDEX:SPTW", "code": "CNS"},
    {"tv_index": "INDEX:SETW", "code": "ENE"},
    {"tv_index": "INDEX:SFTW", "code": "FIN"},
    {"tv_index": "INDEX:SVTW", "code": "HLT"},
    {"tv_index": "INDEX:SITW", "code": "IND"},
    {"tv_index": "INDEX:SBTW", "code": "MAT"},
    {"tv_index": "INDEX:SSTW", "code": "REI"},
    {"tv_index": "INDEX:SKTW", "code": "TEC"},
    {"tv_index": "INDEX:SUTW", "code": "UTL"},
    {"tv_index": "INDEX:S5TW", "code": "SPX"},
]

INDEXS = []

def gtasks():
    if len(INDEX_DICT) > 12:
        return INDEXS
    for item in INDEX_DICT:
        code = item["tv_index"].replace(":", "-")
        url = cf.BASE_URL + code
        index = IndexInfo(tv_index=item["tv_index"], url=url, code=item['code'])
        INDEXS.append(index)
    return INDEXS

@utils.extract_context_info
def start(br):
    tasks = gtasks()
    for index, item in enumerate(tasks):
        if index == 0:
            get_url(br, item)
        else:
            new_tab(br, item)
        new_item = IndexInfoHandle(code=item.code, url=item.url, handle=handles(br)[index], tv_index=item.tv_index)
        logger.info(new_item)
        INDEXS[index] = new_item

@utils.extract_context_info
def loading_page(br):
    for i, item in enumerate(INDEXS):
        logger.info("switch to window: {}, tv: {}.".format(item.handle, item.tv_index))
        switch_window(br, item)
        time.sleep(0.2)
        if utils.is_market_open() == 1:
            return

@utils.extract_context_info
def save_last(br):
    tmp = {}
    for i, item in enumerate(INDEXS):
        switch_window(br, item)
        close = get_element(br, '//*[@id="anchor-page-1"]/div/div[3]/div[1]/div/div/div/div[1]/div[1]')
        pre_close = get_element(br, '//*[@id="anchor-page-1"]/div/div[3]/div[3]/div[1]/div[1]')
        open_price = get_element(br, '//*[@id="anchor-page-1"]/div/div[3]/div[3]/div[2]/div[1]')
        day_range = get_element(br, '//*[@id="anchor-page-1"]/div/div[3]/div[3]/div[4]/div[1]')

        low, high = [i.strip() for i in day_range.text.split("—")]
        logger.info("{} - {}".format(close.text, item))
        data = {
            "code": item.code,
            "pre": float(pre_close.text),
            "high": float(high),
            "low": float(low),
            "close": float(close.text),
            "open": float(open_price.text),
            "current_day": utils.today(),
            "merket_type": "US",
            "other": item.tv_index
        }
        tmp[item.code] = {
            "close": float(close.text),
            "open": float(open_price.text),
            "pre": float(pre_close.text),
            "low": float(low),
            "high": float(high),
            "other": item.tv_index,
            'day': utils.today().isoformat(),

        }
        logger.info("ITEM DATA CODE : {} --- {}".format(item.code, data))
    current = {'time': utils.today().isoformat(), 'data': tmp}
    return current

@utils.extract_context_info
def quit_browser(br):
    br.quit()

@utils.extract_context_info
def split_total(data, f=None, sp=[1,30,100,360]):
    data = utils.sort_data(data)

    for i in sp:
        last_sp = copy.deepcopy(data[-i:])
        t1  = list()
        t2  = list()
        for j in last_sp:
            t1.append({
                "time": j['time'],
                "total": utils.total_dict(j['data'], 'close')
            })
            t2.append({
                "time": j['time'],
                "data": j['data'],
            })
        path1 = os.path.join(f, 'sp500_total_{}.json'.format(str(i)))
        path2 = os.path.join(f, 'sp500_class_{}.json'.format(str(i)))
        utils.save_ouput(t1, path1)
        utils.save_ouput(t2, path2)

