import time
import json
import os
import copy
import datetime
import requests
import logging
from uuid import uuid4
from functools import wraps, partial
from pprint import pprint as pr
from decimal import Decimal
import base64

import tushare as ts
from retrying import retry
from dateutil.relativedelta import relativedelta

import config

def extract_context_info(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("FUNC NAME: {}".format(func.__name__))
        return func(*args, **kwargs)

    return wrapper

class ToJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        "datetime.datetime.fromisoformat(t)"
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

@extract_context_info
def check_tradecal():
    """是否是美股交易日"""
    if config.DEBUG:
        return  True
    t = today().strftime("%Y%m%d")
    if t in get_ts_us_tradecal():
        # check_day_json()
        return True
    return False


def check_day_json():
    day_path = os.path.join(config.OUTPUT, now().strftime("%Y-%m-%d")+ '.json')
    if not os.path.isfile(day_path):
        os.mknod(day_path)

def set_us_tradecal(days): pass


@retry(stop_max_attempt_number=3, wait_fixed=90 * 1000)
@extract_context_info
def get_ts_us_tradecal():
    """获取美股当月所有的交易日 保存到月份.json文件 并返回所有交易日"""
    path = os.path.join(config.DATA_US_DIR, now().strftime("%Y-%m")+ '.json')
    if not os.path.isfile(path):
        f = open(path, 'w')
        logging.info('not find json {} file!'.format(path))
        pro = ts.pro_api(config.TOKEN)
        start_date, end_date = gen_range_mouth()
        start_date, end_date = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
        logging.info("query ...")
        df = pro.us_tradecal(start_date=start_date, end_date=end_date)
        logging.info("res: {}".format(df))
        d = df.to_dict('records')
        tradecal_days = [i['cal_date'] for i in d if i['is_open'] == 1]
        set_us_tradecal(tradecal_days)
        json.dump(tradecal_days, f)
        f.flush()
        f.close()
        logging.info('tradecal_days: {}'.format(tradecal_days))
    else:
        logging.info('find {} file.'.format(path))
        f = open(path, 'r')
        tradecal_days = json.load(f)
        f.close()
    return tradecal_days

def is_dst():
    return bool(time.localtime().tm_isdst)


def gen_range_mouth():
    """当月第一天和最后一天"""
    now = datetime.date.today()
    year = now.year
    month = now.month
    month_day = datetime.date(year, month, 1)
    days = []
    for i in range(31):
        if i == 0:
            days.append(month_day)
        else:
            month_day = month_day + relativedelta(days=1)
            if month_day.month > month:
                break
            else:
                days.append(month_day)
    return days[0], days[-1]


def is_market_open(offset=1):
    """
    当地时间： 9：30  16：30
    :return 2, 开盘前等 1, 开盘run，0, 结束
    """
    if config.DEBUG:
        return 1
    open_time = now().replace(hour=9, minute=30, second=0)
    close_time = now().replace(hour=16, minute=30, second=0)
    current = now()
    return current < open_time

def now(tz=config.US_TIMEZONE):
    "美国东部时间"
    now = datetime.datetime.now(tz=config.CN_TIMEZONE)
    return now.astimezone(tz)

def read_json_file(path):
    with open(path, 'r') as f:
        all_list = json.load(f)
        f.close()
    all_dict = dict()
    for i in all_list:
        all_dict[i["time"]] = i['data']
    return all_dict

def save_to_json(path, content):
    f = open(path, 'w')
    data = sort_data(content)
    json.dump(data, f)
    f.flush()
    f.close()

def sort_data(data):
    res = list()
    for k,v in data.items():
        res.append(
            {
                'time':k,
                'data': v,
            }
        )
    last_data = sorted(res, key=lambda i: datetime.datetime.strptime(i['time'], "%Y-%m-%d"), reverse=False)
    return last_data

def save_ouput(data, file_name):
    json_data = json.dumps(data)
    tmp = dict(
        code=1000,
        encryption="AES",
        data = str(base64.b64encode(json_data.encode("utf-8")), encoding='utf-8'),
        last_time = now().strftime("%Y-%m-%d %H:%M:%S")
    )
    f = open(file_name, 'w')
    json.dump(tmp, f)
    f.flush()
    f.close()
    return

def split_save_json(data, f=None, ft=None, sp=[1,30,100,360]):
    li = sort_data(data)
    for i in sp:
        last_sp = copy.deepcopy(li[-i:])
        for j in last_sp:
            j['TOTAL'] = total_dict(j['data'], 'close')
            j['LOW_TOTAL'] = total_dict(j['data'], 'low')
            j['OPEN_TOTAL'] = total_dict(j['data'], 'open')
            j['HIGH_TOTAL'] = total_dict(j['data'], 'high')
        path = os.path.join(f, ft.format(str(i)))
        save_ouput(last_sp, path)

def total_dict(data, data_key):
    num = Decimal(0.00)
    for k,v in data.items():
        if k == "SPX":continue
        num += Decimal(v[data_key])
    return float(num.quantize(Decimal('0.00')))

def day_trading_save(data):
    data = sort_data(data)
    tmp = dict(
        code=1000,
        encryption="AES",
        data = None,
        last_time = now().strftime("%Y-%m-%d %H:%M:%S")
    )
    day_path = os.path.join(config.OUTPUT, now().strftime("%Y-%m-%d")+ '.json')
    last_tead_path = os.path.join(config.OUTPUT,'last_tead_day.json')
    day_file = open(day_path, 'r+')
    last_tead_file = open(last_tead_path, 'r+')
    try:
        day_data = json.load(day_file)
        last_data = json.load(last_tead_file)
    except Exception:
        last_data = []

    last_data.append(data[-1])
    json_data = json.dumps(last_data)
    tmp['data'] = str(base64.b64encode(json_data.encode("utf-8")), encoding='utf-8')

    json.dump(tmp, day_file)
    json.dump(tmp, last_tead_file)

    last_tead_file.flush()
    last_tead_file.close()
    day_file.flush()
    day_file.close()


def today():
    "美国东部日期"
    n = now()
    today = datetime.date(year=n.year, month=n.month, day=n.day)
    return today

def init_sentry():
    import sentry_sdk
    sentry_sdk.init(
        config.SENTRY_SDN,
        traces_sample_rate=1.0,
    )

def header():
    return {'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'en-US,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            }

@retry(stop_max_attempt_number=20, wait_fixed=3)
def get(url, *args, **kwargs):
    rep = requests.get(url, verify=False, headers=header(), *args, **kwargs)
    return rep

def write(f,data):
    _f = open(f, 'w')
    json.dump(data, _f, default=ToJsonEncoder)
    _f.flush()
    _f.close()

def read(src):
    f = open(src, 'r')
    _dt = json.load(f)
    f.close()
    return _dt



if __name__ == '__main__':
    print(now())
    # print(is_market_open())
    # print(datetime.datetime.today().tzinfo)
    # print(is_dst())
    # datetime.datetime.today().weekday()
    # gen_range_mouth()
    # print(is_summer())
    from pytz import timezone
    # n = datetime.datetime.now(tz=config.CN_TIMEZONE)
    # us = n.astimezone(config.US_TIMEZONE)
    # print(n, "/n", us)
    # utc = datetime.datetime.utcnow()
    # print(utc)
    # utc_us = utc.astimezone(config.US_TIMEZONE)
    # print(utc_us)
    # print(datetime.datetime.now(config.CN_TIMEZONE))
    # print(is_market_open())
    # print(today())
    # from pytz import timezone
    # n = datetime.datetime.now
    # a = datetime.datetime.now(tz=timezone("Asia/Shanghai"))
    # print(a, "===", a.timetz())
    # a = a.astimezone(config.STOCK_MARKET["TZ"])
    # print(a, "===", a.timetz())
    # # print(n(config.STOCK_MARKET["TZ"]))
    # # print(n(tz=timezone("Asia/Shanghai")))
    # # ------------------
    # a = datetime.datetime.now(tz=timezone("Asia/Shanghai"))
    # a = a.replace(month=12)
    # print(a, "===", a.timetz())
    # a = a.astimezone(config.STOCK_MARKET["TZ"])
    # print(a, "===", a.timetz())

    # print(datetime.date.today().month)

