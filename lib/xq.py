
import time
import requests
import urllib3
from lib import utils
from pprint import pprint as pt
from requests import HTTPError

urllib3.disable_warnings()

BASE_FORMAT_URI = "https://stock.xueqiu.com/v5/stock/chart/kline.json?" \
                  "symbol={}&" \
                  "begin={}&" \
                  "period=day&" \
                  "type=before&" \
                  "count={}"

COOLIES = None

def get_cookies():
    global COOLIES
    if COOLIES is None:
        resp = utils.get('https://xueqiu.com')
        COOLIES = "xq_a_token={};".format(resp.cookies["xq_a_token"])
        return  COOLIES
    return COOLIES


def get_cookie():
    resp = requests.get('https://xueqiu.com')
    return  resp.cookies["xq_a_token"]

def get_kline(symbol, day=-30):
    url = BASE_FORMAT_URI.format(symbol, int(time.time() * 1000), day)
    resp = utils.get(url, cookies=get_cookies())
    if resp.status_code != 200:
        raise HTTPError()
    return resp.json()


def format_url(symbol, day=-30):
    url = BASE_FORMAT_URI.format(symbol, int(time.time() * 1000), day)
    return url


def xq_format(data):
    print(data)

if __name__ == '__main__':
    # import pysnowball as ball
    # ball.set_token('xq_a_token={};'.format(get_cookie()))
    # a = ball.quotec('ITA')
    # pt(a)
    data = get_kline('ITA')
    pt(data)
    column = data['data']['column']
    print(column)
    items = data['data']['item']
    for i in items:
        print(i[0])
        print(time.strftime("%Y-%m-%d",time.localtime(i[0]/1000)))
