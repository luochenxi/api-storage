import logging
import copy
import yfinance as yf
import pandas as pd
import awswrangler as wr
import pandas_datareader as pdr
from requests import exceptions as req_exec
from botocore import exceptions as boto_exec

import config as cf
from lib import utils

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

logger = logging.getLogger(__name__)
yf.pdr_override()

OLI_COPPER_GOLD = {}
OCG = ['HG=F', 'GC=F', 'CL=F', '^TNX']
OCG_MAP = {}
OCG_DF = pd.DataFrame(columns=['date', 'crude_gold', 'copper_gold', 'tnx'])


def yf_float(df):
    '''浮点数转换'''
    for f in ['h','l','c','o','v','ac', 'chgp1d', 'chgp5d', 'chgp20d']:
        df[f] = utils.df_float(df, f, 6)
    df.index = range(0,len(df))
    return df

@utils.extract_context_info
def get(li = cf.LEFT):
    global OCG_DF, OCG
    for i in li:
        try:
            logger.info(f'get: {i}')
            df = pdr.get_data_yahoo(i, interval = "d")
            df = df.rename(columns={'High':'h','Low':'l','Open':'o','Close':'c', 'Volume':'v', 'Adj Close':'ac'})
            df['date'] = list(df.index)
            df['n'] = cf.LEFT_MAP[i]
            df['i18n'] = 'dashboard.item.US_{}'.format(ss(i))
            df['hash_key'] = 'US_{}'.format(ss(i))
            df['date'] = df['date'].apply(lambda x: str(x).split()[0])
            df['chgp1d'] = df["c"].pct_change(1)  * 100
            df['chgp5d'] = df["c"].pct_change(5)  * 100
            df['chgp20d'] = df["c"].pct_change(20)  * 100
            # 油金比
            if i in OCG:
                OCG_MAP[i] = copy.deepcopy(df)

            df = yf_float(df)
            df = df.tail(3) # 取最后数据
            # 写入 aws
            logger.info(f'Write: {i}')
            wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)
        except req_exec.ConnectionError  as e  :
            logger.exception(i, e)
            continue
        except boto_exec.ClientError as e:
            logger.exception(i, e)
            continue

@utils.extract_context_info
def oli_copper_gold():
    logger.info(f'run oli_copper_gold...')
    hg = OCG_MAP['HG=F']    # Copper
    gc = OCG_MAP['GC=F']    # Gold
    cl = OCG_MAP['CL=F']    # WEI Crude
    tnx = OCG_MAP['^TNX']
    # 铜金比
    hgr = copy.deepcopy(hg)
    hgr['value'] = hg['c']/ gc['c']
    hgr['hash_key'] = 'US_COPPER_GOLD_RATIO'
    hgr['chgp1d'] = hgr["value"].pct_change(1)  * 100
    hgr['chgp5d'] = hgr["value"].pct_change(5)  * 100
    hgr['chgp20d'] = hgr["value"].pct_change(20)  * 100
    hgr['n'] = 'CopperGold Ratio'
    hgr['i18n'] = 'economic.chart.US_GOLD_COPPER_RATIO'
    hgr = utils.target_float(hgr, ['chgp1d', 'chgp5d', 'chgp20d', 'value'])
    hgr = hgr['date value chgp1d chgp5d chgp20d n i18n hash_key'.split()]
    # 油金比
    cgr = copy.deepcopy(cl)
    cgr['value'] = cl['c']/ gc['c']
    cgr['hash_key'] = 'US_CRUDE_GOLD_RATIO'
    cgr['chgp1d'] = cgr["value"].pct_change(1)  * 100
    cgr['chgp5d'] = cgr["value"].pct_change(5)  * 100
    cgr['chgp20d'] = cgr["value"].pct_change(20)  * 100
    cgr['n'] = 'CrudeGold Ratio'
    cgr['i18n'] = 'economic.chart.US_GOLD_CRUDE_RATIO'
    cgr = utils.target_float(cgr, ['chgp1d', 'chgp5d', 'chgp20d', 'value'])
    cgr = cgr['date value chgp1d chgp5d chgp20d n i18n hash_key'.split()]
    df = pd.concat([hgr, cgr])
    df = df.tail(3) # 取最后数据
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

def ss(s):
    return s.strip('^').replace('=', '_').replace('-','_')

if __name__ == '__main__':
    get()
    oli_copper_gold()
    # TODO BTC 数据不对
    # symbol = "^TNX"
    # df = pdr.get_zdata_yahoo("^TNX", interval = "d")
    # df = df.rename(columns={'High':'h','Low':'l','Open':'o','Close':'c', 'Volume':'v', 'Adj Close':'ac'})
    # df['date'] = list(df.index)
    # df['hash_key'] = 'US_{}'.format(ss(symbol))
    # df['date'] = df['date'].apply(lambda x: str(x).split()[0])
    # for i in ['h','l','c','o','v','ac']:
    #     if i == 'date':
    #         df['date'] = df['date'].apply(lambda x: str(x).split()[0])
    #         continue
    #     df[i] = df[i].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
    # # df = df.tail(3)
    # df.index = range(0,len(df))
    # print(df)