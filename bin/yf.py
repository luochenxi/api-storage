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
    for f in ['h','l','c','o','v','ac']:
        df[f] = utils.df_float(df, f, 6)
    df.index = range(0,len(df))
    return df

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
            # 油金比
            if i in OCG:
                OCG_MAP[i] = copy.deepcopy(df)

            df = yf_float(df)
            df = df.tail(5) # 取最后数据
            # 写入 aws
            logger.info(f'Write: {i}')
            wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)
        except req_exec.ConnectionError:
            logger.error('req_exec.ConnectionError: {}'.format(i))
            continue
        except boto_exec.ClientError:
            logger.error('req_exec.ConnectionError: {}'.format(i))
            continue

def oli_copper_gold():
    logger.info(f'run oli_copper_gold...')
    hg = OCG_MAP['HG=F']
    gc = OCG_MAP['GC=F']
    cl = OCG_MAP['CL=F']
    tnx = OCG_MAP['^TNX']
    tnx['tnx'] = tnx['c']
    tnx['crude_gold'] = cl['c']/ gc['c']
    tnx['crude_gold'] = utils.df_float(tnx, 'crude_gold', 7)
    tnx['copper_gold'] = hg['c']/ gc['c']
    tnx['copper_gold'] = utils.df_float(tnx, 'copper_gold', 7)
    tnx['tnx'] = tnx['c']
    tnx['tnx'] = utils.df_float(tnx, 'tnx', 3)
    tnx['hash_key'] = 'US_GOLD_COPPER_CRUDE_TNX'
    tnx['i18n'] = 'economic.chart.US_GOLD_COPPER_CRUDE_TNX'
    tnx['n'] = 'OilCopperGold Ratio'
    tnx = tnx[['date', 'tnx', 'crude_gold', 'copper_gold', 'i18n', 'n', 'hash_key']]
    df = tnx.fillna("NULL")
    # 写入 aws
    df = df.tail(5) # 取最后数据
    logger.info('Write oli_copper_gold...')
    wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)

def ss(s):
    return s.strip('^').replace('=', '_').replace('-','_')

if __name__ == '__main__':
    get()
    oli_copper_gold()
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