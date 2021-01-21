import logging

import yfinance as yf
import pandas as pd
import awswrangler as wr
import pandas_datareader as pdr
from requests import exceptions as req_exec
from botocore import exceptions as boto_exec
import config as cf

pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

logger = logging.getLogger(__name__)
yf.pdr_override()

def get(li = cf.LEFT):
    for i in li:
        try:
            df = pdr.get_data_yahoo(i, interval = "d")
            df = df.rename(columns={'High':'h','Low':'l','Open':'o','Close':'c', 'Volume':'v', 'Adj Close':'ac'})
            df['date'] = list(df.index)
            df['n'] = cf.LEFT_MAP[i]
            df['i18n'] = 'dashboard.item.US_{}'.format(ss(i))
            df['hash_key'] = 'US_{}'.format(ss(i))
            df['date'] = df['date'].apply(lambda x: str(x).split()[0])
            for i in ['h','l','c','o','v','ac']:
                if i == 'date':
                    df['date'] = df['date'].apply(lambda x: str(x).split()[0])
                    continue
                df[i] = df[i].apply(lambda x: '{:.3f}'.format(x) if type(x) is int or type(x) is float else x)
            df.index = range(0,len(df))
            df = df.tail(20) # 取最后20条数据
            # 写入 aws
            wr.dynamodb.put_df(df=df, table_name=cf.BREADTH_TABLE_NAME)
        except req_exec.ConnectionError:
            logger.error('req_exec.ConnectionError: {}'.format(i))
            continue
        except boto_exec.ClientError:
            logger.error('req_exec.ConnectionError: {}'.format(i))
            continue

def ss(s):
    return s.strip('^').replace('=', '_').replace('-','_')

if __name__ == '__main__':
    get()
    # symbol = "^TNX"
    # df = pdr.get_data_yahoo("^TNX", interval = "d")
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