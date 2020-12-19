#!/usr/bin/env python
# encoding=utf-8

import sys
import os
import io
from datetime import datetime, timedelta, timezone
from time import time
import numpy as np
import pandas as pd
from retrying import retry
import logging
import pytz

import config as cf
from lib import utils

logger = logging.getLogger(__name__)
ARK_CSV_KEYS = ['date','fund','company','ticker','cusip','shares','market value($)','weight(%)']

def strpdate(str_date, f):
    return datetime.strptime(str_date, f).replace(tzinfo=pytz.timezone('America/New_York')).date()

def strfdate(date, f):
    return datetime.strftime(date, f)

def now():
    return datetime.now(pytz.timezone("America/New_York"))

def get_filepath(file):
    return os.path.join(cf.DATA_US_ETF_DIR, 'ark', file)

def get_outputfile(file):
    return os.path.join(cf.US_ETF_OUTPUT, 'ark', file)


def download(last_date_str):
    dfs = []
    for ark in cf.ARK_DATA_URL_LIST:
        logger.info('Download {}'.format(ark['url']))
        response = utils.get(ark['url'])
        if response.status_code != 200:
            raise Exception('Request url error: {}'.format(ark['url']))

        df = pd.read_csv(io.StringIO(response.text), header=0)
        # 把最后4行无效的数据删除掉
        for i in range(4):
            df.drop(index = len(df)-1, inplace = True)

        # 拉取到的数据为重复数据时，则直接结束下载任务
        if last_date_str != None and df.iloc[0,0] == last_date_str:
            return create_empty_df()

        dfs.append(df)

    df = pd.concat(dfs, sort = False)
    df = astype(df)
    # 标准格式化python中的日期字符串格式
    df['date'] = df.apply(lambda item : strfdate(strpdate(item['date'],'%m/%d/%Y'),'%m/%d/%Y'), axis = 1)
    #df.to_csv('./arks/test.csv')

    return df

def save(df):
    filepath = get_filepath('ark.csv')

    if logger.isEnabledFor(logging.DEBUG) == True:
        base_df.to_csv(get_filepath('debug_ark_data.csv'))
        bak_file = get_filepath(f'ark-data-bak-{datetime.strftime(now(),"%Y%m%d%H%M%S")}.csv')
        os.rename(filepath, bak_file)

    directory = os.path.dirname(filepath)
    if os.path.exists(directory) == False:
        os.mkdir(directory)

    df.to_csv(filepath, index = False)
    
    logger.info(f'{filepath} has been saved.')
    return

def read():
    filepath = get_filepath('ark.csv')

    if os.path.exists(filepath) == True:
        df1 = pd.read_csv(filepath, header = 0)
        df1 = astype(df1)
    else:
        df1 = create_empty_df()

    # 查找本地已经存储最新数据的日期
    last_date1 = None
    if len(df1) > 0:
        dates = list(df1['date'])
        last_date1 = str(dates[len(dates)-1])

    # 从ARK官网拉取数据，将本地存储的最新交易日期传入，通过它来避免拉取重复数据
    df_lasted = download(last_date1)
    if len(df_lasted) > 0:
        if len(df1) > 0:
            df1 = df1.append(df_lasted)
        else:
            df1 = df_lasted
        save(df1)

    return df1

def astype(df):
    # date,fund,company,ticker,cusip,shares,market value($),weight(%)
    df = df.astype({'date':str, 'fund':str, 'company':str, 'ticker':str, 'cusip': str, 'shares':int, 'market value($)':float, 'weight(%)': float})
    return df

def create_empty_df():
    df = pd.DataFrame({}, index=[], columns= ARK_CSV_KEYS)
    return df

def build_report(df):
    #
    # 对比前一天前持股增减变化
    #

    # 从df中获取有效的日期数据，并且对它进行去重以及排序
    dates = list(set(df['date']))
    dates = [datetime.strptime(item, '%m/%d/%Y').date() for item in (dates)]
    dates = sorted([datetime.strftime(item, '%m/%d/%Y') for item in (dates)])

    # 创建一个只有最新数据的df，把它作为生成统计数据的基础
    base_df = df[(df['date'] == dates[len(dates)-1])]
    if logger.isEnabledFor(logging.DEBUG) == True:
        base_df.to_csv(get_filepath('debug_ark_base_data.csv'))

    # 提取前10个交易日的数据，并且把它合并到报告中。
    for trade_n in range(1, 11):
        if len(dates) <= trade_n:
            continue
        base_df[f'pre{trade_n}_date'] = dates[len(dates)-trade_n-1]
        base_df = pd.merge(base_df, df, how='left', left_on=[f'pre{trade_n}_date', 'fund', 'cusip'], right_on=['date', 'fund', 'cusip'], suffixes=['',f'_pre{trade_n}'])
        if logger.isEnabledFor(logging.DEBUG) == True:
            base_df.to_csv(get_filepath(f'debug_ark_data_{trade_n}.csv'))

    # 统计：持仓股票数量增减变化
    qoq = lambda a, b: (a - b) / b * 100
    base_df['shares up/down(%)'] = qoq(base_df['shares'], base_df[f'shares_pre1'])
    base_df['total'] = base_df['shares up/down(%)'] + 0
    for trade_n in range(1, 10):
        base_df[f'shares up/down(%) t{trade_n}'] = qoq(base_df[f'shares_pre{trade_n}'], base_df[f'shares_pre{trade_n+1}'])
        
        # 近10天累计持仓变化
        base_df['total'] = base_df['total'] + base_df[f'shares up/down(%) t{trade_n}']

    if logger.isEnabledFor(logging.DEBUG) == True:
        base_df.to_csv(get_filepath('debug_ark_report_data.csv'))

    # 过虑 ticker为nan的数据
    base_df = base_df[base_df['ticker'] != 'nan']

    logger.info('build_report is finished')

    return base_df

def flag(x):
    if x is None or x == '' or x == 0:
        return ''

    if x >= 50:
        return 'U5'
    elif x >= 30:
        return 'U4'
    elif x >= 20:
        return 'U3'
    elif x >= 10:
        return 'U2'
    elif x >= 2:
        return 'U1'
    elif x > 0:
        return 'U0'

    if x <= -50:
        return 'D5'
    elif x <= -30:
        return 'D4'
    elif x <= -20:
        return 'D3'
    elif x <= -10:
        return 'D2'
    elif x <= -2:
        return 'D1'
    elif x < 0:
        return 'D0'

    return ''

def save_output(df):
    # 字段说明：
    # date,fund,company,ticker,shares,shares up/down(%),weight(%),total,shares up/down(%) t{n}
    # 日期，基金，公司名称，股票简称，持股数量，持股增减变动百分比，持仓权重，近10天持股增减变动百分比汇总，前N天的持股增减变动百分比

    ret = []
    for ark in cf.ARK_DATA_URL_LIST:
        records = []
        for index,row_item in df[df['fund']==ark['fund']].iterrows():
            record = {
                'date':strfdate(strpdate(row_item['date'],'%m/%d/%Y'),'%Y-%m-%d'),
                'company':row_item['company'], 
                'ticker':row_item['ticker'], 
                'shares':row_item['shares'], 
                'shares_up_down_percent':round(row_item['shares up/down(%)'],2), 
                'shares_up_down_flag':flag(row_item['shares up/down(%)']), 
                'weight':row_item['weight(%)'],
                'total':round(row_item['total'],2)
            }
            trade_history = []
            history = {}
            for trade_n in range(1, 10):
                col = f'shares up/down(%) t{trade_n}'
                if col not in df.columns.values:
                    continue
                history[f't{trade_n}_percent'] = round(row_item[col],2)
                history[f't{trade_n}_flag'] = flag(row_item[col])
                trade_history.append(history)
                history = {}

            record['trade_history'] = trade_history
            records.append(record)

        fund = {'fund': ark['fund'], 'records': records}
        ret.append(fund)

        filepath = get_outputfile('{}.json'.format(ark['fund']))
        utils.save_ouput(fund, filepath)
        logger.info(f'save_output to {filepath}')

    filepath = get_outputfile('ALL.json')
    utils.save_ouput(ret, filepath)
    logger.info(f'save_output to {filepath}')

def bin():
    current = utils.now()
    close_time = utils.now().replace(hour=16, minute=30, second=0)
    if current > close_time:
        df = read()
        df = build_report(df)
        save_output(df)
        logger.info('ETF ARK finished.')
        return
    logger.info('NOT ETF ARK.')

if __name__ == '__main__':

    bin()

    exit()
