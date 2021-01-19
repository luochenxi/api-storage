import json
import csv
import datetime
from collections import namedtuple
from pprint import pprint as pr
from lib import utils

CF = namedtuple("cf", ["code", "f", "index"])
ALL = dict()


def csv_to_dict(cf):
    # global all
    t = list()
    with open(cf.f, 'r') as f:
        content = f
        d = csv.DictReader(f)
        for i in d:
            ddd = i['time'].strip().split("T")[0]

            td = dict(day=ddd, open=float(i['open']), high=float(i['high']), low=float(i['low']),
                      close=float(i['close']),
                      ema20=float(i['ema20']), ma20=float(i['ma20']), ema60=float(i['ema60']), ma60=float(i['ma60']),
                      ema120=float(i['ema120']),
                      ma120=float(i['ma120']), )
            # print(t['day'], i['time'])
            t.append(td)

            # ddd = i['time'].strip().split("T")[0]
            # ddd = i['time']
            # print(ddd, "3333")
            if not ALL.get(ddd, None):
                ALL[ddd] = {
                    cf.code: td,
                }
            else:
                old = ALL[ddd]
                old[cf.code] = td
                ALL[ddd] = old

    return ALL


def read_csvs(li):
    for cf in li:
        csv_to_dict(cf)
    last_data = list()
    for k, v in ALL.items():
        print(k, v)
        tmp = {'time': k, 'data': v}
        last_data.append(tmp)

    last_data = sorted(last_data, key=lambda i: datetime.datetime.strptime(i['time'], "%Y-%m-%d"), reverse=False)
    for i in last_data:
        print(i)
    print(len(last_data))
    save_to_jsonf(last_data, './sp500_all.json')


def save_to_jsonf(context, path):
    a = open(path, 'w')
    json.dump(context, a)
    a.flush()
    a.close()


if __name__ == '__main__':
    flist = [
        CF(f='../data/INDEX_SLTW, 1D.csv', code='COM', index='SLTW'),
        CF(f='../data/INDEX_SYTW, 1D.csv', code='CND', index='SYTW'),
        CF(f='../data/INDEX_SPTW, 1D.csv', code='CNS', index='SPTW'),
        CF(f='../data/INDEX_SETW, 1D.csv', code='ENE', index='SETW'),
        CF(f='../data/INDEX_SFTW, 1D.csv', code='FIN', index='SFTW'),
        CF(f='../data/INDEX_SVTW, 1D.csv', code='HLT', index='SVTW'),
        CF(f='../data/INDEX_SITW, 1D.csv', code='IND', index='SITW'),
        CF(f='../data/INDEX_SBTW, 1D.csv', code='MAT', index='SBTW'),
        CF(f='../data/INDEX_SSTW, 1D.csv', code='REI', index='SSTW'),
        CF(f='../data/INDEX_SKTW, 1D.csv', code='TEC', index='SKTW'),
        CF(f='../data/INDEX_SUTW, 1D.csv', code='UTL', index='SUTW'),
        CF(f='../data/INDEX_S5TW, 1D.csv', code='SPX', index='S5TW'),
    ]
    all = read_csvs(flist)
    # print(type(utils.read_json_file('./data/us/all.json')), len(utils.read_json_file('./data/us/all.json')))
    print(type(utils.read_json_file('./sp500_all.json')), len(utils.read_json_file('./sp500_all.json')))
