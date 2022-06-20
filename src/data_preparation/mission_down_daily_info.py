# -*- coding: utf-8 -*-
# @Time : 2022/3/24 11:18
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_down_daily_info.py
# @Project : data-analysis
# @Description: 任务：下载单日股价信息
import datetime as datetime
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import tushare as ts
import yaml
import os
import datetime
import sys

def load_config():
    f_path = './config.yml'
    if not os.path.exists(f_path):
        raise FileNotFoundError("没有在工作目录中找到配置文件")

    with open(f_path, 'r', encoding='utf-8') as f:
        content = f.read()
    conf = yaml.load(content, yaml.FullLoader)
    return conf

def pg_engine_finance(conf):
    username = conf['pg']['username']
    password = conf['pg']['password']
    host = conf['pg']['host']
    port = conf['pg']['port']
    db_name = conf['pg']['dbname']['finance']
    return create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, db_name))


def pg_engine_log(conf):
    username = conf['pg']['username']
    password = conf['pg']['password']
    host = conf['pg']['host']
    port = conf['pg']['port']
    db_name = conf['pg']['dbname']['log']
    return create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, db_name))

def ts_client(conf):
    '''
    tushare客户端
    :param conf:
    :return:
    '''
    token = conf['ts']['token']
    pro = ts.pro_api(token)
    return pro


def all_ts_code(engine):
    '''
    获取所有股票代码
    :param engine:
    :return:
    '''
    frame = pd.read_sql_query("select ts_code from t_tscode_company", engine)
    return frame

def load_daily_stock_info(codes, start_date, end_date, pro):
    df = pro.daily(ts_code=codes, start_date=start_date, end_date=end_date)
    return df


def spilt2batch(codes, batch_size):
    '''
    股票代码切块
    :param codes:
    :param batch_size:
    :return:
    '''
    n_sizes = len(codes)
    batch_list = list()

    start_index = 0
    while True:
        if start_index >= n_sizes: break
        batch_list.append(codes[start_index: start_index + batch_size])
        start_index += batch_size

    return batch_list

def to_pg(frame, engine, table_name):
    frame.to_sql(table_name, engine, index=None, if_exists='append', chunksize=200)



def mission(record_date):
    #   股票代码
    all_stock_codes = all_ts_code(engine_finance_db)['ts_code']

    frame_list = list()
    for e in spilt2batch(all_stock_codes, 200):
        frame_list.append(load_daily_stock_info(','.join(e), record_date, record_date, pro))


    all_data = pd.concat(frame_list)
    if len(all_data) == 0:
        raise Exception("没有查询到{}的数据".format(record_date))
    to_pg(pd.concat(frame_list), engine_finance_db, 't_daily_info')

if __name__ == '__main__':
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    #   日志库
    engine_log_db = pg_engine_log(config)
    # ts客户端
    pro = ts_client(config)

    if len(sys.argv) <= 1:
        record_date = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    else:
        record_date = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")

    mission_id = 'ts_down_daily_1'

    execute_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    try:
        mission(record_date)
    except Exception as e:
        status = False
        message = repr(e)
    else:
        status = True
        message = None

    pd.DataFrame.from_records([{'mission_id': mission_id, 'execute_time': execute_time, 'status': status, 'message': message, 'comment': 'record_date: {}'.format(record_date)}])\
        .to_sql("t_mission_log", engine_log_db, index=None, if_exists='append')
