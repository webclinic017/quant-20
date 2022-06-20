# -*- coding: utf-8 -*-
# @Time : 2022/3/25 10:17
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : common_tool.py
# @Project : data-analysis

import pandas as pd
from sqlalchemy import create_engine
import tushare as ts
import yaml
import os
import logging.config

def get_console_logger():
    logging.config.fileConfig('./logging.conf')
    logger_console = logging.getLogger('localLoger')
    return logger_console

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

def all_index_code(engine):
    '''
    获取所有指数代码
    :param engine:
    :return:
    '''
    frame = pd.read_sql_query("select ts_code  from t_index_info where market in ('SZSE', 'SSE')", engine)
    return frame

def to_pg(frame, engine, table_name):
    frame.to_sql(table_name, engine, index=None, if_exists='append', chunksize=200)


def record_log_info(mission_id, execute_time, status, message, comment, engine_log_db):
    '''
    记录任务执行日志
    :param mission_id:
    :param execute_time:
    :param status:
    :param message:
    :param comment:
    :param engine_log_db:
    :return:
    '''
    pd.DataFrame.from_records([{'mission_id': mission_id, 'execute_time': execute_time, 'status': status, 'message': message, 'comment': comment}])\
        .to_sql("t_mission_log", engine_log_db, index=None, if_exists='append')
