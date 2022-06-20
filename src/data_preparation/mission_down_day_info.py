# -*- coding: utf-8 -*-
# @Time : 2022/5/31 13:51
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_down_day_info.py
# @Project : data-analysis

'''
任务：查询日期是否是交易日(交易日历)
'''
import datetime as datetime
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import tushare as ts
import yaml
import os
import datetime
import sys
from common_tool import *

def mission(start_date, end_date):
    df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    df['is_open'] = df['is_open'].astype(bool)
    df.to_sql("t_trade_cal", engine_finance_db, index=None, if_exists='append')

if __name__ == '__main__':
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    #   日志库
    engine_log_db = pg_engine_log(config)
    # ts客户端
    pro = ts_client(config)

    if len(sys.argv) <= 1:
        start_date = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
        end_date = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
    else:
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(sys.argv[1], "%Y%m%d"), "%Y%m%d")
        end_date = datetime.datetime.strftime(datetime.datetime.strptime(sys.argv[2], "%Y%m%d"), "%Y%m%d")

    mission_id = 'ts_trade_cal'

    execute_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    try:
        mission(start_date, end_date)
    except Exception as e:
        status = False
        message = repr(e)
        print(message)
    else:
        status = True
        message = None
    pd.DataFrame.from_records([{'mission_id': mission_id, 'execute_time': execute_time, 'status': status, 'message': message, 'comment': 'start_date: {}, end_date: {}'.format(start_date, end_date)}])\
        .to_sql("t_mission_log", engine_log_db, index=None, if_exists='append')