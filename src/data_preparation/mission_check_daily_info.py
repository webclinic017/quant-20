# -*- coding: utf-8 -*-
# @Time : 2022/5/31 15:12
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_check_daily_info.py
# @Project : data-analysis
'''
任务：检查指定指数在特定区间内的日交易数据是否被完整下载
'''

import datetime
import sys

from common_tool import *


def process_single_stock(start_date, end_date, ts_code):
    frame_trade_cal = pd.read_sql_query("select distinct cal_date from t_trade_cal ttc where cal_date >= '{0}' and cal_date <= '{1}' and is_open = true".format(
        datetime.datetime.strftime(start_date, '%Y-%m-%d'), datetime.datetime.strftime(end_date, '%Y-%m-%d')),
                              engine_finance_db)
    frame_daily_info = pd.read_sql_query("select distinct trade_date from t_daily_info where trade_date >= '{0}' and trade_date <='{1}' and ts_code='{2}'".format(
        datetime.datetime.strftime(start_date, '%Y-%m-%d'), datetime.datetime.strftime(end_date, '%Y-%m-%d'), ts_code),
                              engine_finance_db)

    records = list()
    for e in frame_trade_cal['cal_date'].values:
        tag = e in frame_daily_info['trade_date'].values
        records.append({'ts_code': ts_code, 'trade_date': e, 'is_down': tag})
    pd.DataFrame.from_records(records) \
        .to_sql("t_daily_info_down_log", engine_log_db, index=None, if_exists='append',chunksize=300)


def mission(start_date, end_date, ts_codes):
    for ts_code in ts_codes:
        process_single_stock(start_date, end_date, ts_code)


if __name__ == '__main__':
    logger = get_console_logger()
    '''
    参数说明：
    第一个 开始日期；
    第二个 结束日期；
    第三个 要检查的股票代码，多个需要以逗号分隔
    '''
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    #   日志库
    engine_log_db = pg_engine_log(config)
    # ts客户端
    pro = ts_client(config)

    if len(sys.argv) <= 1:
        start_date = datetime.datetime.now()
        end_date = datetime.datetime.now()
        ts_codes = all_index_code(engine_finance_db)['ts_code']
    elif len(sys.argv) == 3:
        start_date = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        end_date = datetime.datetime.strptime(sys.argv[2], "%Y%m%d")
        ts_codes = all_index_code(engine_finance_db)['ts_code']
    elif len(sys.argv) == 4:
        start_date = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        end_date = datetime.datetime.strptime(sys.argv[2], "%Y%m%d")
        ts_codes = sys.argv[3].split(',')
    mission_id = 'ts_trade_cal'

    execute_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    try:
        mission(start_date, end_date, ts_codes)
    except Exception as e:
        status = False
        message = repr(e)
        logger.error(message)
    else:
        status = True
        message = None
    # pd.DataFrame.from_records([{'mission_id': mission_id, 'execute_time': execute_time, 'status': status,
    #                             'message': message,
    #                             'comment': 'start_date: {}, end_date: {}'.format(start_date, end_date)}]) \
    #     .to_sql("t_mission_log", engine_log_db, index=None, if_exists='append')
