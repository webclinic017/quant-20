# -*- coding: utf-8 -*-
# @Time : 2022/3/24 11:18
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_down_daily_info.py
# @Project : data-analysis
# @Description: 任务：下载给定日期的指数交易信息
import pandas as pd
import datetime
import sys
from common_tool import *
import time

def load_daily_index_info(code, start_date, end_date, pro):
    df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)
    return df

def mission(start_date, end_date):
    #   指数代码
    all_index_codes = all_index_code(engine_finance_db)['ts_code']

    for code in all_index_codes:
        try:
            frame = load_daily_index_info(code, start_date, end_date, pro)

            if len(frame) == 0:
                raise Exception("没有查询到{}, {}-{}的数据".format(code, start_date, end_date))
            to_pg(frame, engine_finance_db, 't_daily_info')
            logger.info("指数%s,与%s至%s期间的交易信息已入库", code, start_date, end_date)
        except Exception as e:
            logger.error(repr(e))
            if '抱歉' in repr(e): time.sleep(60)
            continue

if __name__ == '__main__':
    logger = get_console_logger()

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

    mission_id = 'ts_down_index'
    start_date = datetime.datetime.strftime(start_date, "%Y%m%d")
    end_date = datetime.datetime.strftime(end_date, "%Y%m%d")


    execute_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    try:
        mission(start_date, end_date)
    except Exception as e:
        status = False
        message = repr(e)
        logger.error(message)
    else:
        status = True
        message = None

    pd.DataFrame.from_records([{'mission_id': mission_id, 'execute_time': execute_time, 'status': status, 'message': message, 'comment': 'record_date: {}'.format(record_date)}])\
        .to_sql("t_mission_log", engine_log_db, index=None, if_exists='append')
