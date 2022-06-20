# -*- coding: utf-8 -*-
# @Time : 2022/3/25 10:15
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_feature_nhnl.py
# @Project : data-analysis
# @Description: 任务：生成新高-新低指标：NHNL
import datetime

from common_tool import *
import sys
def generate_feature(current_day):
    start_date = datetime.datetime.strftime(current_day - datetime.timedelta(days=52*7 - 1), "%Y-%m-%d")
    end_date = datetime.datetime.strftime(current_day, "%Y-%m-%d")
    # print(start_date, end_date)

    sql_nh = '''
    select
        *
    from
        (
        select
            ts_code,
            max("close") as extremum
        from
            t_daily_info
        where
            trade_date between '{0}' and '{1}'
        group by
            ts_code) a
    join 
    (
        select
            ts_code,
            close
        from
            t_daily_info
        where
            trade_date = '{1}') b on
        a.ts_code = b.ts_code
        and a.extremum = b.close
    '''.format(start_date, end_date)


    sql_nl = '''
    select
        *
    from
        (
        select
            ts_code,
            min("close") as extremum
        from
            t_daily_info
        where
            trade_date between '{0}' and '{1}'
        group by
            ts_code) a
    join 
    (
        select
            ts_code,
            close
        from
            t_daily_info
        where
            trade_date = '{1}') b on
        a.ts_code = b.ts_code
        and a.extremum = b.close
    '''.format(start_date, end_date)


    sql_cur = '''
    select
        ts_code,
        close
    from
        t_daily_info
    where
        trade_date = '{0}'
    '''.format(end_date)

    if len(pd.read_sql_query(sql_cur, engine_finance_db)) == 0:
        raise Exception("没有日期{}的股价数据".format(end_date))

    frame_nh = pd.read_sql_query(sql_nh, engine_finance_db)
    frame_nl = pd.read_sql_query(sql_nl, engine_finance_db)

    # print(frame_nh)
    # print(frame_nl)
    nl = len(frame_nl)
    nh = len(frame_nh)
    nhnl = nh - nl

    records = [{'ts_code': 'ALL_MARKET', 'trade_date': end_date, 'field': 'NH', 'value': nh},
            {'ts_code': 'ALL_MARKET', 'trade_date': end_date, 'field': 'NL', 'value': nl},
            {'ts_code': 'ALL_MARKET', 'trade_date': end_date, 'field': 'NHNL', 'value': nhnl}]

    return pd.DataFrame.from_records(records)


def mission(record_date):
    mission_id = 'generate_feature_NHNL-NH-NL'
    execute_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
    try:
        frame = generate_feature(record_date)
        to_pg(frame, engine_finance_db, 't_feature_numberic')
    except Exception as e:
        status = False
        message = repr(e)
    else:
        status = True
        message = None

    record_log_info(mission_id, execute_time, status, message, 'record_date: {}'.format(record_date), engine_log_db)

if __name__ == '__main__':
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    #   日志库
    engine_log_db = pg_engine_log(config)


    if len(sys.argv) == 1:
        record_date = datetime.datetime.now()
    else:
        record_date = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")

    mission(record_date)

