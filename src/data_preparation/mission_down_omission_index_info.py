# -*- coding: utf-8 -*-
# @Time : 2022/6/1 10:02
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : mission_down_omission_daliy_info.py
# @Project : data-analysis
'''
任务：检查指定指数在特定时间段内日交易数据是否下载完成，如果没有下载则进行下载
前置条件：t_daily_info_down_log中已经检查各交易日的数据是否下载

步骤:
    1、执行mission_down_index_info.py下载指定时间区间内交易日信息
    2、执行mission_check_index_info.py核对
'''

import datetime
import sys
import time
from common_tool import *


def process_single_stock(start_date, end_date, ts_code):
    frame = pd.read_sql_query(
        "select trade_date  from t_daily_info_down_log where trade_date >= '{0}' and trade_date <= '{1}' and is_down = false and ts_code = '{2}' and is_confirm is null order by trade_date".format(
            datetime.datetime.strftime(start_date, '%Y-%m-%d'), datetime.datetime.strftime(end_date, '%Y-%m-%d'),
            ts_code), engine_log_db)
    if len(frame) == 0:
        logger.info("%s在%s到%s期间的日交易数据此前已确认，本次不执行！", ts_code, start_date, end_date)
        return

    duration_list = list()

    tmp_start = None
    tmp_end = None
    for e in frame['trade_date']:
        if tmp_start is None and tmp_end is None:
            tmp_start = e
            tmp_end = e
            continue

        if (e - tmp_end).days <= 10:
            tmp_end = e
        else:
            duration_list.append({'start': tmp_start, 'end': tmp_end})
            tmp_start = e
            tmp_end = e
    duration_list.append({'start': tmp_start, 'end': tmp_end})

    conn = engine_log_db.connect()
    try:
        for record in duration_list:
            df = pro.daily(ts_code=ts_code, start_date=datetime.datetime.strftime(record['start'], "%Y%m%d"),
                           end_date=datetime.datetime.strftime(record['end'], "%Y%m%d"))
            # print(df)

            if len(df) == 0:
                '''
                整个持续期间确实没有交易数据
                '''
                sql = "update t_daily_info_down_log set is_confirm = true where trade_date >= '{0}' and trade_date <= '{1}' and ts_code ='{2}'".format(
                    datetime.datetime.strftime(record['start'], '%Y-%m-%d'),
                    datetime.datetime.strftime(record['end'], '%Y-%m-%d'),
                    ts_code)
                conn.execute(sql)
                logger.info("%s在%s到%s期间的日交易数据确认完毕！", ts_code, record['start'], record['end'])
            else:
                '''
                持续期间有数据，二次确认该数据是已经下载过的。规避合并小区间过程中而将部分已开盘日期引入。
                '''
                logger.info("%s在%s到%s期间的日交易数据需要二次确认！", ts_code, record['start'], record['end'])
                day_omission = pd.read_sql_query(
                    "select trade_date  from t_daily_info_down_log where trade_date >= '{0}' and trade_date <= '{1}' and is_down = false and ts_code = '{2}' and is_confirm is null order by trade_date".format(
                        datetime.datetime.strftime(record['start'], '%Y-%m-%d'),
                        datetime.datetime.strftime(record['end'], '%Y-%m-%d'),
                        ts_code), engine_log_db)['trade_date'].values
                # 需要补充的交易数据
                frame_ommision = df.loc[df.trade_date.apply(func=lambda e: datetime.datetime.strptime(e, '%Y%m%d').date() in day_omission).values,]
                # print(df['trade_date'])
                # print(day_omission)
                # print(datetime.datetime.strptime('20180502', '%Y%m%d').date() in day_omission)
                # print(dapetime.datetime.strptime('20180502', '%Y%m%d').date())
                # print(df)
                if len(frame_ommision) != 0:
                    frame_ommision.to_sql('t_daily_info', engine_finance_db, index=None, if_exists='append', chunksize=200)
                    days_update = ["'{0}'".format(datetime.datetime.strftime(datetime.datetime.strptime(e, '%Y%m%d'), '%Y-%m-%d')) for e in frame_ommision['trade_date'].values]
                    sql = "update t_daily_info_down_log set is_down = true where trade_date in ({0}) and ts_code ='{1}'".format(
                        ','.join(days_update), ts_code)
                    conn.execute(sql)
                    logger.info("已补充{0}，在{1}的交易数据".format(ts_code, days_update))
                else:
                    logger.info("已确认{}在{}至{}期间除已下载的数据外没有其他数据需要补充！！".format(ts_code, record['start'], record['end']))
                sql = "update t_daily_info_down_log set is_confirm = true where trade_date >= '{0}' and trade_date <= '{1}' and ts_code ='{2}'".format(
                    datetime.datetime.strftime(record['start'], '%Y-%m-%d'),
                    datetime.datetime.strftime(record['end'], '%Y-%m-%d'),
                    ts_code)
                conn.execute(sql)
                logger.info("%s在%s到%s期间的日交易数据确认完毕！", ts_code, record['start'], record['end'])
    except Exception as e:
        logger.error(e)
        if '每分钟最多' in str(e):
            logger.warn("请求频次过高，暂停30秒。")
            time.sleep(30)
    finally:
        conn.close()


def mission(start_date, end_date, ts_codes):
    for ts_code in ts_codes:
        try:
            logger.info("开始：进行任务，指数：%s，在%s 到 %s期间的日交易数据检测及补充！", ts_code, start_date, end_date)
            process_single_stock(start_date, end_date, ts_code)
            logger.info("结束：结束任务，指数：%s，在%s 到 %s期间的日交易数据检测及补充完毕！", ts_code, start_date, end_date)
        except Exception as e:
            logger.error(repr(e))
            if '抱歉' in repr(e): time.sleep(60)
            continue



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

    logger.info("输入参数:%s-%s-%s", start_date, end_date, ts_codes)
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
