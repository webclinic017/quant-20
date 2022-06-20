# -*- coding: utf-8 -*-
# @Time : 2022/3/24 13:54
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : import_data2pg.py
# @Project : data-analysis

import pandas as pd
from sqlalchemy import create_engine
import tushare as ts
import yaml
import os

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



if __name__ == '__main__':
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    frame = pd.read_excel('./公司股票代码.xlsx', index_col=None)
    frame.to_sql("t_tscode_company", engine_finance_db, index=None, if_exists='append')
