# -*- coding: utf-8 -*-
# @Time : 2022/6/17 13:37
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : down_index_info.py
# @Project : data-analysis

#   下载指数信息

from common_tool import *

if __name__ == '__main__':
    config = load_config()
    # ts客户端
    pro = ts_client(config)

    df = pro.index_basic(market='SZSE')

    print(df)