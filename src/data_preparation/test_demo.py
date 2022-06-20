# -*- coding: utf-8 -*-
# @Time : 2022/3/24 15:13
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : test_demo.py
# @Project : data-analysis


import pandas as pd
import os
import datetime


for e in pd.date_range('20220218', '20220322'):
    record_date = datetime.datetime.strftime(e, "%Y%m%d")
    # print(record_date)
    os.system("python ./mission_down_daily_info.py {}".format(record_date))
    print("{}下载完成！".format(e))
    # break