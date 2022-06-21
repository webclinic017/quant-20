# -*- coding: utf-8 -*-
# @Time : 2022/5/19 17:12
# @Author : Think
# @Email : zhangcongcong25@crpower.com.cn
# @File : trade_bollinger.py
# @Project : stock

import backtrader as bt
import backtrader.indicators as btind
from src.common.common_tool import *


def load_history_data(ts_code):
    f_path = os.path.join("../job3/stock_price", "{0}.csv".format(ts_code))
    if not os.path.exists(f_path):
        return None

    frame = pd.read_csv(f_path, encoding='utf-8', index_col=None)
    frame = frame.rename(columns={'vol': 'volume'})
    frame['datetime'] = pd.to_datetime(frame['trade_date'], format="%Y%m%d")
    frame = frame.drop(['trade_date', 'change', 'pre_close', 'pct_chg', 'amount'], axis=1)
    frame.set_index('datetime', inplace=True)  # 设置索引覆盖原来的数据
    frame = frame.sort_index(ascending=True)  # 将时间顺序升序，符合时间序列
    frame['openinterest'] = 0
    return frame


def load_history_data_from_pg(ts_code, start_time, end_time):
    frame = pd.read_sql_query(
        "SELECT * FROM t_daily_info t WHERE t.ts_code='{0}' and t.trade_date BETWEEN '{1}' and '{2}' ORDER BY t.trade_date"
            .format(ts_code, start_time, end_time),
        engine_finance_db)
    frame = frame.rename(columns={'vol': 'volume'})
    frame['datetime'] = pd.to_datetime(frame['trade_date'], format="%Y-%m-%d")
    frame = frame.drop(['trade_date', 'change', 'pre_close', 'pct_chg', 'amount'], axis=1)
    frame.set_index('datetime', inplace=True)  # 设置索引覆盖原来的数据
    frame = frame.sort_index(ascending=True)  # 将时间顺序升序，符合时间序列
    frame['openinterest'] = 0
    return frame


class BollingerStrategy(bt.Strategy):
    params = {'period': 20, "size": 100}

    def __init__(self):
        self.lines.top = btind.BollingerBands(self.data.close, period=self.params.period).top
        self.lines.bot = btind.BollingerBands(self.data.close, period=self.params.period).bot
        self.ma200 = btind.SimpleMovingAverage(self.data.close, period=self.params.period)
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.tag = self.ma200 > self.lines.bot

        self.condition4 = self.lines.top / self.lines.bot < 1.05
        self.condition1 = bt.And(self.data.close[-1] > self.data.open, self.data.close[-1] / self.data.open < 1.1)
        self.condition2 = bt.And(self.data.close[-1] < self.data.open, self.data.open / self.data.close[-1] < 1.1)
        self.condition3 = bt.Or(self.condition1, self.condition2)

        # bt.LinePlotterIndicator(self.tag, cmpval='tag')

    def next(self):
        # if not self.position:
        #     if self.data.close > self.top:
        #         self.order = self.buy(size=self.params.size)
        # else:
        #     if self.data.close < self.bot:
        #         self.order = self.sell(size=self.params.size)
        print('是否满足条件：', self.condition4[0])
        #   上穿
        if self.data.close > self.top and self.condition4[0]:
            print("买入")
            self.order = self.buy(size=self.params.size)
        bt.indicators.CrossOver
        # 下穿
        if self.data.close < self.bot and self.condition4[0]:
            self.order = self.sell(size=self.params.size)
        #     print("卖出")

        print("持有量： {}".format(self.position))


if __name__ == '__main__':
    config = load_config()
    #   金融数据库
    engine_finance_db = pg_engine_finance(config)
    #   日志库
    engine_log_db = pg_engine_log(config)
    # ts客户端
    pro = ts_client(config)

    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere

    # Create a Data Feed

    data = bt.feeds.PandasData(dataname=load_history_data_from_pg("601288.SH", '2013-01-01', '2021-12-31'))
    # data = bt.feeds.PandasData(dataname=load_history_data("601288.SH"))
    # print(load_history_data("000657.SZ"))

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(100000.0)
    # cerebro.addstrategy(TestStrategy)
    cerebro.addstrategy(BollingerStrategy)
    # 每次固定交易数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    # 手续费
    cerebro.broker.setcommission(commission=0.001)
    # Print out the starting conditions
    # Run over everything
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    import datetime

    start = datetime.datetime.now()
    print("开始", start)
    results = cerebro.run()
    # print("结束", datetime.datetime.now())
    print("用时：", (datetime.datetime.now() - start).microseconds)
    strat = results[0]
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    print('夏普比率:', strat.analyzers.SharpeRatio.get_analysis())
    print('回撤指标:', strat.analyzers.DW.get_analysis())

    # cerebro.plot(style='candlestick')
    cerebro.plot()
