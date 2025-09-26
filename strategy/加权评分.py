# 克隆自聚宽文章：https://www.joinquant.com/post/42673
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

# 克隆自聚宽文章：https://www.joinquant.com/post/42673
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

import math
import numpy as np
import pandas as pd


#初始化函数 
def initialize(context):
    # 设定基准
    set_benchmark('159509.XSHE')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 设置滑点 https://www.joinquant.com/view/community/detail/a31a822d1cfa7e83b1dda228d4562a70
    set_slippage(FixedSlippage(0.000))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0, min_commission=5), type='fund')
    # 过滤一定级别的日志
    log.set_level('system', 'error')
    # 参数
    g.etf_pool = [
        # '161116.XSHE', #黄金ETF（大宗商品）
        '159509.XSHE', #纳指科技ETF（海外资产）
        '518880.XSHG', #黄金ETF（大宗商品）
    ]
    g.m_days = 25 #动量参考天数
    g.m_days1 =3
    run_daily(trade,  "9:30") #每天运行确保即时捕捉动量变化

def sigmoid(X):  
    return 1.0 / (1 + np.exp(-float(X)))

        
# 基于年化收益和判定系数打分的动量因子轮动 https://www.joinquant.com/post/26142
def get_rank(etf_pool):
    score_records = []
    for etf in etf_pool:
        df_long = attribute_history(etf, g.m_days, '1d', ['close'])
        y_long = df_long['log'] = np.log(df_long.close)
        x_long = df_long['num'] = np.arange(df_long.log.size)
        slope_long, intercept_long = np.polyfit(x_long, y_long, 1)
        annualized_returns_long = math.pow(math.exp(slope_long), 250) - 1
        r_squared_long = 1 - (sum((y_long - (slope_long * x_long + intercept_long))**2) / ((len(y_long) - 1) * np.var(y_long, ddof=1)))
        long_term_raw_score = annualized_returns_long * r_squared_long

        df_short = attribute_history(etf, g.m_days1, '1d', ['close'])
        y_short = df_short['log'] = np.log(df_short.close)
        x_short = df_short['num'] = np.arange(df_short.log.size)
        slope_short, intercept_short = np.polyfit(x_short, y_short, 1)
        annualized_returns_short = math.pow(math.exp(slope_short), 250) - 1
        r_squared_short = 1 - (sum((y_short - (slope_short * x_short + intercept_short))**2) / ((len(y_short) - 1) * np.var(y_short, ddof=1)))
        short_term_raw_score = slope_short

        long_term_score = sigmoid(long_term_raw_score)
        short_term_score = sigmoid(short_term_raw_score)
        combined_score = long_term_score * short_term_score
        if long_term_score < 0 and short_term_score < 0:
            combined_score *= -1

        score_records.append({
            'etf': etf,
            'score': combined_score,
        })

    df = pd.DataFrame(score_records).set_index('etf')
    df = df.sort_values(by='score', ascending=False)
    print('Daily combined score:')
    print(df)
    rank_list = list(df.index)
    return rank_list

# 交易
def trade(context):
    # 获取动量最高的一只ETF
    target_num = 1    
    target_list = get_rank(g.etf_pool)[:target_num]
    # 卖出    
    hold_list = list(context.portfolio.positions)
    for etf in hold_list:
        if etf not in target_list:
            order_target_value(etf, 0)
            print('卖出' + str(etf))
        else:
            print('继续持有' + str(etf))
    # 买入
    hold_list = list(context.portfolio.positions)
    if len(hold_list) < target_num:
        value = context.portfolio.available_cash / (target_num - len(hold_list))
        for etf in target_list:
            if context.portfolio.positions[etf].total_amount == 0:
                order_target_value(etf, value)
                print('买入' + str(etf))
