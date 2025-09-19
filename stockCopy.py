# 克隆自聚宽文章：https://www.joinquant.com/post/48951
# 标题：用纳指科技ETF代替纳指ETF收益翻倍
# 作者：Dx198611

# 克隆自聚宽文章：https://www.joinquant.com/post/47888
# 标题：ETF策略之核心资产轮动（线性增加权重）
# 作者：MarioC

# 克隆自聚宽文章：https://www.joinquant.com/post/42673
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

# 综合年化收益率（趋势强度）和R²（趋势稳定性），
# 得分高的ETF兼具强劲且稳定的上涨趋势，优先被策略选中。

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
        '518880.XSHG', #黄金ETF（大宗商品）
        '159509.XSHE', #纳指科技ETF（海外资产）
        #'513500.XSHG', #sp500 
        #'513100.XSHG' #qqq
        #'161128.XSHE'  #sp tech
    ]
    g.m_days = 25#动量参考天数
    run_daily(trade, '9:30') #每天运行确保即时捕捉动量变化

    
def MOM(etf):
    df = attribute_history(etf, g.m_days, '1d', ['close'])
    y = np.log(df['close'].values)
    n = len(y)  
    x = np.arange(n)
    weights = np.linspace(1, 2, n)  # 线性增加权重
    slope, intercept = np.polyfit(x, y, 1, w=weights)
    annualized_returns = math.pow(math.exp(slope), 250) - 1
    residuals = y - (slope * x + intercept)
    weighted_residuals = weights * residuals**2
    r_squared = 1 - (np.sum(weighted_residuals) / np.sum(weights * (y - np.mean(y))**2))
    score = annualized_returns * r_squared
    return score

    
# 基于年化收益和判定系数打分的动量因子轮动 https://www.joinquant.com/post/26142
def get_rank(etf_pool):
    score_list = []
    for etf in etf_pool:
        score = MOM(etf)
        score_list.append(score)
    df = pd.DataFrame(index=etf_pool, data={'score':score_list})
    df = df.sort_values(by='score', ascending=False)
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
            pass
    # 买入
    hold_list = list(context.portfolio.positions)
    if len(hold_list) < target_num:
        value = context.portfolio.available_cash / (target_num - len(hold_list))
        for etf in target_list:
            if context.portfolio.positions[etf].total_amount == 0:
                order_target_value(etf, value)
                print('买入' + str(etf))

