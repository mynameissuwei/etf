#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF切换表现分析工具
计算每次切换后15个交易日的涨幅对比
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def get_next_n_trading_days(data, start_date, n_days):
    """获取指定日期后N个交易日的数据"""
    try:
        # 找到开始日期及之后的数据
        future_data = data[data.index > start_date]
        if len(future_data) >= n_days:
            return future_data.iloc[:n_days]
        else:
            return future_data
    except:
        return pd.DataFrame()

def calculate_switch_performance(days=15):
    """计算每次切换后N个交易日的表现对比"""
    
    # 读取交易记录
    trades_df = pd.read_csv('/home/suwei/回测策略/analysis_results/trades_record.csv')
    trades_df['date'] = pd.to_datetime(trades_df['date'])
    
    # 读取价格数据
    data_159509 = pd.read_csv('/home/suwei/回测策略/data/159509_data.csv')
    data_159509['date'] = pd.to_datetime(data_159509['date'])
    data_159509 = data_159509.sort_values('date').set_index('date')
    data_159509.rename(columns={'net_value': 'close'}, inplace=True)
    
    data_161116 = pd.read_csv('/home/suwei/回测策略/data/161116_data.csv')
    data_161116['date'] = pd.to_datetime(data_161116['date'])
    data_161116 = data_161116.sort_values('date').set_index('date')
    data_161116.rename(columns={'net_value': 'close'}, inplace=True)
    
    # 获取所有买入交易（切换点）
    buy_trades = trades_df[trades_df['type'] == 'buy'].copy()
    
    results = []
    
    for i, trade in buy_trades.iterrows():
        trade_date = trade['date']
        buy_code = trade['code']
        buy_name = trade['name']
        
        # 确定买入和未买入的标的
        if buy_code == '159509':
            buy_data = data_159509
            other_data = data_161116
            buy_symbol = '纳指'
            other_symbol = '黄金'
        else:
            buy_data = data_161116
            other_data = data_159509
            buy_symbol = '黄金'
            other_symbol = '纳指'
        
        try:
            # 获取切换日的价格作为基准
            trade_date_data = buy_data[buy_data.index <= trade_date]
            other_trade_date_data = other_data[other_data.index <= trade_date]
            
            if len(trade_date_data) == 0 or len(other_trade_date_data) == 0:
                continue
                
            buy_start_price = trade_date_data.iloc[-1]['close']
            other_start_price = other_trade_date_data.iloc[-1]['close']
            
            # 获取后续N个交易日的数据
            buy_future = get_next_n_trading_days(buy_data, trade_date, days)
            other_future = get_next_n_trading_days(other_data, trade_date, days)
            
            if len(buy_future) == 0 or len(other_future) == 0:
                continue
            
            # 计算N个交易日后的收益率
            buy_end_price = buy_future.iloc[-1]['close']
            other_end_price = other_future.iloc[-1]['close']
            
            # 获取具体的开始和结束日期
            buy_start_date = trade_date_data.index[-1].strftime('%Y-%m-%d')
            buy_end_date = buy_future.index[-1].strftime('%Y-%m-%d')
            other_start_date = other_trade_date_data.index[-1].strftime('%Y-%m-%d')
            other_end_date = other_future.index[-1].strftime('%Y-%m-%d')
            
            buy_return = (buy_end_price / buy_start_price - 1) * 100
            other_return = (other_end_price / other_start_price - 1) * 100
            
            # 判断哪个更好
            buy_better = buy_return > other_return
            
            results.append({
                '切换日期': trade_date.strftime('%Y-%m-%d'),
                '买入标的': buy_symbol,
                '未买标的': other_symbol,
                '买入收益率': f"{buy_return:.2f}%",
                '未买收益率': f"{other_return:.2f}%",
                '选择正确': "是" if buy_better else "否",
                '收益差': f"{buy_return - other_return:.2f}%",
                # 详细计算数据
                '买入开始价格': buy_start_price,
                '买入结束价格': buy_end_price,
                '未买开始价格': other_start_price,
                '未买结束价格': other_end_price,
                '买入原始收益率': buy_return,
                '未买原始收益率': other_return,
                # 日期信息
                '买入开始日期': buy_start_date,
                '买入结束日期': buy_end_date,
                '未买开始日期': other_start_date,
                '未买结束日期': other_end_date
            })
            
        except Exception as e:
            print(f"处理 {trade_date} 的数据时出错: {e}")
            continue
    
    return pd.DataFrame(results)

def generate_formatted_output(days=15):
    """生成格式化的输出"""
    
    # 计算切换表现
    results_df = calculate_switch_performance(days)
    
    if len(results_df) == 0:
        print("没有足够的数据进行分析")
        return
    
    print(f"切换日期,纳指后续{days}日涨幅,黄金后续{days}日涨幅,纳指更好？")
    
    for _, row in results_df.iterrows():
        date = row['切换日期']
        
        if row['买入标的'] == '纳指':
            nasdq_return = float(row['买入收益率'].replace('%', ''))
            gold_return = float(row['未买收益率'].replace('%', ''))
        else:
            nasdq_return = float(row['未买收益率'].replace('%', ''))
            gold_return = float(row['买入收益率'].replace('%', ''))
        
        nasdq_better = nasdq_return > gold_return
        nasdq_better_text = "是" if nasdq_better else "否"
        
        print(f"{date},{nasdq_return:+.2f}%,{gold_return:+.2f}%,{nasdq_better_text}")
    
    # 统计分析
    print(f"\n统计分析:")
    print(f"总切换次数: {len(results_df)}")
    
    # 计算纳指更好的次数
    nasdq_wins = 0
    for _, row in results_df.iterrows():
        if row['买入标的'] == '纳指':
            nasdq_return = float(row['买入收益率'].replace('%', ''))
            gold_return = float(row['未买收益率'].replace('%', ''))
        else:
            nasdq_return = float(row['未买收益率'].replace('%', ''))
            gold_return = float(row['买入收益率'].replace('%', ''))
        
        if nasdq_return > gold_return:
            nasdq_wins += 1
    
    nasdq_win_rate = nasdq_wins / len(results_df) * 100
    print(f"纳指表现更好的次数: {nasdq_wins}/{len(results_df)} = {nasdq_win_rate:.1f}%")
    
    # 分析买入选择的正确率
    correct_choices = len(results_df[results_df['选择正确'] == '是'])
    choice_accuracy = correct_choices / len(results_df) * 100
    print(f"策略选择正确率: {correct_choices}/{len(results_df)} = {choice_accuracy:.1f}%")
    
    # 保存详细结果
    results_df.to_csv(f'/home/suwei/回测策略/analysis_results/switch_{days}d_performance.csv', index=False)
    print(f"\n详细结果已保存到: analysis_results/switch_{days}d_performance.csv")
    
    return results_df

def print_detailed_verification(target_date, days=15):
    """打印特定交易的详细验证信息"""
    results_df = calculate_switch_performance(days)
    target_record = results_df[results_df['切换日期'] == target_date]
    
    if len(target_record) == 0:
        print(f"未找到 {target_date} 的交易记录")
        return
    
    record = target_record.iloc[0]
    
    print(f"\n=== {target_date} 交易详细验证 (后续{days}个交易日) ===")
    print(f"【买入标的】{record['买入标的']}:")
    print(f"  开始日期: {record['买入开始日期']}")
    print(f"  开始价格: {record['买入开始价格']:.6f}")
    print(f"  结束日期: {record['买入结束日期']}")
    print(f"  结束价格: {record['买入结束价格']:.6f}")
    print(f"  收益率: ({record['买入结束价格']:.6f} / {record['买入开始价格']:.6f} - 1) × 100 = {record['买入原始收益率']:.6f}%")
    
    print(f"\n【未买标的】{record['未买标的']}:")
    print(f"  开始日期: {record['未买开始日期']}")
    print(f"  开始价格: {record['未买开始价格']:.6f}")
    print(f"  结束日期: {record['未买结束日期']}")
    print(f"  结束价格: {record['未买结束价格']:.6f}")
    print(f"  收益率: ({record['未买结束价格']:.6f} / {record['未买开始价格']:.6f} - 1) × 100 = {record['未买原始收益率']:.6f}%")
    
    print(f"\n【对比结果】:")
    print(f"  {record['买入标的']}收益率: {record['买入收益率']}")
    print(f"  {record['未买标的']}收益率: {record['未买收益率']}")
    print(f"  策略选择: {'正确' if record['选择正确'] == '是' else '错误'}")
    print(f"  收益差: {record['收益差']}")

if __name__ == "__main__":
    print("=== 15个交易日后表现 ===")
    generate_formatted_output(15)
    
    print("\n\n=== 20个交易日后表现 ===")
    generate_formatted_output(20)
    
    # 显示2025-05-09的详细验证信息
    print_detailed_verification('2025-05-09', 15)
    print_detailed_verification('2025-05-09', 20)