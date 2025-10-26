#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版：策略表现 vs 纯持有纳指分析
正确分析每个持仓期间的超额收益
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def analyze_holding_periods():
    """分析每个持仓期间的表现"""

    # 读取交易记录
    trades_df = pd.read_csv('analysis_results/trades_record.csv')
    trades_df['date'] = pd.to_datetime(trades_df['date'])
    trades_df = trades_df.sort_values('date')

    # 读取数据
    data_159509 = pd.read_csv('data/159509_data.csv')
    data_159509['date'] = pd.to_datetime(data_159509['date'])
    data_159509 = data_159509.sort_values('date').set_index('date')
    data_159509.rename(columns={'net_value': 'close'}, inplace=True)

    data_161116 = pd.read_csv('data/161116_data.csv')
    data_161116['date'] = pd.to_datetime(data_161116['date'])
    data_161116 = data_161116.sort_values('date').set_index('date')
    data_161116.rename(columns={'net_value': 'close'}, inplace=True)
    
    # 获取所有买入交易，按日期排序
    buy_trades = trades_df[trades_df['type'] == 'buy'].copy().reset_index(drop=True)
    
    holding_periods = []
    
    for i in range(len(buy_trades)):
        current_buy = buy_trades.iloc[i]
        buy_date = current_buy['date']
        buy_code = current_buy['code']  # 这次买入的标的代码
        
        # 确定这个持仓期的结束日期
        if i < len(buy_trades) - 1:
            # 不是最后一笔买入，结束日期是下一笔买入的日期
            next_buy = buy_trades.iloc[i + 1]
            sell_date = next_buy['date']
        else:
            # 最后一笔买入，使用数据的最后日期
            sell_date = max(data_159509.index.max(), data_161116.index.max())
        
        # 确定持有的标的和数据  
        if str(buy_code) == '159509':
            held_data = data_159509
            held_name = '纳指科技ETF'
        else:
            held_data = data_161116
            held_name = '易方达黄金ETF'
        
        
        # 获取持有期间的价格
        buy_price_data = held_data[held_data.index <= buy_date]
        sell_price_data = held_data[held_data.index <= sell_date]
        
        if len(buy_price_data) > 0 and len(sell_price_data) > 0:
            buy_price = buy_price_data.iloc[-1]['close']
            sell_price = sell_price_data.iloc[-1]['close']
            
            # 计算策略收益
            strategy_return = (sell_price / buy_price - 1) * 100
            
            # 始终对比纯持有纳指（基准）
            nasdq_buy_data = data_159509[data_159509.index <= buy_date]
            nasdq_sell_data = data_159509[data_159509.index <= sell_date]
            
            if len(nasdq_buy_data) > 0 and len(nasdq_sell_data) > 0:
                nasdq_buy_price = nasdq_buy_data.iloc[-1]['close']
                nasdq_sell_price = nasdq_sell_data.iloc[-1]['close']
                nasdq_return = (nasdq_sell_price / nasdq_buy_price - 1) * 100
                
                # 计算超额收益（策略 vs 纯持有纳指基准）
                excess_return = strategy_return - nasdq_return
                
                holding_periods.append({
                    '期间': i + 1,
                    '开始日期': buy_date.strftime('%Y-%m-%d'),
                    '结束日期': sell_date.strftime('%Y-%m-%d'),
                    '持有标的': held_name,
                    '策略收益率': f"{strategy_return:.2f}%",
                    '纳指基准收益率': f"{nasdq_return:.2f}%",
                    '超额收益': f"{excess_return:.2f}%",
                    '超额收益数值': excess_return,
                    '是否跑赢纳指': excess_return > 0
                })
    
    return pd.DataFrame(holding_periods)

def main():
    print("=== 策略 vs 纯持有纳指 - 修正版分析 ===\n")
    
    # 分析持仓期间
    periods_df = analyze_holding_periods()
    
    # 显示每个期间的详细信息
    print("各持仓期间表现对比:")
    for _, row in periods_df.iterrows():
        if row['超额收益数值'] > 0:
            status = "✓ 跑赢纳指"
        elif row['超额收益数值'] < 0:
            status = "✗ 跑输纳指"
        else:
            status = "— 与纳指持平"
        print(f"期间{row['期间']}: {row['开始日期']} → {row['结束日期']}")
        print(f"  持有: {row['持有标的']}")
        print(f"  策略收益: {row['策略收益率']} vs 纳指基准: {row['纳指基准收益率']}")
        print(f"  超额收益: {row['超额收益']} {status}")
        print()
    
    # 统计分析
    total_periods = len(periods_df)
    winning_periods = len(periods_df[periods_df['超额收益数值'] > 0])
    losing_periods = len(periods_df[periods_df['超额收益数值'] < 0])
    flat_periods = len(periods_df[periods_df['超额收益数值'] == 0])
    
    print("=== 统计总结 ===")
    print(f"总持仓期间: {total_periods}")
    print(f"跑赢纳指期间: {winning_periods} ({winning_periods/total_periods*100:.1f}%)")
    print(f"跑输纳指期间: {losing_periods} ({losing_periods/total_periods*100:.1f}%)")
    print(f"与纳指持平期间: {flat_periods} ({flat_periods/total_periods*100:.1f}%)")
    
    # 计算总超额收益
    total_excess_return = periods_df['超额收益数值'].sum()
    print(f"\\n总超额收益: {total_excess_return:.2f}%")
    print(f"正贡献超额收益: {periods_df[periods_df['超额收益数值'] > 0]['超额收益数值'].sum():.2f}%")
    print(f"负贡献超额收益: {periods_df[periods_df['超额收益数值'] < 0]['超额收益数值'].sum():.2f}%")
    
    # 最大贡献和最大拖累
    if len(periods_df) > 0:
        best_period = periods_df.loc[periods_df['超额收益数值'].idxmax()]
        worst_period = periods_df.loc[periods_df['超额收益数值'].idxmin()]
        
        print(f"\n最大超额收益贡献:")
        print(f"  期间{best_period['期间']}: {best_period['开始日期']} → {best_period['结束日期']}")
        print(f"  持有: {best_period['持有标的']}")
        print(f"  超额收益: {best_period['超额收益']}")
        
        print(f"\n最大超额收益拖累:")
        print(f"  期间{worst_period['期间']}: {worst_period['开始日期']} → {worst_period['结束日期']}")
        print(f"  持有: {worst_period['持有标的']}")
        print(f"  超额收益: {worst_period['超额收益']}")
    
    # 保存结果
    periods_df.to_csv('analysis_results/holding_periods_analysis.csv', index=False, encoding='utf-8-sig')
    print(f"\n详细结果已保存到: analysis_results/holding_periods_analysis.csv")
    
    return periods_df

if __name__ == "__main__":
    main()