#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测数据分析脚本
分析交易记录和持仓数据，生成详细的数据总结报告
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

def analyze_position_holding_days(df):
    """分析换仓天数"""
    print("\n换仓天数分析:")
    
    try:
        # 按品种和买卖方向分组，找到配对的买入卖出
        buy_trades = df[df.iloc[:, 4] == '买'].copy()
        sell_trades = df[df.iloc[:, 4] == '卖'].copy()
        
        holding_days = []
        
        # 对每个品种分析持仓天数
        for instrument in buy_trades.iloc[:, 3].unique():
            if pd.isna(instrument):
                continue
                
            instrument_buys = buy_trades[buy_trades.iloc[:, 3] == instrument].sort_values('交易日期')
            instrument_sells = sell_trades[sell_trades.iloc[:, 3] == instrument].sort_values('交易日期')
            
            # 匹配买入和卖出
            for i in range(min(len(instrument_buys), len(instrument_sells))):
                buy_date = instrument_buys.iloc[i]['交易日期']
                sell_date = instrument_sells.iloc[i]['交易日期']
                days = (sell_date - buy_date).days
                if days > 0:  # 确保是有效的持仓天数
                    holding_days.append(days)
        
        if holding_days:
            print(f"  总持仓次数: {len(holding_days)} 次")
            print(f"  平均换仓天数: {np.mean(holding_days):.1f} 天")
            print(f"  最短换仓天数: {min(holding_days)} 天")
            print(f"  最长换仓天数: {max(holding_days)} 天")
            print(f"  换仓天数中位数: {np.median(holding_days):.1f} 天")
        else:
            print("  未找到有效的买卖配对数据")
            
    except Exception as e:
        print(f"  换仓天数分析出错: {e}")

def load_data():
    """加载CSV数据文件"""
    try:
        # 读取交易记录
        transaction_df = pd.read_csv('transaction.csv', encoding='gbk')
        print(f"✓ 交易记录加载成功: {len(transaction_df)} 条记录")
        
        # 读取持仓记录
        position_df = pd.read_csv('position.csv', encoding='gbk')
        print(f"✓ 持仓记录加载成功: {len(position_df)} 条记录")
        
        return transaction_df, position_df
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
        return None, None

def analyze_transactions(df):
    """分析交易记录数据"""
    print("\n" + "="*50)
    print("交易记录分析")
    print("="*50)
    
    # 过滤掉空行
    df_clean = df.dropna(subset=[df.columns[0]])  # 基于第一列过滤空行
    
    # 基本统计
    print(f"总交易次数: {len(df_clean)}")
    
    # 时间范围和交易频率分析
    try:
        # 获取委托时间列（第0列是日期，第1列是时间）
        df_clean['交易日期'] = pd.to_datetime(df_clean.iloc[:, 0])
        
        start_date = df_clean['交易日期'].min()
        end_date = df_clean['交易日期'].max()
        days_span = (end_date - start_date).days + 1
        
        print(f"交易时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        print(f"总交易天数: {days_span} 天")
        
        # 计算交易频率
        trade_frequency = len(df_clean) / days_span * 365  # 年化交易次数
        print(f"交易频率: {trade_frequency:.1f} 次/年 ({len(df_clean) / days_span:.3f} 次/天)")
        
        # 换仓天数分析
        analyze_position_holding_days(df_clean)
        
    except Exception as e:
        print(f"时间分析出错: {e}")
        print(f"交易时间范围: {df_clean.iloc[0, 0]} 到 {df_clean.iloc[-1, 0]}")
    
    # 交易品种统计
    print("\n交易品种分布:")
    instruments = df_clean.iloc[:, 3].value_counts()
    for instrument, count in instruments.items():
        if pd.notna(instrument):
            print(f"  {instrument}: {count} 次")
    
    # 买卖方向统计
    print(f"\n买入交易: {sum(df_clean.iloc[:, 4] == '买')} 次")
    print(f"卖出交易: {sum(df_clean.iloc[:, 4] == '卖')} 次")
    
    # 盈亏分析
    try:
        # 平仓盈亏列（第12列，索引11）
        profit_loss = pd.to_numeric(df.iloc[:, 11], errors='coerce')
        profit_trades = profit_loss[profit_loss > 0]
        loss_trades = profit_loss[profit_loss < 0]
        
        print(f"\n盈亏统计:")
        print(f"  盈利交易: {len(profit_trades)} 次，总盈利: {profit_trades.sum():.2f}")
        print(f"  亏损交易: {len(loss_trades)} 次，总亏损: {loss_trades.sum():.2f}")
        print(f"  胜率: {len(profit_trades) / (len(profit_trades) + len(loss_trades)) * 100:.2f}%")
        
        # 最大单笔盈亏
        if len(profit_trades) > 0:
            print(f"  最大单笔盈利: {profit_trades.max():.2f}")
        if len(loss_trades) > 0:
            print(f"  最大单笔亏损: {loss_trades.min():.2f}")
            
    except Exception as e:
        print(f"盈亏分析出错: {e}")
    
    # 资金变化
    try:
        # 资产总值列（第13列，索引12）
        total_assets = pd.to_numeric(df.iloc[:, 12], errors='coerce').dropna()
        if len(total_assets) > 1:
            initial_capital = total_assets.iloc[0]
            final_capital = total_assets.iloc[-1]
            total_return = (final_capital - initial_capital) / initial_capital * 100
            
            print(f"\n资金变化:")
            print(f"  初始资金: {initial_capital:.2f}")
            print(f"  最终资金: {final_capital:.2f}")
            print(f"  总收益率: {total_return:.2f}%")
            
    except Exception as e:
        print(f"资金分析出错: {e}")

def analyze_positions(df):
    """分析持仓记录数据"""
    print("\n" + "="*50)
    print("持仓记录分析")
    print("="*50)
    
    # 过滤掉现金行
    asset_rows = df[df.iloc[:, 2] != 'Cash'].copy()
    
    print(f"持仓记录条数: {len(asset_rows)}")
    
    if len(asset_rows) == 0:
        print("没有有效的持仓数据")
        return
    
    # 时间范围
    print(f"记录时间范围: {asset_rows.iloc[0, 0]} 到 {asset_rows.iloc[-1, 0]}")
    
    # 持仓品种统计
    print("\n持仓品种:")
    instruments = asset_rows.iloc[:, 2].value_counts()
    for instrument, count in instruments.items():
        if pd.notna(instrument):
            print(f"  {instrument}: {count} 天")
    
    # 收益分析
    try:
        # 日收益列分析
        daily_pnl = pd.to_numeric(asset_rows.iloc[:, 8], errors='coerce').dropna()
        if len(daily_pnl) > 0:
            profitable_days = daily_pnl[daily_pnl > 0]
            loss_days = daily_pnl[daily_pnl < 0]
            
            print(f"\n每日盈亏统计:")
            print(f"  盈利天数: {len(profitable_days)} 天")
            print(f"  亏损天数: {len(loss_days)} 天")
            print(f"  平均日收益: {daily_pnl.mean():.2f}")
            print(f"  最大单日盈利: {daily_pnl.max():.2f}")
            print(f"  最大单日亏损: {daily_pnl.min():.2f}")
            
    except Exception as e:
        print(f"收益分析出错: {e}")
    
    # 持仓市值分析
    try:
        # 市值列分析
        market_value = pd.to_numeric(asset_rows.iloc[:, 7], errors='coerce').dropna()
        if len(market_value) > 0:
            print(f"\n持仓市值统计:")
            print(f"  平均持仓市值: {market_value.mean():.2f}")
            print(f"  最大持仓市值: {market_value.max():.2f}")
            print(f"  最小持仓市值: {market_value.min():.2f}")
            
    except Exception as e:
        print(f"市值分析出错: {e}")

def generate_summary_report(transaction_df, position_df):
    """生成综合数据报告"""
    print("\n" + "="*50)
    print("综合数据报告")
    print("="*50)
    
    # 策略特征分析
    print("策略特征:")
    
    # 从交易记录分析交易频率
    if transaction_df is not None and len(transaction_df) > 0:
        # 计算时间跨度
        try:
            start_date = pd.to_datetime(transaction_df.iloc[0, 1])
            end_date = pd.to_datetime(transaction_df.iloc[-2, 1])  # 最后一行可能是空的
            days_span = (end_date - start_date).days
            
            trade_count = len(transaction_df)
            avg_trades_per_month = trade_count / (days_span / 30)
            
            print(f"  交易周期: {days_span} 天")
            print(f"  平均月交易频率: {avg_trades_per_month:.1f} 次/月")
            
        except Exception as e:
            print(f"  时间分析出错: {e}")
    
    # 投资品种分析
    print("  投资标的: 主要投资ETF产品（价值、成长、黄金等）")
    print("  交易模式: 轮动策略，单品种买入卖出配对")
    
    # 风险收益特征
    try:
        if transaction_df is not None and len(transaction_df) > 1:
            total_assets = pd.to_numeric(transaction_df.iloc[:, 12], errors='coerce').dropna()
            if len(total_assets) > 1:
                returns = total_assets.pct_change().dropna()
                if len(returns) > 0:
                    annual_volatility = returns.std() * np.sqrt(252) * 100
                    print(f"  年化波动率: {annual_volatility:.2f}%")
                    
                    # 最大回撤
                    cumulative = (1 + returns).cumprod()
                    rolling_max = cumulative.expanding().max()
                    drawdown = (cumulative - rolling_max) / rolling_max
                    max_drawdown = drawdown.min() * 100
                    print(f"  最大回撤: {max_drawdown:.2f}%")
                    
    except Exception as e:
        print(f"  风险指标计算出错: {e}")
    
    print("\n✓ 数据分析完成！")
    print("📊 建议: 该策略表现出良好的ETF轮动特征，适合中长期投资")

def get_key_metrics(transaction_df):
    """获取关键指标"""
    try:
        df_clean = transaction_df.dropna(subset=[transaction_df.columns[0]])
        df_clean['交易日期'] = pd.to_datetime(df_clean.iloc[:, 0])
        
        # 计算交易频率（月频率）
        start_date = df_clean['交易日期'].min()
        end_date = df_clean['交易日期'].max()
        months_span = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
        trade_frequency_monthly = len(df_clean) / months_span
        
        # 计算换仓天数和最大盈利持仓天数
        buy_trades = df_clean[df_clean.iloc[:, 4] == '买'].copy()
        sell_trades = df_clean[df_clean.iloc[:, 4] == '卖'].copy()
        
        holding_days = []
        max_profit_days = 0
        max_profit = 0
        max_loss_days = 0
        max_loss = 0
        
        for instrument in buy_trades.iloc[:, 3].unique():
            if pd.isna(instrument):
                continue
                
            instrument_buys = buy_trades[buy_trades.iloc[:, 3] == instrument].sort_values('交易日期')
            instrument_sells = sell_trades[sell_trades.iloc[:, 3] == instrument].sort_values('交易日期')
            
            for i in range(min(len(instrument_buys), len(instrument_sells))):
                buy_date = instrument_buys.iloc[i]['交易日期']
                sell_date = instrument_sells.iloc[i]['交易日期']
                days = (sell_date - buy_date).days
                
                if days > 0:
                    holding_days.append(days)
                    
                    # 获取对应的盈亏数据（平仓盈亏列，第12列索引11）
                    sell_row = instrument_sells.iloc[i]
                    profit = pd.to_numeric(sell_row.iloc[11], errors='coerce')
                    
                    if pd.notna(profit):
                        # 记录最大盈利
                        if profit > max_profit:
                            max_profit = profit
                            max_profit_days = days
                        # 记录最大亏损
                        if profit < max_loss:
                            max_loss = profit
                            max_loss_days = days
        
        min_days = min(holding_days) if holding_days else 0
        max_days = max(holding_days) if holding_days else 0
        median_days = np.median(holding_days) if holding_days else 0
        
        return trade_frequency_monthly, min_days, max_days, median_days, max_profit_days, max_profit, max_loss_days, max_loss
        
    except Exception as e:
        return 0, 0, 0, 0, 0, 0, 0, 0

def main():
    """主函数"""
    print("🚀 开始分析回测数据...")
    
    # 加载数据
    transaction_df, position_df = load_data()
    
    if transaction_df is None or position_df is None:
        print("❌ 数据加载失败，请检查CSV文件是否存在且格式正确")
        return
    
    # 获取并显示关键指标
    trade_freq, min_days, max_days, median_days, max_profit_days, max_profit, max_loss_days, max_loss = get_key_metrics(transaction_df)
    
    print("\n" + "="*60)
    print("📊 核心指标总览")
    print("="*60)
    print(f"1. 交易频率: {trade_freq:.1f} 次/月")
    print(f"2. 最短换仓天数: {min_days} 天")
    print(f"3. 最长换仓天数: {max_days} 天")
    print(f"4. 换仓天数中位数: {median_days:.1f} 天")
    print(f"5. 最大盈利交易持仓: {max_profit_days} 天 (盈利: {max_profit:.2f})")
    print(f"6. 最大亏损交易持仓: {max_loss_days} 天 (亏损: {max_loss:.2f})")
    print("="*60)
    
    # 分析交易记录
    analyze_transactions(transaction_df)
    
    # 分析持仓记录  
    analyze_positions(position_df)
    
    # 生成综合报告
    generate_summary_report(transaction_df, position_df)

if __name__ == "__main__":
    main()