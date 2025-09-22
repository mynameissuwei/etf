#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地化ETF轮动策略
基于原聚宽策略改写，使用本地数据源实现ETF动量轮动
"""

import numpy as np
import pandas as pd
import math
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class LocalETFStrategy:
    def __init__(self, data_dir='/home/suwei/回测策略/data'):
        """
        初始化策略
        """
        self.data_dir = data_dir
        # ETF池配置 - 对应我们获取的数据
        self.etf_config = {
            # '518880': {'name': '黄金ETF', 'file': '518880_data.csv'},
            '159509': {'name': '纳指科技ETF', 'file': '159509_data.csv'}, 
            # '513500': {'name': '中概ETF', 'file': '513500_data.csv'},
            '161116': {'name': '易方达黄金ETF', 'file': '161116_data.csv'}
        }
        
        # 策略参数
        self.m_days = 25  # 动量参考天数
        self.target_num = 1  # 目标持仓ETF数量
        self.initial_capital = 100000  # 初始资金10万
        
        # 数据容器
        self.etf_data = {}
        self.portfolio = {
            'cash': self.initial_capital,
            'positions': {},  # {etf_code: {'shares': 0, 'value': 0}}
            'total_value': self.initial_capital,
            'history': [],  # 记录每日组合价值
            'trades': []  # 记录交易记录
        }

        # 保存每日得分及拆分明细
        self.daily_scores = {}
        self.daily_score_details = {}
        
        self.load_data()
    
    def load_data(self):
        """
        加载ETF数据
        """
        print("正在加载ETF数据...")
        
        for etf_code, config in self.etf_config.items():
            file_path = os.path.join(self.data_dir, config['file'])
            
            try:
                df = pd.read_csv(file_path)
                # 标准化列名
                if 'net_value' in df.columns:
                    df.rename(columns={'net_value': 'close'}, inplace=True)
                elif '单位净值' in df.columns:
                    df.rename(columns={'单位净值': 'close', '净值日期': 'date'}, inplace=True)
                
                # 确保日期格式正确
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df.set_index('date', inplace=True)
                
                # 确保数据类型
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df = df.dropna()
                
                self.etf_data[etf_code] = df
                print(f"✓ {config['name']}({etf_code}): {len(df)} 条数据, 时间范围: {df.index[0].date()} 到 {df.index[-1].date()}")
                
                # 初始化持仓
                self.portfolio['positions'][etf_code] = {'shares': 0, 'value': 0}
                
            except Exception as e:
                print(f"✗ 加载 {config['name']}({etf_code}) 数据失败: {e}")
        
        print(f"数据加载完成，共 {len(self.etf_data)} 只ETF")
    
    def get_trading_dates(self):
        """
        获取所有ETF的共同交易日期
        """
        if not self.etf_data:
            return []
        
        # 找到所有ETF的交集日期
        date_sets = [set(df.index) for df in self.etf_data.values()]
        common_dates = set.intersection(*date_sets)
        
        # 排序并确保有足够的历史数据
        common_dates = sorted(list(common_dates))
        
        # 从第m_days天开始，确保有足够历史数据计算动量
        if len(common_dates) > self.m_days:
            return common_dates[self.m_days:]
        else:
            print(f"警告：共同交易日期不足，需要至少{self.m_days}天历史数据")
            return []
    
    def MOM(self, etf_code, end_date):
        """
        计算动量因子 - 完全按照聚宽stock.py的逻辑

        MOM评分计算步骤说明（便于手工验证）：
        1. 获取end_date之前的25个交易日价格数据（不包括end_date当天）
        2. 对价格取对数：y = ln(price)
        3. 创建时间序列：x = [0, 1, 2, ..., 24]
        4. 创建线性权重：weights = [1.0, 1.04, 1.08, ..., 2.0] （25个值）
        5. 加权线性回归：y = slope * x + intercept
        6. 计算年化收益率：annualized_returns = (e^slope)^250 - 1
        7. 计算加权R平方：r_squared = 1 - 加权残差平方和/加权总平方和
        8. 最终评分：score = annualized_returns * r_squared
        """
        try:
            df = self.etf_data[etf_code]
            
            # 获取指定日期前m_days天的数据
            end_date = pd.to_datetime(end_date)
            
            # 模拟聚宽attribute_history: 获取end_date之前的25个交易日（不包括end_date）
            mask = df.index < end_date  # 注意这里是 < 不是 <=
            price_data = df.loc[mask, 'close'].tail(self.m_days)
            
            if len(price_data) < self.m_days:
                return -999, {
                    'score': -999,
                    'message': '历史数据不足',
                    'annualized_returns': np.nan,
                    'r_squared': np.nan,
                    'slope': np.nan,
                    'start_price': price_data.iloc[0] if len(price_data) > 0 else np.nan,
                    'end_price': price_data.iloc[-1] if len(price_data) > 0 else np.nan
                }
            
            # 【步骤详解】完全按照聚宽stock.py的MOM函数逻辑
            # 步骤1: 对价格取对数
            y = np.log(price_data.values)
            n = len(y)  # 应该是25
            
            # 步骤2: 创建时间序列 [0, 1, 2, ..., 24]
            x = np.arange(n)
            
            # 步骤3: 创建线性权重 [1.0, 1.04, 1.08, ..., 2.0]
            weights = np.linspace(1, 2, n)  # 线性增加权重
            
            # 步骤4: 加权线性回归
            slope, intercept = np.polyfit(x, y, 1, w=weights)
            
            # 步骤5: 计算年化收益率
            annualized_returns = math.pow(math.exp(slope), 250) - 1
            
            # 步骤6: 计算残差和加权R平方
            residuals = y - (slope * x + intercept)
            weighted_residuals = weights * residuals**2
            weighted_total_variance = weights * (y - np.mean(y))**2
            r_squared = 1 - (np.sum(weighted_residuals) / np.sum(weighted_total_variance))
            
            # 步骤7: 最终评分
            score = annualized_returns * r_squared
            
            details = {
                'score': score,
                'annualized_returns': annualized_returns,
                'r_squared': r_squared,
                'slope': slope,
                'start_price': float(price_data.iloc[0]),
                'end_price': float(price_data.iloc[-1])
            }

            return score, details

        except Exception as e:
            print(f"计算{etf_code}动量因子时出错: {e}")
            return -999, {
                'score': -999,
                'message': str(e),
                'annualized_returns': np.nan,
                'r_squared': np.nan,
                'slope': np.nan,
                'start_price': np.nan,
                'end_price': np.nan
            }
    
    def get_rank(self, date):
        """
        获取ETF排名（按动量得分）
        """
        scores = {}
        score_details = {}

        for etf_code in self.etf_data.keys():
            score, detail = self.MOM(etf_code, date)
            scores[etf_code] = score
            score_details[etf_code] = detail

        # 按得分排序
        ranked_etfs = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        # 返回排序后的ETF代码列表
        return [etf for etf, score in ranked_etfs], scores, score_details
    
    def get_current_price(self, etf_code, date):
        """
        获取指定日期的ETF价格
        """
        try:
            df = self.etf_data[etf_code]
            date = pd.to_datetime(date)
            
            if date in df.index:
                return df.loc[date, 'close']
            else:
                # 找最近的交易日
                available_dates = df.index[df.index <= date]
                if len(available_dates) > 0:
                    latest_date = available_dates[-1]
                    return df.loc[latest_date, 'close']
                else:
                    return None
        except:
            return None
    
    def update_portfolio_value(self, date):
        """
        更新组合总价值
        """
        total_value = self.portfolio['cash']
        
        for etf_code, position in self.portfolio['positions'].items():
            if position['shares'] > 0:
                current_price = self.get_current_price(etf_code, date)
                if current_price:
                    position_value = position['shares'] * current_price
                    position['value'] = position_value
                    total_value += position_value
        
        self.portfolio['total_value'] = total_value
        return total_value
    
    def trade(self, date):
        """
        执行交易逻辑 - 完全模拟聚宽平台逻辑
        """
        # 获取当前最优ETF（只选择得分最高的1只）
        ranked_etfs, scores, score_details = self.get_rank(date)

        if not ranked_etfs:
            print(f"{date.strftime('%Y-%m-%d')} 无可用ETF评分，跳过交易")
            return

        target_etf = ranked_etfs[0]

        # 保存当日的评分数据，用于CSV导出
        self.daily_scores = scores
        self.daily_score_details = score_details
        
        # 获取当前持仓
        current_holdings = [etf for etf, pos in self.portfolio['positions'].items() 
                          if pos['shares'] > 0]
        
        # 判断是否需要交易
        need_trade = False
        if len(current_holdings) == 0:
            # 没有持仓，需要买入
            need_trade = True
        elif len(current_holdings) > 0 and target_etf not in current_holdings:
            # 当前持仓不是最优ETF，需要切换
            need_trade = True
        
        if need_trade:
            print(f"\n{date.strftime('%Y-%m-%d')} 交易信号:")
            for etf, score in scores.items():
                name = self.etf_config[etf]['name']
                print(f"  {name}({etf}): {score:.4f}")
            
            # 卖出所有当前持仓
            for etf_code in current_holdings:
                if etf_code != target_etf:
                    shares = self.portfolio['positions'][etf_code]['shares']
                    current_price = self.get_current_price(etf_code, date)
                    
                    if current_price and shares > 0:
                        sell_value = shares * current_price
                        self.portfolio['cash'] += sell_value
                        self.portfolio['positions'][etf_code]['shares'] = 0
                        self.portfolio['positions'][etf_code]['value'] = 0
                        
                        # 记录交易
                        self.portfolio['trades'].append({
                            'date': date.strftime('%Y-%m-%d'),
                            'type': 'sell',
                            'code': etf_code,
                            'name': self.etf_config[etf_code]['name'],
                            'shares': shares,
                            'price': current_price,
                            'amount': sell_value
                        })
                        
                        name = self.etf_config[etf_code]['name']
                        print(f"  ✗ 卖出 {name}({etf_code}): {shares:.0f}股, 价值{sell_value:.2f}元")
            
            # 买入目标ETF
            if target_etf not in current_holdings or self.portfolio['positions'][target_etf]['shares'] == 0:
                current_price = self.get_current_price(target_etf, date)
                
                if current_price and self.portfolio['cash'] > 0:
                    # 用所有现金买入
                    buy_value = self.portfolio['cash']
                    shares = buy_value / current_price
                    
                    self.portfolio['positions'][target_etf]['shares'] = shares
                    self.portfolio['positions'][target_etf]['value'] = buy_value
                    self.portfolio['cash'] = 0
                    
                    # 记录交易
                    self.portfolio['trades'].append({
                        'date': date.strftime('%Y-%m-%d'),
                        'type': 'buy',
                        'code': target_etf,
                        'name': self.etf_config[target_etf]['name'],
                        'shares': shares,
                        'price': current_price,
                        'amount': buy_value
                    })
                    
                    name = self.etf_config[target_etf]['name']
                    print(f"  ✓ 买入 {name}({target_etf}): {shares:.0f}股, 价值{buy_value:.2f}元")
    
    def run_backtest(self, start_date=None, end_date=None):
        """
        运行回测
        """
        print("\n" + "="*60)
        print("开始回测...")
        print("="*60)
        
        trading_dates = self.get_trading_dates()
        
        if not trading_dates:
            print("错误：没有足够的交易数据")
            return
        
        # 设置回测时间范围
        if start_date:
            start_date = pd.to_datetime(start_date)
            trading_dates = [d for d in trading_dates if d >= start_date]
        
        if end_date:
            end_date = pd.to_datetime(end_date)
            trading_dates = [d for d in trading_dates if d <= end_date]
        
        print(f"回测期间: {trading_dates[0].strftime('%Y-%m-%d')} 到 {trading_dates[-1].strftime('%Y-%m-%d')}")
        print(f"交易日数: {len(trading_dates)}天")
        print(f"初始资金: {self.initial_capital:,.0f}元")
        
        # 执行回测 - 每天运行交易函数（模拟聚宽的run_daily）
        for i, date in enumerate(trading_dates):
            # 每天都运行trade函数，但只在需要时才实际交易
            self.trade(date)
            
            # 更新组合价值
            portfolio_value = self.update_portfolio_value(date)
            
            # 获取当前持仓ETF名称
            current_position = "现金"
            for etf_code, position in self.portfolio['positions'].items():
                if position['shares'] > 0:
                    current_position = f"{self.etf_config[etf_code]['name']}({etf_code})"
                    break
            
            # 记录历史
            history_record = {
                'date': date,
                'total_value': portfolio_value,
                'cash': self.portfolio['cash'],
                'current_position': current_position,
                'positions': dict(self.portfolio['positions'])
            }
            
            # 添加ETF评分数据
            if hasattr(self, 'daily_scores'):
                for etf_code, score in self.daily_scores.items():
                    etf_name = self.etf_config[etf_code]['name']
                    history_record[f'{etf_name}_评分'] = round(score, 6)
                    detail = self.daily_score_details.get(etf_code, {}) if hasattr(self, 'daily_score_details') else {}
                    history_record[f'{etf_name}_年化收益率'] = round(detail.get('annualized_returns', np.nan), 6)
                    history_record[f'{etf_name}_R平方'] = round(detail.get('r_squared', np.nan), 6)
                    history_record[f'{etf_name}_斜率'] = round(detail.get('slope', np.nan), 6)
                    history_record[f'{etf_name}_起始净值'] = round(detail.get('start_price', np.nan), 6)
                    history_record[f'{etf_name}_结束净值'] = round(detail.get('end_price', np.nan), 6)

            self.portfolio['history'].append(history_record)
        
        # 输出回测结果
        self.print_backtest_results()
    
    def print_backtest_results(self):
        """
        输出回测结果
        """
        if not self.portfolio['history']:
            print("没有回测数据")
            return
        
        history_df = pd.DataFrame(self.portfolio['history'])
        
        initial_value = history_df['total_value'].iloc[0]
        final_value = history_df['total_value'].iloc[-1]
        total_return = (final_value / initial_value - 1) * 100
        
        start_date = history_df['date'].iloc[0]
        end_date = history_df['date'].iloc[-1]
        days = (end_date - start_date).days
        years = days / 365.25
        
        annual_return = (final_value / initial_value) ** (1/years) - 1 if years > 0 else 0
        
        print("\n" + "="*60)
        print("回测结果")
        print("="*60)
        print(f"初始资金: {initial_value:,.0f}元")
        print(f"最终资金: {final_value:,.0f}元")
        print(f"总收益率: {total_return:.2f}%")
        print(f"年化收益率: {annual_return*100:.2f}%")
        print(f"回测天数: {days}天 ({years:.2f}年)")
        
        # 最大回撤
        rolling_max = history_df['total_value'].expanding().max()
        drawdown = (history_df['total_value'] / rolling_max - 1) * 100
        max_drawdown = drawdown.min()
        print(f"最大回撤: {max_drawdown:.2f}%")
        
        # 最终持仓
        print("\n最终持仓:")
        final_positions = self.portfolio['positions']
        for etf_code, position in final_positions.items():
            if position['shares'] > 0:
                name = self.etf_config[etf_code]['name']
                print(f"  {name}({etf_code}): {position['shares']:.0f}股, 价值{position['value']:.2f}元")
        
        print(f"  现金: {self.portfolio['cash']:.2f}元")
        
        # 保存详细结果到CSV - 只保留用户需要的列，使用中文列名
        export_columns = {
            'date': '日期',
            'total_value': '总市值', 
            'cash': '现金',
            'current_position': '当前持仓'
        }
        
        # 添加ETF评分列
        for etf_code, config in self.etf_config.items():
            etf_name = config['name']
            detail_columns = {
                f'{etf_name}_评分': f'{etf_name}评分',
                f'{etf_name}_年化收益率': f'{etf_name}年化收益率',
                f'{etf_name}_R平方': f'{etf_name}R平方',
                f'{etf_name}_斜率': f'{etf_name}斜率',
                f'{etf_name}_起始净值': f'{etf_name}起始净值',
                f'{etf_name}_结束净值': f'{etf_name}结束净值'
            }

            for col_key, col_name in detail_columns.items():
                if col_key in history_df.columns:
                    export_columns[col_key] = col_name
        
        # 只导出需要的列并重命名
        export_df = history_df[list(export_columns.keys())].copy()
        export_df.rename(columns=export_columns, inplace=True)
        
        # 格式化数据
        export_df['日期'] = history_df['date'].dt.strftime('%Y-%m-%d')
        export_df['总市值'] = export_df['总市值'].round(2)
        export_df['现金'] = export_df['现金'].round(2)

        for col in export_df.columns:
            if col.endswith(('评分', '年化收益率', 'R平方', '斜率')):
                export_df[col] = pd.to_numeric(export_df[col], errors='coerce').round(6)
            elif col.endswith('净值'):
                export_df[col] = pd.to_numeric(export_df[col], errors='coerce').round(4)

        export_df.to_csv('/home/suwei/回测策略/analysis_results/backtest_results.csv', index=False, encoding='utf-8-sig')
        print(f"\n详细结果已保存到: /home/suwei/回测策略/analysis_results/backtest_results.csv")
        
        # 保存交易记录到CSV
        if self.portfolio['trades']:
            trades_df = pd.DataFrame(self.portfolio['trades'])
            trades_df.to_csv('/home/suwei/回测策略/analysis_results/trades_record.csv', index=False)
            print(f"交易记录已保存到: /home/suwei/回测策略/analysis_results/trades_record.csv")
            print(f"共记录 {len(trades_df)} 笔交易")

def main():
    """
    主函数
    """
    # 创建策略实例
    strategy = LocalETFStrategy()
    
    # 运行回测
    strategy.run_backtest(start_date='2024-01-01', end_date='2025-09-19')

if __name__ == "__main__":
    main()
