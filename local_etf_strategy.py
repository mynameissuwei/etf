"""
本地化ETF轮动策略
基于动量因子的ETF轮动策略，使用本地数据实现
"""

import numpy as np
import pandas as pd
import math
from typing import List, Dict, Tuple
from datetime import datetime, timedelta


class ETFMomentumStrategy:
    """ETF动量轮动策略类"""
    
    def __init__(self, 
                 initial_capital: float = 100000,
                 momentum_days: int = 25,
                 commission_rate: float = 0.0002,
                 min_commission: float = 5.0):
        """
        初始化策略参数
        
        Args:
            initial_capital: 初始资金
            momentum_days: 动量计算天数
            commission_rate: 手续费率
            min_commission: 最小手续费
        """
        self.initial_capital = initial_capital
        self.momentum_days = momentum_days
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        
        # ETF池
        self.etf_pool = [
            '518880.XSHG',  # 黄金ETF
            '159509.XSHE',  # 纳指科技ETF
            '513500.XSHG',  # 标普500ETF
        ]
        
        # 初始化回测状态
        self.cash = initial_capital
        self.positions = {}  # {symbol: shares}
        self.portfolio_value = initial_capital
        self.trade_records = []
        self.daily_returns = []
        
    def load_data(self, data_dict: Dict[str, pd.DataFrame]) -> None:
        """
        加载ETF数据
        
        Args:
            data_dict: {symbol: DataFrame} 格式的数据字典
                      DataFrame需包含['date', 'close']列
        """
        self.data_dict = {}
        for symbol, df in data_dict.items():
            if symbol in self.etf_pool:
                df = df.copy()
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                self.data_dict[symbol] = df
                
        # 获取交易日期范围
        all_dates = set()
        for df in self.data_dict.values():
            all_dates.update(df.index)
        self.trading_dates = sorted(list(all_dates))
        
    def calculate_momentum_score(self, symbol: str, end_date: pd.Timestamp) -> float:
        """
        计算动量得分 (年化收益率 × R²)
        
        Args:
            symbol: ETF代码
            end_date: 计算截止日期
            
        Returns:
            动量得分
        """
        if symbol not in self.data_dict:
            return -np.inf
            
        df = self.data_dict[symbol]
        
        # 获取过去momentum_days天的数据
        end_idx = df.index.get_loc(end_date) if end_date in df.index else -1
        if end_idx < self.momentum_days - 1:
            return -np.inf
            
        start_idx = max(0, end_idx - self.momentum_days + 1)
        price_data = df.iloc[start_idx:end_idx + 1]['close'].values
        
        if len(price_data) < self.momentum_days:
            return -np.inf
            
        # 计算对数收益率
        y = np.log(price_data)
        n = len(y)
        x = np.arange(n)
        
        # 线性增加权重
        weights = np.linspace(1, 2, n)
        
        # 加权线性回归
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        
        # 年化收益率
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        
        # 计算R²
        residuals = y - (slope * x + intercept)
        weighted_residuals = weights * residuals**2
        weighted_total = weights * (y - np.mean(y))**2
        r_squared = 1 - (np.sum(weighted_residuals) / np.sum(weighted_total))
        
        # 综合得分
        score = annualized_returns * r_squared
        return score
        
    def get_etf_ranking(self, date: pd.Timestamp) -> List[str]:
        """
        获取ETF动量排名
        
        Args:
            date: 计算日期
            
        Returns:
            按动量得分降序排列的ETF列表
        """
        scores = []
        valid_etfs = []
        
        for etf in self.etf_pool:
            score = self.calculate_momentum_score(etf, date)
            if score != -np.inf:
                scores.append(score)
                valid_etfs.append(etf)
                
        if not scores:
            return []
            
        # 按得分排序
        ranked_indices = np.argsort(scores)[::-1]
        return [valid_etfs[i] for i in ranked_indices]
        
    def calculate_commission(self, trade_value: float) -> float:
        """计算交易手续费"""
        commission = abs(trade_value) * self.commission_rate
        return max(commission, self.min_commission)
        
    def get_current_price(self, symbol: str, date: pd.Timestamp) -> float:
        """获取当前价格"""
        if symbol not in self.data_dict or date not in self.data_dict[symbol].index:
            return 0.0
        return self.data_dict[symbol].loc[date, 'close']
        
    def execute_trade(self, date: pd.Timestamp) -> None:
        """
        执行交易逻辑
        
        Args:
            date: 交易日期
        """
        # 获取动量排名最高的ETF
        ranked_etfs = self.get_etf_ranking(date)
        if not ranked_etfs:
            return
            
        target_etf = ranked_etfs[0]  # 选择动量最高的ETF
        
        # 计算当前持仓价值
        current_positions = list(self.positions.keys())
        total_position_value = 0
        
        for symbol in current_positions:
            if self.positions[symbol] > 0:
                price = self.get_current_price(symbol, date)
                position_value = self.positions[symbol] * price
                total_position_value += position_value
                
        # 卖出非目标ETF
        for symbol in current_positions:
            if symbol != target_etf and self.positions[symbol] > 0:
                price = self.get_current_price(symbol, date)
                if price > 0:
                    trade_value = self.positions[symbol] * price
                    commission = self.calculate_commission(trade_value)
                    
                    self.cash += trade_value - commission
                    self.trade_records.append({
                        'date': date,
                        'symbol': symbol,
                        'action': 'sell',
                        'shares': self.positions[symbol],
                        'price': price,
                        'value': trade_value,
                        'commission': commission
                    })
                    self.positions[symbol] = 0
                    
        # 买入目标ETF（如果还未持有）
        if target_etf not in self.positions or self.positions[target_etf] == 0:
            target_price = self.get_current_price(target_etf, date)
            if target_price > 0 and self.cash > self.min_commission:
                # 使用所有现金买入
                available_cash = self.cash - self.min_commission
                commission = self.calculate_commission(available_cash)
                net_investment = available_cash - commission
                
                if net_investment > 0:
                    shares = int(net_investment / target_price)
                    if shares > 0:
                        actual_cost = shares * target_price + commission
                        
                        self.cash -= actual_cost
                        self.positions[target_etf] = shares
                        
                        self.trade_records.append({
                            'date': date,
                            'symbol': target_etf,
                            'action': 'buy',
                            'shares': shares,
                            'price': target_price,
                            'value': shares * target_price,
                            'commission': commission
                        })
                        
    def update_portfolio_value(self, date: pd.Timestamp) -> None:
        """更新组合价值"""
        total_value = self.cash
        
        for symbol, shares in self.positions.items():
            if shares > 0:
                price = self.get_current_price(symbol, date)
                total_value += shares * price
                
        self.portfolio_value = total_value
        
    def run_backtest(self, start_date: str = None, end_date: str = None) -> Dict:
        """
        运行回测
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            回测结果字典
        """
        if start_date:
            start_date = pd.to_datetime(start_date)
        else:
            start_date = self.trading_dates[self.momentum_days]
            
        if end_date:
            end_date = pd.to_datetime(end_date)
        else:
            end_date = self.trading_dates[-1]
            
        # 筛选交易日期
        backtest_dates = [d for d in self.trading_dates if start_date <= d <= end_date]
        
        portfolio_values = []
        dates = []
        
        for date in backtest_dates:
            # 执行交易
            self.execute_trade(date)
            
            # 更新组合价值
            self.update_portfolio_value(date)
            
            portfolio_values.append(self.portfolio_value)
            dates.append(date)
            
        # 计算收益率
        portfolio_df = pd.DataFrame({
            'date': dates,
            'portfolio_value': portfolio_values
        })
        portfolio_df['daily_return'] = portfolio_df['portfolio_value'].pct_change()
        
        # 计算性能指标
        total_return = (portfolio_values[-1] - self.initial_capital) / self.initial_capital
        annual_return = (portfolio_values[-1] / self.initial_capital) ** (252 / len(portfolio_values)) - 1
        
        daily_returns = portfolio_df['daily_return'].dropna()
        sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0
        max_drawdown = self._calculate_max_drawdown(portfolio_values)
        
        return {
            'portfolio_df': portfolio_df,
            'trade_records': pd.DataFrame(self.trade_records),
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'final_value': portfolio_values[-1]
        }
        
    def _calculate_max_drawdown(self, portfolio_values: List[float]) -> float:
        """计算最大回撤"""
        peak = portfolio_values[0]
        max_dd = 0
        
        for value in portfolio_values:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
                
        return max_dd


def create_sample_data() -> Dict[str, pd.DataFrame]:
    """
    创建示例数据用于测试
    实际使用时请替换为真实的ETF数据
    """
    import numpy as np
    
    # 生成示例数据
    dates = pd.date_range('2023-01-01', '2024-12-31', freq='D')
    
    sample_data = {}
    
    etf_params = {
        '518880.XSHG': {'drift': 0.0002, 'volatility': 0.015},  # 黄金ETF
        '159509.XSHE': {'drift': 0.0008, 'volatility': 0.025},  # 纳指科技ETF
        '513500.XSHG': {'drift': 0.0005, 'volatility': 0.018},  # 标普500ETF
    }
    
    for symbol, params in etf_params.items():
        np.random.seed(42 + hash(symbol) % 1000)  # 确保可重现的随机数
        
        returns = np.random.normal(params['drift'], params['volatility'], len(dates))
        prices = 100 * np.exp(np.cumsum(returns))  # 从100开始的价格序列
        
        df = pd.DataFrame({
            'date': dates,
            'close': prices
        })
        
        sample_data[symbol] = df
        
    return sample_data


# 使用示例
if __name__ == "__main__":
    # 创建策略实例
    strategy = ETFMomentumStrategy(
        initial_capital=100000,
        momentum_days=25,
        commission_rate=0.0002,
        min_commission=5.0
    )
    
    # 加载数据（这里使用示例数据，实际使用时请替换为真实数据）
    data_dict = create_sample_data()
    strategy.load_data(data_dict)
    
    # 运行回测
    results = strategy.run_backtest('2023-02-01', '2024-11-30')
    
    # 打印结果
    print("=== 回测结果 ===")
    print(f"总收益率: {results['total_return']:.2%}")
    print(f"年化收益率: {results['annual_return']:.2%}")
    print(f"夏普比率: {results['sharpe_ratio']:.2f}")
    print(f"最大回撤: {results['max_drawdown']:.2%}")
    print(f"最终资产: {results['final_value']:,.2f}")
    print(f"交易次数: {len(results['trade_records'])}")