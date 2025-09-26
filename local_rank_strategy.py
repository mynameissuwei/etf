#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地化核心资产轮动策略
基于聚宽strategy/rank.py逻辑改写，使用本地数据源复现动量打分与轮动

"""

import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class ScoreDetail:
    combined_score: float
    long_term_raw: float
    long_term_sigmoid: float
    short_term_raw: float
    short_term_sigmoid: float
    annualized_returns: float
    r_squared: float
    long_term_slope: float
    short_term_slope: float
    long_start_price: float
    long_end_price: float
    short_start_price: float
    short_end_price: float


class LocalRankStrategy:
    def __init__(
        self,
        data_dir: str = '/home/suwei/回测策略/data',
        output_dir: str = '/home/suwei/回测策略/analysis_results_rank',
    ) -> None:
        """
        初始化策略
        """
        self.data_dir = data_dir
        self.output_dir = output_dir

        # ETF池配置 - 对应聚宽rank.py中的ETF
        self.etf_config = {
            # '161116': {'name': '易方达黄金ETF', 'file': '161116_data.csv'},
            '159509': {'name': '纳指科技ETF', 'file': '159509_data.csv'},
            '518880': {'name': '易方达黄金ETF', 'file': '518880_data.csv'},
        }

        # 策略参数，保持与聚宽脚本一致
        self.m_days = 25
        self.m_days_short = 3
        self.target_num = 1
        self.initial_capital = 100000

        # 组合信息
        self.etf_data: Dict[str, pd.DataFrame] = {}
        self.portfolio = {
            'cash': self.initial_capital,
            'positions': {code: {'shares': 0.0, 'value': 0.0} for code in self.etf_config},
            'total_value': self.initial_capital,
            'history': [],
            'trades': [],
        }

        # 每日排名及打分明细
        self.daily_scores: Dict[str, float] = {}
        self.daily_score_details: Dict[str, ScoreDetail] = {}

        os.makedirs(self.output_dir, exist_ok=True)
        self.load_data()

    def load_data(self) -> None:
        """
        加载ETF数据
        """
        print('正在加载ETF数据...')

        for etf_code, config in self.etf_config.items():
            file_path = os.path.join(self.data_dir, config['file'])

            try:
                df = pd.read_csv(file_path)

                # 标准化列名
                if 'net_value' in df.columns:
                    df = df.rename(columns={'net_value': 'close'})
                elif '单位净值' in df.columns:
                    df = df.rename(columns={'单位净值': 'close', '净值日期': 'date'})

                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').set_index('date')
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df = df.dropna(subset=['close'])

                self.etf_data[etf_code] = df
                print(
                    f"✓ {config['name']}({etf_code}): {len(df)} 条数据, 时间范围: "
                    f"{df.index[0].date()} 到 {df.index[-1].date()}"
                )

            except Exception as exc:
                print(f"✗ 加载 {config['name']}({etf_code}) 数据失败: {exc}")

        print(f"数据加载完成，共 {len(self.etf_data)} 只ETF")

    @staticmethod
    def sigmoid(x: float) -> float:
        return 1.0 / (1.0 + math.exp(-float(x)))

    def get_trading_dates(self) -> List[pd.Timestamp]:
        if not self.etf_data:
            return []

        date_sets = [set(df.index) for df in self.etf_data.values()]
        common_dates = sorted(set.intersection(*date_sets))
        if len(common_dates) > self.m_days:
            return common_dates[self.m_days:]

        print(f"警告：共同交易日期不足，需要至少 {self.m_days} 天历史数据")
        return []

    def get_rank(
        self, date: pd.Timestamp
    ) -> Tuple[List[str], Dict[str, float], Dict[str, ScoreDetail]]:
        scores: Dict[str, float] = {}
        details: Dict[str, ScoreDetail] = {}

        for etf_code, df in self.etf_data.items():
            past_mask = df.index < date
            long_prices = df.loc[past_mask, 'close'].tail(self.m_days)
            short_prices = df.loc[past_mask, 'close'].tail(self.m_days_short)

            if len(long_prices) < self.m_days or len(short_prices) < self.m_days_short:
                scores[etf_code] = -999.0
                details[etf_code] = ScoreDetail(
                    combined_score=-999.0,
                    long_term_raw=float('nan'),
                    long_term_sigmoid=float('nan'),
                    short_term_raw=float('nan'),
                    short_term_sigmoid=float('nan'),
                    annualized_returns=float('nan'),
                    r_squared=float('nan'),
                    long_term_slope=float('nan'),
                    short_term_slope=float('nan'),
                    long_start_price=float('nan'),
                    long_end_price=float('nan'),
                    short_start_price=float('nan'),
                    short_end_price=float('nan'),
                )
                continue

            # 长周期动量得分
            y_long = np.log(long_prices.values)
            x_long = np.arange(len(y_long))
            slope_long, intercept_long = np.polyfit(x_long, y_long, 1)
            annualized_returns = math.pow(math.exp(slope_long), 250) - 1

            residuals_long = y_long - (slope_long * x_long + intercept_long)
            denominator = (len(y_long) - 1) * np.var(y_long, ddof=1)
            r_squared = 1 - (np.sum(residuals_long**2) / denominator) if denominator != 0 else 0.0
            long_raw = annualized_returns * r_squared
            long_sigmoid = self.sigmoid(long_raw)

            # 短周期趋势过滤
            y_short = np.log(short_prices.values)
            x_short = np.arange(len(y_short))
            slope_short, _ = np.polyfit(x_short, y_short, 1)
            short_raw = slope_short
            short_sigmoid = self.sigmoid(short_raw)

            combined_score = long_sigmoid * short_sigmoid
            if long_raw < 0 and short_raw < 0:
                combined_score *= -1

            scores[etf_code] = combined_score
            details[etf_code] = ScoreDetail(
                combined_score=combined_score,
                long_term_raw=long_raw,
                long_term_sigmoid=long_sigmoid,
                short_term_raw=short_raw,
                short_term_sigmoid=short_sigmoid,
                annualized_returns=annualized_returns,
                r_squared=r_squared,
                long_term_slope=slope_long,
                short_term_slope=slope_short,
                long_start_price=float(long_prices.iloc[0]),
                long_end_price=float(long_prices.iloc[-1]),
                short_start_price=float(short_prices.iloc[0]),
                short_end_price=float(short_prices.iloc[-1]),
            )

        ranked_etfs = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        ranked_list = [etf_code for etf_code, _ in ranked_etfs]
        return ranked_list, scores, details

    def get_current_price(self, etf_code: str, date: pd.Timestamp) -> float:
        df = self.etf_data[etf_code]
        if date in df.index:
            return float(df.loc[date, 'close'])

        available_dates = df.index[df.index <= date]
        if len(available_dates) == 0:
            return float('nan')
        latest_date = available_dates[-1]
        return float(df.loc[latest_date, 'close'])

    def update_portfolio_value(self, date: pd.Timestamp) -> float:
        total_value = self.portfolio['cash']
        for etf_code, position in self.portfolio['positions'].items():
            if position['shares'] <= 0:
                continue

            current_price = self.get_current_price(etf_code, date)
            if not np.isnan(current_price):
                position_value = position['shares'] * current_price
                position['value'] = position_value
                total_value += position_value

        self.portfolio['total_value'] = total_value
        return total_value

    def trade(self, date: pd.Timestamp) -> None:
        ranked_etfs, scores, details = self.get_rank(date)

        if not ranked_etfs:
            print(f"{date.strftime('%Y-%m-%d')} 无可用ETF评分，跳过交易")
            return

        self.daily_scores = scores
        self.daily_score_details = details

        target_etf = ranked_etfs[0]
        current_holdings = [
            code for code, pos in self.portfolio['positions'].items() if pos['shares'] > 0
        ]

        need_trade = False
        if not current_holdings:
            need_trade = True
        elif target_etf not in current_holdings:
            need_trade = True

        if not need_trade:
            return

        print(f"\n{date.strftime('%Y-%m-%d')} 交易信号:")
        for etf_code, score in sorted(scores.items(), key=lambda item: item[1], reverse=True):
            etf_name = self.etf_config[etf_code]['name']
            print(f"  {etf_name}({etf_code}) 综合得分: {score:.6f}")

        # 卖出非目标持仓
        for etf_code in current_holdings:
            if etf_code == target_etf:
                continue

            current_price = self.get_current_price(etf_code, date)
            shares = self.portfolio['positions'][etf_code]['shares']
            if shares <= 0 or np.isnan(current_price):
                continue

            sell_value = shares * current_price
            self.portfolio['cash'] += sell_value
            self.portfolio['positions'][etf_code]['shares'] = 0.0
            self.portfolio['positions'][etf_code]['value'] = 0.0

            self.portfolio['trades'].append(
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'type': 'sell',
                    'code': etf_code,
                    'name': self.etf_config[etf_code]['name'],
                    'shares': shares,
                    'price': current_price,
                    'amount': sell_value,
                }
            )

            etf_name = self.etf_config[etf_code]['name']
            print(f"  ✗ 卖出 {etf_name}({etf_code}): {shares:.0f}股, 价值{sell_value:.2f}元")

        # 买入目标ETF
        if self.portfolio['cash'] > 0:
            current_price = self.get_current_price(target_etf, date)
            if np.isnan(current_price) or current_price <= 0:
                return

            buy_value = self.portfolio['cash']
            shares = buy_value / current_price
            self.portfolio['cash'] = 0.0
            self.portfolio['positions'][target_etf]['shares'] = shares
            self.portfolio['positions'][target_etf]['value'] = buy_value

            self.portfolio['trades'].append(
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'type': 'buy',
                    'code': target_etf,
                    'name': self.etf_config[target_etf]['name'],
                    'shares': shares,
                    'price': current_price,
                    'amount': buy_value,
                }
            )

            etf_name = self.etf_config[target_etf]['name']
            print(f"  ✓ 买入 {etf_name}({target_etf}): {shares:.0f}股, 价值{buy_value:.2f}元")

    def run_backtest(self, start_date: str | None = None, end_date: str | None = None) -> None:
        print('\n' + '=' * 60)
        print('开始回测...')
        print('=' * 60)

        trading_dates = self.get_trading_dates()
        if not trading_dates:
            print('错误：没有足够的交易数据')
            return

        if start_date:
            start_ts = pd.to_datetime(start_date)
            trading_dates = [d for d in trading_dates if d >= start_ts]
        if end_date:
            end_ts = pd.to_datetime(end_date)
            trading_dates = [d for d in trading_dates if d <= end_ts]

        if not trading_dates:
            print('错误：筛选后的交易日期为空')
            return

        print(
            f"回测期间: {trading_dates[0].strftime('%Y-%m-%d')} 到 "
            f"{trading_dates[-1].strftime('%Y-%m-%d')}"
        )
        print(f"交易日数: {len(trading_dates)} 天")
        print(f"初始资金: {self.initial_capital:,.0f} 元")

        for date in trading_dates:
            self.trade(date)
            portfolio_value = self.update_portfolio_value(date)

            current_position_name = '现金'
            for etf_code, position in self.portfolio['positions'].items():
                if position['shares'] > 0:
                    current_position_name = f"{self.etf_config[etf_code]['name']}({etf_code})"
                    break

            history_record = {
                'date': date,
                'total_value': portfolio_value,
                'cash': self.portfolio['cash'],
                'current_position': current_position_name,
            }

            for etf_code, detail in self.daily_score_details.items():
                etf_name = self.etf_config[etf_code]['name']
                history_record[f'{etf_name}_综合得分'] = detail.combined_score
                history_record[f'{etf_name}_长周期原始得分'] = detail.long_term_raw
                history_record[f'{etf_name}_长周期Sigmoid'] = detail.long_term_sigmoid
                history_record[f'{etf_name}_短周期原始得分'] = detail.short_term_raw
                history_record[f'{etf_name}_短周期Sigmoid'] = detail.short_term_sigmoid
                history_record[f'{etf_name}_年化收益率'] = detail.annualized_returns
                history_record[f'{etf_name}_R平方'] = detail.r_squared
                history_record[f'{etf_name}_长周期斜率'] = detail.long_term_slope
                history_record[f'{etf_name}_短周期斜率'] = detail.short_term_slope
                history_record[f'{etf_name}_长周期起始净值'] = detail.long_start_price
                history_record[f'{etf_name}_长周期结束净值'] = detail.long_end_price
                history_record[f'{etf_name}_短周期起始净值'] = detail.short_start_price
                history_record[f'{etf_name}_短周期结束净值'] = detail.short_end_price

            self.portfolio['history'].append(history_record)

        self.print_backtest_results()

    def print_backtest_results(self) -> None:
        if not self.portfolio['history']:
            print('没有回测数据')
            return

        history_df = pd.DataFrame(self.portfolio['history'])
        initial_value = history_df['total_value'].iloc[0]
        final_value = history_df['total_value'].iloc[-1]
        total_return = (final_value / initial_value - 1) * 100

        start_date = history_df['date'].iloc[0]
        end_date = history_df['date'].iloc[-1]
        days = (end_date - start_date).days
        years = days / 365.25 if days > 0 else 0
        annual_return = (final_value / initial_value) ** (1 / years) - 1 if years > 0 else 0

        rolling_max = history_df['total_value'].expanding().max()
        drawdown = (history_df['total_value'] / rolling_max - 1) * 100
        max_drawdown = drawdown.min()

        print('\n' + '=' * 60)
        print('回测结果')
        print('=' * 60)
        print(f"初始资金: {initial_value:,.0f} 元")
        print(f"最终资金: {final_value:,.0f} 元")
        print(f"总收益率: {total_return:.2f}%")
        print(f"年化收益率: {annual_return * 100:.2f}%")
        print(f"最大回撤: {max_drawdown:.2f}%")
        print(f"回测天数: {days} 天 ({years:.2f} 年)")

        print('\n最终持仓:')
        for etf_code, position in self.portfolio['positions'].items():
            if position['shares'] <= 0:
                continue
            etf_name = self.etf_config[etf_code]['name']
            print(f"  {etf_name}({etf_code}): {position['shares']:.0f}股, 价值{position['value']:.2f}元")
        print(f"  现金: {self.portfolio['cash']:.2f}元")

        base_pairs = [
            ('date', '日期'),
            ('total_value', '总市值'),
            ('cash', '现金'),
            ('current_position', '当前持仓'),
        ]
        score_pairs: List[Tuple[str, str]] = []
        detail_pairs: List[Tuple[str, str]] = []
        for etf_code, config in self.etf_config.items():
            etf_name = config['name']
            score_key = f'{etf_name}_综合得分'
            if score_key in history_df.columns:
                score_pairs.append((score_key, f'{etf_name}综合得分'))

            column_mapping = [
                (f'{etf_name}_长周期原始得分', f'{etf_name}长周期原始得分'),
                (f'{etf_name}_长周期Sigmoid', f'{etf_name}长周期Sigmoid'),
                (f'{etf_name}_短周期原始得分', f'{etf_name}短周期原始得分'),
                (f'{etf_name}_短周期Sigmoid', f'{etf_name}短周期Sigmoid'),
                (f'{etf_name}_年化收益率', f'{etf_name}年化收益率'),
                (f'{etf_name}_R平方', f'{etf_name}R平方'),
                (f'{etf_name}_长周期斜率', f'{etf_name}长周期斜率'),
                (f'{etf_name}_短周期斜率', f'{etf_name}短周期斜率'),
                (f'{etf_name}_长周期起始净值', f'{etf_name}长周期起始净值'),
                (f'{etf_name}_长周期结束净值', f'{etf_name}长周期结束净值'),
                (f'{etf_name}_短周期起始净值', f'{etf_name}短周期起始净值'),
                (f'{etf_name}_短周期结束净值', f'{etf_name}短周期结束净值'),
            ]

            for key, value in column_mapping:
                if key in history_df.columns:
                    detail_pairs.append((key, value))

        ordered_pairs = base_pairs + score_pairs + detail_pairs
        selected_columns = [key for key, _ in ordered_pairs if key in history_df.columns]
        rename_map = {key: value for key, value in ordered_pairs if key in history_df.columns}

        export_df = history_df[selected_columns].copy()
        export_df.rename(columns=rename_map, inplace=True)
        export_df['日期'] = pd.to_datetime(export_df['日期']).dt.strftime('%Y-%m-%d')
        export_df['总市值'] = export_df['总市值'].round(2)
        export_df['现金'] = export_df['现金'].round(2)

        metric_suffixes = ('综合得分', '长周期原始得分', '长周期Sigmoid', '短周期原始得分', '短周期Sigmoid')
        for column in export_df.columns:
            if column.endswith(metric_suffixes):
                export_df[column] = pd.to_numeric(export_df[column], errors='coerce').round(6)
            elif column.endswith(('年化收益率', 'R平方', '长周期斜率', '短周期斜率')):
                export_df[column] = pd.to_numeric(export_df[column], errors='coerce').round(6)
            elif column.endswith('净值'):
                export_df[column] = pd.to_numeric(export_df[column], errors='coerce').round(4)

        results_path = os.path.join(self.output_dir, 'rank_backtest_results.csv')
        export_df.to_csv(results_path, index=False, encoding='utf-8-sig')
        print(f"\n详细结果已保存到: {results_path}")

        if self.portfolio['trades']:
            trades_df = pd.DataFrame(self.portfolio['trades'])
            trades_path = os.path.join(self.output_dir, 'rank_trades_record.csv')
            trades_df.to_csv(trades_path, index=False, encoding='utf-8-sig')
            print(f"交易记录已保存到: {trades_path}")
            print(f"共记录 {len(trades_df)} 笔交易")


def main() -> None:
    strategy = LocalRankStrategy()
    today = datetime.now().strftime('%Y-%m-%d')
    strategy.run_backtest(start_date='2024-01-01', end_date=today)


if __name__ == '__main__':
    main()
