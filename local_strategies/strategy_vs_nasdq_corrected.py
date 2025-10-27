#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""基于 rank_trades_record.csv 的策略 vs 纯持有纳指分析。"""

from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import warnings

warnings.filterwarnings('ignore')

ROOT_DIR = Path(__file__).resolve().parent.parent
RANK_RESULTS_DIR = ROOT_DIR / 'analysis_results_rank'
DATA_DIR = ROOT_DIR / 'data'

TRADES_FILE = RANK_RESULTS_DIR / 'rank_trades_record.csv'
OUTPUT_FILE = RANK_RESULTS_DIR / 'rank_holding_periods_analysis.csv'

ETF_CONFIG: Dict[str, Dict[str, object]] = {
    '159509': {'name': '纳指科技ETF', 'file': DATA_DIR / '159509_data.csv'},
    '518880': {'name': '易方达黄金ETF', 'file': DATA_DIR / '518880_data.csv'},
}

BASELINE_CODE = '159509'


def _load_price_series(csv_path: Path) -> pd.DataFrame:
    """Load a price series CSV and normalize columns."""

    df = pd.read_csv(csv_path)
    if 'close' not in df.columns:
        rename_map = {
            'net_value': 'close',
            '单位净值': 'close',
            '净值日期': 'date',
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    return df.dropna(subset=['close'])


def _load_etf_data(codes: set[str]) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    for code in codes:
        config = ETF_CONFIG.get(code)
        if not config:
            print(f"⚠️ 未找到代码 {code} 的数据配置，已跳过")
            continue

        csv_path = config['file']
        if not csv_path.exists():
            print(f"⚠️ 数据文件缺失: {csv_path}")
            continue

        data[code] = _load_price_series(csv_path)
    return data


def _price_on_or_before(df: Optional[pd.DataFrame], target_date: pd.Timestamp) -> Optional[float]:
    if df is None or df.empty:
        return None
    subset = df.loc[:target_date]
    if subset.empty:
        return None
    return float(subset['close'].iloc[-1])


def _compute_return(df: Optional[pd.DataFrame], start_date: pd.Timestamp, end_date: pd.Timestamp) -> Optional[float]:
    buy_price = _price_on_or_before(df, start_date)
    sell_price = _price_on_or_before(df, end_date)
    if buy_price is None or sell_price is None:
        return None
    return (sell_price / buy_price - 1.0) * 100


def _make_period_record(
    index: int,
    position: dict,
    sell_date: pd.Timestamp,
    sell_price: Optional[float],
    etf_data: Dict[str, pd.DataFrame],
    baseline_data: Optional[pd.DataFrame],
) -> Optional[dict]:
    code = position['code']
    held_data = etf_data.get(code)
    buy_date = position['date']

    buy_price = position.get('price')
    if pd.isna(buy_price):
        buy_price = _price_on_or_before(held_data, buy_date)

    exit_price = sell_price
    if exit_price is None or pd.isna(exit_price):
        exit_price = _price_on_or_before(held_data, sell_date)

    if buy_price is None or exit_price is None:
        print(f"⚠️ 无法获取 {code} 在 {buy_date:%Y-%m-%d} 至 {sell_date:%Y-%m-%d} 的价格，已跳过")
        return None

    strategy_return = (exit_price / buy_price - 1.0) * 100
    baseline_return = _compute_return(baseline_data, buy_date, sell_date)

    if baseline_return is None:
        baseline_label = '数据不足'
        excess_return = float('nan')
    else:
        baseline_label = f"{baseline_return:.2f}%"
        excess_return = strategy_return - baseline_return

    name = position['name']
    record = {
        '期间': index,
        '开始日期': buy_date.strftime('%Y-%m-%d'),
        '结束日期': sell_date.strftime('%Y-%m-%d'),
        '持有标的': name,
        '策略收益率': f"{strategy_return:.2f}%",
        '纳指基准收益率': baseline_label,
        '超额收益': f"{excess_return:.2f}%" if pd.notna(excess_return) else '数据不足',
        '超额收益数值': excess_return,
        '是否跑赢纳指': pd.notna(excess_return) and excess_return > 0,
    }
    return record


def analyze_holding_periods() -> pd.DataFrame:
    """读取 rank_trades_record.csv，输出逐段超额收益分析。"""

    if not TRADES_FILE.exists():
        raise FileNotFoundError(f"未找到交易记录文件: {TRADES_FILE}")

    trades_df = pd.read_csv(TRADES_FILE, dtype={'code': str}, encoding='utf-8-sig')
    trades_df['date'] = pd.to_datetime(trades_df['date'])
    trades_df['code'] = trades_df['code'].astype(str)
    trades_df = trades_df.sort_values('date').reset_index(drop=True)

    if trades_df.empty:
        return pd.DataFrame()

    required_codes = set(trades_df['code'])
    required_codes.add(BASELINE_CODE)
    etf_data = _load_etf_data(required_codes)
    baseline_data = etf_data.get(BASELINE_CODE)

    holding_periods = []
    current_position: Optional[dict] = None
    period_index = 1

    for _, trade in trades_df.iterrows():
        trade_type = str(trade['type']).lower()
        code = trade['code']
        trade_date = trade['date']
        trade_price = trade.get('price')
        trade_name = trade.get('name') or ETF_CONFIG.get(code, {}).get('name', code)

        if trade_type == 'buy':
            current_position = {
                'code': code,
                'date': trade_date,
                'price': trade_price,
                'name': trade_name,
            }
        elif trade_type == 'sell' and current_position and current_position['code'] == code:
            record = _make_period_record(
                period_index,
                current_position,
                trade_date,
                trade_price,
                etf_data,
                baseline_data,
            )
            if record:
                holding_periods.append(record)
                period_index += 1
            current_position = None

    if current_position:
        max_dates = [df.index.max() for df in etf_data.values() if not df.empty]
        if max_dates:
            last_date = max(max_dates)
            record = _make_period_record(
                period_index,
                current_position,
                last_date,
                None,
                etf_data,
                baseline_data,
            )
            if record:
                holding_periods.append(record)

    return pd.DataFrame(holding_periods)


def main() -> pd.DataFrame:
    print("=== 策略 vs 纯持有纳指 - Rank 版本分析 ===\n")

    periods_df = analyze_holding_periods()
    if periods_df.empty:
        print("无交易记录，无法生成分析。")
        return periods_df

    print("各持仓期间表现对比:")
    for _, row in periods_df.iterrows():
        excess = row['超额收益数值']
        if pd.isna(excess):
            status = '数据不足'
        elif excess > 0:
            status = '✓ 跑赢纳指'
        elif excess < 0:
            status = '✗ 跑输纳指'
        else:
            status = '— 与纳指持平'

        print(f"期间{row['期间']}: {row['开始日期']} → {row['结束日期']}")
        print(f"  持有: {row['持有标的']}")
        print(f"  策略收益: {row['策略收益率']} vs 纳指基准: {row['纳指基准收益率']}")
        print(f"  超额收益: {row['超额收益']} {status}")
        print()

    total_periods = len(periods_df)
    valid_periods = periods_df[periods_df['超额收益数值'].notna()]
    total_valid = len(valid_periods)
    winning_periods = len(valid_periods[valid_periods['超额收益数值'] > 0])
    losing_periods = len(valid_periods[valid_periods['超额收益数值'] < 0])
    flat_periods = len(valid_periods[valid_periods['超额收益数值'] == 0])

    print("=== 统计总结 ===")
    print(f"总持仓期间: {total_periods}")
    print(f"可与纳指比较的期间: {total_valid}")

    if total_valid > 0:
        print(f"跑赢纳指期间: {winning_periods} ({winning_periods / total_valid * 100:.1f}%)")
        print(f"跑输纳指期间: {losing_periods} ({losing_periods / total_valid * 100:.1f}%)")
        print(f"与纳指持平期间: {flat_periods} ({flat_periods / total_valid * 100:.1f}%)")

        total_excess_return = valid_periods['超额收益数值'].sum()
        positive_excess = valid_periods[valid_periods['超额收益数值'] > 0]['超额收益数值'].sum()
        negative_excess = valid_periods[valid_periods['超额收益数值'] < 0]['超额收益数值'].sum()

        print(f"\n总超额收益: {total_excess_return:.2f}%")
        print(f"正贡献超额收益: {positive_excess:.2f}%")
        print(f"负贡献超额收益: {negative_excess:.2f}%")

        best_period = valid_periods.loc[valid_periods['超额收益数值'].idxmax()]
        worst_period = valid_periods.loc[valid_periods['超额收益数值'].idxmin()]

        print("\n最大超额收益贡献:")
        print(f"  期间{best_period['期间']}: {best_period['开始日期']} → {best_period['结束日期']}")
        print(f"  持有: {best_period['持有标的']}")
        print(f"  超额收益: {best_period['超额收益']}")

        print("\n最大超额收益拖累:")
        print(f"  期间{worst_period['期间']}: {worst_period['开始日期']} → {worst_period['结束日期']}")
        print(f"  持有: {worst_period['持有标的']}")
        print(f"  超额收益: {worst_period['超额收益']}")
    else:
        print("基准数据不足，无法统计跑赢/跑输情况。")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    periods_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n详细结果已保存到: {OUTPUT_FILE}")

    return periods_df


if __name__ == '__main__':
    main()
