import pandas as pd
import numpy as np
from datetime import datetime

# 读取CSV文件
file_path = '/home/suwei/回测策略/__pycache__/csv_analyze/fundhistoryd2f310d6c26b8b9189bc4a307d2bd0e6 (3).csv'
df = pd.read_csv(file_path, header=None, names=['date', 'nav', 'premium'])

# 转换日期格式
df['date'] = pd.to_datetime(df['date'])

# 按日期升序排列
df = df.sort_values('date').reset_index(drop=True)

# 创建Excel writer
excel_path = '/home/suwei/回测策略/__pycache__/csv_analyze/溢价率详细分析数据表.xlsx'

with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:

    # 工作表1: 所有数据
    df_output = df.copy()
    df_output['date'] = df_output['date'].dt.strftime('%Y-%m-%d')
    df_output['nav'] = df_output['nav'].round(4)
    df_output['premium'] = df_output['premium'].round(4)
    df_output.columns = ['日期', '基金净值', '溢价率%']
    df_output.to_excel(writer, sheet_name='全部数据', index=False)

    # 工作表2: 统计汇总
    stats_data = {
        '指标': [
            '数据时间跨度(起)',
            '数据时间跨度(止)',
            '总数据行数',
            '最高溢价率',
            '最低溢价率',
            '平均溢价率',
            '中位数溢价率',
            '标准差',
            '当前溢价率',
            '当前日期'
        ],
        '数值': [
            df['date'].min().strftime('%Y-%m-%d'),
            df['date'].max().strftime('%Y-%m-%d'),
            len(df),
            f"{df['premium'].max():.2f}%",
            f"{df['premium'].min():.2f}%",
            f"{df['premium'].mean():.2f}%",
            f"{df['premium'].median():.2f}%",
            f"{df['premium'].std():.2f}%",
            f"{df.iloc[-1]['premium']:.2f}%",
            df.iloc[-1]['date'].strftime('%Y-%m-%d')
        ]
    }
    pd.DataFrame(stats_data).to_excel(writer, sheet_name='数据统计', index=False)

    # 工作表3: 溢价率分段统计
    bins = np.arange(-5, 25, 5)
    df['premium_range'] = pd.cut(df['premium'], bins=bins, include_lowest=True)

    range_counts = df.groupby('premium_range', observed=True).size().reset_index(name='出现次数')
    range_counts['占比%'] = (range_counts['出现次数'] / len(df) * 100).round(2)
    range_counts = range_counts.rename(columns={'premium_range': '溢价率区间'})
    range_counts.to_excel(writer, sheet_name='区间统计', index=False)

    # 工作表4: 高溢价期间详细分析
    high_premium = df[df['premium'] >= 15].copy()
    high_premium['date'] = high_premium['date'].dt.strftime('%Y-%m-%d')
    high_premium = high_premium[['date', 'nav', 'premium']].rename(
        columns={'date': '日期', 'nav': '基金净值', 'premium': '溢价率%'}
    )
    high_premium['溢价率%'] = high_premium['溢价率%'].round(2)
    high_premium.to_excel(writer, sheet_name='高溢价(15%+)', index=False)

    # 工作表5: 关键卖出时机
    sell_signals = {
        '溢价率范围': ['19%及以上', '15-19%', '10-15%', '5-10%', '0-5%', '低于0%'],
        '出现次数': [
            len(df[df['premium'] >= 19]),
            len(df[(df['premium'] >= 15) & (df['premium'] < 19)]),
            len(df[(df['premium'] >= 10) & (df['premium'] < 15)]),
            len(df[(df['premium'] >= 5) & (df['premium'] < 10)]),
            len(df[(df['premium'] >= 0) & (df['premium'] < 5)]),
            len(df[df['premium'] < 0])
        ],
        '占比%': [
            f"{len(df[df['premium'] >= 19])/len(df)*100:.2f}%",
            f"{len(df[(df['premium'] >= 15) & (df['premium'] < 19)])/len(df)*100:.2f}%",
            f"{len(df[(df['premium'] >= 10) & (df['premium'] < 15)])/len(df)*100:.2f}%",
            f"{len(df[(df['premium'] >= 5) & (df['premium'] < 10)])/len(df)*100:.2f}%",
            f"{len(df[(df['premium'] >= 0) & (df['premium'] < 5)])/len(df)*100:.2f}%",
            f"{len(df[df['premium'] < 0])/len(df)*100:.2f}%"
        ],
        '建议操作': ['卖出', '分批卖出', '观察持有', '正常持有', '正常持有', '正常持有'],
        '紧迫程度': ['高', '中等', '低', '无', '无', '无']
    }
    pd.DataFrame(sell_signals).to_excel(writer, sheet_name='卖出指引', index=False)

    # 工作表6: 月度统计
    df['year_month'] = df['date'].dt.to_period('M')
    monthly_stats = df.groupby('year_month').agg({
        'premium': ['count', 'mean', 'max', 'min']
    }).reset_index()
    monthly_stats.columns = ['年月', '交易日数', '平均溢价率%', '最高溢价率%', '最低溢价率%']
    monthly_stats['年月'] = monthly_stats['年月'].astype(str)
    for col in ['平均溢价率%', '最高溢价率%', '最低溢价率%']:
        monthly_stats[col] = monthly_stats[col].round(2)
    monthly_stats.to_excel(writer, sheet_name='月度统计', index=False)

print(f"✅ Excel报告已生成: {excel_path}")
print(f"包含以下工作表:")
print("  1. 全部数据 - 所有历史数据")
print("  2. 数据统计 - 关键指标统计")
print("  3. 区间统计 - 按5%区间分类统计")
print("  4. 高溢价(15%+) - 高溢价时期详细数据")
print("  5. 卖出指引 - 分级操作建议")
print("  6. 月度统计 - 按月统计汇总")
