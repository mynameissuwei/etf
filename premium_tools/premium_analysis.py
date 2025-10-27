import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

WINDOWS = [5, 10, 20]

# 读取CSV文件
file_path = '/home/suwei/回测策略/__pycache__/csv_analyze/fundhistoryd2f310d6c26b8b9189bc4a307d2bd0e6 (3).csv'
df = pd.read_csv(file_path, header=None, names=['date', 'nav', 'premium'])

# 转换日期格式
df['date'] = pd.to_datetime(df['date'])
df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
df['premium'] = pd.to_numeric(df['premium'], errors='coerce')

# 按日期升序排列（从最早到最新）
df = df.sort_values('date').dropna(subset=['nav', 'premium']).reset_index(drop=True)

nav_return_samples = {window: [] for window in WINDOWS}
nav_return_stats = {window: None for window in WINDOWS}

print("="*80)
print("溢价率数据分析报告")
print("="*80)
print(f"\n基本信息:")
print(f"数据时间跨度: {df['date'].min().strftime('%Y-%m-%d')} 至 {df['date'].max().strftime('%Y-%m-%d')}")
print(f"总数据行数: {len(df)}")
print(f"\n溢价率统计:")
print(f"最高溢价率: {df['premium'].max():.2f}%")
print(f"最低溢价率: {df['premium'].min():.2f}%")
print(f"平均溢价率: {df['premium'].mean():.2f}%")
print(f"中位数溢价率: {df['premium'].median():.2f}%")
print(f"当前溢价率（最新）: {df.iloc[-1]['premium']:.2f}%")
print(f"当前日期: {df.iloc[-1]['date'].strftime('%Y-%m-%d')}")

# 分析：在19%溢价率卖出后，溢价下跌的情况
print("\n" + "="*80)
print("关键分析：19%溢价率卖出策略")
print("="*80)

# 找出所有溢价率 >= 19% 的时间点
high_premium_indices = df[df['premium'] >= 19].index.tolist()

if len(high_premium_indices) > 0:
    print(f"\n溢价率 >= 19% 的日期数: {len(high_premium_indices)}")
    print("\n溢价率 >= 19% 的时间点:")
    for idx in high_premium_indices[-10:]:  # 显示最后10个
        print(f"  {df.loc[idx, 'date'].strftime('%Y-%m-%d')}: {df.loc[idx, 'premium']:.2f}%")

    if len(high_premium_indices) > 10:
        print(f"  ... (共{len(high_premium_indices)}个时间点)")

    # 分析在19%溢价卖出后的下跌情况
    print("\n\n如果在19%溢价卖出，之后的走势分析:")

    subsequent_drop_count = 0
    subsequent_drop_avg = []

    for idx in high_premium_indices:
        if idx < len(df) - 1:  # 确保有后续数据
            current_premium = df.loc[idx, 'premium']
            # 查看后续所有数据中溢价率下跌的情况
            future_data = df.loc[idx+1:, 'premium']

            # 统计后续数据中小于当前溢价的天数
            drops = (future_data < current_premium).sum()
            total_future = len(future_data)
            drop_rate = drops / total_future * 100 if total_future > 0 else 0
            subsequent_drop_count += 1
            subsequent_drop_avg.append(drop_rate)

        # 统计溢价高位后的净值涨跌幅（5/10/20日）
        start_nav = df.loc[idx, 'nav']
        if pd.notna(start_nav) and start_nav != 0:
            for window in WINDOWS:
                future_idx = idx + window
                if future_idx < len(df):
                    end_nav = df.loc[future_idx, 'nav']
                    if pd.notna(end_nav):
                        nav_return_samples[window].append((end_nav / start_nav - 1) * 100)

    if subsequent_drop_avg:
        avg_drop_rate = np.mean(subsequent_drop_avg)
        print(f"在19%溢价卖出后，后续溢价下跌的平均概率: {avg_drop_rate:.2f}%")
        print(f"统计样本数: {subsequent_drop_count}次")

    print("\n溢价率 >= 19% 后基金净值表现:")
    for window in WINDOWS:
        returns = nav_return_samples[window]
        if returns:
            sample_size = len(returns)
            drop_prob = sum(ret < 0 for ret in returns) / sample_size * 100
            avg_return = np.mean(returns)
            median_return = np.median(returns)
            min_return = np.min(returns)
            max_return = np.max(returns)

            nav_return_stats[window] = {
                'sample_size': sample_size,
                'drop_prob': drop_prob,
                'avg_return': avg_return,
                'median_return': median_return,
                'min_return': min_return,
                'max_return': max_return,
            }

            print(
                f"{window}日: 样本 {sample_size} 次, 下跌概率 {drop_prob:.2f}%, "
                f"平均涨跌幅 {avg_return:.2f}%, 中位数 {median_return:.2f}%, "
                f"最佳/最差 {max_return:.2f}% / {min_return:.2f}%"
            )
        else:
            print(f"{window}日: 数据不足")

# 按5个百分点分段统计溢价持续时间
print("\n" + "="*80)
print("溢价率区间持续时间统计（每5个百分点）")
print("="*80)

# 创建溢价率区间
bins = np.arange(-5, 25, 5)
df['premium_range'] = pd.cut(df['premium'], bins=bins, include_lowest=True)

# 统计每个区间的持续天数
range_duration = defaultdict(lambda: {'days': 0, 'count': 0, 'dates': []})

for i in range(len(df)):
    range_label = df.loc[i, 'premium_range']
    if pd.notna(range_label):
        range_duration[range_label]['count'] += 1
        range_duration[range_label]['dates'].append(df.loc[i, 'date'].strftime('%Y-%m-%d'))

# 计算每个区间的连续天数
for i in range(len(df)):
    if i == 0:
        current_range = df.loc[i, 'premium_range']
        start_idx = i
    else:
        prev_range = df.loc[i-1, 'premium_range']
        current_range = df.loc[i, 'premium_range']

        if prev_range != current_range:
            start_idx = i

# 改进的统计方法：计算连续天数
consecutive_ranges = []
current_range = None
start_idx = 0
range_count = 0

for i in range(len(df)):
    premium_range = df.loc[i, 'premium_range']

    if premium_range != current_range:
        if current_range is not None and range_count > 0:
            consecutive_ranges.append({
                'range': current_range,
                'duration': range_count,
                'start_date': df.loc[start_idx, 'date'].strftime('%Y-%m-%d'),
                'end_date': df.loc[i-1, 'date'].strftime('%Y-%m-%d')
            })
        current_range = premium_range
        start_idx = i
        range_count = 1
    else:
        range_count += 1

# 添加最后一个区间
if current_range is not None and range_count > 0:
    consecutive_ranges.append({
        'range': current_range,
        'duration': range_count,
        'start_date': df.loc[start_idx, 'date'].strftime('%Y-%m-%d'),
        'end_date': df.loc[len(df)-1, 'date'].strftime('%Y-%m-%d')
    })

# 按区间统计
range_stats = {}
for item in consecutive_ranges:
    range_label = str(item['range'])
    if range_label not in range_stats:
        range_stats[range_label] = {'durations': [], 'count': 0}
    range_stats[range_label]['durations'].append(item['duration'])
    range_stats[range_label]['count'] += 1

print("\n按溢价率区间统计（连续天数分析）:")
print(f"{'溢价率区间':<20} {'出现次数':<12} {'平均持续天数':<15} {'最长持续':<12} {'最短持续':<12}")
print("-" * 70)

for range_label in sorted(range_stats.keys()):
    durations = range_stats[range_label]['durations']
    count = range_stats[range_label]['count']
    avg_duration = np.mean(durations)
    max_duration = np.max(durations)
    min_duration = np.min(durations)
    print(f"{range_label:<20} {count:<12} {avg_duration:<15.1f} {max_duration:<12} {min_duration:<12}")

# 对于15%-20%的溢价区间进行详细分析
print("\n" + "="*80)
print("15%-20%溢价率区间详细分析")
print("="*80)

premium_15_20 = df[(df['premium'] >= 15) & (df['premium'] < 20)]
print(f"\n在15%-20%溢价率区间的数据点数: {len(premium_15_20)}")
print(f"占比: {len(premium_15_20) / len(df) * 100:.2f}%")

if len(premium_15_20) > 0:
    print(f"该区间的平均溢价率: {premium_15_20['premium'].mean():.2f}%")
    print(f"该区间的最高溢价率: {premium_15_20['premium'].max():.2f}%")
    print(f"该区间的最低溢价率: {premium_15_20['premium'].min():.2f}%")

    # 计算从15%上升到20%的周期
    print(f"\n该溢价率区间最早出现: {premium_15_20['date'].min().strftime('%Y-%m-%d')}")
    print(f"该溢价率区间最晚出现: {premium_15_20['date'].max().strftime('%Y-%m-%d')}")

# 在19%以上的溢价率
print("\n" + "="*80)
print("19%及以上溢价率分析")
print("="*80)

premium_19_plus = df[df['premium'] >= 19]
print(f"\n溢价率 >= 19% 的数据点数: {len(premium_19_plus)}")
print(f"占比: {len(premium_19_plus) / len(df) * 100:.2f}%")

if len(premium_19_plus) > 0:
    print(f"平均溢价率: {premium_19_plus['premium'].mean():.2f}%")
    print(f"最高溢价率: {premium_19_plus['premium'].max():.2f}%")
    print(f"最早出现日期: {premium_19_plus['date'].min().strftime('%Y-%m-%d')}")
    print(f"最晚出现日期: {premium_19_plus['date'].max().strftime('%Y-%m-%d')}")

    # 统计连续天数
    print(f"\n溢价率 >= 19% 时期统计:")

    in_range = False
    period_start = None
    period_count = 0
    periods = []

    for i in range(len(df)):
        if df.loc[i, 'premium'] >= 19:
            if not in_range:
                in_range = True
                period_start = i
                period_count = 1
            else:
                period_count += 1
        else:
            if in_range:
                periods.append({
                    'start': df.loc[period_start, 'date'].strftime('%Y-%m-%d'),
                    'end': df.loc[i-1, 'date'].strftime('%Y-%m-%d'),
                    'days': period_count
                })
                in_range = False

    if in_range:
        periods.append({
            'start': df.loc[period_start, 'date'].strftime('%Y-%m-%d'),
            'end': df.loc[len(df)-1, 'date'].strftime('%Y-%m-%d'),
            'days': period_count
        })

    print(f"共出现 {len(periods)} 个时期")
    for idx, period in enumerate(periods[-5:], 1):  # 显示最后5个
        print(f"  时期{idx}: {period['start']} 至 {period['end']} ({period['days']}天)")

print("\n" + "="*80)
print("卖出建议")
print("="*80)

if any(nav_return_stats.values()):
    nav_lines = ["2. 高溢价卖出后基金净值表现:"]
    for window in WINDOWS:
        stats = nav_return_stats.get(window)
        if stats:
            nav_lines.append(
                f"   - {window}日: 下跌概率 {stats['drop_prob']:.2f}% (样本 {stats['sample_size']} 次), "
                f"平均涨跌幅 {stats['avg_return']:.2f}%"
            )
        else:
            nav_lines.append(f"   - {window}日: 数据不足")
    nav_block = "\n".join(nav_lines)
else:
    nav_block = "2. 当前数据不足以评估高溢价后的基金净值表现"

print(f"""
当前溢价率: {df.iloc[-1]['premium']:.2f}%

分析结论:
1. 历史数据显示,溢价率 >= 19% 的情况较少出现,属于高溢价状态
{nav_block}
3. 建议根据个人风险偏好决策:
   - 保守策略: 在19%-20%溢价率时考虑卖出,锁定收益
   - 平衡策略: 在溢价率超过15%时分批卖出
   - 激进策略: 等待更高溢价率或其他技术面信号

4. 注意事项:
   - 溢价率会根据市场供需关系波动
   - 建议结合基金本身的基本面和市场环境综合判断
   - 不建议盲目追高或恐慌性卖出
""")
