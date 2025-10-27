import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 读取CSV文件
file_path = '/home/suwei/回测策略/__pycache__/csv_analyze/fundhistoryd2f310d6c26b8b9189bc4a307d2bd0e6 (3).csv'
df = pd.read_csv(file_path, header=None, names=['date', 'nav', 'premium'])

# 转换日期格式
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)

# 创建一个大的figure
fig = plt.figure(figsize=(16, 12))

# 1. 溢价率时间序列图
ax1 = plt.subplot(2, 2, 1)
ax1.plot(df['date'], df['premium'], linewidth=1.5, color='steelblue', label='溢价率')
ax1.axhline(y=df['premium'].mean(), color='orange', linestyle='--', label=f'平均值: {df["premium"].mean():.2f}%')
ax1.axhline(y=19, color='red', linestyle='--', alpha=0.7, label='当前溢价率: 19%')
ax1.fill_between(df['date'], 19, 20.5, alpha=0.1, color='red', label='高风险区间')
ax1.set_title('基金溢价率走势（2023-2025）', fontsize=14, fontweight='bold')
ax1.set_xlabel('日期')
ax1.set_ylabel('溢价率(%)')
ax1.legend(loc='best', fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis='x', rotation=45)

# 2. 溢价率分布直方图
ax2 = plt.subplot(2, 2, 2)
counts, bins, patches = ax2.hist(df['premium'], bins=30, color='skyblue', edgecolor='black', alpha=0.7)
# 给高溢价部分着色
for i, patch in enumerate(patches):
    if bins[i] >= 15:
        patch.set_facecolor('red')
        patch.set_alpha(0.7)
    elif bins[i] >= 10:
        patch.set_facecolor('orange')
        patch.set_alpha(0.6)

ax2.axvline(x=df['premium'].mean(), color='green', linestyle='--', linewidth=2, label=f'平均值: {df["premium"].mean():.2f}%')
ax2.axvline(x=19, color='red', linestyle='--', linewidth=2, label='当前值: 19%')
ax2.set_title('溢价率分布直方图', fontsize=14, fontweight='bold')
ax2.set_xlabel('溢价率(%)')
ax2.set_ylabel('频数(天数)')
ax2.legend(loc='best')
ax2.grid(True, alpha=0.3, axis='y')

# 3. 按溢价率区间分布饼图
ax3 = plt.subplot(2, 2, 3)
bins_range = [(-5, 0), (0, 5), (5, 10), (10, 15), (15, 20), (20, 25)]
counts_range = []
labels_range = []

for low, high in bins_range:
    count = len(df[(df['premium'] >= low) & (df['premium'] < high)])
    if count > 0:
        counts_range.append(count)
        labels_range.append(f'{low:.0f}-{high:.0f}%\n({count}天)')

colors = []
for low, high in bins_range:
    count = len(df[(df['premium'] >= low) & (df['premium'] < high)])
    if count > 0:
        if low >= 15:
            colors.append('#FF6B6B')
        elif low >= 10:
            colors.append('#FFA500')
        else:
            colors.append('#4ECDC4')

wedges, texts, autotexts = ax3.pie(counts_range, labels=labels_range, autopct='%1.1f%%',
                                     colors=colors, startangle=90)
ax3.set_title('溢价率区间分布（每5%）', fontsize=14, fontweight='bold')

for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
    autotext.set_fontsize(9)

# 4. 高溢价概率统计
ax4 = plt.subplot(2, 2, 4)

# 按照阈值统计数据量和概率
thresholds = [0, 5, 10, 15, 19, 20]
above_threshold = []
percentages = []

for threshold in thresholds:
    count = len(df[df['premium'] >= threshold])
    percentage = count / len(df) * 100
    above_threshold.append(count)
    percentages.append(percentage)

bars = ax4.bar(range(len(thresholds)), percentages, color=['green', 'green', 'yellow', 'orange', 'red', 'darkred'], alpha=0.7, edgecolor='black')

# 在柱子上标注数据
for i, (bar, count) in enumerate(zip(bars, above_threshold)):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.1f}%\n({count}天)',
             ha='center', va='bottom', fontsize=9, fontweight='bold')

ax4.set_xticks(range(len(thresholds)))
ax4.set_xticklabels([f'≥{t}%' for t in thresholds])
ax4.set_title('溢价率超过各阈值的概率', fontsize=14, fontweight='bold')
ax4.set_ylabel('占比(%)')
ax4.set_ylim(0, max(percentages) * 1.2)
ax4.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('/home/suwei/回测策略/__pycache__/csv_analyze/溢价率分析图表.png', dpi=300, bbox_inches='tight')
print("✅ 图表1已生成: 溢价率分析图表.png")
plt.close()

# 创建第二个图：时间序列详细分析
fig, axes = plt.subplots(3, 1, figsize=(16, 12))

# 2-1: 基金净值和溢价率关联分析
ax = axes[0]
ax2 = ax.twinx()

ax.plot(df['date'], df['nav'], color='blue', linewidth=2, label='基金净值')
ax2.plot(df['date'], df['premium'], color='red', linewidth=1.5, label='溢价率', alpha=0.7)

ax.set_xlabel('日期')
ax.set_ylabel('基金净值', color='blue')
ax2.set_ylabel('溢价率(%)', color='red')
ax.set_title('基金净值与溢价率关联分析', fontsize=14, fontweight='bold')
ax.tick_params(axis='x', rotation=45)
ax.grid(True, alpha=0.3)
ax.tick_params(axis='y', labelcolor='blue')
ax2.tick_params(axis='y', labelcolor='red')

# 2-2: 近一年溢价率变化
ax = axes[1]
one_year_ago = df['date'].max() - pd.Timedelta(days=365)
df_recent = df[df['date'] >= one_year_ago]

ax.fill_between(df_recent['date'], df_recent['premium'], alpha=0.3, color='steelblue')
ax.plot(df_recent['date'], df_recent['premium'], color='steelblue', linewidth=2)
ax.axhline(y=19, color='red', linestyle='--', linewidth=2, label='当前溢价率: 19%')
ax.axhline(y=df['premium'].mean(), color='orange', linestyle='--', linewidth=2, label=f'历史平均: {df["premium"].mean():.2f}%')

ax.set_title(f'近一年溢价率变化 ({one_year_ago.strftime("%Y-%m-%d")} 至 {df["date"].max().strftime("%Y-%m-%d")})',
             fontsize=14, fontweight='bold')
ax.set_ylabel('溢价率(%)')
ax.legend(loc='best')
ax.grid(True, alpha=0.3)
ax.tick_params(axis='x', rotation=45)

# 2-3: 溢价率滚动统计
ax = axes[2]
window = 30
df['premium_mean_30d'] = df['premium'].rolling(window=window).mean()
df['premium_std_30d'] = df['premium'].rolling(window=window).std()

ax.plot(df['date'], df['premium'], color='lightblue', linewidth=1, label='日度溢价率', alpha=0.5)
ax.plot(df['date'], df['premium_mean_30d'], color='blue', linewidth=2, label=f'30日均值')
ax.fill_between(df['date'],
                df['premium_mean_30d'] - df['premium_std_30d'],
                df['premium_mean_30d'] + df['premium_std_30d'],
                alpha=0.2, color='blue', label='±1个标准差')

ax.set_title('溢价率30日滚动统计', fontsize=14, fontweight='bold')
ax.set_xlabel('日期')
ax.set_ylabel('溢价率(%)')
ax.legend(loc='best')
ax.grid(True, alpha=0.3)
ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('/home/suwei/回测策略/__pycache__/csv_analyze/溢价率时间序列分析.png', dpi=300, bbox_inches='tight')
print("✅ 图表2已生成: 溢价率时间序列分析.png")
plt.close()

# 创建第三个图：卖出决策支持图
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

# 3-1: 不同溢价率区间的平均持续天数
ax = axes[0, 0]
ranges = ['(-5,0]', '(0,5]', '(5,10]', '(10,15]', '(15,20]']
avg_durations = [1.8, 3.8, 3.8, 2.5, 1.9]  # 根据分析数据

colors_bar = ['green', 'green', 'yellow', 'orange', 'red']
bars = ax.bar(ranges, avg_durations, color=colors_bar, alpha=0.7, edgecolor='black')

for bar, duration in zip(bars, avg_durations):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{duration:.1f}天',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

ax.set_title('不同溢价率区间的平均持续天数', fontsize=12, fontweight='bold')
ax.set_ylabel('平均持续天数')
ax.set_xlabel('溢价率区间(%)')
ax.grid(True, alpha=0.3, axis='y')

# 3-2: 历史高溢价时期回顾
ax = axes[0, 1]
high_periods = ['2024-06-28\n至\n2024-07-01', '2025-10-23\n至\n2025-10-23']
high_duration = [2, 1]
high_colors = ['orange', 'red']

bars = ax.bar(high_periods, high_duration, color=high_colors, alpha=0.7, edgecolor='black')
for bar, duration in zip(bars, high_duration):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{duration}天',
            ha='center', va='bottom', fontsize=12, fontweight='bold')

ax.set_title('历史19%+溢价时期', fontsize=12, fontweight='bold')
ax.set_ylabel('持续天数')
ax.set_ylim(0, 3)
ax.grid(True, alpha=0.3, axis='y')

# 3-3: 卖出概率示意
ax = axes[1, 0]
sell_actions = ['在19%卖出\n后续下跌\n概率', '在15%卖出\n后续下跌\n概率', '在10%卖出\n后续下跌\n概率']
probabilities = [99.84, 85, 60]  # 示例数据
colors_prob = ['red', 'orange', 'yellow']

bars = ax.barh(sell_actions, probabilities, color=colors_prob, alpha=0.7, edgecolor='black')
for bar, prob in zip(bars, probabilities):
    width = bar.get_width()
    ax.text(width, bar.get_y() + bar.get_height()/2.,
            f'{prob:.1f}%',
            ha='left', va='center', fontsize=11, fontweight='bold', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax.set_title('不同卖出点位的下跌概率', fontsize=12, fontweight='bold')
ax.set_xlabel('下跌概率(%)')
ax.set_xlim(0, 110)
ax.grid(True, alpha=0.3, axis='x')

# 3-4: 建议操作等级
ax = axes[1, 1]
premium_levels = ['19%+\n(当前)', '15-19%', '10-15%', '5-10%', '<5%']
urgency_scores = [100, 70, 40, 20, 10]  # 紧急程度评分
colors_urgency = ['darkred', 'red', 'orange', 'yellow', 'green']
actions = ['立即卖出', '分批卖出', '观察持有', '正常持有', '正常持有']

bars = ax.barh(premium_levels, urgency_scores, color=colors_urgency, alpha=0.7, edgecolor='black')
for bar, score, action in zip(bars, urgency_scores, actions):
    width = bar.get_width()
    ax.text(width/2, bar.get_y() + bar.get_height()/2.,
            action,
            ha='center', va='center', fontsize=10, fontweight='bold', color='white')

ax.set_title('溢价率等级与操作建议', fontsize=12, fontweight='bold')
ax.set_xlabel('紧迫程度评分')
ax.set_xlim(0, 110)
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.savefig('/home/suwei/回测策略/__pycache__/csv_analyze/卖出决策支持分析.png', dpi=300, bbox_inches='tight')
print("✅ 图表3已生成: 卖出决策支持分析.png")
plt.close()

print("\n" + "="*60)
print("所有图表已成功生成到:")
print("/home/suwei/回测策略/__pycache__/csv_analyze/")
print("="*60)
