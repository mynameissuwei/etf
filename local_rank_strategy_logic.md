# 本地化核心资产轮动策略说明

自述文件基于 `local_rank_strategy.py` 的实现逻辑，帮助在本地环境复现聚宽文章《ETF策略之核心资产轮动》的回测流程。

## 策略文件与目录
- 代码位置：`local_rank_strategy.py`
- 输入数据目录：`/home/suwei/回测策略/data`
- 输出目录（自动创建）：`/home/suwei/回测策略/analysis_results_rank`

## 关键参数
| 参数 | 位置 | 说明 |
| --- | --- | --- |
| `self.m_days = 25` | `local_rank_strategy.py:54` | 长周期动量窗口（交易日前 25 个有效价格） |
| `self.m_days_short = 3` | `local_rank_strategy.py:55` | 短周期趋势窗口（交易日前 3 个有效价格） |
| `self.target_num = 1` | `local_rank_strategy.py:56` | 每次仅持有一只评分最高的 ETF |
| `self.initial_capital = 100000` | `local_rank_strategy.py:57` | 回测初始资金（人民币） |

## 数据加载
- `LocalRankStrategy.__init__` 创建输出目录并调用 `load_data()`（`local_rank_strategy.py:35-108`）。
- 支持两种净值字段命名：`net_value` 或 `单位净值`，最终统一为 `close`（`local_rank_strategy.py:89-97`）。
- 读入后以日期为索引，强制数值化并剔除缺失行，加载结果在控制台打印数据覆盖范围（`local_rank_strategy.py:99-103`）。

## 打分逻辑
得分在 `get_rank()` 中计算（`local_rank_strategy.py:126-198`）：

1. **窗口截取**：对每只 ETF，取交易日前 `self.m_days` 与 `self.m_days_short` 的收盘价序列；数据不足时得分设为 `-999`，跳过交易。
2. **长周期动量**：
   - 取对数价格 `y_long = ln(price)`，创建时间序列 `x_long = [0, ..., m_days-1]`。
   - 线性回归斜率 `slope_long` 计算年化收益：`annualized = (e^{slope_long})^{250} - 1`。
   - 残差平方和估算 R²，得到原始动量分数 `long_raw = annualized * R²`。
   - 将原始分数通过 Sigmoid 压缩（`sigmoid` 定义于 `local_rank_strategy.py:110-112`）。
3. **短周期趋势过滤**：同样对 3 日窗口进行线性回归，取斜率作为 `short_raw` 并经过 Sigmoid。
4. **组合得分**：`combined = sigmoid(long_raw) * sigmoid(short_raw)`；当两项原始得分均为负时，乘积翻转符号以突出双重看空情形。
5. **细节记录**：使用 `ScoreDetail` 数据类完整保存当日指标，供回测记录导出（`local_rank_strategy.py:18-33`）。

最终返回按 `combined` 倒序排序的 ETF 列表，用于后续调仓。

## 交易执行
- `trade()` 每个交易日执行（`local_rank_strategy.py:226-311`）。
- 逻辑要点：
  1. 取得当日排名，记录评分明细并选出第一名 `target_etf`。
  2. 若当前无持仓或持仓不含目标 ETF，则触发调仓流程；否则维持原仓位。
  3. 调仓时先全部卖出目标以外的持仓，再把全部现金按目标收盘价买入目标 ETF。
  4. 每次交易写入 `self.portfolio['trades']` 供 CSV 导出。
- 控制台会输出当日得分与成交明细，便于调试。

## 回测流程
- `run_backtest()` 负责确定交易日与循环执行交易（`local_rank_strategy.py:312-375`）。
- 日期处理：
  - `get_trading_dates()` 返回各 ETF 交集且满足长周期窗口的日期序列（`local_rank_strategy.py:114-124`）。
  - 通过 `start_date`、`end_date` 参数截取区间后再运行。
- 每日记录包括：
  - 组合总市值、现金、当前持仓名称。
  - 每只 ETF 的综合得分、长/短周期得分、Sigmoid 值、年化收益率、R²、斜率与窗口首尾净值。

## 回测结果与导出
- `print_backtest_results()` 汇总表现并导出 CSV（`local_rank_strategy.py:377-468`）。
- 控制台输出：初始/最终资金、总收益、年化收益、最大回撤、最终持仓。
- 导出文件：
  - `rank_backtest_results.csv`：组合轨迹与打分细项（中文列名，UTF-8 BOM 编码）。
  - `rank_trades_record.csv`：若有交易，保存交易明细。
- 数据保存在 `analysis_results_rank` 目录，确保与原 `local_strategy.py` 结果区分。

## 运行方式
```bash
python3 local_rank_strategy.py
```
默认回测区间为 `2024-01-01` 至脚本运行当天，可在 `main()` 中修改。

## 自定义提示
- **ETF 池调整**：修改 `self.etf_config`（`local_rank_strategy.py:47-51`），在数据目录放入对应 CSV 即可。
- **窗口或资金参数**：在构造函数中直接更改 `m_days`、`m_days_short`、`initial_capital`，无需改动其他代码。
- **输出列扩展**：如需增加指标，可在 `ScoreDetail` 和 `history_record` 中添加字段，再补充 `export_columns` 映射。
