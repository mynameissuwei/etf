import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import json

def scrape_palmmicro_data(symbol, code):
    """爬取palmmicro网站的股票历史数据"""
    url = f"https://palmmicro.com/woody/res/stockhistorycn.php?symbol={symbol}&num=500"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 查找表格数据
        table = soup.find('table')
        if not table:
            print(f"未找到{symbol}的数据表格")
            return []
        
        data = []
        rows = table.find_all('tr')[1:]  # 跳过表头
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                date = cols[0].text.strip()
                try:
                    # 尝试获取收盘价作为净值
                    net_value = float(cols[4].text.strip()) if len(cols) > 4 else float(cols[1].text.strip())
                    data.append({
                        'date': date,
                        'net_value': net_value,
                        'code': code
                    })
                except (ValueError, IndexError):
                    continue
        
        return data
    
    except Exception as e:
        print(f"爬取{symbol}数据时出错: {e}")
        return []

def scrape_eastmoney_data(code):
    """爬取东方财富网站的基金净值数据"""
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        # 方法1：使用东方财富基金净值API（优先）
        print(f"尝试基金净值API获取{code}数据...")
        import time
        timestamp = int(time.time() * 1000)
        fund_api_url = f"http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={code}&pageIndex=1&pageSize=500&startDate=&endDate="
        
        api_response = requests.get(fund_api_url, headers=headers, timeout=30)
        print(f"基金净值API响应状态: {api_response.status_code}")
        
        if api_response.status_code == 200:
            content = api_response.text
            print(f"基金净值API响应内容前200字符: {content[:200]}")
            
            # 提取JSONP中的JSON数据
            start_idx = content.find('(') + 1
            end_idx = content.rfind(')')
            if start_idx > 0 and end_idx > start_idx:
                json_str = content[start_idx:end_idx]
                json_data = json.loads(json_str)
                
                data = []
                if 'Data' in json_data and json_data['Data'] and 'LSJZList' in json_data['Data']:
                    lsjz_list = json_data['Data']['LSJZList']
                    print(f"获取到 {len(lsjz_list)} 条基金净值数据")
                    for item in lsjz_list:
                        if item.get('DWJZ'):  # 单位净值不为空
                            data.append({
                                'date': item['FSRQ'],
                                'net_value': float(item['DWJZ']),
                                'code': code
                            })
                    if data:
                        print(f"成功提取基金净值数据，最新：{data[0]['date']} - {data[0]['net_value']}")
                        return data
                elif json_data.get('Data') is None or not json_data['Data']:
                    print(f"基金净值API返回空数据，可能{code}不是普通基金，尝试其他方法")
        
        # 方法2：尝试ETF专用API
        print(f"尝试ETF专用API获取{code}数据...")
        etf_api_url = f"https://push2.eastmoney.com/api/qt/ulist.np/get?fields=f12,f14,f2,f3,f62,f184,f225,f165,f263,f109,f175,f264,f160,f100,f124,f265&secids=1.{code}&_={timestamp}"
        
        response = requests.get(etf_api_url, headers=headers, timeout=30)
        print(f"ETF API响应状态: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"ETF API响应: {result}")
            if 'data' in result and result['data'] and 'diff' in result['data']:
                diff_data = result['data']['diff']
                if diff_data and len(diff_data) > 0:
                    etf_info = diff_data[0]
                    current_price = etf_info.get('f2')  # 当前价格
                    if current_price:
                        print(f"获取到ETF当前净值: {current_price}")
                        # 获取历史数据
                        hist_api_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{code}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&end=20251231&lmt=500"
                        hist_response = requests.get(hist_api_url, headers=headers, timeout=30)
                        
                        if hist_response.status_code == 200:
                            hist_result = hist_response.json()
                            if 'data' in hist_result and hist_result['data'] and 'klines' in hist_result['data']:
                                klines = hist_result['data']['klines']
                                print(f"获取到 {len(klines)} 条K线数据")
                                
                                data = []
                                for line in klines:
                                    parts = line.split(',')
                                    if len(parts) >= 3:
                                        date = parts[0]
                                        close_price = float(parts[2])  # 收盘价
                                        data.append({
                                            'date': date,
                                            'net_value': close_price,
                                            'code': code
                                        })
                                
                                if data:
                                    print(f"K线数据最新：{data[-1]['date']} - {data[-1]['net_value']}")
                                    return data
        
        # 方法3：尝试Choice数据（可能更准确）
        print(f"尝试Choice数据接口...")
        choice_url = f"https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=TRADE_DATE&sortTypes=-1&pageSize=500&pageNumber=1&reportName=RPT_LSJZ_FB&columns=ALL&quoteColumns=f2~01~{code}~CURRENT_PRICE&filter=(SECURITY_CODE=\"{code}\")"
        
        try:
            response = requests.get(choice_url, headers=headers, timeout=30)
            print(f"Choice API响应状态: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if 'result' in result and result['result'] and 'data' in result['result']:
                    choice_data = result['result']['data']
                    print(f"获取到 {len(choice_data)} 条Choice数据")
                    
                    data = []
                    for item in choice_data:
                        if item.get('UNIT_NAV'):  # 单位净值
                            trade_date = item.get('TRADE_DATE', '')
                            if trade_date:
                                # 转换日期格式
                                if len(trade_date) >= 10:
                                    date = trade_date[:10]  # 取前10位 YYYY-MM-DD
                                    data.append({
                                        'date': date,
                                        'net_value': float(item['UNIT_NAV']),
                                        'code': code
                                    })
                    
                    if data:
                        print(f"Choice数据最新：{data[0]['date']} - {data[0]['net_value']}")
                        return data
        except Exception as e:
            print(f"Choice API错误: {e}")
        
        # 方法3：天天基金网（主要用于518880等ETF）
        print(f"尝试天天基金网数据...")
        ttjj_url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        response = requests.get(ttjj_url, headers=headers, timeout=30)
        if response.status_code == 200:
            content = response.text
            print(f"天天基金响应内容前200字符: {content[:200]}")
            
            # 查找净值数据
            if 'Data_netWorthTrend' in content:
                # 提取净值走势数据
                import re
                pattern = r'Data_netWorthTrend = (\[.*?\]);'
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    trend_data = json.loads(match.group(1))
                    print(f"从天天基金获取到 {len(trend_data)} 条净值走势数据")
                    
                    data = []
                    for item in trend_data:
                        if isinstance(item, list) and len(item) >= 2:
                            timestamp = item[0]
                            net_value = item[1]
                            if timestamp and net_value:
                                # 直接在抓取时转换时间戳为日期
                                try:
                                    date = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
                                    data.append({
                                        'date': date,
                                        'net_value': float(net_value),
                                        'code': code
                                    })
                                except Exception as e:
                                    print(f"时间戳转换错误: {timestamp}, {e}")
                                    continue
                    
                    if data:
                        print(f"成功转换并获取 {len(data)} 条数据，最新：{data[-1]['date']} - {data[-1]['net_value']}")
                        return data
        
        return []
    
    except Exception as e:
        print(f"爬取{code}数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    """主函数：爬取所有数据并保存为CSV文件"""
    
    # 定义要爬取的数据源
    sources = [
        ('SZ159509', '159509', 'palmmicro'),
        ('SH513500', '513500', 'palmmicro'), 
        ('518880', '518880', 'eastmoney')
    ]
    
    all_data = []
    
    for symbol, code, source_type in sources:
        print(f"正在爬取 {code} 的数据...")
        
        if source_type == 'palmmicro':
            data = scrape_palmmicro_data(symbol, code)
        elif source_type == 'eastmoney':
            data = scrape_eastmoney_data(code)
        else:
            continue
            
        if data:
            print(f"成功获取 {code} 的 {len(data)} 条数据")
            all_data.extend(data)
            
            # 为每个基金单独保存CSV文件
            df = pd.DataFrame(data)
            df.to_csv(f'/home/suwei/回测策略/data/{code}_data.csv', index=False, encoding='utf-8-sig')
            print(f"已保存 {code} 数据到 {code}_data.csv")
        else:
            print(f"未能获取 {code} 的数据")
    
    # 保存所有数据到一个综合文件
    if all_data:
        df_all = pd.DataFrame(all_data)
        df_all = df_all.sort_values(['code', 'date'])
        df_all.to_csv('/home/suwei/回测策略/data/all_funds_data.csv', index=False, encoding='utf-8-sig')
        print(f"已保存所有数据到 all_funds_data.csv，共 {len(all_data)} 条记录")
    else:
        print("未获取到任何数据")

if __name__ == "__main__":
    main()