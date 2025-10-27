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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'https://fundf10.eastmoney.com/jjjz_{code}.html',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }

        # 先尝试ETF/LOF的日K线接口，以获取更准确的收盘价
        print(f"尝试东方财富K线接口获取{code}收盘价...")
        market_prefix = '1' if code.startswith(('5', '6')) else '0'
        kline_url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={market_prefix}.{code}"
            "&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
            "&klt=101&fqt=0&end=20500101&lmt=600"
        )

        kline_resp = requests.get(kline_url, headers=headers, timeout=30)
        if kline_resp.status_code == 200:
            try:
                kline_json = kline_resp.json()
                klines = kline_json.get('data', {}).get('klines')
                if klines:
                    data = []
                    for item in klines:
                        parts = item.split(',')
                        if len(parts) >= 3:
                            date = parts[0].strip()
                            close_price = parts[2].strip()
                            try:
                                close_value = round(float(close_price), 3)
                            except ValueError:
                                continue
                            data.append({
                                'date': date,
                                'net_value': close_value,
                                'code': code
                            })
                    if data:
                        data.sort(key=lambda x: x['date'], reverse=True)
                        print(f"K线接口成功返回 {len(data)} 条收盘价记录，最新 {data[0]['date']} = {data[0]['net_value']}")
                        return data
            except (ValueError, json.JSONDecodeError) as err:
                print(f"解析K线数据失败: {err}")

        # 方法1：天天基金网净值走势数据（主要用于518880等ETF）
        print(f"尝试天天基金网数据获取{code}...")
        ttjj_url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        response = requests.get(ttjj_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            content = response.text
            print(f"天天基金响应状态: {response.status_code}")
            
            # 查找净值数据 - 使用正确的抓取方法
            if 'Data_netWorthTrend' in content:
                print("找到净值走势数据！")
                # 提取净值走势数据
                import re
                pattern = r'Data_netWorthTrend = (\[.*?\]);'
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    trend_data = json.loads(match.group(1))
                    print(f"从天天基金获取到 {len(trend_data)} 条净值走势数据")
                    
                    data = []
                    for item in trend_data:
                        # 处理不同的数据格式
                        if isinstance(item, dict) and 'x' in item and 'y' in item:
                            # 处理 {x: timestamp, y: value} 格式
                            timestamp = item['x']
                            net_value = item['y']
                            if timestamp and net_value:
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
                        elif isinstance(item, list) and len(item) >= 2:
                            # 处理 [timestamp, value] 格式
                            timestamp = item[0]
                            net_value = item[1]
                            if timestamp and net_value:
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
                        # 按日期排序，最新的在前
                        data.sort(key=lambda x: x['date'], reverse=True)
                        print(f"成功转换并获取 {len(data)} 条数据")
                        print(f"最新数据：{data[0]['date']} - {data[0]['net_value']}")
                        print(f"最早数据：{data[-1]['date']} - {data[-1]['net_value']}")
                        return data
        
        # 方法2：东方财富基金净值API（备用）
        print(f"尝试基金净值API获取{code}数据...")
        import time
        timestamp = int(time.time() * 1000)
        fund_api_url = f"http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={code}&pageIndex=1&pageSize=500&startDate=&endDate="
        
        api_response = requests.get(fund_api_url, headers=headers, timeout=30)
        print(f"基金净值API响应状态: {api_response.status_code}")
        
        if api_response.status_code == 200:
            content = api_response.text
            
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
        ('SZ161116', '161116', 'palmmicro'),
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
            df.to_csv(f'{code}_data.csv', index=False, encoding='utf-8-sig')
            print(f"已保存 {code} 数据到 {code}_data.csv")
        else:
            print(f"未能获取 {code} 的数据")
    
    # 保存所有数据到一个综合文件
    if all_data:
        df_all = pd.DataFrame(all_data)
        df_all = df_all.sort_values(['code', 'date'])
        df_all.to_csv('all_funds_data.csv', index=False, encoding='utf-8-sig')
        print(f"已保存所有数据到 all_funds_data.csv，共 {len(all_data)} 条记录")
    else:
        print("未获取到任何数据")

if __name__ == "__main__":
    main()
