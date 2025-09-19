import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json

def test_518880():
    code = '518880'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # 方法1：直接爬取东方财富页面
    print("方法1：爬取东方财富页面")
    url = f"https://fundf10.eastmoney.com/jjjz_{code}.html"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"状态码: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 查找所有表格
        tables = soup.find_all('table')
        print(f"找到 {len(tables)} 个表格")
        
        data = []
        for i, table in enumerate(tables):
            print(f"检查表格 {i+1}")
            rows = table.find_all('tr')
            print(f"表格 {i+1} 有 {len(rows)} 行")
            
            for j, row in enumerate(rows[:5]):  # 只看前5行
                cols = row.find_all(['td', 'th'])
                if cols:
                    row_text = [col.text.strip() for col in cols]
                    print(f"  行 {j+1}: {row_text}")
                    
                    # 尝试提取数据
                    if len(cols) >= 2 and j > 0:  # 跳过表头
                        try:
                            date_text = cols[0].text.strip()
                            net_value_text = cols[1].text.strip()
                            
                            # 检查是否是日期格式
                            if re.match(r'\d{4}-\d{2}-\d{2}', date_text):
                                net_value = float(net_value_text)
                                data.append({
                                    'date': date_text,
                                    'net_value': net_value,
                                    'code': code
                                })
                        except:
                            pass
            
            if data:
                print(f"从表格 {i+1} 提取到 {len(data)} 条数据")
                break
        
        if data:
            df = pd.DataFrame(data)
            df.to_csv(f'/home/suwei/回测策略/data/{code}_test.csv', index=False)
            print(f"成功保存 {len(data)} 条数据")
            print("前5条数据:")
            print(df.head())
        else:
            print("未提取到数据")
            
    except Exception as e:
        print(f"错误: {e}")
    
    # 方法2：尝试其他API
    print("\n方法2：尝试其他数据源")
    try:
        # 尝试choice数据
        api_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{code}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&end=20251231&lmt=500"
        response = requests.get(api_url, headers=headers, timeout=30)
        print(f"API状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("API响应结构:", list(result.keys()) if isinstance(result, dict) else "非字典")
            
            if 'data' in result and result['data'] and 'klines' in result['data']:
                klines = result['data']['klines']
                print(f"获取到 {len(klines)} 条K线数据")
                
                data = []
                for line in klines[:5]:  # 看前5条
                    parts = line.split(',')
                    print(f"K线数据: {parts}")
                    if len(parts) >= 2:
                        date = parts[0]
                        close_price = float(parts[2])  # 收盘价作为净值
                        data.append({
                            'date': date,
                            'net_value': close_price,
                            'code': code
                        })
                
                if data:
                    df = pd.DataFrame(data)
                    print("K线数据前5条:")
                    print(df.head())
        
    except Exception as e:
        print(f"API错误: {e}")

if __name__ == "__main__":
    test_518880()