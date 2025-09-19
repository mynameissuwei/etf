import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import time

def get_accurate_518880_data():
    """获取准确的518880基金净值数据"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    # 方法1：直接访问东方财富基金页面并解析JavaScript数据
    print("方法1：解析东方财富基金页面...")
    url = "https://fundf10.eastmoney.com/jjjz_518880.html"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        content = response.text
        
        # 查找页面中的净值数据JavaScript
        # 寻找类似这样的模式：var apidata = {...}
        js_patterns = [
            r'var\s+apidata\s*=\s*({.*?});',
            r'var\s+Data_netWorthTrend\s*=\s*(\[.*?\]);',
            r'var\s+Data_ACWorthTrend\s*=\s*(\[.*?\]);',
            r'jjjz_list\s*=\s*(\[.*?\]);'
        ]
        
        for pattern in js_patterns:
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            if matches:
                print(f"找到JavaScript数据模式: {pattern}")
                try:
                    js_data = json.loads(matches[0])
                    print(f"解析到的数据类型: {type(js_data)}")
                    print(f"数据内容示例: {str(js_data)[:200]}...")
                    
                    # 如果是数组格式的净值数据
                    if isinstance(js_data, list) and len(js_data) > 0:
                        data = []
                        for item in js_data:
                            if isinstance(item, list) and len(item) >= 2:
                                # 时间戳格式
                                timestamp = item[0]
                                net_value = item[1]
                                if timestamp and net_value:
                                    date = time.strftime('%Y-%m-%d', time.localtime(timestamp/1000))
                                    data.append({
                                        'date': date,
                                        'net_value': float(net_value),
                                        'code': '518880'
                                    })
                        
                        if data:
                            print(f"从JavaScript提取到 {len(data)} 条数据")
                            return data
                            
                except json.JSONDecodeError as e:
                    print(f"JSON解析错误: {e}")
                    continue
        
        # 方法2：使用正则直接查找页面中的净值表格数据
        print("方法2：使用正则表达式提取表格数据...")
        soup = BeautifulSoup(content, 'html.parser')
        
        # 查找包含净值的表格
        tables = soup.find_all('table')
        print(f"找到 {len(tables)} 个表格")
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            print(f"表格 {i+1} 有 {len(rows)} 行")
            
            data = []
            for j, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    print(f"  行 {j}: {cell_texts}")
                    
                    # 检查是否是净值数据行
                    if len(cell_texts) >= 2 and j > 0:  # 跳过表头
                        date_text = cell_texts[0]
                        net_value_text = cell_texts[1]
                        
                        # 验证日期格式
                        date_pattern = r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})'
                        date_match = re.search(date_pattern, date_text)
                        
                        if date_match:
                            try:
                                # 清理净值文本，去除多余字符
                                net_value_clean = re.sub(r'[^\d\.]', '', net_value_text)
                                if net_value_clean and '.' in net_value_clean:
                                    net_value = float(net_value_clean)
                                    date_clean = date_match.group(1).replace('/', '-')
                                    
                                    data.append({
                                        'date': date_clean,
                                        'net_value': net_value,
                                        'code': '518880'
                                    })
                                    
                            except ValueError:
                                continue
            
            if data:
                print(f"从表格 {i+1} 提取到 {len(data)} 条数据")
                # 显示前几条数据验证
                for item in data[:5]:
                    print(f"  {item['date']}: {item['net_value']}")
                return data
        
        return []
        
    except Exception as e:
        print(f"获取数据时出错: {e}")
        return []

def save_corrected_data():
    """保存修正后的518880数据"""
    data = get_accurate_518880_data()
    
    if data:
        df = pd.DataFrame(data)
        # 按日期排序
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=False)
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # 保存修正后的数据
        df.to_csv('/home/suwei/回测策略/data/518880_corrected.csv', index=False)
        print(f"已保存修正后的518880数据，共 {len(data)} 条记录")
        
        # 显示最新几条数据
        print("最新5条数据:")
        print(df.head())
        
        # 检查是否包含2025-09-19和2025-09-18的数据
        recent_dates = ['2025-09-19', '2025-09-18']
        for date in recent_dates:
            matching_rows = df[df['date'] == date]
            if not matching_rows.empty:
                net_value = matching_rows.iloc[0]['net_value']
                print(f"{date}: {net_value}")
            else:
                print(f"{date}: 未找到数据")
    else:
        print("未能获取到数据")

if __name__ == "__main__":
    save_corrected_data()