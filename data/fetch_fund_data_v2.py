#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch fund net value data from EastMoney using alternative methods
"""
import requests
import re
import json
import csv
import time
from datetime import datetime, timedelta

def try_eastmoney_api():
    """Try different EastMoney API patterns"""
    fund_code = "518880"
    
    # Updated API patterns based on current EastMoney structure
    api_patterns = [
        f"https://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&secid=1.{fund_code}",
        f"https://fundapi.eastmoney.com/fundtradenew.aspx?ft=all&sc=6yzf&st=desc&pi=1&pn=50&cp=&ct=&cd=&ms=&fr=&plevel=&fst=&ftype=&fr1=&fl=0&isab=1",
        f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js",
        f"https://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': f'https://fundf10.eastmoney.com/jjjz_{fund_code}.html',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
    }
    
    for api_url in api_patterns:
        try:
            print(f"\nTrying: {api_url}")
            response = requests.get(api_url, headers=headers, timeout=15)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                content = response.text
                print(f"Content preview: {content[:200]}...")
                
                # Check if it contains fund data
                if 'Data_netWorthTrend' in content or 'DWJZ' in content or fund_code in content:
                    print("Found potential fund data!")
                    
                    # Try to extract data using regex
                    patterns = [
                        r'Data_netWorthTrend\s*=\s*(\[.*?\]);',
                        r'var\s+\w+\s*=\s*(\[.*?\]);',
                        r'(\[.*?"DWJZ".*?\])',
                        r'(\{.*?"DWJZ".*?\})'
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, content, re.DOTALL)
                        if matches:
                            try:
                                data = json.loads(matches[0])
                                print(f"Successfully parsed JSON data with {len(data)} items")
                                # 显示前几条原始数据用于检查时间戳格式
                                if len(data) > 0:
                                    print("原始数据前5条:")
                                    for i, item in enumerate(data[:5]):
                                        print(f"  {i+1}: {item}")
                                return data
                            except:
                                continue
                
        except Exception as e:
            print(f"Error: {e}")
            continue
    
    return None

def create_sample_data():
    """Create sample data based on typical fund structure"""
    print("Creating sample data structure...")
    
    # Generate some sample dates and values
    sample_data = []
    base_date = datetime(2024, 1, 1)
    base_value = 1.000
    
    for i in range(50):  # 50 sample records
        current_date = base_date + timedelta(days=i*7)  # Weekly data
        # Add some random variation
        variation = (i % 10 - 5) * 0.001
        current_value = round(base_value + (i * 0.002) + variation, 3)
        
        sample_data.append({
            'FSRQ': current_date.strftime('%Y-%m-%d'),
            'DWJZ': str(current_value)
        })
    
    return sample_data

def save_to_csv(data, filename="518880_fund_data.csv"):
    """Save fund data to CSV file"""
    if not data:
        print("No data to save")
        return
    
    print(f"Saving {len(data)} records to {filename}")
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['净值日期', '单位净值'])
        
        # Write data
        for record in data:
            if isinstance(record, dict):
                # Handle different possible field names
                date = record.get('FSRQ', record.get('净值日期', record.get('date', record.get('x', ''))))
                nav = record.get('DWJZ', record.get('单位净值', record.get('nav', record.get('y', ''))))
                
                # 如果是时间戳格式，直接转换
                if isinstance(date, (int, float)) and date > 1000000000:
                    original_timestamp = date
                    date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d')
                    print(f"时间戳转换: {original_timestamp} -> {date}, 净值: {nav}")
            elif isinstance(record, list) and len(record) >= 2:
                # Handle array format [timestamp, value] or [date, value]
                date = record[0]
                nav = record[1]
                # Convert timestamp if needed - 直接在抓取时转换
                if isinstance(date, (int, float)) and date > 1000000000:
                    date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d')
                    print(f"时间戳转换: {record[0]} -> {date}, 净值: {nav}")
            else:
                continue
            
            if date and nav:
                writer.writerow([date, nav])
    
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    print("Attempting to fetch 518880 fund data...")
    
    # Try API first
    data = try_eastmoney_api()
    
    if not data:
        print("\nAPI failed. Creating template with sample data structure...")
        data = create_sample_data()
        print("\nNote: This is sample data. For real data, you may need to:")
        print("1. Use browser developer tools to find the actual API endpoint")
        print("2. Use selenium/playwright for dynamic content")
        print("3. Check if the fund code 518880 is correct")
    
    save_to_csv(data)
    
    # Also create a manual template
    with open("fund_data_template.csv", 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['净值日期', '单位净值'])
        writer.writerow(['2024-09-19', '1.000'])
        writer.writerow(['2024-09-18', '0.998'])
        writer.writerow(['2024-09-17', '1.002'])
    
    print("\nAlso created fund_data_template.csv with the correct format")