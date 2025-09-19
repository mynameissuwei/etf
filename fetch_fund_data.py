#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch fund net value data from EastMoney
"""
import requests
import re
import json
import csv
import sys
from datetime import datetime

def fetch_fund_data(fund_code="518880"):
    """Fetch fund net value data from EastMoney API"""
    
    # Try different API endpoints that EastMoney uses
    api_urls = [
        f"http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery&fundCode={fund_code}&pageIndex=1&pageSize=1000&startDate=&endDate=",
        f"https://fundf10.eastmoney.com/api/FundNetWorth.ashx?fundcode={fund_code}&pageIndex=1&pageSize=1000",
        f"http://fund.eastmoney.com/api/FUNDNETWORTH.ashx?fundcode={fund_code}&pageIndex=1&pageSize=1000"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': f'https://fundf10.eastmoney.com/jjjz_{fund_code}.html'
    }
    
    for url in api_urls:
        try:
            print(f"Trying URL: {url}")
            response = requests.get(url, headers=headers, timeout=10)
            print(f"Response status: {response.status_code}")
            print(f"Response content preview: {response.text[:200]}")
            
            if response.status_code == 200:
                content = response.text
                
                # Try to extract JSON from JSONP callback
                if 'jQuery' in content and '(' in content:
                    # Remove JSONP callback wrapper
                    json_start = content.find('(') + 1
                    json_end = content.rfind(')')
                    if json_start > 0 and json_end > json_start:
                        content = content[json_start:json_end]
                
                try:
                    data = json.loads(content)
                    if 'Data' in data and 'LSJZList' in data['Data']:
                        return data['Data']['LSJZList']
                    elif isinstance(data, list):
                        return data
                    else:
                        print(f"Unexpected data structure: {data}")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    print(f"Raw content: {content[:500]}")
                    
        except Exception as e:
            print(f"Error with URL {url}: {e}")
            continue
    
    return None

def save_to_csv(data, filename="518880_fund_data.csv"):
    """Save fund data to CSV file"""
    if not data:
        print("No data to save")
        return
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['净值日期', '单位净值'])
        
        # Write data
        for record in data:
            # Handle different possible field names
            date = record.get('FSRQ', record.get('净值日期', record.get('date', '')))
            nav = record.get('DWJZ', record.get('单位净值', record.get('nav', '')))
            
            if date and nav:
                writer.writerow([date, nav])
    
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    print("Fetching fund data for 518880...")
    data = fetch_fund_data("518880")
    
    if data:
        print(f"Found {len(data)} records")
        save_to_csv(data)
    else:
        print("Failed to fetch data. Let's try a backup approach...")
        # Backup: try to scrape the HTML table directly
        url = "https://fundf10.eastmoney.com/jjjz_518880.html"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers)
        print(f"HTML response status: {response.status_code}")
        print("Manual extraction required - the data is loaded dynamically")