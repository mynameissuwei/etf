#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert timestamp format data to readable date format
"""
import pandas as pd
from datetime import datetime
import json
import re

def convert_timestamp_to_date(timestamp):
    """Convert Unix timestamp (milliseconds) to YYYY-MM-DD format"""
    try:
        if isinstance(timestamp, (int, float)) and timestamp > 1000000000:
            return datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
        else:
            return str(timestamp)
    except:
        return str(timestamp)

def convert_sample_data():
    """Convert the sample timestamp data you mentioned"""
    
    # Your sample data
    sample_data = [
        [1758124800000, 7.8934],  # 2025-09-18
        [1758211200000, 7.9135]   # 2025-09-19
    ]
    
    print("原始时间戳数据:")
    for item in sample_data:
        timestamp, value = item
        date = convert_timestamp_to_date(timestamp)
        print(f"时间戳: {timestamp} -> 日期: {date}, 净值: {value}")
    
    # Convert to DataFrame
    converted_data = []
    for item in sample_data:
        timestamp, value = item
        date = convert_timestamp_to_date(timestamp)
        converted_data.append({
            'date': date,
            'net_value': value,
            'code': '518880'
        })
    
    # Save to CSV
    df = pd.DataFrame(converted_data)
    df.to_csv('/home/suwei/回测策略/data/518880_converted_sample.csv', index=False)
    print(f"\n已保存转换后的样本数据到 518880_converted_sample.csv")
    print("转换后的数据:")
    print(df)

def convert_fund_data_file():
    """Convert data from fetch_fund_data_v2.py if it exists"""
    try:
        # Try to find and read any files with timestamp data
        import os
        data_dir = '/home/suwei/回测策略/data'
        
        # Look for CSV files that might contain timestamp data
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        
        for file in csv_files:
            file_path = os.path.join(data_dir, file)
            try:
                df = pd.read_csv(file_path)
                
                # Check if date column contains timestamps
                if 'date' in df.columns:
                    # Try to detect if dates are timestamps
                    sample_date = str(df['date'].iloc[0])
                    if sample_date.isdigit() and len(sample_date) >= 10:
                        print(f"\n发现时间戳格式文件: {file}")
                        print("原始数据前5行:")
                        print(df.head())
                        
                        # Convert timestamps
                        df['date'] = df['date'].apply(convert_timestamp_to_date)
                        
                        # Save converted file
                        new_filename = file.replace('.csv', '_converted.csv')
                        new_path = os.path.join(data_dir, new_filename)
                        df.to_csv(new_path, index=False)
                        
                        print(f"转换后数据前5行:")
                        print(df.head())
                        print(f"已保存转换后的文件: {new_filename}")
                        
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"处理文件时出错: {e}")

def manual_timestamp_converter():
    """Interactive timestamp converter"""
    print("\n=== 手动时间戳转换器 ===")
    
    while True:
        user_input = input("\n请输入时间戳 (13位毫秒格式，或输入 'quit' 退出): ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            break
            
        try:
            timestamp = int(user_input)
            date = convert_timestamp_to_date(timestamp)
            readable_datetime = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"时间戳: {timestamp}")
            print(f"日期: {date}")
            print(f"完整时间: {readable_datetime}")
            
        except ValueError:
            print("请输入有效的数字时间戳")
        except Exception as e:
            print(f"转换错误: {e}")

if __name__ == "__main__":
    print("时间戳数据转换工具")
    print("=" * 40)
    
    # Convert sample data
    convert_sample_data()
    
    # Try to convert existing files
    convert_fund_data_file()
    
    # Manual converter
    # manual_timestamp_converter()