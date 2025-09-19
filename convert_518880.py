#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert 518880_fund_data.csv timestamp format to readable dates
"""
import pandas as pd
from datetime import datetime

def convert_518880_data():
    """Convert the timestamp data in 518880_fund_data.csv"""
    
    # Read the file with timestamp data
    try:
        df = pd.read_csv('/home/suwei/回测策略/518880_fund_data.csv', encoding='utf-8-sig')
        print(f"读取到 {len(df)} 条记录")
        print("原始数据前10条:")
        print(df.head(10))
        
        # Convert timestamp column to readable dates
        def convert_timestamp(timestamp):
            try:
                if pd.isna(timestamp):
                    return timestamp
                # Convert milliseconds to seconds and then to date
                timestamp_int = int(float(str(timestamp)))
                return datetime.fromtimestamp(timestamp_int/1000).strftime('%Y-%m-%d')
            except:
                return timestamp
        
        # Apply conversion to the date column
        df['净值日期'] = df['净值日期'].apply(convert_timestamp)
        
        print("\n转换后数据前10条:")
        print(df.head(10))
        
        # Check the latest dates for the specific values you mentioned
        print(f"\n查找 7.8934 和 7.9135 对应的日期:")
        target_values = [7.8934, 7.9135]
        for value in target_values:
            matching_rows = df[df['单位净值'].astype(float) == value]
            if not matching_rows.empty:
                for _, row in matching_rows.iterrows():
                    print(f"净值 {value} 对应日期: {row['净值日期']}")
        
        # Save converted data
        output_file = '/home/suwei/回测策略/518880_fund_data_converted.csv'
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n已保存转换后的数据到: {output_file}")
        
        # Also create a standard format for the data directory
        df_standard = df.copy()
        df_standard.columns = ['date', 'net_value']  # Rename columns to standard format
        df_standard['code'] = '518880'  # Add code column
        
        output_file_standard = '/home/suwei/回测策略/data/518880_data_converted.csv'
        df_standard.to_csv(output_file_standard, index=False)
        print(f"已保存标准格式数据到: {output_file_standard}")
        
        # Show the most recent data
        print(f"\n最新10条数据:")
        print(df.tail(10))
        
        return df
        
    except Exception as e:
        print(f"转换过程中出错: {e}")
        return None

if __name__ == "__main__":
    print("转换518880基金数据时间戳格式")
    print("=" * 50)
    convert_518880_data()