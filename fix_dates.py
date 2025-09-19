#!/usr/bin/env python3
import csv
import datetime

# Read the original file and convert timestamps to dates
with open('518880_fund_data.csv', 'r', encoding='utf-8-sig') as infile:
    reader = csv.reader(infile)
    header = next(reader)  # Skip header
    
    data = []
    for row in reader:
        if len(row) >= 2:
            timestamp = int(float(row[0]))
            # Convert from milliseconds to seconds if needed
            if timestamp > 10000000000:  # If it's in milliseconds
                timestamp = timestamp / 1000
            
            date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            nav = row[1]
            data.append([date, nav])

# Sort by date (newest first)
data.sort(key=lambda x: x[0], reverse=True)

# Write the corrected file
with open('518880_fund_data.csv', 'w', newline='', encoding='utf-8-sig') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['净值日期', '单位净值'])
    for row in data:
        writer.writerow(row)

print(f"Fixed {len(data)} records with proper date format")