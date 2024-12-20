import requests
from datetime import datetime, timedelta
import time
import pandas as pd

# Read the CSV file
data = pd.read_csv('new_file.csv')

# Iterate through each row in the CSV file
for index, row in data.iterrows():
    # Extract start_date, end_date, and account_id from the row
    start_date_str = row['start_date']
    end_date_str = row['end_date']
    account_id = int(row['accountId'])  # Convert account_id to integer

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")

    print(f"Processing account_id: {account_id} from {start_date_str} to {end_date_str}")

    # Iterate through each day within the date range
    while start_date <= end_date:
        # Format the date in the required string format
        formatted_date = start_date.strftime("%Y-%m-%d 00:00:00")
        
        # API endpoint URL with date as path variable
        url = f"https://engine-web.gomatimvvnl.in/trigger_task/daily_ledger_task/{start_date.strftime('%Y-%m-%d 00:00:00')}"
        
        params = {
            "account_id": account_id  # Account ID as an integer
        }
        
        print(f"Input: Date: {formatted_date}, Account ID: {account_id}")
        
        # GET request
        response = requests.get(url, params=params)
        
        print(f"Response: {response.status_code} - {response.text}\n")
        
        # Move to the next day
        start_date += timedelta(days=1)
        
        time.sleep(10)
