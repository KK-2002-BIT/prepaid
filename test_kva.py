import requests
import csv
import random
import json
from datetime import datetime, timedelta

def fetch_parameters_from_csv(filename):
    parameters = []
    with open(filename, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            parameters.append(row)
    return parameters

def generate_daily_consumption(min_val, max_val):
    return random.randint(min_val, max_val)

def post_energy_record(url, payload):
    try:
        response = requests.post(url, headers={'Content-Type': 'application/json'},
                data=json.dumps(payload),
                timeout=15)
    except requests.exceptions.RequestException as e:
        print(f"Error Details: {e}")
    return response


def generate_random_incremental_values(start, end, days):
    if days < 1:
        return []

    step = round((end - start) / days, 2)
    values = []
    current = start

    for _ in range(days):
        next_value = round(current + step, 2)
        if next_value > end:
            next_value = round(random.uniform(start, end), 2)
        values.append(next_value)
        current = next_value

    return values

# API endpoint URL
url = "https://engine-web.gomatimvvnl.in/daily_energy_consumption/many/"

# Fetch parameters from CSV
csv_filename = f"new_file.csv"
rows = fetch_parameters_from_csv(csv_filename)

# Process each record from the CSV
for row in rows:
    # # Convert dates
    start_date = datetime.strptime(row['start_date'], "%Y-%m-%d")
    end_date = datetime.strptime(row['end_date'], "%Y-%m-%d")
    kvah_min = int(row['consumption_min'])
    kvah_max = int(row['consumption_max'])
    min_max_demand = float(row['min_max_demand'])
    max_max_demand = float(row['max_max_demand'])
    net_metering_flag = row['netMeterflag']
    
    # Initial parameters
    start_import_vah_previous = 0  # Initial value
    days_count = (end_date - start_date).days + 1  # Include the start day
    max_demand_values = generate_random_incremental_values(min_max_demand, max_max_demand, days_count)
    index = 0


    # Iterate through each day in the range
    current_date = start_date
    while current_date <= end_date:
        start_daily_datetime = current_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_daily_datetime = (current_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        max_demand = max_demand_values[index]
        index += 1


        # Generate daily consumption value in kVAh
        energy_consumption_kvah = generate_daily_consumption(kvah_min, kvah_max)
        start_import_vah = start_import_vah_previous
        end_import_vah = energy_consumption_kvah + start_import_vah 

        # Define the payload
        payload = {
            "data": [
                {
                    "start_daily_datetime": start_daily_datetime,
                    "end_daily_datetime": end_daily_datetime,
                    "account_id": row["accountId"],
                    "meter_number": row["meterSrno"],
                    "energy_consumption_kwh": "00",  
                    "energy_consumption_kvah": energy_consumption_kvah,
                    "energy_consumption_export_kwh": "00",
                    "energy_consumption_export_kvah": "00",
                    "start_import_wh": "00",  
                    "end_import_wh": "00",    
                    "start_import_vah": start_import_vah,
                    "end_import_vah": end_import_vah,
                    "start_export_wh": "00",
                    "end_export_wh": "00",
                    "start_export_vah": "00",
                    "end_export_vah": "00",
                    "net_metering_flag": net_metering_flag,
                    "max_demand": max_demand,
                    "multiplying_factor": 1
                       }
            ]
        }


        # Post the request
        response = post_energy_record(url, payload)

        # Check the response status
        if response.status_code == 200:
            print(f"Record for {row['account_id']} on {start_daily_datetime} successfully sent.")

            print(response) 
        else:
            # print(f"Failed to send record for {row['account_id']} on {start_daily_datetime}. Status Code: {response.status_code}")
            print(response.text)  # Print the response for debugging

        # Update previous end_import_vah
        start_import_vah_previous = end_import_vah

        # Increment the date
        current_date += timedelta(days=1)
