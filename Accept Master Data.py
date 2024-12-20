import pandas as pd
import requests
import json
import random
from datetime import datetime, timedelta
import time

exec(open('clear_csv.py').read())

# Read data from CSV file
df = pd.read_csv('new_file.csv')

# Define API endpoint
api_url = 'https://gateway.test.gomatimvvnl.in/integration/initial_master_sync/'

# Define Authorization Token
auth_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6IlZhbGlkYXRpb24tdGVhbS1UZXN0aW5nIn0.n30Zc4biuuNphxx4w2hn4xZLPsjLhANxPu5dE9GzfRo"

# Function to generate random email
def generate_email(consumer_name):
    if not consumer_name or pd.isna(consumer_name):
        consumer_name = "user"
    consumer_name = consumer_name.replace(" ", "").lower()
    return f"{consumer_name}@example.com"

# Function to generate random 10-digit number
def generate_random_10_digit():
    return random.randint(1000000000, 9999999999)

# Function to generate random address
def generate_random_address():
    house_number = random.randint(1, 999)
    street = random.choice(["A", "B", "C", "D", "E", "Gazipur", "XYZ-Colony"])
    return f"{house_number}-{street}"

# Function to generate random postcode
def generate_random_postcode():
    return random.randint(100000, 999999)

# Function to generate random requestId
def generate_random_request_id():
    return random.randint(0, 9)

# Function to generate a random date within 2 days of meterInstalldate
def generate_random_date(base_date_str):
    try:
        base_date = datetime.strptime(base_date_str, "%Y-%m-%d")
        random_days = random.randint(1, 2)
        random_date = base_date + timedelta(days=random_days)
        return random_date.strftime("%Y-%m-%d")  # Ensure no time part is included
    except ValueError:
        return ""

# Function to post data to API
def post_data_to_api(data):
    headers = {
        "Authorization": f"Bearer {auth_token}",
        'Content-Type': 'application/json'
        
    }
    response = requests.post(api_url, headers=headers, data=json.dumps(data))
    return response

# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    # Prepare data in the format required by the API
    data = {
        "requestId": str(generate_random_request_id()),
        "accountId": str(row.get('accountId', '')),
        "consumerName": str(row.get('consumerName', '')),
        "address1": generate_random_address(),
        "address2": str(row.get('address2', '')),
        "address3": str(row.get('address3', '')),
        "postcode": generate_random_postcode(),
        "mobileNumber": int(row.get('mobileNumber', 0)),
        "whatsAppnumber": int(row.get('mobileNumber', 0)),
        "email": generate_email(row.get('consumerName', '')),
        "badgeNumber": str(row.get('badgeNumber', '')),
        "supplyTypecode": str(row.get('supplyTypecode', '')),
        "meterSrno": str(row.get('meterSrno', '')),
        "sanctionedLoad": float(row.get('sanctionedLoad', 0.0)),
        "loadUnit": str(row.get('loadUnit', '')),
        "meterInstalldate": str(row.get('meterInstalldate', '')),
        "customerEntrydate": str(row.get('customerEntrydate', '')),
        "connectionStatus": str(row.get('connectionStatus', '')),
        "prepaidPostpaidflag": str(row.get('prepaidPostpaidflag', '')),
        "netMeterflag": str(row.get('netMeterflag', '')),
        "shuntCapacitorflag": str(row.get('shuntCapacitorflag', '')),
        "greenEnergyflag": str(row.get('greenEnergyflag', '')),
        "powerLoomcount": 0,
        "rateSchedule": str(row.get('rateSchedule', '')),
        "meterType": str(row.get('meterType', '')),
        "meterMake": str(row.get('meterMake', '')),
        "multiplyingFactor": 0,
        "meterStatus": str(row.get('meterStatus', '')),
        "arrears": float(row.get('arrears', 0.0)),
        "prepaidOpeningbalance": float(row.get('prepaidOpeningbalance', 0.0)),
        "divisionCode": str(row.get('divisionCode', '')),
        "subDivCode": str(row.get('subDivCode', '')),
        "dtrCode": str(row.get('dtrCode', '')),
        "feederCode": str(row.get('feederCode', '')),
        "substaionCode": str(row.get('substaionCode', '')),
        "serviceAgreementid": str(row.get('serviceAgreementid', '')),
        "billCycle": "M",
        "edApplicable": "1",
        "lpsc": float(row.get('lpsc', 0.0)),
        "param1": str(row.get('param1', '')),
        "param2": str(row.get('param2', '')),
        "param3": str(row.get('param3', '')),
        "param4": str(row.get('param4', '')),
        "param5": str(row.get('param5', ''))
    }

    # Post data to API
    response = post_data_to_api(data)

    # Print response
    print(f"Response for row {index + 1}: {response.status_code} - {response.text}")

    time.sleep(2)

    exec(open('test_kva.py').read())

    time.sleep(1)

    exec(open('test_trigger.py').read())

    time.sleep(2)

    exec(open('get_consumption.py').read())

    time.sleep(2)

    exec(open('import_csv.py').read())
