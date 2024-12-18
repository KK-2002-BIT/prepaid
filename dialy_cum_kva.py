import csv
import requests
import json
import time
from datetime import datetime

# API endpoint
API_URL = "https://engine-web.stage.gomatimvvnl.in/daily_energy_consumption/many/"

# Function to send API request
def send_api_request(payload):
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
        return response.status_code, response.text
    except Exception as e:
        return None, str(e)

# Read CSV and process
def process_csv(file_path):
    results = []
    with open(file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            payload = {
                "start_daily_datetime": row["start_daily_datetime"],
                "end_daily_datetime": row["end_daily_datetime"],
                "account_id": row["account_id"],
                "meter_number": row["meter_number"],
                "energy_consumption_kvah": float(row["energy_consumption_kvah"]),
                "energy_consumption_export_kvah": float(row["energy_consumption_export_kvah"]),
                "start_import_vah": float(row["start_import_vah"]),
                "end_import_vah": float(row["end_import_vah"]),
                "start_export_vah": float(row["start_export_vah"]),
                "end_export_vah": float(row["end_export_vah"]),
                "net_metering_flag": row["net_metering_flag"],
                "max_demand": float(row["max_demand"]),
                "multiplying_factor": float(row["multiplying_factor"]),
            }
            # Send API request
            status_code, response = send_api_request(payload)
            results.append({
                "payload": payload,
                "status_code": status_code,
                "response": response
            })
            print(f"Sent: {payload} \nStatus: {status_code}, Response: {response}\n")
            time.sleep(1)  # Delay between requests
    return results

# Save results to output CSV file
def save_results_to_csv(results, output_file):
    with open(output_file, "w", newline="") as file:
        fieldnames = ["payload", "status_code", "response"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)

# Main Function
def main():
    input_file = "dialy.csv"  # Replace with your CSV file name
    output_file = f"output_results_{datetime.now().strftime('%Y-%m-%d')}.csv"

    print("Processing CSV file and sending API requests...")
    results = process_csv(input_file)
    save_results_to_csv(results, output_file)
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
