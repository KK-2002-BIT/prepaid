import csv
import os
import requests
import pandas as pd

# File paths
input_csv = "new_file.csv"  # Input CSV file containing account IDs
api_response_csv = "api_response_data.csv"  # Intermediate file for API response
output_csv = "updated_data.csv"  # Final processed CSV file

# API base URL
api_url = "https://engine-web.stage.gomatimvvnl.in/daily_energy_consumption/{account_id}/"

# Specify the columns for data processing
columns_to_import = [
    "start_daily_datetime", "end_daily_datetime", "account_id", "meter_number",
    "energy_consumption_kvah", "energy_consumption_export_kvah", "end_import_vah",
    "end_export_vah", "net_metering_flag", "max_demand", "multiplying_factor"
]

# Define the rate per unit for calculation
rate_per_unit = 6.3  # Adjust this value as needed

def fetch_api_data(account_id):
    """Fetch data from the API for a given account ID."""
    url = api_url.format(account_id=account_id)
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()  # Return JSON data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for Account ID {account_id}: {e}")
        return []

def save_data_to_csv(data, output_file):
    """Save a list of dictionaries to a CSV file."""
    if not data:
        print("No data to save.")
        return

    # Get fieldnames from the first record
    fieldnames = data[0].keys()

    # Write header and rows to CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data saved to {output_file}")

def process_and_save_final_data(api_response_csv, input_csv, output_csv):
    """Process the API response CSV and add calculated columns and merge with input CSV data."""
    if not os.path.exists(api_response_csv):
        print(f"API response file {api_response_csv} not found.")
        return

    # Read the specified columns from the API response CSV
    df_api = pd.read_csv(api_response_csv, usecols=columns_to_import)

    # Update the header names
    df_api.columns = [
        "Start date time", "End date time", "Account id", "Meter serial number",
        "Daily consumption", "Daily consumption export", "Cumm daily consumption mtd",
        "Cumm daily consumption export mtd", "Net metering flag", "Max demand", "Multiplying factor"
    ]

    # Create the `Daily consumption in rupees` column
    df_api["Daily consumption in rupees"] = df_api["Daily consumption"] * rate_per_unit

    # Create the `Cumm daily consumption rupees mtd` column as cumulative summation of `Daily consumption in rupees`
    df_api["Cumm daily consumption rupees mtd"] = df_api["Daily consumption in rupees"].cumsum()

    # Reorder columns
    column_order = [
        "Start date time", "End date time", "Account id", "Meter serial number",
        "Daily consumption", "Daily consumption in rupees", "Daily consumption export",
        "Cumm daily consumption mtd", "Cumm daily consumption rupees mtd",
        "Cumm daily consumption export mtd", "Net metering flag", "Max demand", "Multiplying factor"
    ]
    df_api = df_api[column_order]

    # Read the additional columns from the input CSV
    additional_columns = [
        "accountId", "supplyTypecode", "sanctionedLoad", "loadUnit", "arrears",
        "prepaidOpeningbalance", "lpsc" , 'meterInstalldate'
    ]
    df_input = pd.read_csv(input_csv, usecols=additional_columns)

    # Merge API data with additional columns from input CSV
    df_merged = pd.merge(df_api, df_input, left_on="Account id", right_on="accountId", how="left")

    # Drop the duplicate accountId column
    df_merged.drop(columns=["accountId"], inplace=True)

    # Save the updated DataFrame to the final CSV
    df_merged.to_csv(output_csv, index=False)
    print(f"Updated data with new columns saved to {output_csv}")

def main():
    """Main function to orchestrate the data processing."""
    # Step 1: Fetch data from API for each account ID and save to an intermediate CSV
    if not os.path.exists(input_csv):
        print(f"Input file {input_csv} not found.")
        return

    with open(input_csv, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        account_ids = [row["accountId"] for row in reader]

    all_data = []
    for account_id in account_ids:
        print(f"Fetching data for Account ID: {account_id}")
        response_data = fetch_api_data(account_id)
        if response_data:  # Check if response_data is not empty
            all_data.extend(response_data)

    if all_data:
        save_data_to_csv(all_data, api_response_csv)

    # Step 2: Process the saved API response CSV and save the final updated data
    process_and_save_final_data(api_response_csv, input_csv, output_csv)

if __name__ == "__main__":
    main()
