import pandas as pd
from datetime import datetime
import calendar

# Read the CSV file into a DataFrame
df = pd.read_csv('updated_data.csv')

fc = 99
rate_per_unit = 6.3

# Convert 'Start date time' and 'meterInstalldate' to datetime
df['Start date time'] = pd.to_datetime(df['Start date time'])
df['meterInstalldate'] = pd.to_datetime(df['meterInstalldate'])

# Function to get the number of days in the month of a given date
def get_days_in_month(date):
    year = date.year
    month = date.month
    return calendar.monthrange(year, month)[1]

# Apply the function to the 'Start date time' column to create a new column 'Days in Month'
df['Days in Month'] = df['Start date time'].apply(get_days_in_month)


# Function to calculate Daily max demand penalty
def calculate_daily_penalty(row):
    if row['Max demand'] > row['sanctionedLoad']:
        day_of_month = row['Start date time'].day
        days_in_month = row['Days in Month']
        penalty = fc * (row['Max demand'] - row['sanctionedLoad']) / (days_in_month - day_of_month)
        return penalty
    return 0


 # Create the `Daily consumption in rupees` column
df["Daily consumption in rupees"] = df["Daily consumption"] * rate_per_unit


# Create the `Cumm daily consumption rupees mtd` column as cumulative summation of `Daily consumption in rupees`
df["Cumm daily consumption rupees mtd"] = df["Daily consumption in rupees"].cumsum()

# Create the 'Daily max demand penalty' column
df['Daily max demand penalty'] = df.apply(calculate_daily_penalty, axis=1)

# Create the 'Cumm daily max demand penalty mtd' column as a cumulative sum of the 'Daily max demand penalty'
df['Cumm daily max demand penalty mtd'] = df['Daily max demand penalty'].cumsum()

# Function to calculate Daily fixed charges before subsidy
def calculate_daily_fixed_charges(row, previous_daily_fixed, is_first_row):
    days_in_month = row['Days in Month']
    sanctioned_load = row['sanctionedLoad']
    max_demand = row['Max demand']
    start_day = row['Start date time'].day
    meter_install_day = row['meterInstalldate'].day

    if max_demand <= 0.75:
        daily_fixed = fc * 0.75 * sanctioned_load / days_in_month
    else:
        if previous_daily_fixed is None:  # For the first row, no previous day exists
            previous_daily_fixed = 0
        daily_fixed = (fc * max_demand * sanctioned_load / days_in_month) + (
            ((fc * max_demand * sanctioned_load / days_in_month) - previous_daily_fixed) * (start_day - 1))

    # Adjust calculation if this is the first row and Start date time > meterInstalldate based on DD comparison
    if is_first_row and start_day > meter_install_day:
        days_diff = start_day - meter_install_day + 1
        daily_fixed *= days_diff

    return daily_fixed

# Initialize variables for the new column
previous_daily_fixed = None
daily_fixed_charges = []

# Calculate Daily fixed charges before subsidy iteratively
for index, row in df.iterrows():
    is_first_row = index == 0
    daily_fixed = calculate_daily_fixed_charges(row, previous_daily_fixed, is_first_row)
    daily_fixed_charges.append(daily_fixed)
    previous_daily_fixed = daily_fixed

# Add the column to the DataFrame
df['Daily fixed charges before subsidy'] = daily_fixed_charges

# Create the 'Cumm daily fixed charges before subsidy mtd' column as a cumulative sum
df['Cumm daily fixed charges before subsidy mtd'] = df['Daily fixed charges before subsidy'].cumsum()

# Calculate the 'Daily net payable' column
df['Daily net payable'] = df['Daily consumption in rupees'] + df['Daily fixed charges before subsidy']

# Create the 'Cumm net payable mtd' column as a cumulative sum of 'Daily net payable'
df['Cumm net payable mtd'] = df['Daily net payable'].cumsum()

# Add new columns as per the requirement
# Calculate 'Daily ed charge' as 5% of 'Daily net payable'
df['Daily ed charge'] = df['Daily net payable'] * 0.05

# Calculate 'Cumm ed charges mtd' as a cumulative sum of 'Daily ed charge'
df['Cumm ed charges mtd'] = df['Daily ed charge'].cumsum()

# Calculate 'Daily final rebate' as 2% of 'Daily net payable'
df['Daily final rebate'] = df['Daily net payable'] * 0.02

# Calculate 'Cumm daily final rebate mtd' as a cumulative sum of 'Daily final rebate'
df['Cumm daily final rebate mtd'] = df['Daily final rebate'].cumsum()

# Calculate 'Daily green energy consumption in rupees'
df['Daily green energy consumption in rupees'] = 0.36 * df['Daily consumption']

# Calculate 'Cumm daily green energy consumption rupees mtd' as a cumulative sum of 'Daily green energy consumption in rupees'
df['Cumm daily green energy consumption rupees mtd'] = df['Daily green energy consumption in rupees'].cumsum()

# Calculate 'Daily final charge'
df['Daily final charge'] = (
    df['Daily net payable'] +
    df['Daily ed charge'] +
    df['Daily max demand penalty'] +
    df['Daily green energy consumption in rupees'] -
    df['Daily final rebate']
)

# Calculate 'Cumm daily final charge mtd' as a cumulative sum of 'Daily final charge'
df['Cumm daily final charge mtd'] = df['Daily final charge'].cumsum()

# Create Opening balance and Closing balance columns
df['Opening balance'] = df['prepaidOpeningbalance'].iloc[0]  # Set for first row
df['Closing balance'] = df['Opening balance'] - df['Daily final charge']

# Loop through the rows to set Opening balance for subsequent rows
for i in range(1, len(df)):
    df.at[i, 'Opening balance'] = df.at[i-1, 'Closing balance']
    df.at[i, 'Closing balance'] = df.at[i, 'Opening balance'] - df.at[i, 'Daily final charge']

# Round all numerical columns to 2 decimal places
df = df.round(3)

# Save the updated DataFrame to a CSV file
df.to_csv('final_result.csv', index=False)

# Display the updated DataFrame
print(df)
