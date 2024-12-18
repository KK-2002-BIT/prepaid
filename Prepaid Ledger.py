import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText
import requests
from datetime import datetime, timedelta
import time
import json
import random

'''---------------------------------------------------------------------------------------------------------------------------------------------'''

                        # 1.  Function to call Create Daily Energy Consumption API






API_URL = 'https://engine-web.stage.gomatimvvnl.in/daily_energy_consumption/many/'

# Validation Functions
def validate_account_id(account_id):
    if len(account_id) != 10 or not account_id.isdigit():
        messagebox.showerror("Validation Error", "Account ID must be 10 digits.")
        return False
    return True

def validate_meter_number(meter_number):
    if len(meter_number) != 10 or not meter_number.isalnum():
        messagebox.showerror("Validation Error", "Meter Number must be 10 alphanumeric characters.")
        return False
    return True

def validate_datetime(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        messagebox.showerror("Validation Error", "Invalid datetime format! Use YYYY-MM-DDTHH:MM:SS.")
        return None

def validate_max_demand(min_max_demand, max_max_demand):
    if not (0.50 <= min_max_demand <= 1.75 and 0.50 <= max_max_demand <= 1.75):
        messagebox.showerror("Validation Error", "Max Demand must be between 0.50 and 1.75 kW.")
        return False
    if min_max_demand > max_max_demand:
        messagebox.showerror("Validation Error", "Min Max Demand cannot be greater than Max Max Demand.")
        return False
    return True

# Generate Random Incremental Values
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

# Send Data to API
def send_data_to_api(payload, account_id, current_date, energy_consumption, max_demand):
    try:
        response = requests.post(
            API_URL,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(payload),
            timeout=15
        )
        if response.status_code == 201:
            log_message = f"Successfully Created: {energy_consumption} kWh, Max Demand: {max_demand} for Acc_ID: {account_id} on {current_date.strftime('%d-%m-%Y %H:%M:%S')}"
        else:
            log_message = f"Duplicate Entry for Acc_ID: {account_id} on {current_date.strftime('%d-%m-%Y %H:%M:%S')}"
    except requests.exceptions.Timeout:
        log_message = f"Timeout for Acc_ID: {account_id} on {current_date.strftime('%d-%m-%Y')}."
    except requests.exceptions.RequestException as e:
        log_message = f"Error for Acc_ID: {account_id}. Details: {e}"
    
    output_create.insert(tk.END, log_message + "\n")

# Generate and Send Data
def generate_and_send_data(account_id, meter_number, start_date, end_date,
                           min_energy, max_energy, previous_consumption, min_max_demand, max_max_demand, net_metering_flag, energy_type):
    days = (end_date - start_date).days + 1
    max_demand_values = generate_random_incremental_values(min_max_demand, max_max_demand, days)
    current_date = start_date
    index = 0
    table_data = []

    while current_date <= end_date:
        energy_consumption = round(random.uniform(min_energy, max_energy), 2)
        max_demand = max_demand_values[index]
        index += 1

        if energy_type == "KW":
            start_import_wh = previous_consumption
            end_import_wh = start_import_wh + energy_consumption

            payload = {
                "data": [{
                    "start_daily_datetime": current_date.strftime("%Y-%m-%dT%H:%M:%S"),
                    "end_daily_datetime": (current_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                    "account_id": account_id,
                    "meter_number": meter_number,
                    "energy_consumption_kwh": energy_consumption,
                    "energy_consumption_kvah": 0.00,
                    "energy_consumption_export_kwh": 0.00,
                    "energy_consumption_export_kvah": 0.00,
                    "start_import_wh": start_import_wh,
                    "end_import_wh": end_import_wh,
                    "start_import_vah": 0.00,
                    "end_import_vah": 0.00,
                    "start_export_wh": 0.00,
                    "end_export_wh": 0.00,
                    "net_metering_flag": net_metering_flag,
                    "max_demand": max_demand,
                    "multiplying_factor": 1
                }]
            }
            previous_consumption = end_import_wh

        elif energy_type == "KVA":
            start_import_vah = previous_consumption
            end_import_vah = start_import_vah + energy_consumption

            payload = {
                "data": [{
                    "start_daily_datetime": current_date.strftime("%Y-%m-%dT%H:%M:%S"),
                    "end_daily_datetime": (current_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                    "account_id": account_id,
                    "meter_number": meter_number,
                    "energy_consumption_kwh": 0.00,
                    "energy_consumption_kvah": energy_consumption,
                    "energy_consumption_export_kwh": 0.00,
                    "energy_consumption_export_kvah": 0.00,
                    "start_import_wh": 0.00,
                    "end_import_wh": 0.00,
                    "start_import_vah": start_import_vah,
                    "end_import_vah": end_import_vah,
                    "start_export_wh": 0.00,
                    "end_export_wh": 0.00,
                    "net_metering_flag": net_metering_flag,
                    "max_demand": max_demand,
                    "multiplying_factor": 1
                }]
            }
            previous_consumption = end_import_vah

        table_data.append((energy_consumption, max_demand))
        send_data_to_api(payload, account_id, current_date, energy_consumption, max_demand)
        current_date += timedelta(days=1)
        time.sleep(1)

    return table_data

# Execute Selected File
def execute_selected_file():
    try:
        account_id = account_id_entry.get()
        meter_number = meter_number_entry.get()
        start_date_str = start_date_entry.get()
        end_date_str = end_date_entry.get()
        min_energy = float(min_energy_entry.get())
        max_energy = float(max_energy_entry.get())
        previous_consumption = float(previous_consumption_entry.get())
        min_max_demand = float(min_max_demand_entry.get())
        max_max_demand = float(max_max_demand_entry.get())
        net_metering_flag = net_metering_flag_entry.get()
        energy_type = energy_type_var.get()

        if not (validate_account_id(account_id) and validate_meter_number(meter_number)):
            return

        start_date = validate_datetime(start_date_str)
        end_date = validate_datetime(end_date_str)
        if not start_date or not end_date:
            return

        if not validate_max_demand(min_max_demand, max_max_demand):
            return

        table_data = generate_and_send_data(
            account_id, meter_number, start_date, end_date,
            min_energy, max_energy, previous_consumption, min_max_demand, max_max_demand, net_metering_flag, energy_type
        )

        output_create.insert(tk.END, f"Data sent for {energy_type} successfully!\n")

        for widget in table_frame.winfo_children():
            widget.destroy()

        tk.Label(table_frame, text="Energy Consumption (kWh/KVA)").grid(row=0, column=0, padx=5, pady=5)
        tk.Label(table_frame, text="Max Demand (kW)").grid(row=0, column=1, padx=5, pady=5)

        for i, (energy, demand) in enumerate(table_data, start=1):
            tk.Label(table_frame, text=str(energy)).grid(row=i, column=0, padx=5, pady=5)
            tk.Label(table_frame, text=str(demand)).grid(row=i, column=1, padx=5, pady=5)

    except ValueError as e:
        messagebox.showerror("Error", str(e))












'''-------------------------------------------------------------------------------------------------------------------------------------------'''


# 2.  Function to call Read Daily Energy Consumption API
def call_read_daily_energy_consumption():
    account_id = entry_read_energy.get()
    try:
        url = f"https://engine-web.gomatimvvnl.in/daily_energy_consumption/{account_id}/"
        response = requests.get(url)
        output_read_energy.delete(1.0, tk.END)
        
        # Try to format the response as JSON
        try:
            response_json = response.json()  # Parse response as JSON
            formatted_json = json.dumps(response_json, indent=4)  # Pretty print JSON
            output_read_energy.insert(tk.END, formatted_json)
        except json.JSONDecodeError:
            # If response is not JSON, display as plain text
            output_read_energy.insert(tk.END, response.text)
    except Exception as e:
        output_read_energy.insert(tk.END, str(e))


'''---------------------------------------------------------------------------------------------------------------------------------------------'''

# 3. Function to call Read Daily Prepaid Ledger History API
def call_read_daily_prepaid_ledger_history():
    account_id = entry_read_ledger.get()
    try:
        url = f"https://engine-web.gomatimvvnl.in/daily_prepaid_ledger/{account_id}/"
        response = requests.get(url)
        output_read_ledger.delete(1.0, tk.END)
        
        # Check if the response contains JSON
        if response.headers.get('Content-Type') == 'application/json' or response.text.startswith('{') or response.text.startswith('['):
            # Format JSON for readability
            formatted_json = json.dumps(response.json(), indent=4)
            output_read_ledger.insert(tk.END, formatted_json)
        else:
            # If not JSON, display raw text
            output_read_ledger.insert(tk.END, response.text)
    except Exception as e:
        output_read_ledger.insert(tk.END, str(e))


'''---------------------------------------------------------------------------------------------------------------------------------------------'''

def execute_requests_with_progress(account_ids, start_datetime_str, end_datetime_str, output_text, progress_bar):
    base_url = 'https://engine-web.gomatimvvnl.in/trigger_task/daily_ledger_task/'
    delay_time = 25  # Delay in seconds

    try:
        start_datetime = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
        end_datetime = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        messagebox.showerror("Error", "Invalid Date-Time format. Use 'YYYY-MM-DD HH:MM:SS'.")
        return

    total_requests = 0
    for account_id in account_ids:
        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            total_requests += 1
            current_datetime += timedelta(days=1)

    progress_bar['maximum'] = total_requests
    progress_bar['value'] = 0

    for account_id in account_ids:
        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            url = f"{base_url}{formatted_datetime}?account_id={account_id.strip()}"
            headers = {'Accept': 'application/json'}
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    output_text.insert(tk.END, f"Successfully triggered the Account Id: {account_id.strip()} on {formatted_datetime}\n", 'output')
                else:
                    output_text.insert(tk.END, f"Failed to trigger for Date {formatted_datetime}, status code: {response.status_code}\n", 'output')
            except requests.exceptions.RequestException as e:
                output_text.insert(tk.END, f"Request failed for {formatted_datetime}: Reason = {e}\n", 'output')

            output_text.see(tk.END)
            output_text.update()
            time.sleep(delay_time)
            current_datetime += timedelta(days=1)

            # Update the progress bar
            progress_bar['value'] += 1
            progress_bar.update_idletasks()

    progress_bar['value'] = total_requests
    messagebox.showinfo("Task Completed", "Successfully triggered the Daily Prepaid Ledger Task.")

# Function to start the execution process for Trigger Daily Prepaid Ledger Task with Progress
def start_execution_with_progress():
    account_ids = account_id_entry.get().split(",")
    start_datetime = start_datetime_entry.get()
    end_datetime = end_datetime_entry.get()

    if not account_ids or all(not acc.strip() for acc in account_ids):
        messagebox.showerror("Error", "At least one Account ID is required.")
        return

    for account_id in account_ids:
        if len(account_id.strip()) != 10 or not account_id.strip().isdigit():
            messagebox.showerror("Error", f"Invalid Account ID: {account_id.strip()}\nAccount Id must be exactly 10 digits and numeric.")
            return

    try:
        datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        messagebox.showerror("Error", "Invalid Date-Time format.\nDate-Time should be in 'YYYY-MM-DD HH:MM:SS' Format.")
        return

    output_text.delete(1.0, tk.END)
    execute_requests_with_progress(account_ids, start_datetime, end_datetime, output_text, progress)

'''----------------------------------------------------------------------------------------------------------------------------------------------------'''

# Main application
app = tk.Tk()
app.title("Prepaid Engine/Ledger")
app.geometry("800x600")

# Create Style for Tabs
style = ttk.Style()
style.configure('TNotebook', tabposition='nw')  # Move tabs to top-left
style.configure('TNotebook.Tab', font=('Times New Roman', 10, 'bold'))  # Bold tab text

# Set custom colors for tabs
style.map(
    'TNotebook.Tab',
    background=[("selected", "#FFDDC1"), ("!selected", "#FFE4C4")],
    foreground=[("selected", "black"), ("!selected", "black")]
)

tab_control = ttk.Notebook(app)  # Initialize tab control



'''+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'''





# Tab 1: Create Daily Energy Consumption
tab_create = tk.Frame(tab_control, bg="#FFDDC1")
tab_control.add(tab_create, text="Create Daily Energy Consumption")

url_label = tk.Label(tab_create, text=f"Create Daily Energy Consumption\n{API_URL}", font=("Arial", 10, "bold"), fg="blue", bg="#FFDDC1")
url_label.grid(row=0, column=0, columnspan=2, padx=10, pady=10)

fields = [
    ("Account ID (10-digit):", "account_id_entry"),
    ("Meter Number (10-digit):", "meter_number_entry"),
    ("Start Date (YYYY-MM-DDTHH:MM:SS):", "start_date_entry"),
    ("End Date (YYYY-MM-DDTHH:MM:SS):", "end_date_entry"),
    ("Min Energy Consumption:", "min_energy_entry"),
    ("Max Energy Consumption:", "max_energy_entry"),
    ("Previous Consumption:", "previous_consumption_entry"),
    ("Min Max Demand:", "min_max_demand_entry"),
    ("Max Max Demand:", "max_max_demand_entry"),
    ("Net Metering Flag:", "net_metering_flag_entry"),
]

entries = {}
for idx, (label_text, entry_var) in enumerate(fields):
    tk.Label(tab_create, text=label_text, bg="#FFDDC1").grid(row=idx + 1, column=0, padx=5, pady=5)
    entry_var = tk.Entry(tab_create)
    entry_var.grid(row=idx + 1, column=1, padx=5, pady=5)
    entries[entry_var] = entry_var

energy_type_var = tk.StringVar()
energy_type_dropdown = ttk.Combobox(tab_create, textvariable=energy_type_var, values=["KW", "KVA"], state="readonly")
energy_type_dropdown.grid(row=12, column=1, padx=5, pady=5)

output_create = ScrolledText(tab_create, height=10, width=100, bg="#E9E9E9")
output_create.grid(row=13, column=0, columnspan=3, padx=5, pady=10)

table_frame = tk.Frame(tab_create, bg="#FFDDC1")
table_frame.grid(row=14, column=0, columnspan=2, padx=5, pady=10)

execute_button = tk.Button(tab_create, text="Execute", command=execute_selected_file, bg="green", fg="white", font=("Arial", 12, "bold"))
execute_button.grid(row=15, column=0, columnspan=3, pady=10)
















'''---------------------------------------------------------------------------------------------------------------------------------------------------'''

# Tab 2: Read Daily Energy Consumption
tab_read_energy = tk.Frame(tab_control, bg="#C8E6C9")
tab_control.add(tab_read_energy, text='Read Daily Energy Consumption')

# Add URL label at the top of the tab
url_label = tk.Label(
    tab_read_energy,
    text="Read Daily Energy Consumption\nhttps://engine-web.gomatimvvnl.in/daily_energy_consumption/{account_id}/",
    font=("Times New Roman", 10, "bold"),
    fg="#0000b3",  # Set the font color to blue
    bg="#C8E6C9"   # Match the tab's background color
)
url_label.grid(row=0, column=0, columnspan=2, padx=200, pady=10, sticky="w")

# Add input for Account ID
tk.Label(tab_read_energy, text="Account ID:", bg="#C8E6C9").grid(row=1, column=0, padx=5, pady=5, sticky="e")
entry_read_energy = tk.Entry(tab_read_energy, width=25)  # Adjusted width for better alignment
entry_read_energy.grid(row=1, column=1, padx=5, pady=5, sticky="w")

# Add Execute Button close to Entry
btn_read_energy = tk.Button(tab_read_energy, text="Execute", command=call_read_daily_energy_consumption, bg="#33ff33", font=("Times New Roman", 10, "bold"))
btn_read_energy.grid(row=2, column=1, padx=10, pady=5, sticky="w")

# Add ScrolledText for Output with Black Text and Times New Roman Font
output_read_energy = ScrolledText(tab_read_energy, height=25, width=110, font=("Times New Roman", 10), fg="black")
output_read_energy.grid(row=3, column=0, columnspan=2, padx=10, pady=10)



'''-----------------------------------------------------------------------------------------------------------------------------------------------'''

# Tab 3: Trigger Daily Prepaid Ledger Task with Progress Bar
tab_trigger_task = tk.Frame(tab_control, bg="#BBDEFB")
tab_control.add(tab_trigger_task, text='Trigger Daily Prepaid Ledger Task')

# Add URL label at the top of the tab
url_label = tk.Label(
    tab_trigger_task,
    text="Daily Prepaid Ledger Task Trigger\nhttps://engine-web.gomatimvvnl.in/trigger_task/daily_ledger_task",
    font=("Times New Roman", 10, "bold"),
    fg="#0000b3",  # Set the font color to blue
    bg="#BBDEFB"   # Match the tab's background color
)
url_label.grid(row=0, column=0, columnspan=2, padx=200, pady=10, sticky="w")

# Input fields and labels for Trigger Task
tk.Label(tab_trigger_task, text="Account IDs :", bg="#BBDEFB").grid(row=1, column=0, padx=10, pady=0, sticky="e")
account_id_entry = tk.Entry(tab_trigger_task, width=30)
account_id_entry.grid(row=1, column=1, padx=10, pady=0, sticky="w")

tk.Label(tab_trigger_task, text="Start Date-Time :", bg="#BBDEFB").grid(row=2, column=0, padx=10, pady=5, sticky="e")
start_datetime_entry = tk.Entry(tab_trigger_task, width=30)
start_datetime_entry.grid(row=2, column=1, padx=10, pady=5, sticky="w")
start_datetime_entry.insert(0, "2024-09-01 00:00:00")

tk.Label(tab_trigger_task, text="End Date-Time :", bg="#BBDEFB").grid(row=3, column=0, padx=10, pady=5, sticky="e")
end_datetime_entry = tk.Entry(tab_trigger_task, width=30)
end_datetime_entry.grid(row=3, column=1, padx=10, pady=5, sticky="w")
end_datetime_entry.insert(0, "2024-09-30 00:00:00")

# Add Execute Button
btn_trigger_task = tk.Button(tab_trigger_task, text="Execute", command=start_execution_with_progress, bg="#33ff33", font=("Times New Roman", 10, "bold"))
btn_trigger_task.grid(row=4, column=1, padx=10, pady=10, sticky="w")

# Add Progress Bar
progress = ttk.Progressbar(tab_trigger_task, orient="horizontal", length=500, mode="determinate")
progress.grid(row=6, column=0, columnspan=2, padx=10, pady=10)

# Add ScrolledText for Output
output_text = ScrolledText(tab_trigger_task, height=20, width=80, font=("Times New Roman", 10), fg="black")
output_text.grid(row=5, column=0, columnspan=2, padx=10, pady=10)


'''---------------------------------------------------------------------------------------------------------------------------------------------------'''

# Tab 4: Read Daily Prepaid Ledger History
tab_read_ledger = tk.Frame(tab_control, bg="#BBDEFB")
tab_control.add(tab_read_ledger, text='Read Daily Prepaid Ledger History')

# Add URL label at the top of the tab
url_label = tk.Label(
    tab_read_ledger,
    text="Read Daily Prepaid Ledger History\nhttps://engine-web.gomatimvvnl.in/daily_prepaid_ledger/{account_id}/",
    font=("Times New Roman", 10, "bold"),
    fg="#0000b3",  # Set the font color to blue
    bg="#BBDEFB"   # Match the tab's background color
)
url_label.grid(row=0, column=0, columnspan=2, padx=200, pady=10, sticky="w")

# Add input for Account ID
tk.Label(tab_read_ledger, text="Account ID:", bg="#BBDEFB").grid(row=1, column=0, padx=5, pady=5, sticky="e")
entry_read_ledger = tk.Entry(tab_read_ledger, width=25)  # Adjusted width for better alignment
entry_read_ledger.grid(row=1, column=1, padx=5, pady=5, sticky="w")

# Add Execute Button close to Entry
btn_read_ledger = tk.Button(tab_read_ledger, text="Execute", command=call_read_daily_prepaid_ledger_history, bg="#33ff33", font=("Times New Roman", 10, "bold"))
btn_read_ledger.grid(row=2, column=1, padx=10, pady=5, sticky="w")

# Add ScrolledText for Output with black text and Times New Roman font
output_read_ledger = ScrolledText(tab_read_ledger, height=25, width=110, font=("Times New Roman", 10), fg="black")
output_read_ledger.grid(row=3, column=0, columnspan=2, padx=10, pady=10)



tab_control.pack(expand=1, fill="both")

app.mainloop()
