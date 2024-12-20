import os

def clear_multiple_csv_files(file_paths):
    for file_path in file_paths:
        with open(file_path, 'w', newline='') as file:
            pass  # Just open and close the file to clear its contents

# Example usage
file_paths = [
    'updated_data.csv',
    'api_respone_data.csv',
    # 'path_to_third_csv_file.csv'
]

clear_multiple_csv_files(file_paths)
