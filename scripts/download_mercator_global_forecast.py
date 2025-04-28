"""
CMEMS Data Download Automation Script

Description:
    This script automates the download of oceanographic data from the Copernicus Marine Service (CMEMS).
    It handles multiple datasets including currents, salinity, temperature, and sea surface height.
    The script filters files based on date ranges and manages temporary download lists.

Features:
    - Downloads multiple CMEMS datasets in one run
    - Filters 6-hourly data files by specified date range
    - Handles both PT6H (6-hourly) and PT1H (hourly) datasets
    - Automatically cleans up temporary files
    - Provides progress logging

Usage:
    1. Set your CMEMS credentials in the environment or copernicusmarine configuration
    2. Modify the output_directory variable to your desired location
    3. Run the script (no arguments needed)

Configuration:
    - dataset_config: List of tuples mapping dataset IDs to temporary filenames
    - output_directory: Where downloaded files will be saved
    - Date range: Automatically set to current date + 12 hours through 7 days later

Requirements:
    - Python 3.x
    - copernicusmarine package
    - Valid CMEMS credentials

Author: Zhiguo Mei
Date Created: 20250301
Last Modified: 20250301
Version: 1.0

"""

import copernicusmarine
from datetime import date
import datetime
import os

# [Rest of your existing code follows...]

# Define dataset ID to temporary file mapping
dataset_config = [
    ('cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i', 'cur_PT6H.txt'),
    ('cmems_mod_glo_phy-so_anfc_0.083deg_PT6H-i', 'salt_PT6H.txt'),
    ('cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i', 'temp_PT6H.txt'),
    ('cmems_mod_glo_phy_anfc_merged-sl_PT1H-i', 'ssh_PT1H.txt')
]

# Get today's date (format: YYYY-MM-DD)
today = date.today().strftime("%Y%m%d")  # Convert to string format
start_date = datetime.datetime.strptime(today, "%Y%m%d")
start_time = start_date + datetime.timedelta(hours=12)
end_time = start_time + datetime.timedelta(days=7)

# Create output directories
output_directory = os.path.join('/data/nwpoms-forecast/MERCATOR/', start_time.strftime('%Y-%m-%d'))
os.makedirs(output_directory, exist_ok=True)

def extract_date_from_filename(file, dataset_type):
    """Extract datetime from filename based on dataset type"""
    if "PT6H" in dataset_type:
        # Format: glo12_rg_6h-i_20250427-12h_3D-thetao_fcst_R20250427.nc
        parts = file.split('_')
        datetime_str = parts[3]  # '20250427-12h'
        date_part, time_part = datetime_str.split('-')
        year = int(date_part[:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])
        hour = int(time_part[:2])
    else:
        # Format: MOL_20250426_R20250427.nc (SSH files)
        parts = file.split('_')
        date_str = parts[1]  # '20250426'
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        hour = 12  # Default hour for daily SSH files

    return datetime.datetime(year, month, day, hour)


# Process each dataset and corresponding temporary file
for dataset_id, temp_file in dataset_config:
    print(f"\n{'=' * 40}\nProcessing dataset: {dataset_id}\nTemporary file: {temp_file}\n{'=' * 40}")

    # Clear temp files with existence check
    if os.path.exists(temp_file):
        os.remove(temp_file)
        print(f"Temporary file {temp_file} has been removed")
    else:
        print(f"Temporary file {temp_file} does not exist, skipping deletion")

    # Generate file list
    copernicusmarine.get(
        dataset_id=dataset_id,
        filter=f'*{today}*',
        dry_run=True,
        create_file_list=temp_file
    )

    filtered_lines = []

    # Read file content
    with open(temp_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:  # Skip empty lines
            continue

        # Extract filename from path
        filename = line.split('/')[-1]

        try:
            # Parse datetime from filename
            file_dt = extract_date_from_filename(filename, dataset_id)

            # Check if within time range
            if start_time <= file_dt <= end_time:
                filtered_lines.append(line)
        except (IndexError, ValueError) as e:
            print(f"Failed to parse datetime from filename: {filename}, error: {e}")
            continue

    # Write filtered results back to file
    with open(temp_file, 'w') as f:
        f.write('\n'.join(filtered_lines))

    # Execute download
    if filtered_lines:
        copernicusmarine.get(
            dataset_id=dataset_id,
            file_list=temp_file,
            output_directory=output_directory,
            no_directories=True
        )
        print(f"Download completed: {len(filtered_lines)} files")
    else:
        print(f"No matching data to download for: {dataset_id}")

    # Delete temporary file regardless of success
    if os.path.exists(temp_file):
        os.remove(temp_file)
        print(f"Cleaned up temporary file: {temp_file}")

print("\nAll datasets processed successfully!")