"""
This script automates the process of uploading files to the HealtheIntent platform using the 
'hi-data-upload-utility' (HIDUU). It scans a specified input directory for files with .csv or 
.txt extensions, matches them against a predefined set of dataset names, and executes a custom 
command to upload each matching file.

The variables 'said', 'sas', and 'sid' contain placeholders for authentication credentials required 
to connect to the HealtheIntent system. Replace them with the actual values provided by Oracle.

IMPORTANT: The dictionary 'dataset_files' maps the first part of the filename (excluding the date 
and extension) to their corresponding dataset IDs (dsid) in the HealtheIntent system. This mapping 
provides flexibility in naming the target datasets and ensures files are uploaded to the correct 
location. For example, the key 'SAMPLE_DATASET' should match files like 'SAMPLE_DATASET_20240515.csv' 
or 'SAMPLE_DATASET_20240515.txt' and the script will upload that file to the dataset id (dsid) specified.

Cerner Wiki for HIDUU command usage: https://wiki.cerner.com/pages/releaseview.action?pageId=1391627506

Author: Eddie Davison
Last Modified: 17-May-2024
"""

import subprocess
import os
import re

def run_custom_commands():
    # Define the input folder containing the input files and the directory for the HIDUU
    input_folder_path = r'C:\Users\Username\Directory\Folder'
    hiduu_directory = r'C:\Users\Username\Directory\hi-data-upload-utility-1.11\bin'

    # Variables
    upload_reason = 'Uploaded files dated:' # Reason for uploading the files
    sv = '1' # Spec version
    fid = 'SINGLE_FILE' # The ID of the file within the specific data set specification version e.g. SINGLE_FILE, FILE_1, FILE_2, etc.

    # Authentication credentials for the HealtheIntent system
    said = '1234567890' # System account ID
    sas = '1234567890' # System account secret
    sid = '1234567890' # Source ID

    # Dictionary mapping the first part of the filename (dataset_name) to their corresponding dataset IDs (dsid)
    # Expected filename format: {dataset_name}_{date}.csv or {dataset_name}_{date}.txt
    # Example: 'SAMPLE_DATASET_20240515.csv' matches to the key 'SAMPLE_DATASET' and uploads to the dataset in HealtheIntent with ID 'TARGET_DATASET_ID'
    dataset_files = {
        'SAMPLE_DATASET': 'TARGET_DATASET_ID',
        'SOME_OTHER_DATASET': 'ANOTHER_TARGET_DATASET_ID',
    }

    # Lists to keep track of uploaded and failed files
    uploaded_files = []
    failed_files = []

    try:
        # Get all .csv and .txt files in the input directory
        files = [f for f in os.listdir(input_folder_path) if f.endswith(('.csv', '.txt'))]
        print(f"Detected {len(files)} files in {input_folder_path}: {files}")

        for file in files:
            matched = False
            # Check if the file name matches any dataset pattern in the dictionary
            for dataset_name, dsid in dataset_files.items():
                match = re.match(f"{dataset_name}_(\d{{8}})\.(csv|txt)", file)
                if match:
                    matched = True
                    # Capture the date from the filename
                    file_date = match.group(1)
                    # Define the command to upload the file using HIDUU
                    cmd = f'hi-data-upload-utility uploadDataSetFile -said {said} -sas {sas} -sid {sid} -dsid {dsid} -sv {sv} -fid {fid} -rl {file_date} -f {os.path.join(input_folder_path, file)} -re "{upload_reason} {file_date}"'
                    try:
                        # Execute the command in the specified HIDUU directory
                        subprocess.run(cmd, cwd=hiduu_directory, shell=True, check=True)
                        uploaded_files.append((file, dsid))
                        print(f"Successfully uploaded {file}. Executed in {hiduu_directory}: {cmd}")
                    except subprocess.CalledProcessError as e:
                        failed_files.append((file, str(e)))
                        print(f"Error executing command for file {file}: {e}")
                    break
            
            if not matched:
                print(f"Unexpected file format found: {file}. Skipping this file.")
                failed_files.append((file, "Unexpected file format"))

    except Exception as e:
        print(f"An error occurred: {e}")

    # Summary of the upload process
    print("\nSummary of the upload process:")
    print(f"Total files detected: {len(files)}")
    print(f"Successfully uploaded: {len(uploaded_files)}")
    print(f"Failed to upload: {len(failed_files)}")

    if uploaded_files:
        print("\nUploaded files:")
        for file, dsid in uploaded_files:
            print(f"{file} -> {dsid}")

    if failed_files:
        print("\nFailed files:")
        for file, error in failed_files:
            print(f"{file} -> {error}")

if __name__ == '__main__':
    run_custom_commands()
