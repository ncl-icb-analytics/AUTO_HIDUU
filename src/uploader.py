"""
This module handles the file upload process using the HIDUU utility.
It processes CSV/TXT files from an input directory and:
- Matches files against expected filename patterns
- Extracts dates from filenames
- Calls the validator module to check file contents (see validator.py for validation rules)
- Only uploads files that pass all validation checks
- Uses HIDUU command line tool for uploading valid files
- Tracks and reports upload successes/failures

Files that fail validation are skipped and reported in the final summary.
Successfully uploaded files are moved to a dated 'processed' directory.
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
from .validator import validate_file

def process_directory(input_path, hiduu_dir, auth_credentials, upload_config, dataset_files):
    """Upload all valid files from the input directory"""
    uploaded_files = []
    failed_files = []
    
    try:
        # Verify directories exist
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input directory not found: {input_path}")
        if not os.path.exists(hiduu_dir):
            raise FileNotFoundError(f"HIDUU directory not found: {hiduu_dir}")

        # Get all CSV/TXT files
        all_files = [f for f in os.listdir(input_path) 
                    if f.lower().endswith(('.csv', '.txt'))]
        
        if not all_files:
            print("No .csv or .txt files found in input directory")
            return

        print(f"\nFound {len(all_files)} files in input directory:")
        for f in all_files:
            print(f"  - {f}")

        # Find files that match our dataset patterns
        matching_files = []
        for file in all_files:
            for dataset_name, dataset_config in dataset_files.items():
                if re.match(dataset_config['filename_pattern'], file):
                    matching_files.append((file, dataset_name))
                    break

        if not matching_files:
            print("\nNo files match any configured dataset patterns")
            return

        print(f"\nFound {len(matching_files)} files matching dataset patterns:")
        for file, dataset in matching_files:
            print(f"  - {file} -> {dataset}")

        # Create processed directory with today's date
        today = datetime.now().strftime('%Y%m%d')
        processed_dir = Path(input_path) / 'processed' / today
        processed_dir.mkdir(parents=True, exist_ok=True)

        # Process matching files
        for file, dataset_name in matching_files:
            process_file(
                file=file,
                input_path=input_path,
                processed_dir=processed_dir,
                hiduu_dir=hiduu_dir,
                auth=auth_credentials,
                upload_config=upload_config,
                dataset_config=dataset_files[dataset_name],
                uploaded_files=uploaded_files,
                failed_files=failed_files
            )

        # Show results
        print_summary(uploaded_files, failed_files)

    except Exception as e:
        print(f"\nError: {str(e)}")

def process_file(file, input_path, processed_dir, hiduu_dir, auth, upload_config, 
                dataset_config, uploaded_files, failed_files):
    """Process a single file"""
    file_path = os.path.join(input_path, file)
    
    # Get date from filename for HIDUU upload
    date_match = re.search(r'\d{8}', file)
    if not date_match:
        print(f"\nSkipping {file} - can't find date in filename")
        failed_files.append((file, "No date in filename"))
        return

    # Validate file
    print(f"\nValidating {file}...")
    is_valid, message = validate_file(file_path, dataset_config)
    
    if not is_valid:
        print(f"Validation failed: {message}")
        failed_files.append((file, f"Invalid file: {message}"))
        return

    # Upload file
    print("Validation passed, uploading...")
    success = upload_file(file, file_path, date_match.group(), dataset_config, 
                         hiduu_dir, auth, upload_config, uploaded_files, failed_files)
    
    # Move successful uploads to processed directory
    if success:
        try:
            shutil.move(file_path, processed_dir / file)
            print(f"Moved {file} to processed directory dated {processed_dir.name}")
        except Exception as e:
            print(f"Warning: Could not move {file} to processed directory: {str(e)}")

def upload_file(file, file_path, file_date, config, hiduu_dir, auth, 
                upload_config, uploaded_files, failed_files):
    """Upload file using HIDUU utility"""
    cmd = (f'hi-data-upload-utility uploadDataSetFile '
           f'-said {auth["said"]} '
           f'-sas {auth["sas"]} '
           f'-sid {auth["sid"]} '
           f'-dsid {config["target_hei_dataset"]} '
           f'-sv {upload_config["spec_version"]} '
           f'-fid {upload_config["file_id"]} '
           f'-rl {file_date} '
           f'-f {file_path} '
           f'-re "{upload_config["reason"]} {file_date}"')
    
    try:
        result = subprocess.run(cmd, cwd=hiduu_dir, shell=True, 
                             check=True, capture_output=True, text=True)
        uploaded_files.append((file, config["target_hei_dataset"]))
        print(f"Successfully uploaded {file}")
        print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        failed_files.append((file, f"Upload failed: {e.stderr}"))
        print(f"Error uploading {file}")
        print(f"Error details: {e.stderr}")
        return False

def print_summary(uploaded_files, failed_files):
    """Print summary of what happened"""
    print("\n" + "="*50)
    print("UPLOAD SUMMARY")
    print("="*50)
    print(f"Total files: {len(uploaded_files) + len(failed_files)}")
    print(f"Uploaded: {len(uploaded_files)}")
    print(f"Failed: {len(failed_files)}")

    if uploaded_files:
        print("\nSuccessful uploads:")
        for file, dataset_id in uploaded_files:
            print(f"  ✓ {file} -> {dataset_id}")

    if failed_files:
        print("\nFailed files:")
        for file, error in failed_files:
            print(f"  ✗ {file}")
            print(f"    Reason: {error}")