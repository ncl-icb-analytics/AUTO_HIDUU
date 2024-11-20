"""
This module handles the file upload process using the HIDUU utility.
It processes CSV/TXT files from an input directory and:
- Matches files against expected filename patterns
- Extracts dates from filenames
- Validates file contents
- Uploads valid files using HIDUU
- Moves successful uploads to a processed directory
- Provides upload summary
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
from dataclasses import dataclass
from src.validator import validate_file
from config.dataset_config import DEFAULT_CONFIG

@dataclass
class UploadContext:
    """Configuration context for file uploads"""
    input_path: str
    processed_dir: Path
    hiduu_dir: str
    auth_credentials: dict
    dataset_config: dict

def process_and_upload_files(input_path, hiduu_dir, auth_credentials, dataset_files):
    """
    Process and upload all valid files from the input directory.
    Returns a tuple of (uploaded_files, failed_files) for tracking results.
    """
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
            return uploaded_files, failed_files

        print(f"\nFound {len(all_files)} files in input directory:")
        for f in all_files:
            print(f"  - {f}")

        # Match files to dataset configurations
        matching_files = _find_matching_files(all_files, dataset_files)
        if not matching_files:
            print("\nNo files match any configured dataset patterns")
            return uploaded_files, failed_files

        # Create processed directory
        processed_dir = _create_processed_directory(input_path)

        # Process each matching file
        for file, dataset_name in matching_files:
            context = UploadContext(
                input_path=input_path,
                processed_dir=processed_dir,
                hiduu_dir=hiduu_dir,
                auth_credentials=auth_credentials,
                dataset_config=dataset_files[dataset_name]
            )
            _process_single_file(file, context, uploaded_files, failed_files)

        _print_summary(uploaded_files, failed_files)
        return uploaded_files, failed_files

    except Exception as e:
        print(f"\nError: {str(e)}")
        return uploaded_files, failed_files

def _find_matching_files(all_files, dataset_files):
    """Match files against dataset patterns and return matches"""
    matching_files = []
    for file in all_files:
        for dataset_name, dataset_config in dataset_files.items():
            if re.match(dataset_config['filename_pattern'], file):
                matching_files.append((file, dataset_name))
                break
    
    if matching_files:
        print(f"\nFound {len(matching_files)} files matching dataset patterns:")
        for file, dataset in matching_files:
            print(f"  - {file} -> {dataset}")
            
    return matching_files

def _create_processed_directory(input_path):
    """Create and return the processed directory path"""
    today = datetime.now().strftime('%Y%m%d')
    processed_dir = Path(input_path) / 'processed' / today
    processed_dir.mkdir(parents=True, exist_ok=True)
    return processed_dir

def _process_single_file(file, context, uploaded_files, failed_files):
    """Process and upload a single file"""
    file_path = os.path.join(context.input_path, file)
    
    # Extract date from filename
    date_match = re.search(r'\d{8}', file)
    if not date_match:
        print(f"\nSkipping {file} - can't find date in filename")
        failed_files.append((file, "No date in filename"))
        return

    # Validate file
    print(f"\nValidating {file}...")
    is_valid, message = validate_file(file_path, context.dataset_config)
    
    if not is_valid:
        print(f"Validation failed: {message}")
        failed_files.append((file, f"Invalid file: {message}"))
        return

    # Upload and process successful files
    print("Validation passed, uploading...")
    if _upload_file(file, file_path, date_match.group(), context, uploaded_files, failed_files):
        _move_to_processed(file, file_path, context.processed_dir)

def _upload_file(file, file_path, file_date, context, uploaded_files, failed_files):
    """Upload file using HIDUU utility and return success status"""
    # Get configuration values with fallbacks to defaults
    reason = context.dataset_config.get('upload_reason', DEFAULT_CONFIG['upload_reason'])
    spec_version = context.dataset_config.get('spec_version', DEFAULT_CONFIG['spec_version'])
    file_id = context.dataset_config.get('file_id', DEFAULT_CONFIG['file_id'])
    
    cmd = (f'hi-data-upload-utility uploadDataSetFile '
           f'-said {context.auth_credentials["said"]} '
           f'-sas {context.auth_credentials["sas"]} '
           f'-sid {context.auth_credentials["sid"]} '
           f'-dsid {context.dataset_config["target_hei_dataset"]} '
           f'-sv {spec_version} '
           f'-fid {file_id} '
           f'-rl {file_date} '
           f'-f {file_path} '
           f'-re "{reason} {file_date}"')
    
    try:
        result = subprocess.run(cmd, cwd=context.hiduu_dir, shell=True, 
                             check=True, capture_output=True, text=True)
        uploaded_files.append((file, context.dataset_config["target_hei_dataset"]))
        print(f"Successfully uploaded {file}")
        print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        failed_files.append((file, f"Upload failed: {e.stderr}"))
        print(f"Error uploading {file}")
        print(f"Error details: {e.stderr}")
        return False

def _move_to_processed(file, file_path, processed_dir):
    """Move successfully processed file to the processed directory"""
    try:
        shutil.move(file_path, processed_dir / file)
        print(f"Moved {file} to processed directory dated {processed_dir.name}")
    except Exception as e:
        print(f"Warning: Could not move {file} to processed directory: {str(e)}")

def _print_summary(uploaded_files, failed_files):
    """Print summary of upload results"""
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