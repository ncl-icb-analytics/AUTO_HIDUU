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

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import re
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
from dataclasses import dataclass
from src.validator import validate_file
from config.dataset_config import DEFAULT_CONFIG
import fnmatch

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
    import fnmatch
    import re
    matching_files = []
    def clean_pattern(pattern):
        # Remove all backslashes
        pattern = pattern.replace('\\', '')
        # Replace 2 or more consecutive dots with same number of question marks
        pattern = re.sub(r'\.{2,}', lambda m: '?' * len(m.group(0)), pattern)
        return pattern
    for file in all_files:
        for dataset_name, dataset_config in dataset_files.items():
            raw_pattern = dataset_config['filename_pattern']
            pattern = clean_pattern(raw_pattern)
            print(f"Comparing file: '{file}' | raw pattern: '{raw_pattern}' | cleaned pattern: '{pattern}' | dataset: '{dataset_name}'")
            if fnmatch.fnmatch(file, pattern):
                print(f"MATCHED: {file} -> {pattern} (dataset: {dataset_name})")
                matching_files.append((file, dataset_name))
                break
    if matching_files:
        print(f"\nFound {len(matching_files)} files matching dataset patterns:")
        for file, dataset in matching_files:
            print(f"  - {file} -> {dataset}")
    else:
        print("No files match any configured dataset patterns")
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
    
    # Always use today's date unless we find a date in the filename
    file_date = datetime.now().strftime('%Y%m%d')
    
    # Try to extract date from filename if present
    date_match = re.search(r'\d{8}', file)
    if date_match:
        file_date = date_match.group()

    # Validate file
    print(f"\nValidating {file}...")
    is_valid, message, row_count = validate_file(file_path, context.dataset_config)
    
    if not is_valid:
        print(f"Validation failed: {message}")
        failed_files.append((file, f"Invalid file: {message}"))
        return

    # Upload and process successful files
    print("Validation passed, uploading...")
    if _upload_file(file, file_path, file_date, context, uploaded_files, failed_files, row_count):
        _move_to_processed(file, file_path, context.processed_dir)

def _upload_file(file, file_path, file_date, context, uploaded_files, failed_files, row_count):
    """Upload file using HIDUU utility and return success status"""
    # Get configuration values with fallbacks to defaults
    reason = context.dataset_config.get('upload_reason', DEFAULT_CONFIG['upload_reason'])
    spec_version = context.dataset_config.get('spec_version', DEFAULT_CONFIG['spec_version'])
    file_id = context.dataset_config.get('file_id', DEFAULT_CONFIG['file_id'])
    tenant = context.dataset_config.get('tenant', 'nlhcr')
    
    # Get tenant-specific credentials
    tenant_prefix = 'NLHCR2_' if tenant == 'nlhcr-2' else 'NLHCR_'
    credentials = {
        'said': context.auth_credentials[f'{tenant_prefix}SAID'],
        'sas': context.auth_credentials[f'{tenant_prefix}SAS'],
        'sid': context.auth_credentials[f'{tenant_prefix}SID']
    }
    
    cmd = (f'hi-data-upload-utility uploadDataSetFile '
           f'-said {credentials["said"]} '
           f'-sas {credentials["sas"]} '
           f'-sid {credentials["sid"]} '
           f'-dsid {context.dataset_config["target_hei_dataset"]} '
           f'-sv {spec_version} '
           f'-fid {file_id} '
           f'-rl {file_date} '
           f'-f {file_path} '
           f'-cm {tenant} '
           f'-re "{reason} {file_date}"')
    
    try:
        result = subprocess.run(cmd, cwd=context.hiduu_dir, shell=True, 
                             check=True, capture_output=True, text=True)
        
        # Check if HIDUU output indicates a failure (case insensitive)
        output_upper = result.stdout.upper()
        if "ERROR" in output_upper or "FAILED" in output_upper:
            failed_files.append((file, f"Upload failed: {result.stdout}"))
            print(f"Error uploading {file}")
            print(f"Error details: {result.stdout}")
            return False
            
        uploaded_files.append((file, context.dataset_config["target_hei_dataset"]))
        print(f"Successfully uploaded {file} with {row_count:,} rows")
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