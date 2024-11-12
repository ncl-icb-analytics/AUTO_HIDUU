import os
import re
import subprocess
from .validator import validate_file

def upload_files(input_path, hiduu_dir, auth_credentials, upload_config, dataset_files):
    """Upload all valid files from the input directory"""
    uploaded_files = []
    failed_files = []

    try:
        # Verify directories
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input directory not found: {input_path}")
        if not os.path.exists(hiduu_dir):
            raise FileNotFoundError(f"HIDUU directory not found: {hiduu_dir}")

        # Get files to process
        files = [f for f in os.listdir(input_path) 
                if f.lower().endswith(('.csv', '.txt'))]
        
        if not files:
            print("No .csv or .txt files found")
            return

        print(f"\nFound {len(files)} files to process:")
        for f in files:
            print(f"  - {f}")

        # Process each file
        for file in files:
            process_file(file, input_path, hiduu_dir, auth_credentials, 
                        upload_config, dataset_files, uploaded_files, failed_files)

        # Show results
        print_summary(uploaded_files, failed_files)

    except Exception as e:
        print(f"\nError: {str(e)}")

def process_file(file, input_path, hiduu_dir, auth, upload_config, dataset_files, 
                uploaded_files, failed_files):
    """Process a single file"""
    # Find matching config
    config = None
    for dataset_config in dataset_files.values():
        if re.match(dataset_config['filename_pattern'], file):
            config = dataset_config
            break
    
    if not config:
        print(f"\nSkipping {file} - filename doesn't match any expected patterns")
        failed_files.append((file, "Unexpected filename format"))
        return

    # Get date from filename
    date_match = re.search(r'\d{8}', file)
    if not date_match:
        print(f"\nSkipping {file} - can't find date in filename")
        failed_files.append((file, "No date in filename"))
        return

    # Validate file
    file_path = os.path.join(input_path, file)
    print(f"\nValidating {file}...")
    is_valid, message = validate_file(file_path, config)
    
    if not is_valid:
        print(f"Validation failed: {message}")
        failed_files.append((file, f"Invalid file: {message}"))
        return

    # Upload file
    print("Validation passed, uploading...")
    upload_file(file, file_path, date_match.group(), config, hiduu_dir, 
                auth, upload_config, uploaded_files, failed_files)

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
    except subprocess.CalledProcessError as e:
        failed_files.append((file, f"Upload failed: {e.stderr}"))
        print(f"Error uploading {file}")
        print(f"Error details: {e.stderr}")

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