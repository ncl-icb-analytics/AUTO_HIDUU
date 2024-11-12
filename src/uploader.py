import os
import re
import subprocess
from .validator import validate_file

class FileUploader:
    def __init__(self, input_path, hiduu_dir, auth_credentials, upload_config):
        self.input_path = input_path
        self.hiduu_dir = hiduu_dir
        self.auth = auth_credentials
        self.upload_config = upload_config
        self.uploaded_files = []
        self.failed_files = []

    def upload_files(self, dataset_files):
        """Upload all valid files from the input directory"""
        try:
            # Check directories exist
            if not os.path.exists(self.input_path):
                raise FileNotFoundError(f"Input directory not found: {self.input_path}")
            if not os.path.exists(self.hiduu_dir):
                raise FileNotFoundError(f"HIDUU directory not found: {self.hiduu_dir}")

            # Get CSV/TXT files
            files = [f for f in os.listdir(self.input_path) 
                    if f.lower().endswith(('.csv', '.txt'))]
            
            if not files:
                print("No .csv or .txt files found")
                return

            print(f"\nFound {len(files)} files to process:")
            for f in files:
                print(f"  - {f}")

            # Process each file
            for file in files:
                self._process_file(file, dataset_files)

            # Print results
            self._print_summary()

        except Exception as e:
            print(f"\nError: {str(e)}")

    def _process_file(self, file, dataset_files):
        """Process a single file"""
        # Find matching dataset config
        config = None
        for dataset_config in dataset_files.values():
            if re.match(dataset_config['filename_pattern'], file):
                config = dataset_config
                break
        
        if not config:
            print(f"\nSkipping {file} - filename doesn't match any expected patterns")
            self.failed_files.append((file, "Unexpected filename format"))
            return

        # Get date from filename
        date_match = re.search(r'\d{8}', file)
        if not date_match:
            print(f"\nSkipping {file} - can't find date in filename")
            self.failed_files.append((file, "No date in filename"))
            return

        # Validate file
        file_path = os.path.join(self.input_path, file)
        print(f"\nValidating {file}...")
        is_valid, message = validate_file(file_path, config)
        
        if not is_valid:
            print(f"Validation failed: {message}")
            self.failed_files.append((file, f"Invalid file: {message}"))
            return

        # Upload file
        print("Validation passed, uploading...")
        self._upload_file(file, file_path, date_match.group(), config)

    def _upload_file(self, file, file_path, file_date, config):
        """Upload file using HIDUU utility"""
        cmd = (f'hi-data-upload-utility uploadDataSetFile '
               f'-said {self.auth["said"]} '
               f'-sas {self.auth["sas"]} '
               f'-sid {self.auth["sid"]} '
               f'-dsid {config["target_hei_dataset"]} '
               f'-sv {self.upload_config["spec_version"]} '
               f'-fid {self.upload_config["file_id"]} '
               f'-rl {file_date} '
               f'-f {file_path} '
               f'-re "{self.upload_config["reason"]} {file_date}"')
        
        try:
            result = subprocess.run(cmd, cwd=self.hiduu_dir, shell=True, 
                                 check=True, capture_output=True, text=True)
            self.uploaded_files.append((file, config["target_hei_dataset"]))
            print(f"Successfully uploaded {file}")
            print(f"Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.failed_files.append((file, f"Upload failed: {e.stderr}"))
            print(f"Error uploading {file}")
            print(f"Error details: {e.stderr}")

    def _print_summary(self):
        """Print summary of what happened"""
        print("\n" + "="*50)
        print("UPLOAD SUMMARY")
        print("="*50)
        print(f"Total files: {len(self.uploaded_files) + len(self.failed_files)}")
        print(f"Uploaded: {len(self.uploaded_files)}")
        print(f"Failed: {len(self.failed_files)}")

        if self.uploaded_files:
            print("\nSuccessful uploads:")
            for file, dataset_id in self.uploaded_files:
                print(f"  ✓ {file} -> {dataset_id}")

        if self.failed_files:
            print("\nFailed files:")
            for file, error in self.failed_files:
                print(f"  ✗ {file}")
                print(f"    Reason: {error}")