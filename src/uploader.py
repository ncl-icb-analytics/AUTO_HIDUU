import os
import re
import subprocess
from .csv_validator import validate_csv

class FileUploader:
    def __init__(self, input_path, hiduu_dir, auth_credentials, upload_config):
        self.input_path = input_path
        self.hiduu_dir = hiduu_dir
        self.auth = auth_credentials
        self.upload_config = upload_config
        self.uploaded_files = []
        self.failed_files = []

    def upload_files(self, dataset_files):
        """Process and upload all matching files in the input directory"""
        try:
            self._verify_directories()
            files = self._get_input_files()
            if files:
                self._process_files(files, dataset_files)
                self._print_summary()
        except Exception as e:
            print(f"\nCritical error occurred: {str(e)}")

    def _verify_directories(self):
        """Verify that required directories exist"""
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input directory not found: {self.input_path}")
        if not os.path.exists(self.hiduu_dir):
            raise FileNotFoundError(f"HIDUU directory not found: {self.hiduu_dir}")

    def _get_input_files(self):
        """Get list of CSV/TXT files from input directory"""
        files = [f for f in os.listdir(self.input_path) 
                if f.lower().endswith(('.csv', '.txt'))]
        
        if not files:
            print("No .csv or .txt files found in input directory")
            return None
            
        print(f"Detected {len(files)} files in {self.input_path}:")
        for f in files:
            print(f"  - {f}")
        return files

    def _process_files(self, files, dataset_files):
        """Process each file against dataset configurations"""
        for file in files:
            matched = False
            for dataset_name, config in dataset_files.items():
                if re.match(config['filename_pattern'], file):
                    matched = True
                    self._process_single_file(file, config)
                    break
            
            if not matched:
                print(f"\nUnexpected file format found: {file}")
                self.failed_files.append(
                    (file, "Unexpected file format - does not match any configured patterns")
                )

    def _process_single_file(self, file, config):
        """Process and validate a single file"""
        file_path = os.path.join(self.input_path, file)
        
        # Extract the date from filename
        date_match = re.search(r'\d{8}', file)
        if not date_match:
            self.failed_files.append((file, "Could not extract date from filename"))
            print(f"Error: Could not extract date from filename: {file}")
            return
            
        file_date = date_match.group()
        
        # Validate file contents
        print(f"\nValidating {file}...")
        is_valid, validation_message = validate_csv(file_path, config)
        if not is_valid:
            self.failed_files.append((file, f"Validation failed: {validation_message}"))
            print(f"Validation failed for {file}:")
            print(validation_message)
            return
        
        print(f"Validation passed for {file}")
        self._upload_file(file, file_path, file_date, config)

    def _upload_file(self, file, file_path, file_date, config):
        """Upload file using HIDUU utility"""
        cmd = (f'hi-data-upload-utility uploadDataSetFile '
                f'-said {self.auth["said"]} -sas {self.auth["sas"]} -sid {self.auth["sid"]} '
                f'-dsid {config["target_hei_dataset"]} -sv {self.upload_config["spec_version"]} '
                f'-fid {self.upload_config["file_id"]} -rl {file_date} -f {file_path} '
                f'-re "{self.upload_config["reason"]} {file_date}"')
        
        try:
            print(f"\nUploading {file}...")
            result = subprocess.run(cmd, cwd=self.hiduu_dir, shell=True, check=True, 
            capture_output=True, text=True)
            self.uploaded_files.append((file, config["target_hei_dataset"]))
            print(f"Successfully uploaded {file}")
            print(f"Command output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.failed_files.append((file, f"Upload failed: {str(e)}\nOutput: {e.output}"))
            print(f"Error uploading {file}: {e}")
            print(f"Error output: {e.stderr}")

    def _print_summary(self):
        """Print summary of upload process"""
        print("\n" + "="*50)
        print("UPLOAD PROCESS SUMMARY")
        print("="*50)
        print(f"Total files processed: {len(self.uploaded_files) + len(self.failed_files)}")
        print(f"Successfully uploaded: {len(self.uploaded_files)}")
        print(f"Failed to upload: {len(self.failed_files)}")

        if self.uploaded_files:
            print("\nSuccessfully uploaded files:")
            for file, dsid in self.uploaded_files:
                print(f"  ✓ {file} -> Dataset ID: {dsid}")

        if self.failed_files:
            print("\nFailed files:")
            for file, error in self.failed_files:
                print(f"  ✗ {file}")
                print(f"    Error: {error}")