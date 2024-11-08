import os
from dotenv import load_dotenv
from config import dataset_files
from src import FileUploader

def main():
    # Load environment variables
    load_dotenv()

    # Get configuration from environment variables
    input_path = os.getenv('INPUT_FOLDER_PATH')
    hiduu_dir = os.getenv('HIDUU_DIRECTORY')
    
    # Authentication credentials
    auth_credentials = {
        'said': os.getenv('SAID'),
        'sas': os.getenv('SAS'),
        'sid': os.getenv('SID')
    }
    
    # Upload configuration
    upload_config = {
        'reason': os.getenv('UPLOAD_REASON', 'Uploaded files dated:'),
        'spec_version': os.getenv('SPEC_VERSION', '1'),
        'file_id': os.getenv('FILE_ID', 'SINGLE_FILE')
    }

    # Validate required environment variables
    required_vars = [input_path, hiduu_dir] + list(auth_credentials.values())
    if not all(required_vars):
        raise ValueError("Missing required environment variables. Please check your .env file.")

    # Initialize uploader and process files
    uploader = FileUploader(input_path, hiduu_dir, auth_credentials, upload_config)
    uploader.upload_files(dataset_files)

if __name__ == '__main__':
    main()
