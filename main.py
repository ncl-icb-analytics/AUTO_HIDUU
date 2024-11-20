"""
This is the main entry point for the file upload application.
"""

import os
from dotenv import load_dotenv
from config.dataset_config import dataset_files
from src.uploader import process_and_upload_files

def main():
    # Load settings from .env
    load_dotenv()

    # Get paths and credentials
    input_path = os.getenv('INPUT_FOLDER_PATH')
    hiduu_dir = os.getenv('HIDUU_DIRECTORY')
    auth_credentials = {
        'said': os.getenv('SAID'),
        'sas': os.getenv('SAS'),
        'sid': os.getenv('SID')
    }

    # Validate required settings
    if not all([input_path, hiduu_dir, auth_credentials['said'], 
                auth_credentials['sas'], auth_credentials['sid']]):
        raise ValueError("Missing required environment variables. Please check your .env file.")

    # Start the upload process
    process_and_upload_files(input_path, hiduu_dir, auth_credentials, dataset_files)

if __name__ == '__main__':
    main()
