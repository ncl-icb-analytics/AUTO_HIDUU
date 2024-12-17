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

    # Get paths
    input_path = os.getenv('INPUT_FOLDER_PATH')
    hiduu_dir = os.getenv('HIDUU_DIRECTORY')

    # Get credentials for both tenants
    auth_credentials = {
        'NLHCR_SAID': os.getenv('NLHCR_SAID'),
        'NLHCR_SAS': os.getenv('NLHCR_SAS'),
        'NLHCR_SID': os.getenv('NLHCR_SID'),
        'NLHCR2_SAID': os.getenv('NLHCR2_SAID'),
        'NLHCR2_SAS': os.getenv('NLHCR2_SAS'),
        'NLHCR2_SID': os.getenv('NLHCR2_SID')
    }

    # Validate required settings
    if not all([input_path, hiduu_dir] + list(auth_credentials.values())):
        raise ValueError("Missing required environment variables. Please check your .env file.")

    # Start the upload process
    process_and_upload_files(input_path, hiduu_dir, auth_credentials, dataset_files)

if __name__ == '__main__':
    main()
