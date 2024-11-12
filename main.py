import os
from dotenv import load_dotenv
from config import dataset_files
from src import upload_files

def main():
    # Load our settings from the .env file
    load_dotenv()

    # Get paths from our environment settings
    input_path = os.getenv('INPUT_FOLDER_PATH')    # Where to find the files
    hiduu_dir = os.getenv('HIDUU_DIRECTORY')       # Where the upload tool is installed
    
    # Get authentication details from our environment settings
    auth_credentials = {
        'said': os.getenv('SAID'),
        'sas': os.getenv('SAS'),
        'sid': os.getenv('SID')
    }
    
    # Settings for the upload process
    upload_config = {
        'reason': os.getenv('UPLOAD_REASON', 'Uploaded files dated:'),
        'spec_version': os.getenv('SPEC_VERSION', '1'),
        'file_id': os.getenv('FILE_ID', 'SINGLE_FILE')
    }

    # Make sure we have all required settings
    if not all([input_path, hiduu_dir, auth_credentials['said'], 
                auth_credentials['sas'], auth_credentials['sid']]):
        raise ValueError("Missing required environment variables. Please check your .env file.")

    # Start the upload process
    upload_files(input_path, hiduu_dir, auth_credentials, upload_config, dataset_files)

if __name__ == '__main__':
    main()
