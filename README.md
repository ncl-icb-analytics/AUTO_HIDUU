# AUTO_HIDUU

## Overview

**AUTO_HIDUU** is a Python script that automates the process of uploading files to the HealtheIntent platform using the hi-data-upload-utility (HIDUU). The script validates CSV/TXT files against predefined schemas and uploads them to the correct dataset on the HealtheIntent system.

## Features

* Validates CSV/TXT files against defined schemas:
  * Column presence and naming
  * Date format validation
  * Text length validation
  * Required field (non-null) validation
* Matches files to dataset IDs using configurable patterns
* Automatically uploads valid files to HealtheIntent using HIDUU
* Provides detailed validation feedback and upload summaries

## Prerequisites

* Python 3.x
* pandas library
* HIDUU installed and accessible (refer to the [Cerner Wiki for HIDUU command usage](https://wiki.cerner.com/pages/releaseview.action?pageId=1391627506))

## Setup

1. Clone the repository:

```bash
git clone https://github.com/EddieDavison92/AUTO_HIDUU.git
```

2. Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Create your environment configuration:

```bash
cp .env.example .env
```

4. Edit the `.env` file with your specific configuration:

```ini
# HealtheIntent Authentication Credentials
SAID=your_system_account_id
SAS=your_system_account_secret
SID=your_source_id

# Paths
INPUT_FOLDER_PATH=C:\Path\To\Input\Files
HIDUU_DIRECTORY=C:\Path\To\HIDUU\Installation

# Upload Configuration
UPLOAD_REASON=Uploaded files dated:
SPEC_VERSION=1
FILE_ID=SINGLE_FILE
```

5. Update the dataset configuration in `config/dataset_config.py` to match your requirements:

```python
dataset_files = {
    'dataset_name': {
        # Pattern to match filenames (must include 8-digit date)
        'filename_pattern': r'PATTERN_\d{8}\.(csv|txt)',
      
        # Minimum number of rows required
        'min_rows': 100,
      
        # Define the expected columns and their rules
        'schema': {
            'column_name': {
                'type': 'varchar',      # or 'date'
                'length': 50,           # maximum length for varchar
                'nullable': False,      # whether empty values are allowed
                'format': '%Y-%m-%d'    # format for dates
            }
            # ... more columns ...
        },
      
        # Target dataset ID in HealtheIntent
        'target_hei_dataset': 'TARGET_DATASET_ID'
    }
    # ... more datasets ...
}
```

## Usage

1. Place your CSV/TXT files in the configured input directory
2. Run the script:

```bash
python main.py
```

The script will:

1. Find all CSV/TXT files in the input directory
2. Check if each file matches a configured dataset pattern
3. Validate the file contents
4. Upload valid files to HealtheIntent
5. Show a summary of what happened

## File Naming Convention

Files must match the patterns you define in your dataset configuration. The default patterns look for an 8-digit date (YYYYMMDD) and .csv/.txt files, but you can modify these to match your needs.

Default configuration examples:

```python
# Looks for files like SAMPLE_DATASET_20240515.csv
'filename_pattern': r'SAMPLE_DATASET_\d{8}\.(csv|txt)'

# Looks for files like OTHER_DATASET_20240515.txt
'filename_pattern': r'OTHER_DATASET_\d{8}\.(csv|txt)'
```

You can change these patterns to match your file naming conventions. For example:

```python
# For files like Daily_Extract_2024-05-15.csv
'filename_pattern': r'Daily_Extract_\d{4}-\d{2}-\d{2}\.csv'

# For files that start with a date: 20240515_Report.txt
'filename_pattern': r'\d{8}_Report\.txt'

# For files with different extensions
'filename_pattern': r'DATA_\d{8}\.(csv|txt|dat)'

# Or any other pattern that matches your files
'filename_pattern': r'Your_Pattern_Here'
```

Note: The script uses Python's regular expressions for pattern matching. If you need help creating patterns for your specific filenames, please ask for assistance.

## Validation Rules

The script checks:

* File exists and can be read
* File has minimum required rows
* All required columns are present
* Date values match specified format
* Text values don't exceed maximum length
* Required fields are not empty

## Error Messages

You'll see clear messages about:

* Files that don't match expected patterns
* Missing columns
* Invalid dates
* Text that's too long
* Required fields that are empty
* Upload failures

A summary at the end shows which files were uploaded successfully and which failed.

## Author

Eddie Davison | NHS NCL ICB
