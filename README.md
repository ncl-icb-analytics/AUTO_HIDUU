# AUTO_HIDUU

## Overview

**AUTO_HIDUU** is a Python script that automates the process of uploading files to the HealtheIntent platform using the hi-data-upload-utility (HIDUU). The script validates CSV/TXT files against predefined schemas and uploads them to the correct dataset on the HealtheIntent system.

## Features

* Validates CSV/TXT files against defined schemas:
  * Column presence and naming
  * Data type validation (date, int, float, varchar, uuid)
  * Null value handling
  * String length constraints
  * Custom date format validation
* Matches files to dataset IDs using configurable patterns
* Automatically uploads valid files to HealtheIntent using HIDUU
* Provides detailed validation feedback and upload summaries

## Prerequisites

* Python 3.x
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
        'filename_pattern': r'PATTERN_\d{8}\.(csv|txt)',
        'min_rows': 100,
        'schema': {
            'column_name': {
                'type': 'varchar',  # date, int, float, uuid, varchar
                'length': 50,       # for varchar
                'nullable': False,
                'format': '%Y-%m-%d'  # for dates
            }
            # ... more columns ...
        },
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
1. Scan the input directory for CSV/TXT files
2. Match each file against configured dataset patterns
3. Validate file contents against the defined schema
4. Upload valid files to HealtheIntent
5. Provide a summary of successful and failed uploads

## File Naming Convention

Files should be named following the pattern defined in your dataset configuration. For example:
* `SAMPLE_DATASET_20240515.csv`
* `OTHER_DATASET_20240515.txt`

The date portion (YYYYMMDD) is required and will be used in the upload process.

## Validation Rules

* **File Level**
  * File must exist and be readable
  * File must not be empty
  * Must meet minimum row count
  * Must contain all required columns

* **Data Level**
  * Values must match specified data types
  * Null values only allowed in nullable columns
  * Varchar fields must not exceed maximum length
  * Dates must match specified format
  * Numbers must be valid integers or floats as specified

## Error Handling

The script provides detailed feedback for:
* Missing or unreadable files
* Schema validation failures
* Data type mismatches
* Upload failures

A summary is provided at the end of execution showing successful and failed uploads.

## Author

Eddie Davison | NHS NCL ICB
