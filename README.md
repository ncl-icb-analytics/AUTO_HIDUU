# AUTO_HIDUU

## Overview

**AUTO_HIDUU** is a Python script that automates the process of uploading files to the HealtheIntent platform using the hi-data-upload-utility (HIDUU). The script validates CSV/TXT files against predefined schemas and uploads them to the correct dataset on the HealtheIntent system.

## Features

* Validates CSV/TXT files against defined schemas:
  * Column presence and naming
  * Data type validation (maps to Vertica types)
  * Text length validation
  * Required field (non-null) validation
  * Numeric precision validation
* Matches files using pattern matching with ? wildcards
* Automatically uploads valid files to HealtheIntent using HIDUU
* Provides detailed validation feedback and upload summaries

## Prerequisites

* Python 3.x
* pandas library
* HIDUU installed and accessible

## Setup

1. Clone the repository:

```bash
git clone https://github.com/EddieDavison92/AUTO_HIDUU.git
```

2. Create a virtual environment and install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

3. Create your environment configuration:

```bash
cp .env.example .env
```

4. Edit the `.env` file with your specific configuration:

```ini
# HealtheIntent Authentication
SAID=your_system_account_id
SAS=your_system_account_secret
SID=your_source_id

# File Paths
INPUT_FOLDER_PATH=/path/to/input/files
HIDUU_DIRECTORY=/path/to/hiduu/installation
```

5. Define your datasets in `config/dataset_config.py`:

```python
from .schema_types import (
    Dataset, Column,
    VarcharType, CharType, DateType, TimestampType,
    IntegerType, FloatType, NumericType, BooleanType
)

my_dataset = Dataset(
    name="My Dataset",
    filename_pattern="MY_DATASET_????????.csv",  # ? matches any character
    min_rows=100,
    target_hei_dataset="TARGET_ID",
    columns=[
        Column("id", CharType(10), nullable=False),
        Column("name", VarcharType(50)),
        Column("date", DateType("%Y-%m-%d")),
        Column("timestamp", TimestampType()),  # Accepts any valid timestamp
        Column("count", IntegerType(precision=3)),  # Up to 999
        Column("amount", NumericType(precision=5, scale=2)),  # Up to 999.99
        Column("active", BooleanType(), nullable=False),
    ]
)
```

## Available Column Types

* Text:

  * `VarcharType(max_length)` - Variable length text
  * `CharType(length)` - Fixed length text
* Date/Time:

  * `DateType(format=None)` - Date values (e.g. "%Y-%m-%d")
  * `TimestampType(format=None)` - Timestamp values, with or without timezone
* Numbers:

  * `IntegerType(precision=None)` - Whole numbers
  * `FloatType(precision=None)` - Decimal numbers
  * `NumericType(precision=None, scale=None)` - Exact decimal numbers
* Boolean:

  * `BooleanType()` - True/False values (accepts 1/0)

All types default to nullable=True. Add nullable=False to make a column required.

## File Pattern Matching

The filename_pattern in Dataset configuration supports:

* Question mark (?) to match any single character
* Exact filenames for fixed files

Examples:

```python
# Match files with any 8 characters before .csv
filename_pattern="DATA_????????.csv"  # DATA_20240315.csv, DATA_ABCD1234.csv

# Match exact filename
filename_pattern="REFERENCE.csv"  # Only REFERENCE.csv
```

## Usage

1. Place your CSV/TXT files in the configured input directory
2. Run the script:

```bash
python main.py
```

The script will:

1. Find all CSV/TXT files in the input directory
2. Match files against dataset patterns
3. Validate file contents against schema
4. Upload valid files to HealtheIntent
5. Move successful uploads to a processed directory
6. Show a summary of results

## Author

Eddie Davison | NHS NCL ICB
