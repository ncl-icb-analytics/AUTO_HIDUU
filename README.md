
# AUTO_HIDUU

## Overview

**AUTO_HIDUU** is a Python script that automates the process of uploading files to the HealtheIntent platform using the hi-data-upload-utility (HIDUU). The script scans a specified input directory for `.csv` and `.txt` files, matches them against a predefined set of dataset names, and uploads each matching file to the correct dataset on the HealtheIntent system.

## Features

* Scans a directory for `.csv` and `.txt` files.
* Matches files to dataset IDs using a predefined dictionary.
* Automatically uploads files to HealtheIntent using the HIDUU command-line tool.
* Provides detailed logging and summary of uploaded and failed files.

## Prerequisites

* Python 3.x
* HIDUU installed and accessible (refer to the [Cerner Wiki for HIDUU command usage](https://wiki.cerner.com/pages/releaseview.action?pageId=1391627506))

## Setup


1. Clone the repository:

```bash
git clone https://github.com/EddieDavison92/AUTO_HIDUU.git
```

2. Update the script with your specific paths and credentials:

* `input_folder_path`: Path to the directory containing your input files.
* `hiduu_directory`: Path to the HIDUU binary directory.
* `said`, `sas`, `sid`: Authentication credentials for the HealtheIntent system.
* `dataset_files`: Dictionary mapping dataset names to their corresponding dataset IDs.

Ensure the HIDUU command-line tool is installed and accessible.

## Usage

1. Navigate to the directory containing the script:

```bash
cd path/to/AUTO_HIDUU
```

2. Run the script:

```bash
python main.py
```

## Script Details

### Variables

**Directories**

* `input_folder_path`: Path to the folder containing input files.
* `hiduu_directory`: Path to the HIDUU utility.

**Upload Information**

* `upload_reason`: Reason for uploading files.
* `sv`: Specification version.
* `fid`: File ID within the dataset specification.

**Authentication Credentials**

* `said`: System account ID.
* `sas`: System account secret.
* `sid`: Source ID.

**Dataset Files**

* `dataset_files`: Dictionary mapping the first part of the filename to their corresponding dataset IDs.

### File Matching

The script expects filenames in the format: `{dataset_name}_{date}.csv` or `{dataset_name}_{date}.txt`. For example, `SAMPLE_DATASET_20240515.csv` will match the key `SAMPLE_DATASET` in the `dataset_files` dictionary.

### Upload Process

1. The script scans the `input_folder_path` for files with `.csv` or `.txt` extensions.
2. Each file is checked against the `dataset_files` dictionary to determine the corresponding dataset ID.
3. A custom HIDUU command is constructed and executed to upload the file.
4. Results of the upload process are logged and summarised.

### Example

An example dictionary for `dataset_files`:

```python
dataset_files = {
    'SAMPLE_DATASET': 'TARGET_DATASET_ID',
    'SOME_OTHER_DATASET': 'ANOTHER_TARGET_DATASET_ID',
}
```

### Author

Eddie Davison | NHS NCL ICB
