import os
import csv

# Dictionary of dataset names and corresponding HEI dataset IDs
dataset_files_map = {
    # MOORFIELDS
    'dvPTL_MOOR_OPENPATH': 'dvPTL_MOOR_OPENPATH',
    'dvPTL_MOOR_CSTART': 'dvPTL_MOOR_CSTART',
    'dvPTL_MOOR_CSTOP': 'dvPTL_MOOR_CSTOP',
    'dvPTL_MOOR_DIAGNOSTIC': 'dvPTL_MOOR_DIAGNOSTIC',
    # NORTH MID
    'dvPTL_NMID_OPENPATH': 'dvPTL_NMID_OPENPATH',
    'dvPTL_NMID_CSTART': 'dvPTL_NMID_CSTART',
    'dvPTL_NMID_CSTOP': 'dvPTL_NMID_CSTOP',
    'dvPTL_NMID_DIAGNOSTIC': 'dvPTL_NMID_DIAGNOSTIC',
    # ROYAL FREE
    'dvPTL_RAL_OPENPATH': 'dvPTL_RAL_OPENPATH',
    'dvPTL_RAL_CSTART': 'dvPTL_RAL_CSTART',
    'dvPTL_RAL_CSTOP': 'dvPTL_RAL_CSTOP',
    'dvPTL_RAL_DIAGNOSTIC': 'dvPTL_RAL_DIAGNOSTIC',
    # ROYAL NATIONAL ORTHO
    'dvPTL_RNOH_OPENPATH': 'dvPTL_RNOH_OPENPATH',
    'dvPTL_RNOH_CSTART': 'dvPTL_RNOH_CSTART',
    'dvPTL_RNOH_CSTOP': 'dvPTL_RNOH_CSTOP',
    'dvPTL_RNOH_DIAGNOSTIC': 'dvPTL_RNOH_DIAGNOSTIC',
    # UCLH
    'dvPTL_UCLH_OPENPATH': 'dvPTL_UCLH_OPENPATH',
    'dvPTL_UCLH_CSTART': 'dvPTL_UCLH_CSTART',
    'dvPTL_UCLH_CSTOP': 'dvPTL_UCLH_CSTOP',
    'dvPTL_UCLH_DIAGNOSTIC': 'dvPTL_UCLH_DIAGNOSTIC',
    # WHITTINGTON
    'dvPTL_WHIT_OPENPATH': 'dvPTL_WHIT_OPENPATH',
    'dvPTL_WHIT_CSTART': 'dvPTL_WHIT_CSTART',
    'dvPTL_WHIT_CSTOP': 'dvPTL_WHIT_CSTOP',
    'dvPTL_WHIT_DIAGNOSTIC': 'dvPTL_WHIT_DIAGNOSTIC',
}

# Directory where CSV files are stored
DATA_DIR = "C:\\Users\\JaphetOjo\\Downloads\\PTLs"

# Default configuration values
DEFAULTS = {
    'upload_reason': 'Uploaded file on',
    'spec_version': '1',
    'file_id': 'SINGLE_FILE'
}

def find_example_csv(dataset_name):
    """
    Attempt to find one CSV file in DATA_DIR that matches the pattern:
    dataset_name_YYYYMMDD.csv (8-digit date).
    """
    prefix = dataset_name + "_"
    for fname in os.listdir(DATA_DIR):
        if fname.startswith(prefix) and len(fname) == len(prefix) + 12 and fname.endswith(".csv"):
            # Expected format: dataset_name_YYYYMMDD.csv
            return os.path.join(DATA_DIR, fname)
    return None

def get_columns_from_csv(filepath):
    """Read the first row of the CSV to get column headers."""
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)  # First line
        return headers

# Print header comment
print("# Auto-generated Dataset configurations - COPY AND PASTE INTO dataset_config.py")
print("# ============================================================================")
print("")

for dataset_name, hei_dataset_id in dataset_files_map.items():
    example_file = find_example_csv(dataset_name)
    if example_file:
        headers = get_columns_from_csv(example_file)
    else:
        headers = []

    # Create filename pattern
    filename_pattern = f"{dataset_name}_{'?' * 8}.csv"

    # Create columns: all VarcharType(200), all nullable
    columns_code = []
    for col in headers:
        col_clean = col.strip().replace('"', '\\"').replace("'", "\\'")
        columns_code.append(f"        Column('{col_clean}', VarcharType(200)),")
    if not columns_code:
        columns_code.append("        # No columns found")

    # Print the dataset code
    print(f"{dataset_name.lower()} = Dataset(")
    print(f"    name='{dataset_name}',")
    print(f"    filename_pattern='{filename_pattern}',")
    print(f"    target_hei_dataset='{hei_dataset_id}',")
    for k, v in DEFAULTS.items():
        print(f"    {k}='{v}',")
    print("    columns=[")
    for c in columns_code:
        print(c)
    print("    ]")
    print()

print("# End of auto-generated code")