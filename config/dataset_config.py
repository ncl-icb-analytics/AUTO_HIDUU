"""
Dataset configurations for file uploads.
"""

from .schema_types import (
    Dataset, Column,
    VarcharType, CharType, DateType, TimestampType,
    IntegerType, FloatType, NumericType, BooleanType
)

# Default configuration values
DEFAULT_CONFIG = {
    'upload_reason': 'Uploaded file on',  # Date will be appended
    'spec_version': '1',
    'file_id': 'SINGLE_FILE'
}

# ========================================
# Adding New Datasets
# ========================================
# Required Parameters:
# - name: Dataset identifier
# - filename_pattern: Pattern to match files
#   - Use ? for any character: "DATA_???????.csv"
#   - Or exact name: "FIXED_FILE.csv"
# - target_hei_dataset: HEI dataset ID
# - columns: List of column definitions
#
# Optional Parameters:
# - min_rows: Minimum rows (default: 0 for no minimum)
# - upload_reason: Custom upload reason
# - spec_version: Version number
# - file_id: File identifier
#
# Available Column Types:
# Text:
# - VarcharType(max_length)      # Variable length
# - CharType(length)             # Fixed length
#
# Date/Time:
# - DateType(format=None)        # e.g. "%Y-%m-%d" matches 2024-11-25, None=any valid date
# - TimestampType(format=None)   # e.g. "%Y-%m-%d %H:%M:%S" matches both:
#                               #      - 2024-11-25 12:00:00 (timezone-naive)
#                               #      - 2024-11-25 12:00:00+0000 (timezone-aware)
#                               # None=any valid timestamp format
#
# Numbers:
# - IntegerType(precision=None)  # precision=3 limits to 999, None=no limit (defaults to None) 
# - FloatType(precision=None)    # precision=4 limits to 9999 or 99.99, None=no limit (defaults to None) 
# - NumericType(precision=None, scale=None)  
#   precision=total digits, scale=decimal places
#   e.g. (5,2) allows values up to 999.99
#   None=no limit
#
# Boolean:
# - BooleanType()  # True/False, 1/0
#
# Add nullable=False to make column required
# ========================================

# Example 1: Dataset with date-based filename (using default nlhcr tenant)
patient_visits = Dataset(
    name="Patient Visits",
    filename_pattern="PATIENT_VISITS_????????.csv", # Will match PATIENT_VISITS_20241125.csv
    min_rows=100,
    target_hei_dataset="VISITS_DATASET_ID",
    upload_reason="Patient visits from xxx source dated:", # Date will be appended
    columns=[
        Column("nhs_number", CharType(10), nullable=False),
        Column("hospital_code", CharType(3), nullable=False),
        Column("visit_date", DateType("%Y-%m-%d"), nullable=False),
        Column("appointment_time", TimestampType()),
        Column("consultant_name", VarcharType(50)),
        Column("diagnosis_code", VarcharType(20), nullable=False),
        Column("notes", VarcharType(1000)),
        Column("weight_kg", NumericType(precision=5, scale=2)),     # 999.99
        Column("height_cm", IntegerType(precision=3)),              # 999
        Column("temperature", NumericType(precision=4, scale=1)),   # 999.9
        Column("blood_pressure", VarcharType(7)),                   # 120/80
        Column("heart_rate", IntegerType(precision=3)),             # 999
        Column("is_emergency", BooleanType(), nullable=False),
        Column("needs_followup", BooleanType(), nullable=False)
    ]
)

# Example 2: Example dataset with wildcards (using nlhcr-1 tenant)
example_dataset = Dataset(
    name="Example Dataset",
    filename_pattern="some_csv_file_with_wildcards_???????.csv",
    min_rows=10000,
    target_hei_dataset="TARGET_HEI_TABLE_NAME",
    tenant="nlhcr-1",  # Use nlhcr-1 tenant
    columns=[
        Column("First_column_name", VarcharType(200), nullable=False), # Required
        Column("Second_column_name", VarcharType(200)),
        Column("Third_column_name", VarcharType(200)),
        Column("Fourth_column_name", VarcharType(200)),
        Column("Fifth_column_name", VarcharType(200)),
    ]
)

# Automatically collect all Dataset instances
dataset_files = {
    obj.name: obj.to_dict()
    for name, obj in globals().items()
    if isinstance(obj, Dataset)
} 