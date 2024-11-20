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
# - DateType(format=None)        # e.g. "%Y-%m-%d", None=any valid date (defaults to None)
# - TimestampType(format=None)   # e.g. "%Y-%m-%d %H:%M:%S", None=any valid timestamp (defaults to None)
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

# Example 1: Dataset with date-based filename
patient_visits = Dataset(
    name="Patient Visits",
    filename_pattern="PATIENT_VISITS_????????.csv",
    min_rows=100,
    target_hei_dataset="VISITS_DATASET_ID",
    upload_reason="Patient visits from xxx source dated:", # Date will be appended
    columns=[
        Column("nhs_number", CharType(10), nullable=False),
        Column("hospital_code", CharType(3), nullable=False),
        Column("visit_date", DateType("%Y-%m-%d"), nullable=False),
        Column("appointment_time", TimestampType("%Y-%m-%d %H:%M:%S")),
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

# Example 2: Fixed filename dataset
reference_data = Dataset(
    name="Reference Data",
    filename_pattern="HOSPITAL_REFERENCE.csv",
    min_rows=1,
    target_hei_dataset="REFERENCE_DATASET_ID",
    columns=[
        Column("hospital_code", CharType(3), nullable=False),
        Column("hospital_name", VarcharType(100), nullable=False),
        Column("trust_code", CharType(5), nullable=False),
        Column("region_code", CharType(2), nullable=False),
        Column("bed_count", IntegerType(precision=4)),  # 9999
        Column("is_teaching", BooleanType()),
        Column("last_updated", TimestampType())
    ]
)

# Automatically collect all Dataset instances
dataset_files = {
    obj.name: obj.to_dict()
    for name, obj in globals().items()
    if isinstance(obj, Dataset)
} 