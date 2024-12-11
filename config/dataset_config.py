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

# Example 1: Dataset with date-based filename
patient_visits = Dataset(
    name="Patient Visits",
    filename_pattern="PATIENT_VISITS_????????.csv", # Will match PATIENT_VISITS_20241125.csv
    min_rows=100,
    target_hei_dataset="VISITS_DATASET_ID",
    upload_reason="Patient visits from xxx source dated:", # Date will be appended
    columns=[
        Column("ministry_organization_id", VarcharType(200)),
        Column("ref_record_type", VarcharType(200)),
        Column("source_type", VarcharType(200)),
        Column("source_id", VarcharType(200)),
        Column("source_version", VarcharType(200)),
        Column("source_description", VarcharType(200)),
        Column("ref_record_type_key", VarcharType(200)),
        Column("source_type_key", VarcharType(200)),
        Column("population_id", VarcharType(200)),
        Column("data_point_seq", VarcharType(200)),
        Column("hash_value", VarcharType(200)),
        Column("hash_5", VarcharType(200)),
        Column("dc_extracted", VarcharType(200)),
        Column("lsoa21cd", VarcharType(200))
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
        Column("last_updated", TimestampType())  # Any valid timestamp
    ]
)
# Automatically collect all Dataset instances
dataset_files = {
    obj.name: obj.to_dict()
    for name, obj in globals().items()
    if isinstance(obj, Dataset)
} 
#ministry_organization_id	ref_record_type	source_type	source_id	source_version	source_description	ref_record_type_key	source_type_key	population_id	data_point_seq	hash_value	hash_5	dc_extracted	lsoa21cd
