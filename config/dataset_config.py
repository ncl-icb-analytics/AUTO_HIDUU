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

#aff_hash0: Fixed filename dataset
aff_Hash0 = Dataset(
    name="aff_hash0",
    filename_pattern="Aff_Hash0.csv",
    min_rows=1,
    target_hei_dataset="Aff_Hash0",
    tenant="nlhcr-2",
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

aff_Hash1 = Dataset(
    name="aff_hash1",
    filename_pattern="Aff_Hash1.csv",
    min_rows=1,
    target_hei_dataset="Aff_Hash1",
    tenant="nlhcr-2",
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

aff_Hash2 = Dataset(
    name="aff_hash2",
    filename_pattern="Aff_Hash2.csv",
    min_rows=1,
    target_hei_dataset="Aff_Hash2",
    tenant="nlhcr-2",
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
aff_Hash3 = Dataset(
    name="aff_hash3",
    filename_pattern="Aff_Hash3.csv",
    min_rows=1,
    target_hei_dataset="Aff_Hash3",
    tenant="nlhcr-2",
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
aff_Hash4 = Dataset(
    name="aff_hash4",
    filename_pattern="Aff_Hash4.csv",
    min_rows=1,
    target_hei_dataset="Aff_Hash4",
    tenant="nlhcr-2",
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
# Automatically collect all Dataset instances
dataset_files = {
    obj.name: obj.to_dict()
    for name, obj in globals().items()
    if isinstance(obj, Dataset)
} 
#ministry_organization_id	ref_record_type	source_type	source_id	source_version	source_description	ref_record_type_key	source_type_key	population_id	data_point_seq	hash_value	hash_5	dc_extracted	lsoa21cd
