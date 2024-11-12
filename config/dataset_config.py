"""
This file defines the expected format and validation rules for input files.

To add a new dataset type:
1. Add a new entry to the dataset_files dictionary with a unique key
2. Configure the following required fields:
   - filename_pattern: A regex pattern that matches valid filenames (must include \d{8} for the date)
   - min_rows: Minimum number of rows required in the file
   - schema: A dictionary defining the columns and their rules:
     - type: Either 'varchar' or 'date'
     - length: For varchar fields, the maximum allowed length
     - format: For date fields, the expected date format (e.g. '%Y-%m-%d')
     - nullable: Whether the column can contain empty values
   - target_hei_dataset: The ID of the target dataset in HEI
"""

dataset_files = {
    'sample_dataset': {
        'filename_pattern': r'SAMPLE_DATASET_\d{8}\.(csv|txt)',
        'min_rows': 100,
        'schema': {
            'patient_id': {'type': 'varchar', 'length': 50, 'nullable': False},
            'visit_date': {'type': 'date', 'format': '%Y-%m-%d', 'nullable': False},
            'diagnosis_code': {'type': 'varchar', 'length': 20, 'nullable': True}
        },
        'target_hei_dataset': 'TARGET_DATASET_ID'
    },
    'other_dataset': {
        'filename_pattern': r'SOME_OTHER_DATASET_\d{8}\.(csv|txt)',
        'min_rows': 50,
        'schema': {
            'nhs_number': {'type': 'varchar', 'length': 10, 'nullable': False},
            'appointment_date': {'type': 'date', 'format': '%d/%m/%Y', 'nullable': False},
            'status': {'type': 'varchar', 'length': 20, 'nullable': True}
        },
        'target_hei_dataset': 'ANOTHER_TARGET_DATASET_ID'
    }
} 