# This file defines what our input files should look like

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