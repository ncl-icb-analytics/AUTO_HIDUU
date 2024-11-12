"""
This module provides functionality for validating CSV files against predefined schemas.
It checks files for:
- Existence and readability
- Minimum row count
- Required columns
- Data type validation (dates and varchar)
- Column constraints (nullable status, text length)

The validation rules are defined in config/dataset_config.py for each dataset type.
"""

import os
import pandas as pd

def validate_file(file_path, dataset_config):
    """
    Validates if a CSV file matches our expected format.
    Returns (True/False, "Success/Error message")
    """
    # Basic file checks
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
        
    # Try to read the file
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return False, f"Could not read file: {str(e)}"
        
    # Check if empty
    if len(df) < dataset_config['min_rows']:
        return False, f"File needs at least {dataset_config['min_rows']} rows, but has {len(df)}"
        
    # Check columns match schema
    schema = dataset_config['schema']
    missing_cols = set(schema.keys()) - set(df.columns)
    if missing_cols:
        return False, f"Missing columns: {', '.join(missing_cols)}"
        
    # Check each column's data
    errors = []
    for col_name, rules in schema.items():
        col_errors = _check_column(df[col_name], rules)
        if col_errors:
            errors.extend(f"Column '{col_name}': {error}" for error in col_errors)
            
    if errors:
        return False, "\n".join(errors)
    return True, "File is valid"

def _check_column(column, rules):
    """Checks if a column's data matches the rules"""
    errors = []
    
    # Check for nulls
    if not rules['nullable'] and column.isna().any():
        errors.append("Contains empty values but shouldn't")
        
    # Check non-null values match expected type
    non_null_values = column[~column.isna()]
    if len(non_null_values) > 0:
        if rules['type'] == 'date':
            errors.extend(_check_dates(non_null_values, rules['format']))
        elif rules['type'] == 'varchar':
            errors.extend(_check_text(non_null_values, rules['length']))
            
    return errors

def _check_dates(values, date_format):
    """Checks if values are valid dates"""
    errors = []
    try:
        pd.to_datetime(values, format=date_format)
    except:
        errors.append(f"Contains invalid dates (expected format: {date_format})")
    return errors

def _check_text(values, max_length):
    """Checks if values are valid text"""
    if (values.str.len() > max_length).any():
        return [f"Contains text longer than {max_length} characters"]
    return []