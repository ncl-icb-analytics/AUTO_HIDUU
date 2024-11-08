import os
import pandas as pd
from .value_validator import validate_value

def validate_csv(file_path, dataset_config):
    """Validates a CSV file against expected schema and constraints"""
    # Basic file checks
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
    if not os.access(file_path, os.R_OK):
        return False, f"File not readable: {file_path}"
            
    # Read and validate CSV contents
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return False, f"Error reading CSV file: {str(e)}"
    
    # Basic DataFrame validations
    if df.empty:
        return False, "File is empty"
    if len(df) < dataset_config['min_rows']:
        return False, f"File contains {len(df)} rows, minimum expected is {dataset_config['min_rows']}"
            
    # Column validation
    column_validation = _validate_columns(df, dataset_config['schema'])
    if not column_validation[0]:
        return column_validation
            
    # Data validation
    return _validate_data(df, dataset_config['schema'])

def _validate_columns(df, schema):
    """Validates that all required columns are present"""
    expected_columns = set(schema.keys())
    actual_columns = set(df.columns)
    
    missing = expected_columns - actual_columns
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
        
    extra = actual_columns - expected_columns
    if extra:
        return False, f"Unexpected columns found: {', '.join(extra)}"
        
    return True, "Valid columns"

def _validate_data(df, schema):
    """Validates data in each column matches schema requirements"""
    validation_errors = []
    
    for column, rules in schema.items():
        # Check for nulls
        if not rules.get('nullable', False):
            null_count = df[column].isna().sum()
            if null_count > 0:
                validation_errors.append(
                    f"Column '{column}' contains {null_count} null values but is not nullable"
                )
                continue
        
        # Validate values
        invalid_rows = _find_invalid_rows(df[column], rules)
        if invalid_rows:
            validation_errors.append(
                f"Column '{column}' validation errors:\n" + "\n".join(invalid_rows)
            )
    
    return (False, "\n".join(validation_errors)) if validation_errors else (True, "Validation passed")

def _find_invalid_rows(column, rules, max_errors=5):
    """Finds rows with invalid values in a column"""
    invalid_rows = []
    
    for idx, value in column.items():
        is_valid, error = validate_value(value, rules['type'], rules)
        if not is_valid:
            invalid_rows.append(f"Row {idx+1}: {error}")
            if len(invalid_rows) >= max_errors:
                invalid_rows.append("... and more errors")
                break
                
    return invalid_rows 