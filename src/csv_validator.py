import os
import pandas as pd
from .value_validator import validate_value

def validate_csv(file_path, config):
    """Check if a CSV file is valid according to our rules"""
    
    # Check if file exists and we can read it
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
    if not os.access(file_path, os.R_OK):
        return False, f"Cannot read file: {file_path}"
            
    # Try to read the CSV file
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return False, f"Error reading CSV file: {str(e)}"
    
    # Check if file has data
    if df.empty:
        return False, "File is empty"
    if len(df) < config['min_rows']:
        return False, f"File has {len(df)} rows, but needs at least {config['min_rows']}"
            
    # Check if all required columns exist
    column_check = _check_columns(df, config['schema'])
    if not column_check[0]:
        return column_check
            
    # Check if all data is valid
    return _check_data(df, config['schema'])

def _check_columns(df, schema):
    """Check if all required columns are present"""
    expected_columns = set(schema.keys())
    actual_columns = set(df.columns)
    
    # Check for missing columns
    missing = expected_columns - actual_columns
    if missing:
        return False, f"Missing columns: {', '.join(missing)}"
    
    # Check for extra columns
    extra = actual_columns - expected_columns
    if extra:
        return False, f"Unexpected columns found: {', '.join(extra)}"
    
    return True, "Valid columns"

def _check_data(df, schema):
    """Check if all data matches the rules"""
    errors = []
    
    # Check each column
    for column_name, rules in schema.items():
        # Check for empty values
        if not rules.get('nullable', False):
            empty_count = df[column_name].isna().sum()
            if empty_count > 0:
                errors.append(
                    f"Column '{column_name}' has {empty_count} empty values but cannot be empty"
                )
                continue
        
        # Check each value in the column
        bad_rows = _find_invalid_rows(df[column_name], rules)
        if bad_rows:
            errors.append(
                f"Column '{column_name}' errors:\n" + "\n".join(bad_rows)
            )
    
    if errors:
        return False, "\n".join(errors)
    return True, "All data is valid"

def _find_invalid_rows(column, rules, max_errors=5):
    """Find rows with invalid values (shows up to 5 errors)"""
    errors = []
    
    for row_num, value in column.items():
        is_valid, error = validate_value(value, rules['type'], rules)
        if not is_valid:
            errors.append(f"Row {row_num+1}: {error}")
            if len(errors) >= max_errors:
                errors.append("... and more errors")
                break
                
    return errors