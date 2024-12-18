"""
This module provides functionality for validating CSV files against predefined schemas.
It checks files for:
- Existence and readability
- Minimum row count
- Required columns
- Data type validation (maps to Vertica types)
- Column constraints (nullable status, length, precision)
"""

import os
import pandas as pd
import numpy as np

def _try_read_file(file_path, expected_columns):
    """
    Attempts to read a file with different encodings and formats.
    Verifies that the columns match what we expect before accepting the read.
    
    Args:
        file_path: Path to the file to read
        expected_columns: Set of column names we expect (case-insensitive)
        
    Returns:
        tuple: (dataframe, error_message)
    """
    # List of encodings to try
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252', 'latin1']
    
    # List of delimiters to try, in priority order
    delimiters = [
        (',', 'comma'),    # Try comma first (most common)
        ('|', 'pipe'),     # Then pipe
        ('\t', 'tab'),     # Then tab
        (';', 'semicolon') # Finally semicolon
    ]
    
    # Convert expected columns to lowercase for case-insensitive comparison
    expected_columns_lower = {col.lower() for col in expected_columns}
    
    best_attempt = None  # Store the best read attempt for error reporting
    
    for encoding in encodings:
        try:
            # First try comma delimiter (most common)
            try:
                df = pd.read_csv(file_path, encoding=encoding, delimiter=',')
                if len(df.columns) > 1:
                    file_columns_lower = {col.lower() for col in df.columns}
                    if file_columns_lower == expected_columns_lower:
                        print(f"Successfully read file using {encoding} encoding and comma delimiter")
                        return df, None
                    # Store this attempt if it's the first one or has more columns than previous
                    if not best_attempt or len(df.columns) > len(best_attempt.columns):
                        best_attempt = df
            except Exception:
                pass
                
            # If comma failed or columns didn't match, try other delimiters
            for delimiter, name in delimiters[1:]:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                    if len(df.columns) > 1:
                        file_columns_lower = {col.lower() for col in df.columns}
                        if file_columns_lower == expected_columns_lower:
                            print(f"Successfully read file using {encoding} encoding and {name} delimiter")
                            return df, None
                        # Store this attempt if it has more columns than previous
                        if not best_attempt or len(df.columns) > len(best_attempt.columns):
                            best_attempt = df
                except Exception:
                    continue
                    
        except UnicodeDecodeError:
            continue
        except Exception:
            continue
    
    # If we got here, no attempt matched exactly. Provide detailed mismatch info
    if best_attempt is not None:
        found_columns = set(col.lower() for col in best_attempt.columns)
        missing = expected_columns_lower - found_columns
        unexpected = found_columns - expected_columns_lower
        
        error_msg = [
            f"Could not find exact column match using any encoding or delimiter.",
            f"\nFound {len(found_columns)} columns vs {len(expected_columns)} expected.",
            "\nColumns found in file:",
            ", ".join(sorted(best_attempt.columns)),
            "\nExpected columns:",
            ", ".join(sorted(expected_columns))
        ]
        
        if missing:
            error_msg.append("\nMissing columns:")
            error_msg.append(", ".join(sorted(col for col in expected_columns if col.lower() in missing)))
            
        if unexpected:
            error_msg.append("\nUnexpected columns:")
            error_msg.append(", ".join(sorted(col for col in best_attempt.columns if col.lower() in unexpected)))
            
        return None, "\n".join(error_msg)
    
    return None, f"Could not read file with any encoding (tried: {', '.join(encodings)}) or delimiter (tried: comma, pipe, tab, semicolon)"

def validate_file(file_path, dataset_config):
    """
    Validates if a CSV/TXT file matches our expected format.
    Column names are validated case-insensitively.
    """
    # Basic file checks
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}", 0
        
    # Get expected columns from schema
    expected_columns = set(dataset_config['schema'].keys())
    
    # Try to read the file with different encodings
    df, error = _try_read_file(file_path, expected_columns)
    if error:
        return False, error, 0
        
    row_count = len(df)
        
    # Check if empty
    if row_count < dataset_config['min_rows']:
        return False, f"File needs at least {dataset_config['min_rows']} rows, but has {row_count}", row_count
        
    # Create case-insensitive mappings
    schema = dataset_config['schema']
    file_columns_lower = {col.lower(): col for col in df.columns}
    required_columns_lower = {col.lower(): col for col in schema.keys()}
    
    # Check for missing required columns
    missing_cols = set(required_columns_lower.keys()) - set(file_columns_lower.keys())
    if missing_cols:
        # Show original column names in error message
        missing_original = [required_columns_lower[col] for col in missing_cols]
        return False, f"Missing required columns: {', '.join(sorted(missing_original))}", row_count
        
    # Check for extra columns
    extra_cols = set(file_columns_lower.keys()) - set(required_columns_lower.keys())
    if extra_cols:
        # Show original column names in error message
        extra_original = [file_columns_lower[col] for col in extra_cols]
        return False, f"File contains unexpected columns: {', '.join(sorted(extra_original))}", row_count
        
    # Create mapping of file columns to schema columns
    column_mapping = {
        file_columns_lower[col.lower()]: col 
        for col in schema.keys()
    }
    
    # Check each configured column's data using the mapping
    errors = []
    for file_col, schema_col in column_mapping.items():
        col_errors = _check_column(df[file_col], schema[schema_col], schema_col)
        if col_errors:
            errors.extend(f"Column '{schema_col}': {error}" for error in col_errors)
            
    if errors:
        return False, "\n".join(errors), row_count
    return True, "File is valid", row_count

def _check_column(column, rules, col_name):
    """
    Checks if a column's data matches the validation rules.
    
    Args:
        column (pd.Series): The column data to validate
        rules (dict): Validation rules for this column
        col_name (str): Name of the column (for error messages)
        
    Returns:
        list: List of error messages, empty if validation passed
    """
    errors = []
    
    # Check for nulls if specified
    if not rules.get('nullable', True) and column.isna().any():
        errors.append("Contains empty values but shouldn't")
        
    # Check non-null values match expected type
    non_null_values = column[~column.isna()]
    if len(non_null_values) > 0:
        if rules['type'] in ['date', 'timestamp']:
            errors.extend(_check_datetime(non_null_values, rules.get('format'), rules['type']))
        elif rules['type'] in ['varchar', 'char']:
            errors.extend(_check_text(non_null_values, rules['length'], rules['type']))
        elif rules['type'] == 'numeric':
            errors.extend(_check_numeric_precise(
                non_null_values, 
                rules.get('precision'), 
                rules.get('scale')
            ))
        elif rules['type'] == 'int':
            errors.extend(_check_numeric(non_null_values, 'integer'))
        elif rules['type'] == 'float':
            errors.extend(_check_numeric(non_null_values, 'float'))
        elif rules['type'] == 'boolean':
            errors.extend(_check_boolean(non_null_values))
            
    return errors

def _check_datetime(values, date_format=None, dtype='date'):
    """
    Validates dates or timestamps.
    
    Args:
        values (pd.Series): Series of datetime values to validate
        date_format (str, optional): Expected format (e.g. '%Y-%m-%d' or '%Y-%m-%d %H:%M:%S%z')
        dtype (str): Either 'date' or 'timestamp'
        
    Returns:
        list: List of error messages, empty if validation passed
    """
    errors = []
    try:
        if date_format:
            # Validate specific format
            pd.to_datetime(values, format=date_format)
        else:
            # Accept any valid date/timestamp
            values = pd.to_datetime(values)
            # For dates, check no time component
            if dtype == 'date' and any(values.dt.time != pd.Timestamp('00:00:00').time()):
                errors.append("Contains time values in date column")
            # For timestamps, we accept both with and without timezone
    except Exception as e:
        format_msg = f" (expected format: {date_format})" if date_format else ""
        errors.append(f"Contains invalid {dtype}s{format_msg}")
    return errors

def _check_text(values, max_length, dtype='varchar'):
    """
    Validates text values.
    All values are treated as strings regardless of their original type.
    
    Args:
        values (pd.Series): Series of values to validate
        max_length (int): Maximum allowed length
        dtype (str): Either 'varchar' or 'char'
        
    Returns:
        list: List of error messages, empty if validation passed
    """

    str_values = values.fillna('').astype(str)
    lengths = str_values.str.len()

    # Convert all values to strings first
    str_values = values.fillna('').astype(str)
    lengths = str_values.str.len()
    
    if lengths.gt(max_length).any():
        return [f"Contains text longer than {max_length} characters"]
    if dtype == 'char' and lengths.ne(max_length).any():
        return [f"Contains text not exactly {max_length} characters long"]
    return []

def _check_numeric_precise(values, precision=None, scale=None):
    """
    Validates numeric values with precision and scale.
    
    Args:
        values (pd.Series): Series of numeric values to validate
        precision (int, optional): Maximum total digits
        scale (int, optional): Maximum decimal places
        
    Returns:
        list: List of error messages, empty if validation passed
    """
    errors = []
    try:
        numeric_values = pd.to_numeric(values)
        
        if precision is not None or scale is not None:
            # Convert to strings to check digits
            str_values = numeric_values.abs().astype(str)
            
            if precision is not None:
                # Count total digits
                total_digits = str_values.str.replace('.', '').str.len()
                if total_digits.gt(precision).any():
                    errors.append(f"Contains numbers with more than {precision} total digits")
            
            if scale is not None:
                # Count decimal places
                decimal_places = str_values.str.extract(r'\.(\d+)')[0].str.len().fillna(0)
                if decimal_places.gt(scale).any():
                    errors.append(f"Contains numbers with more than {scale} decimal places")
                    
    except Exception:
        errors.append("Contains invalid numeric values")
    return errors

def _check_numeric(values, number_type):
    """
    Validates that all values are valid numbers of the specified type.
    
    Args:
        values (pd.Series): Series of numeric values to validate
        number_type (str): Either 'integer' or 'float'
        
    Returns:
        list: List of error messages, empty if validation passed
    """
    errors = []
    try:
        if number_type == 'integer':
            # Check if all values can be converted to integers
            numeric_values = pd.to_numeric(values, downcast='integer')
            if not np.all(numeric_values.astype(int) == numeric_values):
                errors.append("Contains non-integer values")
        else:  # float
            pd.to_numeric(values, downcast='float')
    except:
        errors.append(f"Contains invalid {number_type} values")
    return errors

def _check_boolean(values):
    """
    Validates that all values are valid boolean representations.
    Accepts: True/False, 1/0, 'true'/'false' (case insensitive)
    
    Args:
        values (pd.Series): Series of boolean values to validate
        
    Returns:
        list: List of error messages, empty if validation passed
    """
    valid_values = [True, False, 1, 0, '1', '0', 'true', 'false', 'True', 'False', 'TRUE', 'FALSE']
    invalid = ~values.isin(valid_values)
    if invalid.any():
        return ["Contains invalid boolean values"]
    return []