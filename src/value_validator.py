import pandas as pd
import numpy as np
import datetime
import uuid

def validate_value(value, data_type, constraints):
    """Validates a single value matches its expected data type and constraints"""
    try:
        # Handle null values first
        if pd.isna(value):
            return (True, "Valid null value") if constraints.get('nullable', False) \
                   else (False, "Non-nullable field contains null value")
            
        # Validate based on data type
        validators = {
            'date': _validate_date,
            'int': _validate_integer,
            'float': _validate_float,
            'uuid': _validate_uuid,
            'varchar': _validate_varchar
        }
        
        validator = validators.get(data_type)
        if not validator:
            return False, f"Unknown data type: {data_type}"
            
        return validator(value, constraints)
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def _validate_date(value, constraints):
    date_format = constraints.get('format', '%Y-%m-%d')
    try:
        if isinstance(value, str):
            datetime.datetime.strptime(value, date_format)
        else:
            pd.to_datetime(value).strftime(date_format)
        return True, "Valid"
    except:
        return False, f"Invalid date format. Expected {date_format}"

def _validate_integer(value, _):
    try:
        int_val = pd.to_numeric(value, downcast='integer')
        if not isinstance(int_val, (int, np.int64)):
            return False, "Value must be an integer"
        return True, "Valid"
    except:
        return False, "Invalid integer value"

def _validate_float(value, _):
    try:
        float_val = pd.to_numeric(value, downcast='float')
        if not isinstance(float_val, (float, np.float64)):
            return False, "Value must be a float"
        return True, "Valid"
    except:
        return False, "Invalid float value"

def _validate_uuid(value, _):
    try:
        uuid.UUID(str(value))
        return True, "Valid"
    except:
        return False, "Invalid UUID format"

def _validate_varchar(value, constraints):
    if not isinstance(value, str):
        return False, f"Value must be a string, got {type(value)}"
    if 'length' in constraints and len(str(value)) > constraints['length']:
        return False, f"String length {len(str(value))} exceeds maximum of {constraints['length']}"
    return True, "Valid" 