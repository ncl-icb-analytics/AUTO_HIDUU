import pandas as pd
import datetime

def validate_value(value, data_type, rules):
    """Check if a single value matches its expected data type and rules"""
    
    # First check if the value is empty
    if pd.isna(value):
        if rules.get('nullable', False):
            return True, "Valid empty value"
        else:
            return False, "This field cannot be empty"
    
    # Check the value based on its type
    if data_type == 'date':
        return _check_date(value, rules)
    elif data_type == 'varchar':
        return _check_text(value, rules)
    elif data_type == 'int':
        return _check_integer(value)
    elif data_type == 'float':
        return _check_decimal(value)
    else:
        return False, f"Unknown data type: {data_type}"

def _check_date(value, rules):
    """Check if value is a valid date"""
    date_format = rules.get('format', '%Y-%m-%d')
    try:
        if isinstance(value, str):
            datetime.datetime.strptime(value, date_format)
        return True, "Valid"
    except:
        return False, f"Invalid date format. Expected {date_format}"

def _check_text(value, rules):
    """Check if value is valid text"""
    if not isinstance(value, str):
        return False, f"Value must be text, got {type(value)}"
    if 'length' in rules and len(str(value)) > rules['length']:
        return False, f"Text is too long ({len(str(value))} chars, max is {rules['length']})"
    return True, "Valid"

def _check_integer(value):
    """Check if value is a valid integer"""
    try:
        int(value)
        return True, "Valid"
    except:
        return False, "Value must be a whole number"

def _check_decimal(value):
    """Check if value is a valid decimal number"""
    try:
        float(value)
        return True, "Valid"
    except:
        return False, "Value must be a number" 