"""
Data types and structures for dataset configuration.
Maps to Vertica data types.
"""

from dataclasses import dataclass
from typing import List, Union, Optional
import re

# Column Types
@dataclass
class VarcharType:
    """
    Variable-length string with maximum length.
    Maps to Vertica VARCHAR type.
    """
    max_length: int
    type: str = 'varchar'

@dataclass
class CharType:
    """
    Fixed-length string, padded with spaces.
    Maps to Vertica CHAR type.
    """
    length: int
    type: str = 'char'

@dataclass
class DateType:
    """
    Date value.
    Maps to Vertica DATE type.
    Optional format (e.g. '%Y-%m-%d'). If None, accepts any valid date.
    """
    format: Optional[str] = None
    type: str = 'date'

@dataclass
class TimestampType:
    """
    Date and time value.
    Maps to Vertica TIMESTAMP type.
    Optional format (e.g. '%Y-%m-%d %H:%M:%S'). If None, accepts any valid timestamp.
    """
    format: Optional[str] = None
    type: str = 'timestamp'

@dataclass
class IntegerType:
    """
    Integer value.
    Maps to Vertica INTEGER type.
    precision: Optional maximum number of digits
    - If specified: Validates numbers don't exceed this many digits
    - If None (default): No limit on number size
    """
    precision: Optional[int] = None
    type: str = 'int'

@dataclass
class FloatType:
    """
    Floating-point number.
    Maps to Vertica FLOAT type.
    precision: Optional maximum number of digits
    - If specified: Validates numbers don't exceed this many digits
    - If None (default): No limit on number size
    """
    precision: Optional[int] = None
    type: str = 'float'

@dataclass
class NumericType:
    """
    Exact decimal number.
    Maps to Vertica NUMERIC type.
    precision: Optional total number of digits
    scale: Optional number of decimal places
    - If both None (default): No limit on number size or decimals
    - Example: precision=5, scale=2 allows 999.99
    """
    precision: Optional[int] = None
    scale: Optional[int] = None
    type: str = 'numeric'

@dataclass
class BooleanType:
    """
    Boolean value (Accepts True/False, 1/0).
    Maps to Vertica BOOLEAN type.
    """
    type: str = 'boolean'

@dataclass
class Column:
    """Column definition with type and nullability"""
    name: str
    data_type: Union[
        VarcharType, CharType, DateType, TimestampType,
        IntegerType, FloatType, NumericType, BooleanType
    ]
    nullable: bool = True

    def to_dict(self):
        """Convert to dictionary format for validator"""
        config = {'type': self.data_type.type, 'nullable': self.nullable}
        
        if isinstance(self.data_type, (VarcharType, CharType)):
            config['length'] = (
                self.data_type.max_length 
                if isinstance(self.data_type, VarcharType)
                else self.data_type.length
            )
        elif isinstance(self.data_type, (DateType, TimestampType)) and self.data_type.format:
            config['format'] = self.data_type.format
        elif isinstance(self.data_type, (IntegerType, FloatType)) and self.data_type.precision:
            config['precision'] = self.data_type.precision
        elif isinstance(self.data_type, NumericType):
            if self.data_type.precision is not None:
                config['precision'] = self.data_type.precision
            if self.data_type.scale is not None:
                config['scale'] = self.data_type.scale
            
        return config

@dataclass
class Dataset:
    """
    Dataset configuration with filename pattern and schema.
    
    The filename_pattern can:
    - Use ? to match any single character (e.g. DATA_???????.csv)
    - Be an exact filename (e.g. FIXED_FILE.csv)
    """
    name: str
    filename_pattern: str
    min_rows: int
    target_hei_dataset: str
    columns: List[Column]
    tenant: str = "nlhcr"  # Default to nlhcr, can be "nlhcr" or "nlhcr-2"
    upload_reason: Optional[str] = None
    spec_version: Optional[str] = None
    file_id: Optional[str] = None

    def __post_init__(self):
        """Validate and convert configuration"""
        if self.tenant not in ["nlhcr", "nlhcr-2"]:
            raise ValueError("Tenant must be either 'nlhcr' or 'nlhcr-2'")
            
        pattern = self.filename_pattern
        
        if not pattern.endswith(('.csv', '.txt')):
            raise ValueError("Filename pattern must end with .csv or .txt")
            
        # If pattern contains question marks, convert them to regex
        if '?' in pattern:
            pattern = pattern.replace('?', '.')
                
        # Escape dots in file extension
        if pattern.endswith('.csv'):
            pattern = pattern[:-4] + r'\.csv'
        elif pattern.endswith('.txt'):
            pattern = pattern[:-4] + r'\.txt'
            
        self.filename_pattern = pattern

    def to_dict(self):
        """Convert to dictionary format for validator"""
        config = {
            'filename_pattern': self.filename_pattern,
            'min_rows': self.min_rows,
            'target_hei_dataset': self.target_hei_dataset,
            'schema': {col.name: col.to_dict() for col in self.columns},
            'tenant': self.tenant
        }
        
        if self.upload_reason:
            config['upload_reason'] = self.upload_reason
        if self.spec_version:
            config['spec_version'] = self.spec_version
        if self.file_id:
            config['file_id'] = self.file_id
            
        return config 