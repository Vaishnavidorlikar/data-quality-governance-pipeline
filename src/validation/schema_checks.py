"""
Schema validation module for data quality checks.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates data schema against expected structure and types."""
    
    def __init__(self, expected_schema: Dict[str, Any]):
        """
        Initialize schema validator.
        
        Args:
            expected_schema: Dictionary defining expected columns and their types
        """
        self.expected_schema = expected_schema
    
    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary containing validation results
        """
        results = {
            'is_valid': True,
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': [],
            'total_errors': 0
        }
        
        # Check for missing columns
        expected_columns = set(self.expected_schema.keys())
        actual_columns = set(df.columns)
        
        results['missing_columns'] = list(expected_columns - actual_columns)
        results['extra_columns'] = list(actual_columns - expected_columns)
        
        # Check data types
        for column, expected_type in self.expected_schema.items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if not self._is_compatible_type(actual_type, expected_type):
                    results['type_mismatches'].append({
                        'column': column,
                        'expected': expected_type,
                        'actual': actual_type
                    })
        
        results['total_errors'] = (
            len(results['missing_columns']) + 
            len(results['extra_columns']) + 
            len(results['type_mismatches'])
        )
        results['is_valid'] = results['total_errors'] == 0
        
        if not results['is_valid']:
            logger.warning(f"Schema validation failed with {results['total_errors']} errors")
        
        return results
    
    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        """Check if actual data type is compatible with expected type."""
        type_mapping = {
            'int64': ['int', 'integer'],
            'float64': ['float', 'decimal', 'numeric'],
            'object': ['string', 'text', 'str'],
            'bool': ['boolean', 'bool'],
            'datetime64[ns]': ['datetime', 'timestamp', 'date']
        }
        
        expected_lower = expected.lower()
        actual_lower = actual.lower()
        
        for actual_type, compatible_types in type_mapping.items():
            if actual_type in actual_lower and any(comp in expected_lower for comp in compatible_types):
                return True
        
        return expected_lower in actual_lower or actual_lower in expected_lower
    
    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforce schema on DataFrame by adding missing columns and converting types.
        
        Args:
            df: DataFrame to enforce schema on
            
        Returns:
            DataFrame with enforced schema
        """
        df_copy = df.copy()
        
        # Add missing columns with appropriate default values
        for column, expected_type in self.expected_schema.items():
            if column not in df_copy.columns:
                if 'int' in expected_type.lower():
                    df_copy[column] = 0
                elif 'float' in expected_type.lower():
                    df_copy[column] = 0.0
                elif 'bool' in expected_type.lower():
                    df_copy[column] = False
                elif 'date' in expected_type.lower():
                    df_copy[column] = pd.NaT
                else:
                    df_copy[column] = None
        
        # Convert data types
        for column, expected_type in self.expected_schema.items():
            if column in df_copy.columns:
                try:
                    if 'int' in expected_type.lower():
                        df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce').astype('Int64')
                    elif 'float' in expected_type.lower():
                        df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce')
                    elif 'bool' in expected_type.lower():
                        df_copy[column] = df_copy[column].astype('boolean')
                    elif 'date' in expected_type.lower():
                        df_copy[column] = pd.to_datetime(df_copy[column], errors='coerce')
                    else:
                        df_copy[column] = df_copy[column].astype('object')
                except Exception as e:
                    logger.warning(f"Failed to convert {column} to {expected_type}: {e}")
        
        return df_copy
