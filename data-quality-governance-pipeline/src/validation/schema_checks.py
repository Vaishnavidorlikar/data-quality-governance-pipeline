"""
Schema validation logic for data quality pipeline.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates DataFrame schema against expected structure and data types."""
    
    def __init__(self):
        """Initialize schema validator."""
        self.validation_results = {}
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected structure.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dictionary of column names and expected data types
            
        Returns:
            Dictionary containing validation results
        """
        results = {
            'is_valid': True,
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': [],
            'total_columns': len(df.columns)
        }
        
        # Check for missing columns
        for col, expected_type in expected_schema.items():
            if col not in df.columns:
                results['missing_columns'].append(col)
                results['is_valid'] = False
                logger.warning(f"Missing required column: {col}")
        
        # Check for extra columns
        for col in df.columns:
            if col not in expected_schema:
                results['extra_columns'].append(col)
                logger.info(f"Extra column found: {col}")
        
        # Check data type mismatches
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if not self._is_compatible_type(actual_type, expected_type):
                    results['type_mismatches'].append({
                        'column': col,
                        'expected': expected_type,
                        'actual': actual_type
                    })
                    results['is_valid'] = False
                    logger.warning(f"Type mismatch in column {col}: expected {expected_type}, got {actual_type}")
        
        results['missing_columns_count'] = len(results['missing_columns'])
        results['extra_columns_count'] = len(results['extra_columns'])
        results['type_mismatches_count'] = len(results['type_mismatches'])
        
        logger.info(f"Schema validation completed: {len(results['missing_columns'])} missing, {len(results['extra_columns'])} extra, {len(results['type_mismatches'])} type mismatches")
        
        return results
    
    def _is_compatible_type(self, actual_type: str, expected_type: str) -> bool:
        """
        Check if actual data type is compatible with expected type.
        
        Args:
            actual_type: Actual pandas data type
            expected_type: Expected data type string
            
        Returns:
            True if types are compatible
        """
        type_mapping = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32'],
            'string': ['object', 'string'],
            'datetime': ['datetime64[ns]', 'datetime64'],
            'bool': ['bool', 'boolean']
        }
        
        expected_types = type_mapping.get(expected_type, [expected_type])
        return actual_type in expected_types
    
    def get_schema_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary of DataFrame schema.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing schema summary
        """
        summary = {
            'total_columns': len(df.columns),
            'total_rows': len(df),
            'column_types': {},
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict()
        }
        
        # Count columns by type
        for dtype in df.dtypes:
            dtype_str = str(dtype)
            summary['column_types'][dtype_str] = summary['column_types'].get(dtype_str, 0) + 1
        
        logger.info(f"Schema summary generated: {summary['total_columns']} columns, {summary['total_rows']} rows")
        
        return summary
