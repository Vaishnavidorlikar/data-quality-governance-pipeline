"""
Range validation module for data quality checks.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RangeValidator:
    """Validates data ranges and constraints."""
    
    def __init__(self):
        """Initialize range validator."""
        pass
    
    def check_numeric_ranges(self, df: pd.DataFrame, range_rules: Dict[str, Dict[str, Union[float, int]]]) -> Dict[str, Any]:
        """
        Check if numeric values fall within specified ranges.
        
        Args:
            df: DataFrame to validate
            range_rules: Dictionary mapping column names to range specifications
                        {'column_name': {'min': value, 'max': value, 'inclusive': bool}}
            
        Returns:
            Dictionary containing range validation results
        """
        results = {
            'is_valid': True,
            'range_violations': {},
            'total_violations': 0,
            'violation_summary': {}
        }
        
        for column, rules in range_rules.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            violations = {
                'below_min': [],
                'above_max': [],
                'total_violations': 0,
                'violation_percentage': 0.0
            }
            
            min_val = rules.get('min')
            max_val = rules.get('max')
            inclusive = rules.get('inclusive', True)
            
            column_data = df[column].dropna()
            
            if min_val is not None:
                if inclusive:
                    below_min_mask = column_data < min_val
                else:
                    below_min_mask = column_data <= min_val
                
                violations['below_min'] = column_data[below_min_mask].tolist()
            
            if max_val is not None:
                if inclusive:
                    above_max_mask = column_data > max_val
                else:
                    above_max_mask = column_data >= max_val
                
                violations['above_max'] = column_data[above_max_mask].tolist()
            
            violations['total_violations'] = len(violations['below_min']) + len(violations['above_max'])
            violations['violation_percentage'] = (
                violations['total_violations'] / len(column_data) 
                if len(column_data) > 0 else 0
            )
            
            results['range_violations'][column] = violations
            results['total_violations'] += violations['total_violations']
            
            if violations['total_violations'] > 0:
                results['is_valid'] = False
                results['violation_summary'][column] = {
                    'min_rule': min_val,
                    'max_rule': max_val,
                    'violations': violations['total_violations'],
                    'percentage': violations['violation_percentage']
                }
        
        if not results['is_valid']:
            logger.warning(f"Range validation failed with {results['total_violations']} total violations")
        
        return results
    
    def check_date_ranges(self, df: pd.DataFrame, date_rules: Dict[str, Dict[str, Union[str, datetime]]]) -> Dict[str, Any]:
        """
        Check if date values fall within specified ranges.
        
        Args:
            df: DataFrame to validate
            date_rules: Dictionary mapping column names to date range specifications
                       {'column_name': {'start_date': date, 'end_date': date}}
            
        Returns:
            Dictionary containing date range validation results
        """
        results = {
            'is_valid': True,
            'date_violations': {},
            'total_violations': 0
        }
        
        for column, rules in date_rules.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            violations = {
                'before_start': [],
                'after_end': [],
                'total_violations': 0
            }
            
            start_date = rules.get('start_date')
            end_date = rules.get('end_date')
            
            # Convert column to datetime if it's not already
            try:
                date_series = pd.to_datetime(df[column], errors='coerce').dropna()
            except Exception as e:
                logger.error(f"Failed to convert {column} to datetime: {e}")
                continue
            
            if start_date is not None:
                start_date = pd.to_datetime(start_date)
                before_start_mask = date_series < start_date
                violations['before_start'] = date_series[before_start_mask].tolist()
            
            if end_date is not None:
                end_date = pd.to_datetime(end_date)
                after_end_mask = date_series > end_date
                violations['after_end'] = date_series[after_end_mask].tolist()
            
            violations['total_violations'] = len(violations['before_start']) + len(violations['after_end'])
            
            results['date_violations'][column] = violations
            results['total_violations'] += violations['total_violations']
            
            if violations['total_violations'] > 0:
                results['is_valid'] = False
        
        if not results['is_valid']:
            logger.warning(f"Date range validation failed with {results['total_violations']} total violations")
        
        return results
    
    def check_categorical_constraints(self, df: pd.DataFrame, categorical_rules: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Check if categorical values are within allowed sets.
        
        Args:
            df: DataFrame to validate
            categorical_rules: Dictionary mapping column names to categorical constraints
                             {'column_name': {'allowed_values': [list], 'case_sensitive': bool}}
            
        Returns:
            Dictionary containing categorical validation results
        """
        results = {
            'is_valid': True,
            'categorical_violations': {},
            'total_violations': 0
        }
        
        for column, rules in categorical_rules.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            allowed_values = rules.get('allowed_values', [])
            case_sensitive = rules.get('case_sensitive', True)
            
            violations = {
                'invalid_values': [],
                'invalid_count': 0,
                'invalid_percentage': 0.0
            }
            
            column_data = df[column].dropna()
            
            if not case_sensitive:
                # Convert both data and allowed values to lowercase for comparison
                allowed_values_lower = [str(val).lower() for val in allowed_values]
                invalid_mask = ~column_data.astype(str).str.lower().isin(allowed_values_lower)
            else:
                invalid_mask = ~column_data.astype(str).isin(allowed_values)
            
            invalid_values = column_data[invalid_mask].tolist()
            violations['invalid_values'] = list(set(invalid_values))  # Unique values
            violations['invalid_count'] = len(invalid_values)
            violations['invalid_percentage'] = (
                violations['invalid_count'] / len(column_data) 
                if len(column_data) > 0 else 0
            )
            
            results['categorical_violations'][column] = violations
            results['total_violations'] += violations['invalid_count']
            
            if violations['invalid_count'] > 0:
                results['is_valid'] = False
        
        if not results['is_valid']:
            logger.warning(f"Categorical validation failed with {results['total_violations']} total violations")
        
        return results
    
    def check_string_constraints(self, df: pd.DataFrame, string_rules: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Check if string values meet specified constraints.
        
        Args:
            df: DataFrame to validate
            string_rules: Dictionary mapping column names to string constraints
                        {'column_name': {'min_length': int, 'max_length': int, 'pattern': str}}
            
        Returns:
            Dictionary containing string validation results
        """
        results = {
            'is_valid': True,
            'string_violations': {},
            'total_violations': 0
        }
        
        for column, rules in string_rules.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            violations = {
                'length_violations': [],
                'pattern_violations': [],
                'total_violations': 0
            }
            
            min_length = rules.get('min_length')
            max_length = rules.get('max_length')
            pattern = rules.get('pattern')
            
            column_data = df[column].dropna().astype(str)
            
            # Check length constraints
            if min_length is not None or max_length is not None:
                lengths = column_data.str.len()
                
                if min_length is not None:
                    too_short_mask = lengths < min_length
                    violations['length_violations'].extend(
                        column_data[too_short_mask].tolist()
                    )
                
                if max_length is not None:
                    too_long_mask = lengths > max_length
                    violations['length_violations'].extend(
                        column_data[too_long_mask].tolist()
                    )
            
            # Check pattern constraints
            if pattern is not None:
                try:
                    pattern_mask = ~column_data.str.match(pattern)
                    violations['pattern_violations'] = column_data[pattern_mask].tolist()
                except Exception as e:
                    logger.error(f"Invalid regex pattern for column {column}: {e}")
            
            violations['total_violations'] = (
                len(violations['length_violations']) + len(violations['pattern_violations'])
            )
            
            results['string_violations'][column] = violations
            results['total_violations'] += violations['total_violations']
            
            if violations['total_violations'] > 0:
                results['is_valid'] = False
        
        if not results['is_valid']:
            logger.warning(f"String validation failed with {results['total_violations']} total violations")
        
        return results
    
    def detect_outliers(self, df: pd.DataFrame, columns: List[str], method: str = 'iqr', 
                       threshold: float = 1.5) -> Dict[str, Any]:
        """
        Detect outliers in numeric columns using specified method.
        
        Args:
            df: DataFrame to analyze
            columns: List of columns to check for outliers
            method: Method for outlier detection ('iqr', 'zscore', 'modified_zscore')
            threshold: Threshold for outlier detection
            
        Returns:
            Dictionary containing outlier detection results
        """
        results = {
            'outliers': {},
            'total_outliers': 0,
            'method': method,
            'threshold': threshold
        }
        
        for column in columns:
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            if not pd.api.types.is_numeric_dtype(df[column]):
                logger.warning(f"Column {column} is not numeric, skipping outlier detection")
                continue
            
            column_data = df[column].dropna()
            outliers = []
            
            if method == 'iqr':
                Q1 = column_data.quantile(0.25)
                Q3 = column_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                outlier_mask = (column_data < lower_bound) | (column_data > upper_bound)
                outliers = column_data[outlier_mask].tolist()
            
            elif method == 'zscore':
                z_scores = np.abs((column_data - column_data.mean()) / column_data.std())
                outlier_mask = z_scores > threshold
                outliers = column_data[outlier_mask].tolist()
            
            elif method == 'modified_zscore':
                median = column_data.median()
                mad = np.median(np.abs(column_data - median))
                modified_z_scores = 0.6745 * (column_data - median) / mad
                outlier_mask = np.abs(modified_z_scores) > threshold
                outliers = column_data[outlier_mask].tolist()
            
            results['outliers'][column] = {
                'count': len(outliers),
                'values': outliers,
                'percentage': len(outliers) / len(column_data) if len(column_data) > 0 else 0
            }
            results['total_outliers'] += len(outliers)
        
        return results
