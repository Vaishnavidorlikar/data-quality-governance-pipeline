"""
Null value validation module for data quality checks.
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)


class NullValidator:
    """Validates and handles null values in datasets."""
    
    def __init__(self, null_threshold: float = 0.1):
        """
        Initialize null validator.
        
        Args:
            null_threshold: Maximum allowed proportion of null values (default: 0.1 = 10%)
        """
        self.null_threshold = null_threshold
    
    def check_null_values(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Check for null values in specified columns or entire DataFrame.
        
        Args:
            df: DataFrame to check
            columns: Specific columns to check (if None, checks all columns)
            
        Returns:
            Dictionary containing null value analysis results
        """
        if columns is None:
            columns = df.columns.tolist()
        
        results = {
            'is_valid': True,
            'null_summary': {},
            'columns_above_threshold': [],
            'total_nulls': 0,
            'total_cells': 0,
            'overall_null_percentage': 0.0
        }
        
        for column in columns:
            if column in df.columns:
                null_count = df[column].isnull().sum()
                total_count = len(df)
                null_percentage = null_count / total_count if total_count > 0 else 0
                
                results['null_summary'][column] = {
                    'null_count': int(null_count),
                    'total_count': int(total_count),
                    'null_percentage': float(null_percentage)
                }
                
                results['total_nulls'] += null_count
                results['total_cells'] += total_count
                
                if null_percentage > self.null_threshold:
                    results['columns_above_threshold'].append({
                        'column': column,
                        'null_percentage': null_percentage
                    })
                    results['is_valid'] = False
        
        results['overall_null_percentage'] = (
            results['total_nulls'] / results['total_cells'] 
            if results['total_cells'] > 0 else 0
        )
        
        if not results['is_valid']:
            logger.warning(f"Null validation failed. {len(results['columns_above_threshold'])} columns exceed threshold")
        
        return results
    
    def check_null_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze patterns in null values across the dataset.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing null pattern analysis
        """
        results = {
            'completely_null_rows': 0,
            'completely_null_columns': [],
            'null_correlations': {},
            'null_clusters': []
        }
        
        # Find completely null rows
        results['completely_null_rows'] = df.isnull().all(axis=1).sum()
        
        # Find completely null columns
        for column in df.columns:
            if df[column].isnull().all():
                results['completely_null_columns'].append(column)
        
        # Calculate null correlations between columns
        null_matrix = df.isnull().astype(int)
        if len(null_matrix.columns) > 1:
            null_corr = null_matrix.corr()
            results['null_correlations'] = null_corr.to_dict()
        
        # Find null clusters (rows with similar null patterns)
        if len(df) > 0:
            null_patterns = null_matrix.groupby(null_matrix.columns.tolist()).size()
            results['null_clusters'] = [
                {'pattern': str(pattern), 'count': int(count)}
                for pattern, count in null_patterns.items()
                if count > 1
            ]
        
        return results
    
    def handle_nulls(self, df: pd.DataFrame, strategy: str = 'drop', 
                     fill_value: Any = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Handle null values based on specified strategy.
        
        Args:
            df: DataFrame to process
            strategy: Strategy for handling nulls ('drop', 'fill', 'forward_fill', 'backward_fill')
            fill_value: Value to use for 'fill' strategy
            columns: Specific columns to process (if None, processes all columns)
            
        Returns:
            DataFrame with nulls handled
        """
        df_copy = df.copy()
        
        if columns is None:
            columns = df_copy.columns.tolist()
        
        for column in columns:
            if column not in df_copy.columns:
                continue
            
            if strategy == 'drop':
                df_copy = df_copy.dropna(subset=[column])
            elif strategy == 'fill':
                if fill_value is not None:
                    df_copy[column] = df_copy[column].fillna(fill_value)
                else:
                    # Use column-specific default fill values
                    if df_copy[column].dtype in ['int64', 'float64']:
                        df_copy[column] = df_copy[column].fillna(df_copy[column].median())
                    else:
                        df_copy[column] = df_copy[column].fillna(df_copy[column].mode().iloc[0] if not df_copy[column].mode().empty else 'Unknown')
            elif strategy == 'forward_fill':
                df_copy[column] = df_copy[column].fillna(method='ffill')
            elif strategy == 'backward_fill':
                df_copy[column] = df_copy[column].fillna(method='bfill')
        
        return df_copy
    
    def validate_critical_columns(self, df: pd.DataFrame, critical_columns: List[str]) -> Dict[str, Any]:
        """
        Validate that critical columns have no null values.
        
        Args:
            df: DataFrame to validate
            critical_columns: List of columns that must not contain nulls
            
        Returns:
            Dictionary containing validation results for critical columns
        """
        results = {
            'is_valid': True,
            'critical_nulls': {},
            'failed_columns': []
        }
        
        for column in critical_columns:
            if column in df.columns:
                null_count = df[column].isnull().sum()
                if null_count > 0:
                    results['critical_nulls'][column] = int(null_count)
                    results['failed_columns'].append(column)
                    results['is_valid'] = False
            else:
                results['failed_columns'].append(column)
                results['is_valid'] = False
                results['critical_nulls'][column] = 'Column not found'
        
        if not results['is_valid']:
            logger.warning(f"Critical column validation failed for {len(results['failed_columns'])} columns")
        
        return results
