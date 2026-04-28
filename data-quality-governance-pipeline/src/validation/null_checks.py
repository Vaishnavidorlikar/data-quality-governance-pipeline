"""Null value analysis for data quality pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class NullValidator:
    """Analyzes null values in data and provides quality insights."""
    
    def __init__(self):
        """Initialize null validator."""
        self.validation_results = {}
    
    def check_null_values(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check for null values against configured thresholds.
        
        Args:
            df: DataFrame to analyze
            config: Configuration dictionary with thresholds and settings
            
        Returns:
            Dictionary containing null validation results
        """
        threshold = config.get('null_threshold', 0.1)
        critical_columns = config.get('critical_columns', [])
        
        results = {
            'is_acceptable': True,
            'total_nulls': df.isnull().sum().sum(),
            'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'column_analysis': {},
            'violations': []
        }
        
        # Analyze each column
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            
            column_analysis = {
                'null_count': null_count,
                'null_percentage': null_pct,
                'is_critical': col in critical_columns,
                'is_acceptable': null_pct <= (threshold * 100)
            }
            
            results['column_analysis'][col] = column_analysis
            
            # Check for violations
            if null_pct > (threshold * 100):
                violation = {
                    'column': col,
                    'null_percentage': null_pct,
                    'threshold': threshold * 100,
                    'is_critical': col in critical_columns,
                    'severity': 'HIGH' if col in critical_columns else 'MEDIUM'
                }
                results['violations'].append(violation)
                results['is_acceptable'] = False
                logger.warning(f"Null value violation in column {col}: {null_pct:.2f}% > {threshold * 100}%")
        
        results['total_columns'] = len(df.columns)
        results['total_rows'] = len(df)
        results['threshold'] = threshold
        results['violations_count'] = len(results['violations'])
        
        logger.info(f"Null value analysis completed: {results['total_nulls']} nulls ({results['null_percentage']:.2f}%)")
        
        return results
    
    def get_null_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for null values.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing null value summary
        """
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df)) * 100
        
        summary = {
            'total_columns': len(df.columns),
            'total_rows': len(df),
            'columns_with_nulls': (null_counts > 0).sum(),
            'columns_without_nulls': (null_counts == 0).sum(),
            'highest_null_percentage': null_percentages.max() if len(null_percentages) > 0 else 0,
            'average_null_percentage': null_percentages.mean() if len(null_percentages) > 0 else 0,
            'null_counts_by_column': null_counts.to_dict(),
            'null_percentages_by_column': null_percentages.to_dict()
        }
        
        logger.info(f"Null summary generated: {summary['columns_with_nulls']}/{summary['total_columns']} columns have null values")
        
        return summary
    
    def suggest_null_handling(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """
        Suggest strategies for handling null values in a specific column.
        
        Args:
            df: DataFrame to analyze
            column: Column name to analyze
            
        Returns:
            Dictionary containing handling suggestions
        """
        if column not in df.columns:
            return {'error': f"Column '{column}' not found in DataFrame"}
        
        null_count = df[column].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        data_type = str(df[column].dtype)
        
        suggestions = {
            'column': column,
            'null_count': null_count,
            'null_percentage': null_pct,
            'data_type': data_type,
            'strategies': []
        }
        
        # Suggest strategies based on data type and null percentage
        if null_pct == 0:
            suggestions['strategies'].append({
                'strategy': 'No action needed',
                'reason': 'Column has no null values'
            })
        elif null_pct < 5:
            suggestions['strategies'].append({
                'strategy': 'Drop rows',
                'reason': 'Low null percentage, safe to remove affected rows'
            })
        elif 'int' in data_type or 'float' in data_type:
            if null_pct < 20:
                suggestions['strategies'].append({
                    'strategy': 'Mean/median imputation',
                    'reason': 'Numerical column with moderate nulls, use central tendency'
                })
            else:
                suggestions['strategies'].append({
                    'strategy': 'Advanced imputation',
                    'reason': 'High null percentage, consider ML-based imputation'
                })
        elif 'object' in data_type:
            if null_pct < 10:
                suggestions['strategies'].append({
                    'strategy': 'Mode imputation',
                    'reason': 'Categorical column with low nulls, use most frequent value'
                })
            else:
                suggestions['strategies'].append({
                    'strategy': 'Create "Unknown" category',
                    'reason': 'High null percentage in categorical column'
                })
        
        return suggestions
