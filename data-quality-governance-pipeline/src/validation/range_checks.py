"""Range and constraint validation for data quality pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class RangeValidator:
    """Validates data ranges and constraints for quality assurance."""
    
    def __init__(self):
        """Initialize range validator."""
        self.validation_results = {}
    
    def detect_outliers(self, df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect outliers using multiple statistical methods.
        
        Args:
            df: DataFrame to analyze
            config: Configuration dictionary with method and thresholds
            
        Returns:
            Dictionary containing outlier detection results
        """
        method = config.get('method', 'iqr')
        threshold = config.get('threshold', 1.5)
        
        results = {
            'outliers': {},
            'total_outliers': 0,
            'columns_with_outliers': 0,
            'method': method,
            'threshold': threshold
        }
        
        # Get numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if method == 'iqr':
                outlier_results = self._detect_iqr_outliers(df[col], threshold)
            elif method == 'zscore':
                outlier_results = self._detect_zscore_outliers(df[col], threshold)
            elif method == 'isolation_forest':
                outlier_results = self._detect_isolation_forest_outliers(df[col])
            else:
                outlier_results = self._detect_iqr_outliers(df[col], threshold)
            
            results['outliers'][col] = outlier_results
            results['total_outliers'] += outlier_results['count']
            if outlier_results['count'] > 0:
                results['columns_with_outliers'] += 1
        
        logger.info(f"Outlier detection completed: {results['total_outliers']} outliers in {results['columns_with_outliers']} columns")
        
        return results
    
    def _detect_iqr_outliers(self, series: pd.Series, threshold: float = 1.5) -> Dict[str, Any]:
        """
        Detect outliers using Interquartile Range (IQR) method.
        
        Args:
            series: Pandas Series to analyze
            threshold: IQR multiplier for outlier detection
            
        Returns:
            Dictionary containing IQR outlier results
        """
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - (threshold * IQR)
        upper_bound = Q3 + (threshold * IQR)
        
        outlier_mask = (series < lower_bound) | (series > upper_bound)
        outlier_indices = series[outlier_mask].index.tolist()
        outlier_values = series[outlier_mask].tolist()
        
        return {
            'method': 'IQR',
            'Q1': Q1,
            'Q3': Q3,
            'IQR': IQR,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'threshold': threshold,
            'outlier_indices': outlier_indices,
            'outlier_values': outlier_values,
            'count': len(outlier_indices),
            'percentage': (len(outlier_indices) / len(series)) * 100
        }
    
    def _detect_zscore_outliers(self, series: pd.Series, threshold: float = 3.0) -> Dict[str, Any]:
        """
        Detect outliers using Z-score method.
        
        Args:
            series: Pandas Series to analyze
            threshold: Z-score threshold for outlier detection
            
        Returns:
            Dictionary containing Z-score outlier results
        """
        mean = series.mean()
        std = series.std()
        z_scores = np.abs((series - mean) / std)
        
        outlier_mask = z_scores > threshold
        outlier_indices = series[outlier_mask].index.tolist()
        outlier_values = series[outlier_mask].tolist()
        
        return {
            'method': 'Z-Score',
            'mean': mean,
            'std': std,
            'threshold': threshold,
            'outlier_indices': outlier_indices,
            'outlier_values': outlier_values,
            'count': len(outlier_indices),
            'percentage': (len(outlier_indices) / len(series)) * 100
        }
    
    def _detect_isolation_forest_outliers(self, series: pd.Series) -> Dict[str, Any]:
        """
        Detect outliers using Isolation Forest method.
        
        Args:
            series: Pandas Series to analyze
            
        Returns:
            Dictionary containing Isolation Forest outlier results
        """
        try:
            from sklearn.ensemble import IsolationForest
            
            # Reshape data for sklearn
            data = series.values.reshape(-1, 1)
            
            # Fit Isolation Forest
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            outlier_labels = iso_forest.fit_predict(data)
            
            # Get outliers (label = -1)
            outlier_mask = outlier_labels == -1
            outlier_indices = series[outlier_mask].index.tolist()
            outlier_values = series[outlier_mask].tolist()
            
            return {
                'method': 'Isolation Forest',
                'contamination': 0.1,
                'outlier_indices': outlier_indices,
                'outlier_values': outlier_values,
                'count': len(outlier_indices),
                'percentage': (len(outlier_indices) / len(series)) * 100
            }
        except ImportError:
            logger.warning("sklearn not available, falling back to IQR method")
            return self._detect_iqr_outliers(series)
    
    def validate_ranges(self, df: pd.DataFrame, range_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data against specified ranges and constraints.
        
        Args:
            df: DataFrame to validate
            range_config: Configuration dictionary with range definitions
            
        Returns:
            Dictionary containing range validation results
        """
        results = {
            'is_valid': True,
            'violations': [],
            'total_violations': 0
        }
        
        for column, config in range_config.items():
            if column in df.columns:
                violation = self._validate_column_range(df[column], config)
                if violation:
                    results['violations'].append(violation)
                    results['total_violations'] += 1
                    results['is_valid'] = False
                    logger.warning(f"Range violation in column {column}: {violation}")
        
        logger.info(f"Range validation completed: {results['total_violations']} violations")
        
        return results
    
    def _validate_column_range(self, series: pd.Series, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validate a single column against range configuration.
        
        Args:
            series: Pandas Series to validate
            config: Configuration for this column
            
        Returns:
            Violation dictionary or None if valid
        """
        min_value = config.get('min')
        max_value = config.get('max')
        allowed_values = config.get('allowed_values')
        pattern = config.get('pattern')
        
        # Check min/max range
        if min_value is not None or max_value is not None:
            violations = []
            
            if min_value is not None:
                min_violations = (series < min_value)
                if min_violations.any():
                    violations.extend({
                        'type': 'min_violation',
                        'value': val,
                        'min_value': min_value
                    } for val in series[min_violations])
            
            if max_value is not None:
                max_violations = (series > max_value)
                if max_violations.any():
                    violations.extend({
                        'type': 'max_violation',
                        'value': val,
                        'max_value': max_value
                    } for val in series[max_violations])
            
            if violations:
                return {
                    'column': series.name,
                    'violations': violations
                }
        
        # Check allowed values
        if allowed_values is not None:
            invalid_values = series[~series.isin(allowed_values)]
            if not invalid_values.empty:
                return {
                    'column': series.name,
                    'type': 'invalid_values',
                    'invalid_values': invalid_values.unique().tolist(),
                    'allowed_values': allowed_values
                }
        
        # Check pattern matching
        if pattern is not None:
            import re
            pattern_violations = series[~series.astype(str).str.match(pattern, na=False)]
            if not pattern_violations.empty:
                return {
                    'column': series.name,
                    'type': 'pattern_violation',
                    'invalid_values': pattern_violations.astype(str).tolist(),
                    'pattern': pattern
                }
        
        return None
    
    def get_range_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for range validation.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing range summary
        """
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        summary = {
            'total_columns': len(df.columns),
            'numeric_columns': len(numeric_columns),
            'column_ranges': {},
            'outlier_candidates': []
        }
        
        for col in numeric_columns:
            series = df[col]
            summary['column_ranges'][col] = {
                'min': series.min(),
                'max': series.max(),
                'mean': series.mean(),
                'median': series.median(),
                'std': series.std(),
                'q25': series.quantile(0.25),
                'q75': series.quantile(0.75)
            }
        
        logger.info(f"Range summary generated for {len(numeric_columns)} numeric columns")
        
        return summary
