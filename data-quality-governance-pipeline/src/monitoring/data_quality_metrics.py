"""
Data quality metrics and monitoring module.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime, timedelta
import json
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataQualityMetrics:
    """Calculates and tracks data quality metrics over time."""
    
    def __init__(self):
        """Initialize data quality metrics calculator."""
        self.metrics_history = defaultdict(list)
    
    def calculate_completeness_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate completeness metrics for the dataset.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing completeness metrics
        """
        metrics = {
            'overall_completeness': 0.0,
            'column_completeness': {},
            'row_completeness': {},
            'completeness_score': 0.0
        }
        
        total_cells = len(df) * len(df.columns)
        non_null_cells = total_cells - df.isnull().sum().sum()
        
        metrics['overall_completeness'] = non_null_cells / total_cells if total_cells > 0 else 0
        
        # Column-level completeness
        for column in df.columns:
            non_null_count = df[column].count()
            total_count = len(df)
            completeness = non_null_count / total_count if total_count > 0 else 0
            
            metrics['column_completeness'][column] = {
                'completeness': completeness,
                'null_count': int(total_count - non_null_count),
                'total_count': int(total_count)
            }
        
        # Row-level completeness
        row_completeness = df.notnull().sum(axis=1) / len(df.columns)
        metrics['row_completeness'] = {
            'mean_row_completeness': float(row_completeness.mean()),
            'min_row_completeness': float(row_completeness.min()),
            'max_row_completeness': float(row_completeness.max()),
            'rows_with_all_data': int((row_completeness == 1.0).sum()),
            'rows_with_partial_data': int(((row_completeness > 0) & (row_completeness < 1)).sum()),
            'rows_with_no_data': int((row_completeness == 0).sum())
        }
        
        # Overall completeness score (weighted average)
        metrics['completeness_score'] = metrics['overall_completeness']
        
        return metrics
    
    def calculate_accuracy_metrics(self, df: pd.DataFrame, reference_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Calculate accuracy metrics comparing against reference data.
        
        Args:
            df: DataFrame to evaluate
            reference_data: Reference DataFrame for comparison
            
        Returns:
            Dictionary containing accuracy metrics
        """
        metrics = {
            'accuracy_score': 0.0,
            'column_accuracy': {},
            'data_drift': {},
            'validation_errors': 0
        }
        
        if reference_data is None:
            # If no reference data, return placeholder metrics
            metrics['accuracy_score'] = 1.0  # Assume accurate if no reference
            for column in df.columns:
                metrics['column_accuracy'][column] = {
                    'accuracy': 1.0,
                    'mismatch_count': 0,
                    'total_comparisons': len(df)
                }
            return metrics
        
        # Ensure DataFrames have the same structure
        common_columns = set(df.columns) & set(reference_data.columns)
        
        for column in common_columns:
            # Compare values between datasets
            matches = (df[column].fillna('') == reference_data[column].fillna('')).sum()
            total_comparisons = len(df)
            accuracy = matches / total_comparisons if total_comparisons > 0 else 0
            
            metrics['column_accuracy'][column] = {
                'accuracy': accuracy,
                'mismatch_count': int(total_comparisons - matches),
                'total_comparisons': int(total_comparisons)
            }
        
        # Calculate overall accuracy
        if metrics['column_accuracy']:
            overall_accuracy = np.mean([
                col_metrics['accuracy'] 
                for col_metrics in metrics['column_accuracy'].values()
            ])
            metrics['accuracy_score'] = overall_accuracy
        
        return metrics
    
    def calculate_consistency_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate consistency metrics across the dataset.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing consistency metrics
        """
        metrics = {
            'consistency_score': 0.0,
            'duplicate_rows': 0,
            'duplicate_percentage': 0.0,
            'column_consistency': {},
            'format_consistency': {}
        }
        
        # Check for duplicate rows
        total_rows = len(df)
        duplicate_rows = df.duplicated().sum()
        metrics['duplicate_rows'] = int(duplicate_rows)
        metrics['duplicate_percentage'] = duplicate_rows / total_rows if total_rows > 0 else 0
        
        # Column-level consistency checks
        for column in df.columns:
            col_consistency = {
                'unique_values': int(df[column].nunique()),
                'consistency_ratio': 0.0,
                'data_type_changes': 0,
                'format_violations': 0
            }
            
            # Calculate consistency ratio (1 - duplicate ratio for column)
            if len(df) > 0:
                col_consistency['consistency_ratio'] = df[column].nunique() / len(df)
            
            # Check for data type inconsistencies
            if df[column].dtype == 'object':
                # Check if mixed data types in string column
                numeric_in_string = pd.to_numeric(df[column], errors='coerce').notna().sum()
                if numeric_in_string > 0:
                    col_consistency['format_violations'] = int(numeric_in_string)
            
            metrics['column_consistency'][column] = col_consistency
        
        # Calculate overall consistency score
        consistency_factors = [
            1 - metrics['duplicate_percentage'],  # Lower duplicates = higher consistency
            np.mean([
                col['consistency_ratio'] 
                for col in metrics['column_consistency'].values()
            ]) if metrics['column_consistency'] else 1.0
        ]
        metrics['consistency_score'] = np.mean(consistency_factors)
        
        return metrics
    
    def calculate_timeliness_metrics(self, df: pd.DataFrame, date_columns: List[str]) -> Dict[str, Any]:
        """
        Calculate timeliness metrics for date columns.
        
        Args:
            df: DataFrame to analyze
            date_columns: List of date column names to analyze
            
        Returns:
            Dictionary containing timeliness metrics
        """
        metrics = {
            'timeliness_score': 0.0,
            'column_timeliness': {},
            'data_freshness': {},
            'update_frequency': {}
        }
        
        current_date = datetime.now()
        
        for column in date_columns:
            if column not in df.columns:
                continue
            
            try:
                date_series = pd.to_datetime(df[column], errors='coerce').dropna()
                
                if len(date_series) == 0:
                    continue
                
                col_metrics = {
                    'latest_date': date_series.max(),
                    'earliest_date': date_series.min(),
                    'date_range_days': (date_series.max() - date_series.min()).days,
                    'days_since_latest': (current_date - date_series.max()).days,
                    'freshness_score': 0.0
                }
                
                # Calculate freshness score (0 = very old, 1 = very recent)
                days_since_latest = col_metrics['days_since_latest']
                if days_since_latest <= 1:
                    freshness_score = 1.0
                elif days_since_latest <= 7:
                    freshness_score = 0.8
                elif days_since_latest <= 30:
                    freshness_score = 0.6
                elif days_since_latest <= 90:
                    freshness_score = 0.4
                else:
                    freshness_score = 0.2
                
                col_metrics['freshness_score'] = freshness_score
                
                metrics['column_timeliness'][column] = col_metrics
                
            except Exception as e:
                logger.warning(f"Error processing date column {column}: {e}")
                continue
        
        # Calculate overall timeliness score
        if metrics['column_timeliness']:
            metrics['timeliness_score'] = np.mean([
                col['freshness_score'] 
                for col in metrics['column_timeliness'].values()
            ])
        
        return metrics
    
    def calculate_validity_metrics(self, df: pd.DataFrame, validation_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate validity metrics based on validation rules.
        
        Args:
            df: DataFrame to validate
            validation_rules: Dictionary containing validation rules
            
        Returns:
            Dictionary containing validity metrics
        """
        metrics = {
            'validity_score': 0.0,
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'column_validity': {}
        }
        
        for column, rules in validation_rules.items():
            if column not in df.columns:
                continue
            
            col_validity = {
                'valid_records': 0,
                'invalid_records': 0,
                'validity_percentage': 0.0,
                'violations': []
            }
            
            column_data = df[column].dropna()
            total_records = len(column_data)
            
            if total_records == 0:
                metrics['column_validity'][column] = col_validity
                continue
            
            valid_mask = pd.Series([True] * total_records)
            
            # Apply various validation rules
            if 'required' in rules and rules['required']:
                valid_mask &= column_data.notna()
            
            if 'data_type' in rules:
                expected_type = rules['data_type']
                if expected_type == 'numeric':
                    valid_mask &= pd.to_numeric(column_data, errors='coerce').notna()
                elif expected_type == 'email':
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    valid_mask &= column_data.str.match(email_pattern, na=False)
                elif expected_type == 'phone':
                    phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
                    valid_mask &= column_data.str.match(phone_pattern, na=False)
            
            if 'min_length' in rules:
                valid_mask &= column_data.astype(str).str.len() >= rules['min_length']
            
            if 'max_length' in rules:
                valid_mask &= column_data.astype(str).str.len() <= rules['max_length']
            
            if 'allowed_values' in rules:
                valid_mask &= column_data.isin(rules['allowed_values'])
            
            col_validity['valid_records'] = int(valid_mask.sum())
            col_validity['invalid_records'] = int((~valid_mask).sum())
            col_validity['validity_percentage'] = col_validity['valid_records'] / total_records
            
            if col_validity['invalid_records'] > 0:
                invalid_values = column_data[~valid_mask].unique()[:10]  # Limit to first 10
                col_validity['violations'] = invalid_values.tolist()
            
            metrics['column_validity'][column] = col_validity
            metrics['total_validations'] += 1
            if col_validity['validity_percentage'] >= 0.95:  # 95% threshold
                metrics['passed_validations'] += 1
            else:
                metrics['failed_validations'] += 1
        
        # Calculate overall validity score
        if metrics['total_validations'] > 0:
            metrics['validity_score'] = metrics['passed_validations'] / metrics['total_validations']
        
        return metrics
    
    def calculate_overall_quality_score(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate overall data quality score from individual metrics.
        
        Args:
            metrics: Dictionary containing all individual metrics
            
        Returns:
            Dictionary containing overall quality score and breakdown
        """
        weights = {
            'completeness': 0.25,
            'accuracy': 0.25,
            'consistency': 0.20,
            'timeliness': 0.15,
            'validity': 0.15
        }
        
        scores = {}
        
        # Extract individual scores
        if 'completeness_metrics' in metrics:
            scores['completeness'] = metrics['completeness_metrics']['completeness_score']
        
        if 'accuracy_metrics' in metrics:
            scores['accuracy'] = metrics['accuracy_metrics']['accuracy_score']
        
        if 'consistency_metrics' in metrics:
            scores['consistency'] = metrics['consistency_metrics']['consistency_score']
        
        if 'timeliness_metrics' in metrics:
            scores['timeliness'] = metrics['timeliness_metrics']['timeliness_score']
        
        if 'validity_metrics' in metrics:
            scores['validity'] = metrics['validity_metrics']['validity_score']
        
        # Calculate weighted average
        overall_score = 0.0
        total_weight = 0.0
        
        for metric, score in scores.items():
            if metric in weights:
                overall_score += weights[metric] * score
                total_weight += weights[metric]
        
        if total_weight > 0:
            overall_score /= total_weight
        
        return {
            'overall_quality_score': overall_score,
            'individual_scores': scores,
            'weights_used': weights,
            'quality_grade': self._get_quality_grade(overall_score),
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to grade."""
        if score >= 0.95:
            return 'A+ (Excellent)'
        elif score >= 0.90:
            return 'A (Very Good)'
        elif score >= 0.80:
            return 'B (Good)'
        elif score >= 0.70:
            return 'C (Fair)'
        elif score >= 0.60:
            return 'D (Poor)'
        else:
            return 'F (Very Poor)'
    
    def track_metrics_over_time(self, dataset_name: str, metrics: Dict[str, Any]) -> None:
        """
        Store metrics for historical tracking.
        
        Args:
            dataset_name: Name of the dataset
            metrics: Metrics dictionary to store
        """
        timestamp = datetime.now().isoformat()
        
        self.metrics_history[dataset_name].append({
            'timestamp': timestamp,
            'metrics': metrics
        })
        
        # Keep only last 100 entries per dataset to prevent memory issues
        if len(self.metrics_history[dataset_name]) > 100:
            self.metrics_history[dataset_name] = self.metrics_history[dataset_name][-100:]
    
    def get_metrics_trend(self, dataset_name: str, metric_name: str) -> Dict[str, Any]:
        """
        Analyze trend of a specific metric over time.
        
        Args:
            dataset_name: Name of the dataset
            metric_name: Name of the metric to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        if dataset_name not in self.metrics_history:
            return {'trend': 'No data available'}
        
        history = self.metrics_history[dataset_name]
        if len(history) < 2:
            return {'trend': 'Insufficient data for trend analysis'}
        
        # Extract metric values over time
        values = []
        timestamps = []
        
        for entry in history:
            # Navigate through nested structure to find the metric
            metric_value = self._extract_metric_value(entry['metrics'], metric_name)
            if metric_value is not None:
                values.append(metric_value)
                timestamps.append(entry['timestamp'])
        
        if len(values) < 2:
            return {'trend': 'Insufficient data for trend analysis'}
        
        # Calculate trend
        recent_avg = np.mean(values[-3:]) if len(values) >= 3 else values[-1]
        older_avg = np.mean(values[:3]) if len(values) >= 3 else values[0]
        
        trend_direction = 'stable'
        if recent_avg > older_avg * 1.05:
            trend_direction = 'improving'
        elif recent_avg < older_avg * 0.95:
            trend_direction = 'declining'
        
        return {
            'trend': trend_direction,
            'current_value': values[-1],
            'recent_average': recent_avg,
            'historical_average': np.mean(values),
            'data_points': len(values),
            'time_range': {
                'start': timestamps[0],
                'end': timestamps[-1]
            }
        }
    
    def _extract_metric_value(self, metrics: Dict[str, Any], metric_name: str) -> Optional[float]:
        """Extract metric value from nested metrics dictionary."""
        # Try direct access first
        if metric_name in metrics:
            return float(metrics[metric_name])
        
        # Try nested access
        for key, value in metrics.items():
            if isinstance(value, dict) and metric_name in value:
                return float(value[metric_name])
        
        return None
    
    def generate_quality_report(self, dataset_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate a comprehensive quality report.
        
        Args:
            dataset_name: Name of the dataset
            metrics: Complete metrics dictionary
            
        Returns:
            Formatted quality report string
        """
        overall_score = metrics.get('overall_quality_score', {}).get('overall_quality_score', 0)
        quality_grade = metrics.get('overall_quality_score', {}).get('quality_grade', 'Unknown')
        
        report = f"""
DATA QUALITY REPORT
==================
Dataset: {dataset_name}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERALL QUALITY SCORE: {overall_score:.3f}
QUALITY GRADE: {quality_grade}

DETAILED METRICS:
"""
        
        # Add individual metric sections
        if 'completeness_metrics' in metrics:
            comp = metrics['completeness_metrics']
            report += f"""
Completeness:
- Overall Completeness: {comp['overall_completeness']:.3f}
- Mean Row Completeness: {comp['row_completeness']['mean_row_completeness']:.3f}
- Rows with All Data: {comp['row_completeness']['rows_with_all_data']}
"""
        
        if 'accuracy_metrics' in metrics:
            acc = metrics['accuracy_metrics']
            report += f"""
Accuracy:
- Overall Accuracy: {acc['accuracy_score']:.3f}
- Validation Errors: {acc['validation_errors']}
"""
        
        if 'consistency_metrics' in metrics:
            cons = metrics['consistency_metrics']
            report += f"""
Consistency:
- Consistency Score: {cons['consistency_score']:.3f}
- Duplicate Rows: {cons['duplicate_rows']} ({cons['duplicate_percentage']:.3f})
"""
        
        if 'timeliness_metrics' in metrics:
            tim = metrics['timeliness_metrics']
            report += f"""
Timeliness:
- Timeliness Score: {tim['timeliness_score']:.3f}
"""
        
        if 'validity_metrics' in metrics:
            val = metrics['validity_metrics']
            report += f"""
Validity:
- Validity Score: {val['validity_score']:.3f}
- Passed Validations: {val['passed_validations']}/{val['total_validations']}
"""
        
        return report
