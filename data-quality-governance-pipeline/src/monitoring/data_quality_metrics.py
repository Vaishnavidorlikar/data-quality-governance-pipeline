"""Data quality metrics calculation and analysis."""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DataQualityMetrics:
    """Calculates comprehensive data quality metrics and scores."""
    
    def __init__(self):
        """Initialize data quality metrics calculator."""
        self.metrics_history = []
    
    def calculate_quality_score(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate overall data quality score based on validation results.
        
        Args:
            df: DataFrame that was validated
            validation_results: Results from various validation checks
            
        Returns:
            Dictionary containing quality metrics and scores
        """
        metrics = {
            'completeness': self._calculate_completeness(df),
            'consistency': self._calculate_consistency(df),
            'validity': self._calculate_validity(df, validation_results),
            'timeliness': self._calculate_timeliness(df),
            'accuracy': self._calculate_accuracy(df),
            'uniqueness': self._calculate_uniqueness(df)
        }
        
        # Calculate overall score (weighted average)
        weights = {
            'completeness': 0.3,
            'consistency': 0.2,
            'validity': 0.25,
            'timeliness': 0.15,
            'accuracy': 0.1
        }
        
        overall_score = sum(metrics[metric] * weight for metric, weight in weights.items())
        
        # Determine quality grade
        if overall_score >= 90:
            grade = 'A'
        elif overall_score >= 80:
            grade = 'B'
        elif overall_score >= 70:
            grade = 'C'
        elif overall_score >= 60:
            grade = 'D'
        else:
            grade = 'F'
        
        metrics['overall_score'] = overall_score
        metrics['grade'] = grade
        metrics['weighted_scores'] = {metric: metrics[metric] * weight for metric, weight in weights.items()}
        
        logger.info(f"Quality score calculated: {overall_score:.2f} (Grade: {grade})")
        
        return metrics
    
    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        """
        Calculate completeness score based on missing values.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Completeness score (0-100)
        """
        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()
        completeness = ((total_cells - missing_cells) / total_cells) * 100
        
        return completeness
    
    def _calculate_consistency(self, df: pd.DataFrame) -> float:
        """
        Calculate consistency score based on duplicate records and format consistency.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Consistency score (0-100)
        """
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        consistency_penalty = (duplicate_count / len(df)) * 50
        
        # Check format consistency (simplified)
        format_consistency = 100  # Would need business rules for real implementation
        
        consistency_score = format_consistency - consistency_penalty
        
        return max(0, consistency_score)
    
    def _calculate_validity(self, df: pd.DataFrame, validation_results: Dict[str, Any]) -> float:
        """
        Calculate validity score based on validation results.
        
        Args:
            df: DataFrame that was validated
            validation_results: Results from validation checks
            
        Returns:
            Validity score (0-100)
        """
        total_checks = 0
        passed_checks = 0
        
        # Count validation checks
        for check_type, result in validation_results.items():
            if isinstance(result, dict):
                total_checks += 1
                if result.get('is_valid', result.get('is_acceptable', False)):
                    passed_checks += 1
        
        if total_checks == 0:
            return 100
        
        validity_score = (passed_checks / total_checks) * 100
        
        return validity_score
    
    def _calculate_timeliness(self, df: pd.DataFrame) -> float:
        """
        Calculate timeliness score based on data recency.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Timeliness score (0-100)
        """
        # Simplified implementation - would need date columns and business rules
        return 85.0  # Placeholder
    
    def _calculate_accuracy(self, df: pd.DataFrame) -> float:
        """
        Calculate accuracy score based on data format and type consistency.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Accuracy score (0-100)
        """
        # Simplified implementation - would need reference data
        return 90.0  # Placeholder
    
    def _calculate_uniqueness(self, df: pd.DataFrame) -> float:
        """
        Calculate uniqueness score based on duplicate records.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Uniqueness score (0-100)
        """
        duplicate_count = df.duplicated().sum()
        uniqueness_score = ((len(df) - duplicate_count) / len(df)) * 100
        
        return uniqueness_score
    
    def generate_quality_report(self, df: pd.DataFrame, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate comprehensive quality report.
        
        Args:
            df: DataFrame that was analyzed
            metrics: Calculated quality metrics
            
        Returns:
            Dictionary containing quality report
        """
        report = {
            'dataset_info': {
                'total_records': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
                'data_types': df.dtypes.value_counts().to_dict()
            },
            'quality_metrics': metrics,
            'column_analysis': {},
            'recommendations': []
        }
        
        # Analyze each column
        for col in df.columns:
            col_analysis = {
                'data_type': str(df[col].dtype),
                'null_count': df[col].isnull().sum(),
                'null_percentage': (df[col].isnull().sum() / len(df)) * 100,
                'unique_count': df[col].nunique(),
                'unique_percentage': (df[col].nunique() / len(df)) * 100
            }
            
            # Add numeric analysis for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_analysis.update({
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'median': df[col].median(),
                    'std': df[col].std()
                })
            
            report['column_analysis'][col] = col_analysis
        
        # Generate recommendations
        report['recommendations'] = self._generate_recommendations(metrics, df)
        
        return report
    
    def _generate_recommendations(self, metrics: Dict[str, Any], df: pd.DataFrame) -> List[str]:
        """
        Generate actionable recommendations based on quality metrics.
        
        Args:
            metrics: Quality metrics dictionary
            df: DataFrame that was analyzed
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Completeness recommendations
        if metrics['completeness'] < 90:
            recommendations.append(
                "Address missing values: Consider data imputation or improved data collection processes"
            )
        
        # Consistency recommendations
        if metrics['consistency'] < 80:
            recommendations.append(
                "Review data entry processes: Implement validation rules to prevent duplicates"
            )
        
        # Validity recommendations
        if metrics['validity'] < 85:
            recommendations.append(
                "Strengthen data validation: Implement automated checks for data format and business rules"
            )
        
        # Overall quality recommendations
        if metrics['overall_score'] < 75:
            recommendations.append(
                "Establish data governance framework: Implement comprehensive data quality management processes"
            )
        
        # Size-based recommendations
        if len(df) > 100000:
            recommendations.append(
                "Implement data processing optimization: Consider chunking or parallel processing for large datasets"
            )
        
        return recommendations
    
    def get_quality_trends(self, historical_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze quality trends over time.
        
        Args:
            historical_metrics: List of historical quality metrics
            
        Returns:
            Dictionary containing trend analysis
        """
        if len(historical_metrics) < 2:
            return {'error': 'Insufficient data for trend analysis'}
        
        # Extract scores over time
        scores = [m.get('overall_score', 0) for m in historical_metrics]
        
        trends = {
            'average_score': np.mean(scores),
            'min_score': np.min(scores),
            'max_score': np.max(scores),
            'score_trend': 'improving' if scores[-1] > scores[0] else 'declining',
            'score_volatility': np.std(scores),
            'data_points': len(scores)
        }
        
        logger.info(f"Quality trends analyzed: {trends['score_trend']} trend, volatility: {trends['score_volatility']:.2f}")
        
        return trends
