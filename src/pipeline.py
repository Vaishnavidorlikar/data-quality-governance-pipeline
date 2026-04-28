"""
Main data quality governance pipeline orchestrator.
"""

import pandas as pd
import yaml
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from validation.schema_checks import SchemaValidator
from validation.null_checks import NullValidator
from validation.range_checks import RangeValidator
from monitoring.data_quality_metrics import DataQualityMetrics
from governance.lineage_tracker import LineageTracker
from governance.audit_logger import AuditLogger, AuditEventType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityPipeline:
    """Main pipeline for data quality governance."""
    
    def __init__(self, config_path: str = "configs/validation_rules.yaml"):
        """
        Initialize the data quality pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
        # Initialize components
        self.schema_validator = SchemaValidator(self.config.get('schema', {}))
        self.null_validator = NullValidator(self.config.get('null_threshold', 0.1))
        self.range_validator = RangeValidator()
        self.metrics_calculator = DataQualityMetrics()
        self.lineage_tracker = LineageTracker()
        self.audit_logger = AuditLogger()
        
        # Pipeline state
        self.current_dataset_id = None
        self.pipeline_results = {}
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'schema': {},
            'null_threshold': 0.1,
            'range_rules': {},
            'categorical_rules': {},
            'string_rules': {},
            'validation_rules': {},
            'critical_columns': [],
            'date_columns': []
        }
    
    def run_pipeline(self, data_source: str, dataset_name: str, 
                    user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the complete data quality pipeline.
        
        Args:
            data_source: Path to data file or DataFrame
            dataset_name: Name for the dataset
            user_id: ID of the user running the pipeline
            
        Returns:
            Dictionary containing pipeline results
        """
        logger.info(f"Starting data quality pipeline for {dataset_name}")
        
        # Log pipeline start
        pipeline_id = self.audit_logger.log_event(
            event_type=AuditEventType.DATA_PROCESSING,
            user_id=user_id,
            resource_type="pipeline",
            resource_id=dataset_name,
            action="run_quality_pipeline",
            details={'dataset_name': dataset_name, 'data_source': data_source}
        )
        
        try:
            # Load data
            df = self._load_data(data_source)
            
            # Register dataset in lineage tracker
            self.current_dataset_id = self.lineage_tracker.register_dataset(
                df=df,
                dataset_name=dataset_name,
                source_path=data_source if isinstance(data_source, str) else None,
                metadata={'pipeline_run_id': pipeline_id}
            )
            
            # Run validation stages
            self.pipeline_results = {
                'dataset_id': self.current_dataset_id,
                'dataset_name': dataset_name,
                'pipeline_id': pipeline_id,
                'timestamp': datetime.now().isoformat(),
                'validation_results': {},
                'quality_metrics': {},
                'overall_status': 'success'
            }
            
            # Stage 1: Schema Validation
            schema_results = self._run_schema_validation(df)
            self.pipeline_results['validation_results']['schema'] = schema_results
            
            # Stage 2: Null Value Validation
            null_results = self._run_null_validation(df)
            self.pipeline_results['validation_results']['null'] = null_results
            
            # Stage 3: Range and Constraint Validation
            range_results = self._run_range_validation(df)
            self.pipeline_results['validation_results']['range'] = range_results
            
            # Stage 4: Calculate Quality Metrics
            quality_metrics = self._calculate_quality_metrics(df)
            self.pipeline_results['quality_metrics'] = quality_metrics
            
            # Stage 5: Generate Overall Assessment
            overall_assessment = self._generate_overall_assessment()
            self.pipeline_results['overall_assessment'] = overall_assessment
            
            # Log quality check event
            self.audit_logger.log_quality_check(
                user_id=user_id or 'system',
                check_type='comprehensive',
                dataset_id=self.current_dataset_id,
                quality_metrics=quality_metrics
            )
            
            # Generate and save reports
            self._generate_reports()
            
            logger.info(f"Pipeline completed successfully for {dataset_name}")
            
        except Exception as e:
            logger.error(f"Pipeline failed for {dataset_name}: {e}")
            self.pipeline_results['overall_status'] = 'failed'
            self.pipeline_results['error'] = str(e)
            
            # Log error event
            self.audit_logger.log_event(
                event_type=AuditEventType.SYSTEM_ERROR,
                user_id=user_id,
                resource_type="pipeline",
                resource_id=dataset_name,
                action="run_quality_pipeline",
                success=False,
                error_message=str(e)
            )
        
        return self.pipeline_results
    
    def _load_data(self, data_source) -> pd.DataFrame:
        """Load data from various sources."""
        if isinstance(data_source, pd.DataFrame):
            return data_source
        elif isinstance(data_source, str):
            file_path = Path(data_source)
            if not file_path.exists():
                raise FileNotFoundError(f"Data file not found: {data_source}")
            
            # Determine file type and load accordingly
            if file_path.suffix.lower() == '.csv':
                return pd.read_csv(data_source)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                return pd.read_excel(data_source)
            elif file_path.suffix.lower() == '.json':
                return pd.read_json(data_source)
            elif file_path.suffix.lower() == '.parquet':
                return pd.read_parquet(data_source)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
        else:
            raise ValueError("data_source must be a DataFrame or file path")
    
    def _run_schema_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run schema validation stage."""
        logger.info("Running schema validation")
        
        results = self.schema_validator.validate_schema(df)
        
        # Log validation execution
        self.audit_logger.log_validation_execution(
            user_id='system',
            validation_id='schema_check',
            dataset_id=self.current_dataset_id,
            validation_results=results
        )
        
        return results
    
    def _run_null_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run null value validation stage."""
        logger.info("Running null value validation")
        
        results = {}
        
        # General null checks
        null_results = self.null_validator.check_null_values(df)
        results['general'] = null_results
        
        # Critical column checks
        critical_columns = self.config.get('critical_columns', [])
        if critical_columns:
            critical_results = self.null_validator.validate_critical_columns(df, critical_columns)
            results['critical_columns'] = critical_results
        
        # Null pattern analysis
        pattern_results = self.null_validator.check_null_patterns(df)
        results['patterns'] = pattern_results
        
        # Log validation execution
        self.audit_logger.log_validation_execution(
            user_id='system',
            validation_id='null_check',
            dataset_id=self.current_dataset_id,
            validation_results={'is_valid': null_results['is_valid'], 'details': results}
        )
        
        return results
    
    def _run_range_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run range and constraint validation stage."""
        logger.info("Running range and constraint validation")
        
        results = {}
        
        # Numeric range checks
        range_rules = self.config.get('range_rules', {})
        if range_rules:
            numeric_results = self.range_validator.check_numeric_ranges(df, range_rules)
            results['numeric_ranges'] = numeric_results
        
        # Date range checks
        date_columns = self.config.get('date_columns', [])
        if date_columns:
            date_rules = self.config.get('date_rules', {})
            date_results = self.range_validator.check_date_ranges(df, date_rules)
            results['date_ranges'] = date_results
        
        # Categorical constraint checks
        categorical_rules = self.config.get('categorical_rules', {})
        if categorical_rules:
            categorical_results = self.range_validator.check_categorical_constraints(df, categorical_rules)
            results['categorical'] = categorical_results
        
        # String constraint checks
        string_rules = self.config.get('string_rules', {})
        if string_rules:
            string_results = self.range_validator.check_string_constraints(df, string_rules)
            results['string'] = string_results
        
        # Outlier detection
        numeric_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        if numeric_columns:
            outlier_results = self.range_validator.detect_outliers(df, numeric_columns)
            results['outliers'] = outlier_results
        
        # Determine overall validity
        is_valid = True
        for category, category_results in results.items():
            if isinstance(category_results, dict) and 'is_valid' in category_results:
                if not category_results['is_valid']:
                    is_valid = False
                    break
        
        # Log validation execution
        self.audit_logger.log_validation_execution(
            user_id='system',
            validation_id='range_check',
            dataset_id=self.current_dataset_id,
            validation_results={'is_valid': is_valid, 'details': results}
        )
        
        return results
    
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive quality metrics."""
        logger.info("Calculating quality metrics")
        
        metrics = {}
        
        # Completeness metrics
        completeness_metrics = self.metrics_calculator.calculate_completeness_metrics(df)
        metrics['completeness_metrics'] = completeness_metrics
        
        # Consistency metrics
        consistency_metrics = self.metrics_calculator.calculate_consistency_metrics(df)
        metrics['consistency_metrics'] = consistency_metrics
        
        # Timeliness metrics (if date columns available)
        date_columns = self.config.get('date_columns', [])
        if date_columns:
            timeliness_metrics = self.metrics_calculator.calculate_timeliness_metrics(df, date_columns)
            metrics['timeliness_metrics'] = timeliness_metrics
        
        # Validity metrics (if validation rules available)
        validation_rules = self.config.get('validation_rules', {})
        if validation_rules:
            validity_metrics = self.metrics_calculator.calculate_validity_metrics(df, validation_rules)
            metrics['validity_metrics'] = validity_metrics
        
        # Calculate overall quality score
        overall_score = self.metrics_calculator.calculate_overall_quality_score(metrics)
        metrics['overall_quality_score'] = overall_score
        
        # Track metrics over time
        self.metrics_calculator.track_metrics_over_time(
            dataset_name=self.pipeline_results['dataset_name'],
            metrics=metrics
        )
        
        return metrics
    
    def _generate_overall_assessment(self) -> Dict[str, Any]:
        """Generate overall assessment of data quality."""
        validation_results = self.pipeline_results['validation_results']
        quality_metrics = self.pipeline_results['quality_metrics']
        
        assessment = {
            'overall_grade': 'A',
            'total_issues': 0,
            'critical_issues': 0,
            'warnings': 0,
            'recommendations': []
        }
        
        # Count issues from validation results
        for validation_type, results in validation_results.items():
            if isinstance(results, dict):
                if 'is_valid' in results and not results['is_valid']:
                    assessment['total_issues'] += 1
                
                # Count specific issue types
                if validation_type == 'schema':
                    assessment['total_issues'] += len(results.get('missing_columns', []))
                    assessment['total_issues'] += len(results.get('type_mismatches', []))
                
                elif validation_type == 'null':
                    if 'general' in results:
                        assessment['total_issues'] += len(results['general'].get('columns_above_threshold', []))
                    if 'critical_columns' in results and not results['critical_columns']['is_valid']:
                        assessment['critical_issues'] += len(results['critical_columns'].get('failed_columns', []))
                
                elif validation_type == 'range':
                    for category, category_results in results.items():
                        if isinstance(category_results, dict) and 'total_violations' in category_results:
                            assessment['total_issues'] += category_results['total_violations']
        
        # Determine grade based on quality score
        overall_score = quality_metrics.get('overall_quality_score', 1.0)
        if isinstance(overall_score, dict):
            # Handle case where it might be a nested dict
            overall_score = overall_score.get('overall_quality_score', 1.0)
        assessment['overall_grade'] = self.metrics_calculator._get_quality_grade(overall_score)
        
        # Generate recommendations
        if assessment['critical_issues'] > 0:
            assessment['recommendations'].append("Address critical data quality issues immediately")
        
        if overall_score < 0.8:
            assessment['recommendations'].append("Implement data quality improvements to raise score above 80%")
        
        if validation_results.get('null', {}).get('general', {}).get('overall_null_percentage', 0) > 0.1:
            assessment['recommendations'].append("Review and handle null values in the dataset")
        
        return assessment
    
    def _generate_reports(self) -> None:
        """Generate and save quality reports."""
        dataset_name = self.pipeline_results['dataset_name']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Generate quality report
        quality_report = self.metrics_calculator.generate_quality_report(
            dataset_name, 
            self.pipeline_results['quality_metrics']
        )
        
        # Save quality report
        report_path = Path(f"reports/quality_reports/{dataset_name}_quality_report_{timestamp}.txt")
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(quality_report)
        
        # Export lineage report
        lineage_report_path = Path(f"reports/quality_reports/{dataset_name}_lineage_{timestamp}.json")
        self.lineage_tracker.export_lineage_report(self.current_dataset_id, str(lineage_report_path))
        
        logger.info(f"Reports generated for {dataset_name}")
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get a summary of the latest pipeline run."""
        if not self.pipeline_results:
            return {
                'status': 'No pipeline run available',
                'overall_status': 'UNKNOWN',
                'quality_score': 0.0,
                'overall_grade': 'N/A',
                'total_issues': 0,
                'critical_issues': 0
            }
        
        return {
            'dataset_name': self.pipeline_results.get('dataset_name', 'Unknown'),
            'overall_status': self.pipeline_results.get('overall_status', 'UNKNOWN'),
            'overall_grade': self.pipeline_results.get('overall_assessment', {}).get('overall_grade', 'N/A'),
            'quality_score': self.pipeline_results.get('quality_metrics', {}).get('overall_quality_score', 0.0),
            'total_issues': self.pipeline_results.get('overall_assessment', {}).get('total_issues', 0),
            'critical_issues': self.pipeline_results.get('overall_assessment', {}).get('critical_issues', 0),
            'timestamp': self.pipeline_results.get('timestamp', 'N/A')
        }
    
    def run_single_validation(self, data_source: str, validation_type: str, 
                            dataset_name: Optional[str] = None,
                            user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Run a single validation type.
        
        Args:
            data_source: Path to data file or DataFrame
            validation_type: Type of validation to run ('schema', 'null', 'range')
            dataset_name: Optional name for the dataset
            user_id: ID of the user running the validation
            
        Returns:
            Dictionary containing validation results
        """
        logger.info(f"Running {validation_type} validation")
        
        # Load data
        df = self._load_data(data_source)
        
        # Register dataset if name provided
        dataset_id = None
        if dataset_name:
            dataset_id = self.lineage_tracker.register_dataset(
                df=df,
                dataset_name=dataset_name,
                source_path=data_source if isinstance(data_source, str) else None
            )
        
        results = {}
        
        if validation_type == 'schema':
            results = self.schema_validator.validate_schema(df)
        elif validation_type == 'null':
            results = self.null_validator.check_null_values(df)
        elif validation_type == 'range':
            range_rules = self.config.get('range_rules', {})
            results = self.range_validator.check_numeric_ranges(df, range_rules)
        else:
            raise ValueError(f"Unknown validation type: {validation_type}")
        
        # Log validation execution
        if dataset_id:
            self.audit_logger.log_validation_execution(
                user_id=user_id or 'system',
                validation_id=f'{validation_type}_check',
                dataset_id=dataset_id,
                validation_results=results
            )
        
        return results
    
    def update_config(self, new_config: Dict[str, Any]) -> None:
        """
        Update pipeline configuration.
        
        Args:
            new_config: New configuration dictionary
        """
        self.config.update(new_config)
        
        # Reinitialize components with new config
        if 'schema' in new_config:
            self.schema_validator = SchemaValidator(self.config['schema'])
        
        if 'null_threshold' in new_config:
            self.null_validator = NullValidator(self.config['null_threshold'])
        
        logger.info("Pipeline configuration updated")
    
    def save_config(self) -> None:
        """Save current configuration to file."""
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
        
        logger.info(f"Configuration saved to {self.config_path}")
    
    def get_metrics_trend(self, dataset_name: str, metric_name: str) -> Dict[str, Any]:
        """
        Get trend analysis for a specific metric.
        
        Args:
            dataset_name: Name of the dataset
            metric_name: Name of the metric to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        return self.metrics_calculator.get_metrics_trend(dataset_name, metric_name)
    
    def get_dataset_lineage(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get lineage information for a dataset.
        
        Args:
            dataset_id: ID of the dataset
            
        Returns:
            Dictionary containing lineage information
        """
        return self.lineage_tracker.get_lineage_graph(dataset_id)
    
    def get_audit_trail(self, **filters) -> List[Dict[str, Any]]:
        """
        Get audit trail with optional filters.
        
        Args:
            **filters: Filter parameters for audit trail
            
        Returns:
            List of audit events
        """
        return self.audit_logger.get_audit_trail(**filters)
