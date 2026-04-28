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
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.validators = self._initialize_validators()
        self.metrics_calculator = DataQualityMetrics()
        self.lineage_tracker = LineageTracker()
        self.audit_logger = AuditLogger()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self.config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration: {e}")
            raise
    
    def _initialize_validators(self) -> Dict[str, Any]:
        """Initialize validation components."""
        return {
            'schema': SchemaValidator(),
            'null': NullValidator(),
            'range': RangeValidator()
        }
    
    def run_pipeline(self, data_source: str, dataset_name: str, user_id: str) -> Dict[str, Any]:
        """
        Run the complete data quality pipeline.
        
        Args:
            data_source: Path to data file
            dataset_name: Name/identifier for the dataset
            user_id: User ID for audit logging
            
        Returns:
            Dictionary containing pipeline results
        """
        logger.info(f"Starting pipeline for dataset: {dataset_name}")
        
        # Load data
        df = self._load_data(data_source)
        
        # Track data lineage
        self.lineage_tracker.track_data_lineage(
            dataset_name=dataset_name,
            source_path=data_source,
            user_id=user_id,
            timestamp=datetime.now()
        )
        
        # Run validations
        results = {
            'dataset_name': dataset_name,
            'data_source': data_source,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'validation_results': {}
        }
        
        # Schema validation
        schema_config = self.config.get('validation', {}).get('schema', {})
        if schema_config:
            schema_results = self.validators['schema'].validate_schema(df, schema_config)
            results['validation_results']['schema'] = schema_results
            logger.info(f"Schema validation completed: {schema_results.get('is_valid', False)}")
        
        # Null value validation
        null_config = self.config.get('validation', {}).get('null_checks', {})
        if null_config:
            null_results = self.validators['null'].check_null_values(df, null_config)
            results['validation_results']['null'] = null_results
            logger.info(f"Null value validation completed: {null_results.get('is_acceptable', False)}")
        
        # Range validation
        range_config = self.config.get('validation', {}).get('range_checks', {})
        if range_config:
            range_results = self.validators['range'].detect_outliers(df, range_config)
            results['validation_results']['range'] = range_results
            logger.info(f"Range validation completed: {range_results.get('outliers_detected', 0)} outliers detected")
        
        # Calculate quality metrics
        quality_metrics = self.metrics_calculator.calculate_quality_score(
            df, results['validation_results']
        )
        results['quality_metrics'] = quality_metrics
        logger.info(f"Quality score calculated: {quality_metrics.get('overall_score', 0):.2f}")
        
        # Log pipeline execution
        self.audit_logger.log_pipeline_execution(
            dataset_name=dataset_name,
            user_id=user_id,
            results=results
        )
        
        logger.info("Pipeline execution completed successfully")
        return results
    
    def _load_data(self, data_source: str) -> pd.DataFrame:
        """
        Load data from various sources.
        
        Args:
            data_source: Path to data file
            
        Returns:
            Loaded DataFrame
        """
        source_path = Path(data_source)
        
        if not source_path.exists():
            logger.error(f"Data source not found: {data_source}")
            raise FileNotFoundError(f"Data source not found: {data_source}")
        
        # Load based on file extension
        if source_path.suffix.lower() == '.csv':
            df = pd.read_csv(data_source)
            logger.info(f"Loaded CSV data: {len(df)} records, {len(df.columns)} columns")
        elif source_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(data_source)
            logger.info(f"Loaded Excel data: {len(df)} records, {len(df.columns)} columns")
        else:
            logger.error(f"Unsupported file format: {source_path.suffix}")
            raise ValueError(f"Unsupported file format: {source_path.suffix}")
        
        return df
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the pipeline configuration and status.
        
        Returns:
            Dictionary containing pipeline summary
        """
        return {
            'config_path': str(self.config_path),
            'config_loaded': bool(self.config),
            'validators_initialized': len(self.validators) > 0,
            'metrics_calculator_available': self.metrics_calculator is not None,
            'lineage_tracking_enabled': self.lineage_tracker is not None,
            'audit_logging_enabled': self.audit_logger is not None
        }
    
    def generate_report(self, results: Dict[str, Any], output_path: str, format: str = 'html') -> str:
        """
        Generate quality report in specified format.
        
        Args:
            results: Pipeline results dictionary
            output_path: Path for output report
            format: Output format ('html', 'json', 'csv')
            
        Returns:
            Path to generated report
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == 'html':
            report_content = self._generate_html_report(results)
            output_file = output_file.with_suffix('.html')
        elif format.lower() == 'json':
            report_content = self._generate_json_report(results)
            output_file = output_file.with_suffix('.json')
        elif format.lower() == 'csv':
            report_content = self._generate_csv_report(results)
            output_file = output_file.with_suffix('.csv')
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        with open(output_file, 'w') as file:
            file.write(report_content)
        
        logger.info(f"Report generated: {output_file}")
        return str(output_file)
    
    def _generate_html_report(self, results: Dict[str, Any]) -> str:
        """Generate HTML quality report."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report - {results['dataset_name']}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #e9ecef; border-radius: 3px; }}
                .pass {{ color: #28a745; }}
                .fail {{ color: #dc3545; }}
                .warning {{ color: #ffc107; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Quality Governance Report</h1>
                <h2>Dataset: {results['dataset_name']}</h2>
                <p>Generated: {results['timestamp']}</p>
                <p>User: {results['user_id']}</p>
            </div>
            
            <div class="section">
                <h3>Summary</h3>
                <div class="metric">
                    <strong>Total Records:</strong> {results['total_records']:,}
                </div>
                <div class="metric">
                    <strong>Quality Score:</strong> {results['quality_metrics'].get('overall_score', 0):.1f}%
                </div>
            </div>
            
            <div class="section">
                <h3>Validation Results</h3>
                {self._format_validation_results(results['validation_results'])}
            </div>
            
            <div class="section">
                <h3>Quality Metrics</h3>
                {self._format_quality_metrics(results['quality_metrics'])}
            </div>
        </body>
        </html>
        """
        return html_content
    
    def _generate_json_report(self, results: Dict[str, Any]) -> str:
        """Generate JSON quality report."""
        return json.dumps(results, indent=2, default=str)
    
    def _generate_csv_report(self, results: Dict[str, Any]) -> str:
        """Generate CSV quality report."""
        # Create summary CSV
        summary_data = []
        
        for validation_type, validation_result in results['validation_results'].items():
            summary_data.append({
                'Validation Type': validation_type,
                'Status': 'Pass' if validation_result.get('is_valid', validation_result.get('is_acceptable', False)) else 'Fail',
                'Details': str(validation_result)
            })
        
        summary_df = pd.DataFrame(summary_data)
        return summary_df.to_csv(index=False)
    
    def _format_validation_results(self, validation_results: Dict[str, Any]) -> str:
        """Format validation results for HTML report."""
        html = ""
        for validation_type, result in validation_results.items():
            status_class = 'pass' if result.get('is_valid', result.get('is_acceptable', False)) else 'fail'
            html += f"""
            <div class="metric">
                <strong>{validation_type.title()}:</strong>
                <span class="{status_class}">{'Pass' if result.get('is_valid', result.get('is_acceptable', False)) else 'Fail'}</span>
            </div>
            """
        return html
    
    def _format_quality_metrics(self, quality_metrics: Dict[str, Any]) -> str:
        """Format quality metrics for HTML report."""
        html = ""
        for metric, value in quality_metrics.items():
            html += f"""
            <div class="metric">
                <strong>{metric.title()}:</strong> {value}
            </div>
            """
        return html
