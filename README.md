# Data Quality Governance Pipeline

A comprehensive data quality governance pipeline that provides automated validation, monitoring, and governance capabilities for data assets.

## Overview

This pipeline offers a complete solution for ensuring data quality across your organization through:
- **Automated Validation**: Schema checks, null value analysis, range validation, and constraint enforcement
- **Quality Monitoring**: Real-time metrics calculation and trend analysis
- **Data Governance**: Lineage tracking, audit logging, and compliance monitoring
- **Reporting**: Comprehensive quality reports and dashboard integration

## Features

### Validation Modules
- **Schema Validation**: Ensures data structure matches expected schemas
- **Null Check Validation**: Identifies and analyzes null value patterns
- **Range Validation**: Validates numeric ranges, date constraints, and categorical values
- **Outlier Detection**: Identifies statistical outliers in numeric data

### Monitoring & Metrics
- **Completeness Metrics**: Measures data completeness at column and row levels
- **Accuracy Metrics**: Compares data against reference sources
- **Consistency Metrics**: Detects duplicates and format inconsistencies
- **Timeliness Metrics**: Analyzes data freshness and update patterns
- **Overall Quality Score**: Weighted scoring system for comprehensive assessment

### Governance & Compliance
- **Data Lineage Tracking**: Complete audit trail of data transformations
- **Audit Logging**: Comprehensive logging of all data operations
- **Compliance Monitoring**: GDPR, HIPAA, and SOX compliance tracking
- **Access Control**: User-level access monitoring and permissions

### Reporting & Integration
- **Automated Reports**: Quality reports in multiple formats
- **Dashboard Integration**: Looker dashboard integration notes
- **API Access**: RESTful API for integration with other systems
- **Alerts**: Automated alerts for quality degradation

## Project Structure

```
data-quality-governance-pipeline/
│
├── src/
│   ├── validation/
│   │   ├── schema_checks.py      # Schema validation logic
│   │   ├── null_checks.py        # Null value analysis
│   │   └── range_checks.py       # Range and constraint validation
│   │
│   ├── monitoring/
│   │   └── data_quality_metrics.py # Quality metrics calculation
│   │
│   ├── governance/
│   │   ├── lineage_tracker.py    # Data lineage tracking
│   │   └── audit_logger.py       # Audit logging system
│   │
│   └── pipeline.py                # Main pipeline orchestrator
│
├── configs/
│   └── validation_rules.yaml     # Validation configuration
│
├── data/
│   ├── raw/                      # Raw data files
│   └── processed/                # Processed data files
│
├── reports/
│   └── quality_reports/          # Generated quality reports
│
├── dashboards/
│   └── looker_notes.md          # Looker integration guide
│
├── tests/
│   └── test_validations.py      # Test suite
│
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── main.py                       # Entry point
```

## Installation

### Prerequisites
- Python 3.8 or higher
- SQLite (included with Python)

### Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd data-quality-governance-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up configuration:
```bash
cp configs/validation_rules.yaml.example configs/validation_rules.yaml
# Edit validation_rules.yaml with your specific validation rules
```

## Quick Start

### Basic Usage

```python
from src.pipeline import DataQualityPipeline

# Initialize the pipeline
pipeline = DataQualityPipeline(config_path="configs/validation_rules.yaml")

# Run quality checks on a dataset
results = pipeline.run_pipeline(
    data_source="data/raw/sample_data.csv",
    dataset_name="customer_data",
    user_id="data_analyst"
)

# Get summary
summary = pipeline.get_pipeline_summary()
print(f"Quality Score: {summary['quality_score']}")
print(f"Grade: {summary['overall_grade']}")
```

### Running Individual Validations

```python
# Run only schema validation
schema_results = pipeline.run_single_validation(
    data_source="data/raw/sample_data.csv",
    validation_type="schema",
    dataset_name="test_data"
)

# Run only null checks
null_results = pipeline.run_single_validation(
    data_source="data/raw/sample_data.csv",
    validation_type="null",
    dataset_name="test_data"
)
```

### Using with Pandas DataFrames

```python
import pandas as pd

# Load your data
df = pd.read_csv("your_data.csv")

# Run pipeline directly on DataFrame
results = pipeline.run_pipeline(
    data_source=df,
    dataset_name="my_dataset"
)
```

## Configuration

### Validation Rules Configuration

The `configs/validation_rules.yaml` file contains all validation rules and thresholds:

```yaml
# Schema validation
schema:
  id: "int"
  name: "string"
  email: "string"
  age: "int"

# Null threshold
null_threshold: 0.1  # 10% null values allowed

# Range rules
range_rules:
  age:
    min: 18
    max: 65
    inclusive: true

# Critical columns (no nulls allowed)
critical_columns:
  - id
  - email

# Quality thresholds
quality_thresholds:
  completeness_threshold: 0.9
  overall_quality_threshold: 0.8
```

### Custom Validation Rules

You can extend validation rules by adding custom logic:

```python
from validation.schema_checks import SchemaValidator

# Custom validator
class CustomValidator(SchemaValidator):
    def validate_business_rules(self, df):
        # Add your custom validation logic
        pass
```

## API Reference

### DataQualityPipeline

#### Methods

- `run_pipeline(data_source, dataset_name, user_id=None)`: Run complete pipeline
- `run_single_validation(data_source, validation_type, dataset_name=None, user_id=None)`: Run specific validation
- `get_pipeline_summary()`: Get summary of latest run
- `update_config(new_config)`: Update pipeline configuration
- `get_metrics_trend(dataset_name, metric_name)`: Get metric trends
- `get_dataset_lineage(dataset_id)`: Get lineage information

### Validation Classes

#### SchemaValidator
- `validate_schema(df)`: Validate DataFrame schema
- `enforce_schema(df)`: Enforce schema on DataFrame

#### NullValidator
- `check_null_values(df, columns=None)`: Check for null values
- `validate_critical_columns(df, critical_columns)`: Validate critical columns
- `handle_nulls(df, strategy='drop', fill_value=None)`: Handle null values

#### RangeValidator
- `check_numeric_ranges(df, range_rules)`: Check numeric ranges
- `check_date_ranges(df, date_rules)`: Check date ranges
- `check_categorical_constraints(df, categorical_rules)`: Check categorical constraints
- `detect_outliers(df, columns, method='iqr')`: Detect outliers

### Monitoring Classes

#### DataQualityMetrics
- `calculate_completeness_metrics(df)`: Calculate completeness metrics
- `calculate_consistency_metrics(df)`: Calculate consistency metrics
- `calculate_overall_quality_score(metrics)`: Calculate overall quality score
- `generate_quality_report(dataset_name, metrics)`: Generate quality report

### Governance Classes

#### LineageTracker
- `register_dataset(df, dataset_name, source_path=None)`: Register dataset
- `track_transformation(input_id, output_id, type, parameters)`: Track transformation
- `get_lineage_graph(dataset_id)`: Get lineage graph
- `find_upstream_datasets(dataset_id, max_depth=5)`: Find upstream datasets

#### AuditLogger
- `log_event(event_type, user_id, resource_type, resource_id, action, details=None)`: Log event
- `log_data_access(user_id, dataset_id, access_type, details=None)`: Log data access
- `get_audit_trail(**filters)`: Get audit trail
- `get_activity_summary(start_date=None, end_date=None)`: Get activity summary

## Testing

Run the test suite:

```bash
python -m pytest tests/ -v
```

Or run individual test files:

```bash
python tests/test_validations.py
```

## Dashboard Integration

### Looker Integration

See `dashboards/looker_notes.md` for comprehensive Looker integration instructions, including:
- Dashboard design patterns
- LookML examples
- Data source configuration
- Alert setup

### Custom Dashboards

You can create custom dashboards using the metrics data:

```python
# Export metrics for dashboard
pipeline.metrics_calculator.export_metrics(
    dataset_name="my_dataset",
    output_path="dashboard_data/metrics.json"
)
```

## Monitoring & Alerts

### Setting up Alerts

```python
# Configure alert thresholds
pipeline.update_config({
    'monitoring': {
        'alerts': {
            'quality_degradation_threshold': 0.1,
            'error_rate_threshold': 0.05
        }
    }
})
```

### Email Notifications

```python
# Enable email notifications
pipeline.audit_logger.configure_email(
    smtp_server="smtp.company.com",
    recipients=["data-team@company.com"],
    send_on_failure=True
)
```

## Performance Considerations

### Large Datasets

For large datasets, consider:

1. **Batch Processing**: Process data in chunks
```python
pipeline.update_config({
    'performance': {
        'batch_size': 10000,
        'parallel_processing': True,
        'num_workers': 4
    }
})
```

2. **Sampling**: Use representative samples for initial validation
```python
sample_df = df.sample(n=10000, random_state=42)
results = pipeline.run_pipeline(sample_df, "sample_data")
```

### Memory Optimization

- Use `chunksize` parameter for large CSV files
- Enable parallel processing for CPU-intensive operations
- Configure appropriate memory limits in configuration

## Troubleshooting

### Common Issues

1. **Configuration Errors**
   - Ensure YAML syntax is correct
   - Validate file paths in configuration

2. **Memory Issues**
   - Reduce batch size
   - Enable parallel processing
   - Use data sampling

3. **Performance Issues**
   - Optimize validation rules
   - Use appropriate data types
   - Consider database storage for large datasets

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

### Code Style

- Use Black for code formatting
- Follow PEP 8 guidelines
- Add type hints for new functions
- Include docstrings for all public methods

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the repository
- Contact the data governance team
- Check the documentation in `docs/` directory

## Roadmap

### Upcoming Features
- [ ] Machine learning-based quality predictions
- [ ] Real-time streaming data validation
- [ ] Advanced anomaly detection
- [ ] Multi-cloud storage integration
- [ ] Enhanced dashboard templates
- [ ] API rate limiting and authentication
- [ ] Automated data quality recommendations

### Version History
- **v1.0.0**: Initial release with core validation and monitoring
- **v1.1.0**: Added governance and compliance features
- **v1.2.0**: Enhanced dashboard integration and alerts
- **v2.0.0**: Planned major release with ML capabilities

## Best Practices

### Data Quality Management
1. **Define Clear Standards**: Establish clear data quality rules and thresholds
2. **Monitor Continuously**: Set up regular quality checks and monitoring
3. **Automate Where Possible**: Use automated validation to reduce manual effort
4. **Track Metrics**: Monitor quality trends over time
5. **Govern Access**: Implement proper access controls and audit trails

### Pipeline Usage
1. **Start Small**: Begin with basic validations and expand gradually
2. **Customize Rules**: Adapt validation rules to your specific data
3. **Review Reports**: Regularly review quality reports and take action
4. **Maintain Configuration**: Keep validation rules up to date
5. **Document Processes**: Document data quality processes and procedures

---

For more detailed information, see the documentation in the `docs/` directory or contact the data governance team.
