# Data Quality Governance Pipeline

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Domain](https://img.shields.io/badge/Domain-Data%20Engineering-green)
![Status](https://img.shields.io/badge/Project-Production%20Concept-orange)

Enterprise Data Quality & Governance Pipeline for Reliable Data Systems

## What it is

An enterprise data platform for data quality, validation, monitoring, and governance. The system is configuration-driven and built to support real-world data engineering workflows.

### Architecture

```text
Data Source
↓
Validation Engine
↓
Metrics Store
↓
Audit Logs
↓
Reporting
```

### Key Capabilities

- Rule-based validation: schema, null, range, categorical, string
- Config-driven quality rules in YAML
- Audit logging and lineage tracking for governance
- Metrics generation and error reporting
- Kaggle data ingestion support for real-world datasets

## Data Flow

1. Load data from source systems
2. Apply validation and governance rules
3. Generate quality metrics and audit logs
4. Persist results for monitoring and reporting
5. Surface status for operators and analysts

## Production Use Cases

- Data warehouse ingestion validation
- ETL pipeline quality checks
- Financial transaction data validation
- Compliance and audit reporting systems

## Integration

This project can be integrated with real-time pipelines for validation and monitoring.

- Batch ETL workflows (Airflow)
- Streaming pipelines (Pub/Sub, Kafka, Dataflow)
- Data warehouse systems (BigQuery, Snowflake, Redshift)
- Monitoring and reporting platforms

## Failure Handling

- Validation failures are captured and logged
- Structured error reports are saved to `reports/error_reports/`
- Audit logs record failed pipeline runs for debugging
- Retry and recovery workflows can be integrated

## Example Input / Output

### Example Input

```json
{
  "user_id": 123,
  "age": null,
  "salary": -100,
  "email": "invalid_email"
}
```

### Example Output

```json
{
  "overall_status": "failed",
  "overall_grade": "D",
  "quality_score": 0.65,
  "errors": [
    "age is null",
    "salary out of range",
    "email format is invalid"
  ]
}
```

## Getting Started

### Prerequisites

- Python 3.8+
- Install dependencies:

```bash
pip install -r requirements.txt
```

### Run the pipeline

```bash
python -c "from src.pipeline import DataQualityPipeline; print(DataQualityPipeline('configs/validation_rules.yaml').run_pipeline('data/kaggle/telco_customer_churn.csv', 'telco_churn', user_id='data_engineer'))"
```

### Download Kaggle data

```bash
python main.py kaggle --dataset-key telco_churn
```

To download and validate in one command:

```bash
python main.py kaggle --dataset-key telco_churn --run-pipeline --dataset-name telco_churn
```

## Impact

- Improves data reliability and trust in analytics systems
- Reduces manual validation effort with automated checks
- Enables governance through audit and lineage tracking

## Key Learnings

- Designing scalable, data platform-grade validation pipelines
- Building config-driven quality and governance systems
- Integrating validation into batch and streaming workflows
- Capturing failures and audit trails for production readiness

## Repository Structure

```
data-quality-governance-pipeline/
├── configs/
│   └── validation_rules.yaml
├── data/
│   └── kaggle/
├── src/
│   ├── pipeline.py
│   ├── governance/
│   │   ├── audit_logger.py
│   │   └── lineage_tracker.py
│   ├── monitoring/
│   │   └── data_quality_metrics.py
│   └── validation/
│       ├── null_checks.py
│       ├── range_checks.py
│       └── schema_checks.py
├── kaggle_data_loader.py
├── tests/
│   └── test_validations.py
├── requirements.txt
└── README.md
```
