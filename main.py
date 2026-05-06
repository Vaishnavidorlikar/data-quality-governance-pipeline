#!/usr/bin/env python3
"""
Data Quality Governance Pipeline - Main Entry Point

This script provides a command-line interface for running the data quality pipeline.
"""

import argparse
import sys
import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.pipeline import DataQualityPipeline
from kaggle_data_loader import KaggleDataLoader


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """Set up logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )


def create_sample_data(output_path: str = "data/raw/sample_data.csv"):
    """Create sample data for testing."""
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Create sample dataset
    np.random.seed(42)
    n_records = 1000
    
    data = {
        'id': range(1, n_records + 1),
        'name': [f'User_{i}' for i in range(1, n_records + 1)],
        'email': [f'user{i}@example.com' for i in range(1, n_records + 1)],
        'age': np.random.randint(18, 70, n_records),
        'salary': np.random.normal(60000, 15000, n_records),
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'], n_records),
        'join_date': [datetime(2020, 1, 1) + timedelta(days=np.random.randint(0, 1500)) for _ in range(n_records)],
        'is_active': np.random.choice([True, False], n_records, p=[0.8, 0.2])
    }
    
    # Introduce some data quality issues
    # Add some null values
    for i in range(50):
        col = np.random.choice(['name', 'email', 'age'])
        data[col][i] = None
    
    # Add some salary outliers
    for i in range(10, 15):
        data['salary'][i] = np.random.normal(200000, 10000)
    
    # Add some invalid emails
    for i in range(20, 25):
        data['email'][i] = f'invalid_email_{i}'
    
    # Add some ages outside range
    for i in range(30, 35):
        data['age'][i] = np.random.choice([15, 70, 80])
    
    df = pd.DataFrame(data)
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Sample data created: {output_path}")
    print(f"Records: {len(df)}")
    print(f"Columns: {list(df.columns)}")


def download_kaggle_dataset(args):
    """Download a Kaggle dataset into the local data source directory."""
    loader = KaggleDataLoader(data_dir=args.output_dir)

    if args.all:
        print("Downloading all configured Kaggle datasets...")
        success = loader.auto_download_all_datasets()
        if not success:
            print("Kaggle download failed. Check API credentials and network connectivity.")
        return success

    if not args.dataset_key:
        print("Please provide --dataset-key or use --all to download all datasets.")
        return False

    print(f"Downloading Kaggle dataset: {args.dataset_key}")
    success = loader.download_dataset(args.dataset_key, force_download=args.force)
    if not success:
        print("Kaggle download failed. See log output for details.")
        return False

    print(f"Downloaded Kaggle dataset to {args.output_dir}")

    if args.run_pipeline:
        dataset_path = Path(args.output_dir) / loader.datasets[args.dataset_key]['local_name']
        dataset_name = args.dataset_name or args.dataset_key
        print(f"Running pipeline for downloaded dataset: {dataset_path}")
        pipeline = DataQualityPipeline(config_path=args.config)
        results = pipeline.run_pipeline(
            data_source=str(dataset_path),
            dataset_name=dataset_name,
            user_id=args.user_id
        )
        print("\nPipeline completed for downloaded dataset.")
        if args.verbose:
            import json
            print(json.dumps(results, indent=2, default=str))

    return True


def run_pipeline(args):
    """Run the data quality pipeline."""
    try:
        # Initialize pipeline
        pipeline = DataQualityPipeline(config_path=args.config)
        
        # Update config if provided
        if args.update_config:
            with open(args.update_config, 'r') as f:
                new_config = json.load(f)
            pipeline.update_config(new_config)
        
        # Run pipeline
        print(f"Running data quality pipeline on: {args.data_source}")
        print(f"Dataset name: {args.dataset_name}")
        
        results = pipeline.run_pipeline(
            data_source=args.data_source,
            dataset_name=args.dataset_name,
            user_id=args.user_id
        )
        
        # Display results
        print("\n" + "="*60)
        print("PIPELINE RESULTS")
        print("="*60)
        
        summary = pipeline.get_pipeline_summary()
        print(f"Status: {summary['overall_status']}")
        print(f"Quality Score: {summary['quality_score']:.3f}")
        print(f"Grade: {summary['overall_grade']}")
        print(f"Total Issues: {summary['total_issues']}")
        print(f"Critical Issues: {summary['critical_issues']}")
        
        # Detailed results
        if args.verbose:
            print("\n" + "="*60)
            print("DETAILED RESULTS")
            print("="*60)
            print(json.dumps(results, indent=2, default=str))
        
        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to: {args.output}")
        
        return results
        
    except Exception as e:
        print(f"Error running pipeline: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return None


def run_validation(args):
    """Run a single validation type."""
    try:
        pipeline = DataQualityPipeline(config_path=args.config)
        
        print(f"Running {args.validation_type} validation on: {args.data_source}")
        
        results = pipeline.run_single_validation(
            data_source=args.data_source,
            validation_type=args.validation_type,
            dataset_name=args.dataset_name,
            user_id=args.user_id
        )
        
        print(f"\n{args.validation_type.upper()} VALIDATION RESULTS")
        print("="*50)
        print(f"Valid: {results.get('is_valid', True)}")
        
        if 'total_errors' in results:
            print(f"Total Errors: {results['total_errors']}")
        
        if args.verbose:
            print("\nDetailed Results:")
            print(json.dumps(results, indent=2, default=str))
        
        return results
        
    except Exception as e:
        print(f"Error running validation: {e}")
        return None


def show_metrics_trend(args):
    """Show metrics trend for a dataset."""
    try:
        pipeline = DataQualityPipeline(config_path=args.config)
        
        trend = pipeline.get_metrics_trend(args.dataset_name, args.metric_name)
        
        print(f"\nMETRICS TREND: {args.metric_name}")
        print("="*40)
        print(f"Trend: {trend.get('trend', 'No data')}")
        print(f"Current Value: {trend.get('current_value', 'N/A')}")
        print(f"Recent Average: {trend.get('recent_average', 'N/A')}")
        print(f"Historical Average: {trend.get('historical_average', 'N/A')}")
        print(f"Data Points: {trend.get('data_points', 0)}")
        
        return trend
        
    except Exception as e:
        print(f"Error getting metrics trend: {e}")
        return None


def show_lineage(args):
    """Show lineage information for a dataset."""
    try:
        pipeline = DataQualityPipeline(config_path=args.config)
        
        lineage = pipeline.get_dataset_lineage(args.dataset_id)
        
        print(f"\nLINEAGE INFORMATION")
        print("="*30)
        
        if 'error' in lineage:
            print(f"Error: {lineage['error']}")
        else:
            dataset_info = lineage['dataset']
            print(f"Dataset: {dataset_info['name']}")
            print(f"Created: {dataset_info['created_at']}")
            print(f"Rows: {dataset_info['row_count']}")
            
            print(f"\nUpstream Transformations: {len(lineage['upstream_transformations'])}")
            for trans in lineage['upstream_transformations']:
                print(f"  - {trans['transformation_type']} from {trans['input_name']}")
            
            print(f"\nDownstream Transformations: {len(lineage['downstream_transformations'])}")
            for trans in lineage['downstream_transformations']:
                print(f"  - {trans['transformation_type']} to {trans['output_name']}")
        
        return lineage
        
    except Exception as e:
        print(f"Error getting lineage: {e}")
        return None


def show_audit_trail(args):
    """Show audit trail."""
    try:
        pipeline = DataQualityPipeline(config_path=args.config)
        
        # Build filters
        filters = {}
        if args.user_id:
            filters['user_id'] = args.user_id
        if args.event_type:
            filters['event_type'] = args.event_type
        if args.resource_id:
            filters['resource_id'] = args.resource_id
        if args.limit:
            filters['limit'] = args.limit
        
        audit_trail = pipeline.get_audit_trail(**filters)
        
        print(f"\nAUDIT TRAIL")
        print("="*20)
        print(f"Total Events: {len(audit_trail)}")
        
        for event in audit_trail[:10]:  # Show first 10 events
            print(f"{event['timestamp']} - {event['event_type']} - {event['action']} by {event['user_id']}")
        
        if len(audit_trail) > 10:
            print(f"... and {len(audit_trail) - 10} more events")
        
        return audit_trail
        
    except Exception as e:
        print(f"Error getting audit trail: {e}")
        return None


def list_datasets(args):
    """List all registered datasets."""
    try:
        pipeline = DataQualityPipeline(config_path=args.config)
        
        datasets = pipeline.lineage_tracker.get_all_datasets()
        
        print(f"\nREGISTERED DATASETS")
        print("="*25)
        print(f"Total: {len(datasets)}")
        
        for dataset in datasets:
            print(f"{dataset['name']} - {dataset['row_count']} rows - {dataset['created_at']}")
        
        return datasets
        
    except Exception as e:
        print(f"Error listing datasets: {e}")
        return None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Data Quality Governance Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python main.py pipeline --data-source data/sample.csv --dataset-name test_data
  
  # Run single validation
  python main.py validate --data-source data/sample.csv --validation-type schema
  
  # Create sample data
  python main.py create-sample-data
  
  # Show metrics trend
  python main.py trend --dataset-name test_data --metric-name overall_quality_score
        """
    )
    
    # Global arguments
    parser.add_argument('--config', default='configs/validation_rules.yaml',
                       help='Path to configuration file')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('--log-file', help='Path to log file')
    parser.add_argument('--user-id', default='system',
                       help='User ID for audit logging')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Run complete data quality pipeline')
    pipeline_parser.add_argument('--data-source', required=True,
                                help='Path to data file or DataFrame')
    pipeline_parser.add_argument('--dataset-name', required=True,
                                help='Name for the dataset')
    pipeline_parser.add_argument('--output', help='Path to save results JSON')
    pipeline_parser.add_argument('--update-config', help='Path to JSON config updates')
    
    # Validation command
    validate_parser = subparsers.add_parser('validate', help='Run single validation type')
    validate_parser.add_argument('--data-source', required=True,
                                help='Path to data file')
    validate_parser.add_argument('--validation-type', required=True,
                                choices=['schema', 'null', 'range'],
                                help='Type of validation to run')
    validate_parser.add_argument('--dataset-name', help='Name for the dataset')
    
    # Create sample data command
    sample_parser = subparsers.add_parser('create-sample-data', help='Create sample data for testing')
    sample_parser.add_argument('--output', default='data/raw/sample_data.csv',
                              help='Path to save sample data')

    # Kaggle dataset download command
    kaggle_parser = subparsers.add_parser('kaggle', help='Download Kaggle dataset to local data source')
    kaggle_parser.add_argument('--dataset-key', choices=['telco_churn', 'financial_transactions', 'bank_churn'],
                               help='Key for configured Kaggle dataset to download')
    kaggle_parser.add_argument('--all', action='store_true',
                               help='Download all configured Kaggle datasets')
    kaggle_parser.add_argument('--output-dir', default='data/kaggle',
                               help='Target directory for downloaded datasets')
    kaggle_parser.add_argument('--force', action='store_true',
                               help='Force download even if dataset already exists')
    kaggle_parser.add_argument('--run-pipeline', action='store_true',
                               help='Run data quality pipeline after successful download')
    kaggle_parser.add_argument('--dataset-name',
                               help='Pipeline dataset name to use when running validation after download')
    
    # Metrics trend command
    trend_parser = subparsers.add_parser('trend', help='Show metrics trend')
    trend_parser.add_argument('--dataset-name', required=True,
                              help='Dataset name')
    trend_parser.add_argument('--metric-name', required=True,
                              help='Metric name to analyze')
    
    # Lineage command
    lineage_parser = subparsers.add_parser('lineage', help='Show dataset lineage')
    lineage_parser.add_argument('--dataset-id', required=True,
                                help='Dataset ID')
    
    # Audit trail command
    audit_parser = subparsers.add_parser('audit', help='Show audit trail')
    audit_parser.add_argument('--user-id', help='Filter by user ID')
    audit_parser.add_argument('--event-type', help='Filter by event type')
    audit_parser.add_argument('--resource-id', help='Filter by resource ID')
    audit_parser.add_argument('--limit', type=int, default=100,
                              help='Maximum number of events to show')
    
    # List datasets command
    list_parser = subparsers.add_parser('list', help='List all registered datasets')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    log_level = 'DEBUG' if args.verbose else 'INFO'
    setup_logging(log_level, args.log_file)
    
    # Execute command
    if args.command == 'pipeline':
        run_pipeline(args)
    elif args.command == 'validate':
        run_validation(args)
    elif args.command == 'create-sample-data':
        create_sample_data(args.output)
    elif args.command == 'kaggle':
        download_kaggle_dataset(args)
    elif args.command == 'trend':
        show_metrics_trend(args)
    elif args.command == 'lineage':
        show_lineage(args)
    elif args.command == 'audit':
        show_audit_trail(args)
    elif args.command == 'list':
        list_datasets(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
