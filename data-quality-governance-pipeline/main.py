#!/usr/bin/env python3
"""
Data Quality Governance Pipeline - Main Entry Point

This script provides a command-line interface for running data quality pipeline.
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


def create_sample_data(output_file: str = "data/sample_customer_data.csv"):
    """Create sample data with quality issues for demonstration."""
    import pandas as pd
    import numpy as np
    
    np.random.seed(42)
    n_records = 1000
    
    data = {
        'customer_id': [f'CUST_{i:06d}' for i in range(1, n_records + 1)],
        'transaction_amount': np.random.lognormal(3, 1, n_records),
        'transaction_date': pd.date_range('2023-01-01', periods=n_records, freq='H'),
        'customer_age': np.random.randint(18, 80, n_records),
        'customer_income': np.random.normal(50000, 20000, n_records),
        'product_category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books'], n_records)
    }
    
    # Add data quality issues
    df = pd.DataFrame(data)
    
    # Missing values
    for i in range(50):
        col = np.random.choice(['customer_age', 'customer_income'])
        df.loc[i, col] = np.nan
    
    # Outliers
    for i in range(10, 15):
        df.loc[i, 'transaction_amount'] = np.random.uniform(10000, 50000)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Sample data created: {output_file}")
    
    return output_file


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Data Quality Governance Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --data data/customer_transactions.csv
  %(prog)s --data data/sales.csv --config configs/custom_rules.yaml
  %(prog)s --generate-sample-data --output data/test.csv
        """
    )
    
    parser.add_argument(
        '--data', '-d',
        type=str,
        help='Path to input data file (CSV, Excel, etc.)'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default='configs/validation_rules.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='reports/quality_report.html',
        help='Output path for quality report'
    )
    
    parser.add_argument(
        '--dataset-name',
        type=str,
        default='business_data',
        help='Name/identifier for the dataset'
    )
    
    parser.add_argument(
        '--user-id',
        type=str,
        default='data_analyst',
        help='User ID for audit logging'
    )
    
    parser.add_argument(
        '--generate-sample-data',
        action='store_true',
        help='Generate sample data with quality issues'
    )
    
    parser.add_argument(
        '--sample-output',
        type=str,
        default='data/sample_customer_data.csv',
        help='Output path for sample data generation'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log file path (optional)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Generate sample data if requested
        if args.generate_sample_data:
            sample_file = create_sample_data(args.sample_output)
            logger.info(f"Sample data generated: {sample_file}")
            return
        
        # Check if data file is provided
        if not args.data:
            logger.error("No data file specified. Use --data to specify input file.")
            logger.info("Or use --generate-sample-data to create sample data.")
            sys.exit(1)
        
        # Check if data file exists
        if not os.path.exists(args.data):
            logger.error(f"Data file not found: {args.data}")
            sys.exit(1)
        
        # Initialize pipeline
        logger.info("Initializing Data Quality Pipeline...")
        pipeline = DataQualityPipeline(config_path=args.config)
        
        # Run pipeline
        logger.info(f"Processing dataset: {args.dataset_name}")
        logger.info(f"Input data: {args.data}")
        
        results = pipeline.run_pipeline(
            data_source=args.data,
            dataset_name=args.dataset_name,
            user_id=args.user_id
        )
        
        # Generate report
        logger.info(f"Generating quality report: {args.output}")
        report_path = pipeline.generate_report(
            results=results,
            output_path=args.output,
            format='html' if args.output.endswith('.html') else 'json'
        )
        
        # Display summary
        summary = pipeline.get_pipeline_summary()
        print("\n" + "="*60)
        print("DATA QUALITY PIPELINE SUMMARY")
        print("="*60)
        print(f"Dataset: {args.dataset_name}")
        print(f"Records Processed: {summary.get('total_records', 'N/A'):,}")
        print(f"Quality Score: {summary.get('quality_score', 'N/A'):.1%}")
        print(f"Risk Level: {summary.get('overall_grade', 'N/A')}")
        print(f"Report Generated: {report_path}")
        print("="*60)
        
        if args.verbose:
            print("\nDetailed Results:")
            print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
