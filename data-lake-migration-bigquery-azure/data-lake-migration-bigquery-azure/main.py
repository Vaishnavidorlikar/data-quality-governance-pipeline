#!/usr/bin/env python3
"""
Main entry point for the Data Lake Migration pipeline

This script provides a command-line interface for running the ETL pipeline
from Kaggle to BigQuery to Azure Data Lake Storage.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.pipeline import Pipeline


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Set up logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )


def parse_source_argument(source: str) -> Dict[str, Any]:
    """
    Parse source argument into configuration
    
    Args:
        source: Source specification (e.g., "dataset.table" or "query:SELECT * FROM table")
        
    Returns:
        Source configuration dictionary
    """
    if source.startswith("query:"):
        return {"query": source[6:]}
    else:
        parts = source.split(".")
        if len(parts) == 2:
            return {"table": {"dataset": parts[0], "name": parts[1]}}
        else:
            raise ValueError(f"Invalid source format: {source}")


def parse_target_argument(target: str) -> Dict[str, Any]:
    """
    Parse target argument into configuration
    
    Args:
        target: Target specification (e.g., "local:/path/to/file.csv" or "azure_blob:container/blob")
        
    Returns:
        Target configuration dictionary
    """
    parts = target.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid target format: {target}")
    
    target_type, target_path = parts
    
    if target_type == "local":
        return {"local": {"file_path": target_path, "format": "csv"}}
    elif target_type == "azure_blob":
        path_parts = target_path.split("/", 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid Azure blob format: {target_path}")
        return {"azure_blob": {"container": path_parts[0], "blob_name": path_parts[1], "format": "csv"}}
    elif target_type == "azure_datalake":
        path_parts = target_path.split("/", 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid Azure Data Lake format: {target_path}")
        return {"azure_datalake": {"file_system": path_parts[0], "file_path": path_parts[1], "format": "csv"}}
    elif target_type == "gcs":
        path_parts = target_path.split("/", 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid GCS format: {target_path}")
        return {"gcs": {"bucket_name": path_parts[0], "blob_name": path_parts[1], "format": "csv"}}
    else:
        raise ValueError(f"Unsupported target type: {target_type}")


def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Data Lake Migration - BigQuery to Azure ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract from BigQuery table and save locally
  python main.py --source dataset.table --target local:data/processed/output.csv
  
  # Extract with custom query and load to Azure Blob
  python main.py --source "query:SELECT * FROM dataset.table WHERE date > '2023-01-01'" --target azure_blob:container/output.csv
  
  # Use configuration file
  python main.py --config config/config.yaml
        """
    )
    
    # Configuration options
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to configuration file"
    )
    
    # Source options
    parser.add_argument(
        "--source", "-s",
        type=str,
        help="Source specification (dataset.table or query:SELECT...)"
    )
    
    # Target options
    parser.add_argument(
        "--target", "-t",
        type=str,
        help="Target specification (local:/path or azure_blob:container/blob or azure_datalake:fs/path)"
    )
    
    # Transformation options
    parser.add_argument(
        "--no-transform",
        action="store_true",
        help="Skip transformation step"
    )
    
    parser.add_argument(
        "--clean-nulls",
        action="store_true",
        help="Remove rows with null values"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of rows to extract"
    )
    
    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["kaggle", "extract", "transform", "load", "pipeline"],
        default="kaggle",
        help="Pipeline mode to run (kaggle: Kaggle to BigQuery to Azure, pipeline: BigQuery to Azure)"
    )
    
    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    parser.add_argument(
        "--log-file",
        type=str,
        help="Log file path"
    )
    
    # Other options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without actually loading data"
    )
    
    parser.add_argument(
        "--save-log",
        type=str,
        help="Save pipeline execution log to file"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize pipeline
        pipeline = Pipeline(config_path=args.config) if args.config else Pipeline('config/config.yaml')
        
        # Prepare source configuration
        if args.source:
            source_config = parse_source_argument(args.source)
            if args.limit and "table" in source_config:
                source_config["table"]["limit"] = args.limit
        elif args.config:
            # Use source from config
            if "pipeline" in pipeline.config and "source" in pipeline.config["pipeline"]:
                source_config = pipeline.config["pipeline"]["source"]
            else:
                raise ValueError("No source configuration found in config file")
        else:
            raise ValueError("Either --source or --config must be provided")
        
        # Prepare transformation configuration
        transform_config = None
        if not args.no_transform:
            transform_config = {}
            if args.clean_nulls:
                transform_config["clean"] = {"remove_nulls": True}
            
            # Add any transformation config from file
            if args.config and "transformations" in pipeline.config:
                transform_config.update(pipeline.config["transformations"])
        
        # Prepare target configuration
        if args.target:
            target_config = parse_target_argument(args.target)
        elif args.config:
            # Use target from config
            if "pipeline" in pipeline.config and "target" in pipeline.config["pipeline"]:
                target_config = pipeline.config["pipeline"]["target"]
            else:
                # Default to local loading
                target_config = {"local": {"file_path": "data/processed/output.csv", "format": "csv"}}
        else:
            # Default to local loading
            target_config = {"local": {"file_path": "data/processed/output.csv", "format": "csv"}}
        
        # Handle dry run
        if args.dry_run:
            logger.info("DRY RUN: Pipeline will execute but data will not be loaded")
            # Temporarily modify target to use a temporary file
            if "local" in target_config:
                target_config["local"]["file_path"] = f"/tmp/dry_run_output.csv"
        
        # Run pipeline
        logger.info("Starting data lake migration pipeline")
        
        if args.mode == "kaggle":
            # Run complete Kaggle to BigQuery to Azure pipeline
            success = pipeline.run_complete_kaggle_pipeline()
        elif args.mode == "pipeline":
            logger.info(f"Source: {source_config}")
            logger.info(f"Transform: {transform_config}")
            logger.info(f"Target: {target_config}")
            success = pipeline.run_pipeline(source_config, transform_config, target_config)
        elif args.mode == "extract":
            logger.info(f"Source: {source_config}")
            df = pipeline.run_extraction(source_config)
            logger.info(f"Extracted {len(df)} rows")
            success = True
        elif args.mode == "transform":
            # First extract, then transform
            logger.info(f"Source: {source_config}")
            logger.info(f"Transform: {transform_config}")
            df = pipeline.run_extraction(source_config)
            df = pipeline.run_transformation(df, transform_config)
            logger.info(f"Transformed {len(df)} rows")
            success = True
        elif args.mode == "load":
            # Extract and transform, then load
            logger.info(f"Source: {source_config}")
            logger.info(f"Transform: {transform_config}")
            logger.info(f"Target: {target_config}")
            df = pipeline.run_extraction(source_config)
            df = pipeline.run_transformation(df, transform_config)
            success = pipeline.run_loading(df, target_config)
            logger.info(f"Loaded {len(df)} rows")
        else:
            raise ValueError(f"Unknown mode: {args.mode}")
        
        if success:
            logger.info("Pipeline completed successfully")
            
            # Save pipeline log if requested
            if args.save_log:
                pipeline.save_pipeline_log(args.save_log)
                logger.info(f"Pipeline log saved to {args.save_log}")
        else:
            logger.error("Pipeline failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
