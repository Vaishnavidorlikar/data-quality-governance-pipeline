"""
Pipeline module for orchestrating the ETL process
"""

import pandas as pd
from typing import Dict, List, Optional, Any, Callable
import logging
from datetime import datetime
import yaml
from pathlib import Path

from .extract import Extractor
from .transform import Transformer
from .load import Loader

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Orchestrates the complete ETL pipeline from Kaggle to BigQuery to Azure
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the pipeline
        
        Args:
            config_path: Path to configuration file
        """
        self.config = {}
        self.extractor = None
        self.transformer = Transformer()
        self.loader = None
        self.pipeline_log = []
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> None:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
        """
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
            
            # Initialize components based on config
            self._initialize_components()
            logger.info(f"Configuration loaded from {config_path}")
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def _initialize_components(self) -> None:
        """Initialize pipeline components based on configuration"""
        # Initialize extractor with Kaggle and BigQuery support
        if 'bigquery' in self.config:
            bq_config = self.config['bigquery']
            kaggle_config = self.config.get('kaggle', {})
            self.extractor = Extractor(
                project_id=bq_config.get('project_id'),
                credentials_path=bq_config.get('credentials_path'),
                kaggle_credentials_path=kaggle_config.get('credentials_path')
            )
        else:
            # Initialize with default values for pipeline mode
            kaggle_config = self.config.get('kaggle', {})
            self.extractor = Extractor(
                project_id=os.getenv('GCP_PROJECT_ID', 'your-gcp-project-id'),
                credentials_path=os.getenv('GCP_CREDENTIALS_PATH', '${GCP_CREDENTIALS_PATH}'),
                kaggle_credentials_path=os.getenv('KAGGLE_CREDENTIALS_PATH', os.path.expanduser('~/.kaggle/kaggle.json'))
            )
        
        # Initialize loader (supports Azure, GCS, and local)
        self.loader = Loader()  # Initialize without Azure-specific params for GCS support
    
    def run_kaggle_to_bigquery(self, dataset_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Run the Kaggle to BigQuery pipeline step
        
        Args:
            dataset_config: Kaggle dataset configuration
            
        Returns:
            DataFrame loaded to BigQuery
        """
        try:
            start_time = datetime.now()
            
            if not self.extractor:
                raise ValueError("Extractor not initialized")
            
            # Extract from Kaggle
            logger.info(f"Extracting from Kaggle dataset: {dataset_config['name']}")
            df = self.extractor.extract_kaggle_dataset(
                dataset_name=dataset_config['name'],
                file_name=dataset_config.get('file')
            )
            
            # Load to BigQuery
            bq_config = self.config['bigquery']
            table_name = dataset_config['bigquery_table']
            logger.info(f"Loading to BigQuery table: {table_name}")
            self.extractor.load_to_bigquery(
                df=df,
                dataset_id=bq_config['dataset'],
                table_id=table_name,
                if_exists='replace'
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            log_entry = {
                'step': 'kaggle_to_bigquery',
                'dataset': dataset_config['name'],
                'table': table_name,
                'rows': len(df),
                'duration': duration,
                'timestamp': end_time.isoformat()
            }
            self.pipeline_log.append(log_entry)
            
            logger.info(f"Kaggle to BigQuery completed in {duration:.2f} seconds")
            return df
            
        except Exception as e:
            logger.error(f"Error in Kaggle to BigQuery step: {str(e)}")
            raise
    
    def run_extraction(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Run the extraction step from BigQuery
        
        Args:
            source_config: Source configuration
            
        Returns:
            Extracted DataFrame
        """
        try:
            start_time = datetime.now()
            
            if not self.extractor:
                raise ValueError("Extractor not initialized")
            
            # Extract based on configuration
            if 'table' in source_config:
                df = self.extractor.extract_table(
                    dataset_id=source_config['table']['dataset'],
                    table_id=source_config['table']['name'],
                    limit=source_config['table'].get('limit')
                )
            elif 'query' in source_config:
                df = self.extractor.extract_with_query(source_config['query'])
            else:
                raise ValueError("Invalid source configuration")
            
            duration = (datetime.now() - start_time).total_seconds()
            self._log_step("extraction", len(df), duration)
            
            return df
            
        except Exception as e:
            logger.error(f"Error in extraction step: {str(e)}")
            raise
    
    def run_transformation(self, df: pd.DataFrame, 
                         transform_config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Run the transformation step
        
        Args:
            df: Input DataFrame
            transform_config: Transformation configuration
            
        Returns:
            Transformed DataFrame
        """
        try:
            start_time = datetime.now()
            original_count = len(df)
            
            if not transform_config:
                # Default transformations
                df = self.transformer.add_timestamp(df)
            else:
                # Apply configured transformations
                if 'clean' in transform_config:
                    df = self.transformer.clean_data(df, **transform_config['clean'])
                
                if 'data_types' in transform_config:
                    df = self.transformer.transform_data_types(df, transform_config['data_types'])
                
                if 'derived_columns' in transform_config:
                    df = self.transformer.add_derived_columns(df, transform_config['derived_columns'])
                
                if 'filters' in transform_config:
                    df = self.transformer.filter_data(df, transform_config['filters'])
                
                if 'aggregations' in transform_config:
                    df = self.transformer.aggregate_data(
                        df, 
                        transform_config['aggregations']['group_by'],
                        transform_config['aggregations']['functions']
                    )
                
                # Always add timestamp
                df = self.transformer.add_timestamp(df)
            
            duration = (datetime.now() - start_time).total_seconds()
            self._log_step("transformation", len(df), duration)
            
            logger.info(f"Transformation completed: {original_count} -> {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error in transformation step: {str(e)}")
            raise
    
    def run_loading(self, df: pd.DataFrame, 
                   target_config: Dict[str, Any]) -> bool:
        """
        Run the loading step
        
        Args:
            df: DataFrame to load
            target_config: Target configuration
            
        Returns:
            True if successful
        """
        try:
            start_time = datetime.now()
            
            if not self.loader:
                raise ValueError("Loader not initialized")
            
            # Load based on configuration
            if 'azure_blob' in target_config:
                blob_config = target_config['azure_blob']
                success = self.loader.load_to_blob(
                    df,
                    container_name=blob_config['container'],
                    blob_name=blob_config['blob_name'],
                    format=blob_config.get('format', 'csv'),
                    overwrite=blob_config.get('overwrite', True)
                )
            elif 'azure_datalake' in target_config:
                dl_config = target_config['azure_datalake']
                success = self.loader.load_to_data_lake(
                    df,
                    file_system_name=dl_config['file_system'],
                    file_path=dl_config['file_path'],
                    format=dl_config.get('format', 'csv'),
                    overwrite=dl_config.get('overwrite', True)
                )
            elif 'local' in target_config:
                local_config = target_config['local']
                success = self.loader.load_to_local(
                    df,
                    file_path=local_config['file_path'],
                    format=local_config.get('format', 'csv')
                )
            elif 'gcs' in target_config:
                gcs_config = target_config['gcs']
                success = self.loader.load_to_gcs(
                    df,
                    bucket_name=gcs_config['bucket_name'],
                    blob_name=gcs_config['blob_name'],
                    credentials_path=self.config.get('gcs', {}).get('credentials_path'),
                    format=gcs_config.get('format', 'csv'),
                    overwrite=gcs_config.get('overwrite', True)
                )
            else:
                raise ValueError("Invalid target configuration")
            
            duration = (datetime.now() - start_time).total_seconds()
            self._log_step("loading", len(df), duration)
            
            return success
            
        except Exception as e:
            logger.error(f"Error in loading step: {str(e)}")
            raise
    
    def run_complete_kaggle_pipeline(self) -> bool:
        """
        Run the complete Kaggle to BigQuery to Azure pipeline
        
        Returns:
            True if successful
        """
        try:
            pipeline_start = datetime.now()
            logger.info("Starting Kaggle to BigQuery to Azure pipeline")
            
            # Step 1: Load Kaggle datasets to BigQuery
            kaggle_datasets = self.config.get('kaggle', {}).get('datasets', [])
            for dataset_config in kaggle_datasets:
                self.run_kaggle_to_bigquery(dataset_config)
            
            # Step 2: Extract from BigQuery and transform
            bq_config = self.config.get('bigquery', {})
            transform_config = self.config.get('transformations', {})
            
            for table_config in bq_config.get('tables', []):
                # Extract from BigQuery
                source_config = {
                    'table': {
                        'dataset': bq_config['dataset'],
                        'name': table_config['name'],
                        'limit': table_config.get('limit')
                    }
                }
                df = self.run_extraction(source_config)
                
                # Transform data
                df = self.run_transformation(df, transform_config)
                
                # Load to Azure
                azure_config = self.config.get('azure', {})
                target_config = {
                    'azure_blob': {
                        'container': azure_config.get('container'),
                        'blob_name': f"{table_config['name']}.csv",
                        'format': 'csv',
                        'overwrite': True
                    }
                }
                self.run_loading(df, target_config)
            
            pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
            logger.info(f"Complete pipeline finished in {pipeline_duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            logger.error(f"Complete pipeline failed: {str(e)}")
            raise
    
    def run_pipeline(self, source_config: Optional[Dict[str, Any]] = None,
                    transform_config: Optional[Dict[str, Any]] = None,
                    target_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Run the complete ETL pipeline
        
        Args:
            source_config: Source configuration
            transform_config: Transformation configuration
            target_config: Target configuration
            
        Returns:
            True if successful
        """
        try:
            pipeline_start = datetime.now()
            logger.info("Starting ETL pipeline")
            
            # Extraction
            df = self.run_extraction(source_config)
            
            # Transformation
            df = self.run_transformation(df, transform_config)
            
            # Loading
            if target_config:
                success = self.run_loading(df, target_config)
            else:
                # Default to local loading
                success = self.loader.load_to_local(
                    df, 
                    "data/processed/output.csv"
                )
            
            pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
            logger.info(f"ETL pipeline completed successfully in {pipeline_duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
    
    def _log_step(self, step_name: str, row_count: int, duration: float) -> None:
        """
        Log pipeline step information
        
        Args:
            step_name: Name of the step
            row_count: Number of rows processed
            duration: Duration in seconds
        """
        log_entry = {
            'step': step_name,
            'timestamp': datetime.now().isoformat(),
            'row_count': row_count,
            'duration_seconds': duration
        }
        self.pipeline_log.append(log_entry)
        
        logger.info(f"{step_name.capitalize()} step: {row_count} rows in {duration:.2f} seconds")
    
    def get_pipeline_log(self) -> List[Dict[str, Any]]:
        """
        Get the pipeline execution log
        
        Returns:
            List of log entries
        """
        return self.pipeline_log
    
    def save_pipeline_log(self, file_path: str) -> None:
        """
        Save pipeline log to file
        
        Args:
            file_path: Path to save the log
        """
        try:
            with open(file_path, 'w') as file:
                yaml.dump(self.pipeline_log, file, default_flow_style=False)
            logger.info(f"Pipeline log saved to {file_path}")
        except Exception as e:
            logger.error(f"Error saving pipeline log: {str(e)}")
            raise
