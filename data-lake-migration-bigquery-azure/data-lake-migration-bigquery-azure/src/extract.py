"""
Extract module for data extraction from Kaggle to BigQuery
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Dict, List, Optional
import logging
import os
import zipfile
import tempfile

logger = logging.getLogger(__name__)


class Extractor:
    """
    Handles data extraction from Kaggle datasets and loading to BigQuery
    """
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None, 
                 kaggle_credentials_path: Optional[str] = None):
        """
        Initialize the extractor with Kaggle and BigQuery credentials
        
        Args:
            project_id: GCP project ID
            credentials_path: Path to service account credentials JSON file
            kaggle_credentials_path: Path to Kaggle API credentials JSON file
        """
        self.project_id = project_id
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            self.client = bigquery.Client(project=project_id)
        
        # Initialize Kaggle API
        if kaggle_credentials_path and os.path.exists(kaggle_credentials_path):
            os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(kaggle_credentials_path)
        self.api = KaggleApi()
        try:
            self.api.authenticate()
            logger.info("Kaggle API authenticated successfully")
        except Exception as e:
            logger.error(f"Kaggle API authentication failed: {e}")
            self.api = None
    
    def extract_kaggle_dataset(self, dataset_name: str, file_name: Optional[str] = None) -> pd.DataFrame:
        """
        Extract data from Kaggle dataset
        
        Args:
            dataset_name: Kaggle dataset name (e.g., 'user/dataset-name')
            file_name: Specific file to extract from dataset (optional)
            
        Returns:
            DataFrame containing the extracted data
        """
        if not self.api:
            raise ValueError("Kaggle API not authenticated")
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download dataset
                logger.info(f"Downloading Kaggle dataset: {dataset_name}")
                self.api.dataset_download_files(dataset_name, path=temp_dir, unzip=True)
                
                # Find the data file
                if file_name:
                    data_file = os.path.join(temp_dir, file_name)
                else:
                    # Find first CSV file in the directory
                    csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
                    if not csv_files:
                        raise ValueError("No CSV files found in dataset")
                    data_file = os.path.join(temp_dir, csv_files[0])
                
                # Load data into DataFrame
                df = pd.read_csv(data_file)
                logger.info(f"Successfully loaded {len(df)} rows from Kaggle dataset")
                
                return df
                
        except Exception as e:
            logger.error(f"Error extracting Kaggle dataset: {e}")
            raise
    
    def load_to_bigquery(self, df: pd.DataFrame, dataset_id: str, table_id: str, 
                        if_exists: str = 'replace') -> None:
        """
        Load DataFrame to BigQuery table
        
        Args:
            df: DataFrame to load
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            if_exists: Behavior if table exists ('replace', 'append', 'fail')
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            # Create dataset if it doesn't exist
            dataset_ref = self.client.dataset(dataset_id)
            try:
                self.client.get_dataset(dataset_ref)
            except:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset: {dataset_id}")
            
            # Load data to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if if_exists == 'replace' 
                else bigquery.WriteDisposition.WRITE_APPEND if if_exists == 'append'
                else bigquery.WriteDisposition.WRITE_EMPTY
            )
            
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} rows to BigQuery table: {table_ref}")
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {e}")
            raise
    
    def extract_table(self, dataset_id: str, table_id: str, 
                     limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extract data from a BigQuery table
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            limit: Optional limit on number of rows to extract
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            query = f"SELECT * FROM `{table_ref}`"
            
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(f"Extracting data from {table_ref}")
            df = self.client.query(query).to_dataframe()
            
            logger.info(f"Successfully extracted {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from {dataset_id}.{table_id}: {str(e)}")
            raise
    
    def extract_with_query(self, query: str) -> pd.DataFrame:
        """
        Extract data using a custom SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame containing the query results
        """
        try:
            logger.info("Executing custom query")
            df = self.client.query(query).to_dataframe()
            
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def list_tables(self, dataset_id: str) -> List[str]:
        """
        List all tables in a BigQuery dataset
        
        Args:
            dataset_id: BigQuery dataset ID
            
        Returns:
            List of table names
        """
        try:
            dataset_ref = self.client.dataset(dataset_id, project=self.project_id)
            tables = list(self.client.list_tables(dataset_ref))
            
            table_names = [table.table_id for table in tables]
            logger.info(f"Found {len(table_names)} tables in dataset {dataset_id}")
            
            return table_names
            
        except Exception as e:
            logger.error(f"Error listing tables in dataset {dataset_id}: {str(e)}")
            raise
