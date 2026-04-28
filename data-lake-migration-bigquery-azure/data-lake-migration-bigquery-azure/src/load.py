"""
Load module for loading data to Azure Data Lake and other targets
"""

import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from google.cloud import storage
from google.oauth2 import service_account
from typing import Dict, List, Optional, Union
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class Loader:
    """
    Handles data loading to Azure Data Lake and other storage systems
    """
    
    def __init__(self, connection_string: Optional[str] = None,
                 account_name: Optional[str] = None,
                 credential: Optional[object] = None):
        """
        Initialize the loader (supports Azure, GCS, and local storage)
        
        Args:
            connection_string: Azure storage connection string
            account_name: Azure storage account name
            credential: Azure credential object
        """
        self.connection_string = connection_string
        self.account_name = account_name
        self.credential = credential or DefaultAzureCredential()
        
        # Initialize Azure clients only if Azure config is provided
        if connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_name:
            self.blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=self.credential
            )
        else:
            # No Azure initialization - supports GCS and local storage
            self.blob_service_client = None
    
    def load_to_blob(self, df: pd.DataFrame, 
                    container_name: str,
                    blob_name: str,
                    format: str = "csv",
                    overwrite: bool = True) -> bool:
        """
        Load DataFrame to Azure Blob Storage
        
        Args:
            df: DataFrame to load
            container_name: Azure blob container name
            blob_name: Target blob name
            format: File format ('csv', 'json', 'parquet')
            overwrite: Whether to overwrite existing blob
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get container client
            container_client = self.blob_service_client.get_container_client(container_name)
            
            # Create container if it doesn't exist
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created container '{container_name}'")
            
            # Get blob client
            blob_client = container_client.get_blob_client(blob_name)
            
            # Check if blob exists and overwrite is False
            if not overwrite and blob_client.exists():
                logger.warning(f"Blob '{blob_name}' already exists and overwrite is False")
                return False
            
            # Convert DataFrame to bytes based on format
            if format.lower() == "csv":
                data = df.to_csv(index=False)
                content_settings = {"content_type": "text/csv"}
            elif format.lower() == "json":
                data = df.to_json(orient='records', lines=True)
                content_settings = {"content_type": "application/json"}
            elif format.lower() == "parquet":
                data = df.to_parquet(index=False)
                content_settings = {"content_type": "application/octet-stream"}
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Upload data
            blob_client.upload_blob(
                data,
                overwrite=overwrite,
                content_settings=content_settings
            )
            
            logger.info(f"Successfully loaded {len(df)} rows to blob '{blob_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to blob storage: {str(e)}")
            raise
    
    def load_to_data_lake(self, df: pd.DataFrame,
                         file_system_name: str,
                         file_path: str,
                         format: str = "csv",
                         overwrite: bool = True) -> bool:
        """
        Load DataFrame to Azure Data Lake Storage Gen2
        
        Args:
            df: DataFrame to load
            file_system_name: Data Lake file system name
            file_path: Target file path
            format: File format ('csv', 'json', 'parquet')
            overwrite: Whether to overwrite existing file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create Data Lake service client
            if self.connection_string:
                service_client = DataLakeServiceClient.from_connection_string(self.connection_string)
            else:
                service_client = DataLakeServiceClient(
                    account_url=f"https://{self.account_name}.dfs.core.windows.net",
                    credential=self.credential
                )
            
            # Get file system client
            file_system_client = service_client.get_file_system_client(file_system_name)
            
            # Create file system if it doesn't exist
            if not file_system_client.exists():
                file_system_client.create_file_system()
                logger.info(f"Created file system '{file_system_name}'")
            
            # Get file client
            file_client = file_system_client.get_file_client(file_path)
            
            # Check if file exists and overwrite is False
            if not overwrite and file_client.exists():
                logger.warning(f"File '{file_path}' already exists and overwrite is False")
                return False
            
            # Convert DataFrame to bytes based on format
            if format.lower() == "csv":
                data = df.to_csv(index=False).encode('utf-8')
            elif format.lower() == "json":
                data = df.to_json(orient='records', lines=True).encode('utf-8')
            elif format.lower() == "parquet":
                data = df.to_parquet(index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Upload data
            file_client.upload_data(
                data,
                overwrite=overwrite
            )
            
            logger.info(f"Successfully loaded {len(df)} rows to Data Lake file '{file_path}'")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to Data Lake: {str(e)}")
            raise
    
    def load_to_local(self, df: pd.DataFrame,
                     file_path: str,
                     format: str = "csv") -> bool:
        """
        Load DataFrame to local file system
        
        Args:
            df: DataFrame to load
            file_path: Target file path
            format: File format ('csv', 'json', 'parquet')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Save DataFrame based on format
            if format.lower() == "csv":
                df.to_csv(file_path, index=False)
            elif format.lower() == "json":
                df.to_json(file_path, orient='records', lines=True)
            elif format.lower() == "parquet":
                df.to_parquet(file_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Successfully loaded {len(df)} rows to local file '{file_path}'")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to local file: {str(e)}")
            raise
    
    def load_partitioned(self, df: pd.DataFrame,
                        base_path: str,
                        partition_columns: List[str],
                        format: str = "csv") -> bool:
        """
        Load DataFrame as partitioned data
        
        Args:
            df: DataFrame to load
            base_path: Base path for partitioned data
            partition_columns: Columns to partition by
            format: File format ('csv', 'json', 'parquet')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create base directory
            Path(base_path).mkdir(parents=True, exist_ok=True)
            
            # Group by partition columns
            grouped = df.groupby(partition_columns)
            
            for name, group in grouped:
                # Create partition path
                if len(partition_columns) == 1:
                    partition_path = f"{partition_columns[0]}={name}"
                else:
                    partition_path = "/".join([f"{col}={val}" for col, val in zip(partition_columns, name)])
                
                full_path = Path(base_path) / partition_path
                full_path.mkdir(parents=True, exist_ok=True)
                
                # Save partition
                file_name = f"data.{format}"
                file_path = full_path / file_name
                
                if format.lower() == "csv":
                    group.to_csv(file_path, index=False)
                elif format.lower() == "json":
                    group.to_json(file_path, orient='records', lines=True)
                elif format.lower() == "parquet":
                    group.to_parquet(file_path, index=False)
                else:
                    raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Successfully loaded {len(df)} rows as partitioned data to '{base_path}'")
            return True
            
        except Exception as e:
            logger.error(f"Error loading partitioned data: {str(e)}")
            raise
    
    def load_to_gcs(self, df: pd.DataFrame,
                   bucket_name: str,
                   blob_name: str,
                   credentials_path: Optional[str] = None,
                   format: str = "csv",
                   overwrite: bool = True) -> bool:
        """
        Load DataFrame to Google Cloud Storage
        
        Args:
            df: DataFrame to load
            bucket_name: GCS bucket name
            blob_name: Blob name (file path)
            credentials_path: Path to service account credentials
            format: File format (csv, json, parquet)
            overwrite: Whether to overwrite existing blob
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Initialize GCS client
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                client = storage.Client(project=credentials.project_id, credentials=credentials)
            else:
                client = storage.Client()
            
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Convert DataFrame to bytes based on format
            if format.lower() == "csv":
                data = df.to_csv(index=False).encode('utf-8')
            elif format.lower() == "json":
                data = df.to_json(orient='records').encode('utf-8')
            elif format.lower() == "parquet":
                data = df.to_parquet(index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Upload to GCS
            blob.upload_from_string(data, content_type='text/csv' if format.lower() == 'csv' else 'application/json')
            
            logger.info(f"Successfully loaded {len(df)} rows to GCS bucket: {bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to GCS: {str(e)}")
            raise
