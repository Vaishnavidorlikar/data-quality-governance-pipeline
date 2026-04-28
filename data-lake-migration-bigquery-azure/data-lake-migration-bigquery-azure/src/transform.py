"""
Transform module for data transformation and cleaning
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class Transformer:
    """
    Handles data transformation and cleaning operations
    """
    
    def __init__(self):
        """Initialize the transformer"""
        self.transformation_log = []
    
    def clean_data(self, df: pd.DataFrame, 
                   remove_nulls: bool = True,
                   fill_nulls: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Clean the input DataFrame
        
        Args:
            df: Input DataFrame
            remove_nulls: Whether to remove rows with null values
            fill_nulls: Dictionary of column names and fill values
            
        Returns:
            Cleaned DataFrame
        """
        try:
            result_df = df.copy()
            original_shape = result_df.shape
            
            if remove_nulls:
                result_df = result_df.dropna()
                logger.info(f"Removed {original_shape[0] - result_df.shape[0]} rows with null values")
            
            if fill_nulls:
                for column, fill_value in fill_nulls.items():
                    if column in result_df.columns:
                        null_count = result_df[column].isnull().sum()
                        result_df[column].fillna(fill_value, inplace=True)
                        logger.info(f"Filled {null_count} null values in column '{column}'")
            
            logger.info(f"Data cleaning completed. Shape: {original_shape} -> {result_df.shape}")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in data cleaning: {str(e)}")
            raise
    
    def transform_data_types(self, df: pd.DataFrame, 
                           type_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Transform data types according to the provided mapping
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to target data types
            
        Returns:
            DataFrame with transformed data types
        """
        try:
            result_df = df.copy()
            
            for column, target_type in type_mapping.items():
                if column in result_df.columns:
                    try:
                        result_df[column] = result_df[column].astype(target_type)
                        logger.info(f"Converted column '{column}' to {target_type}")
                    except Exception as e:
                        logger.warning(f"Failed to convert column '{column}' to {target_type}: {str(e)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in data type transformation: {str(e)}")
            raise
    
    def add_derived_columns(self, df: pd.DataFrame, 
                          derived_columns: Dict[str, str]) -> pd.DataFrame:
        """
        Add derived columns based on existing columns
        
        Args:
            df: Input DataFrame
            derived_columns: Dictionary mapping new column names to expressions
            
        Returns:
            DataFrame with added derived columns
        """
        try:
            result_df = df.copy()
            
            for new_column, expression in derived_columns.items():
                try:
                    # Simple expression evaluation (be cautious with eval in production)
                    result_df[new_column] = result_df.eval(expression)
                    logger.info(f"Added derived column '{new_column}'")
                except Exception as e:
                    logger.warning(f"Failed to add derived column '{new_column}': {str(e)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error adding derived columns: {str(e)}")
            raise
    
    def filter_data(self, df: pd.DataFrame, 
                   filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Filter data based on specified conditions
        
        Args:
            df: Input DataFrame
            filters: Dictionary of column names and filter conditions
            
        Returns:
            Filtered DataFrame
        """
        try:
            result_df = df.copy()
            original_shape = result_df.shape
            
            for column, condition in filters.items():
                if column in result_df.columns:
                    if isinstance(condition, dict):
                        # Handle range conditions
                        if 'min' in condition:
                            result_df = result_df[result_df[column] >= condition['min']]
                        if 'max' in condition:
                            result_df = result_df[result_df[column] <= condition['max']]
                        if 'values' in condition:
                            result_df = result_df[result_df[column].isin(condition['values'])]
                    else:
                        # Handle simple equality condition
                        result_df = result_df[result_df[column] == condition]
            
            logger.info(f"Data filtering completed. Shape: {original_shape} -> {result_df.shape}")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in data filtering: {str(e)}")
            raise
    
    def aggregate_data(self, df: pd.DataFrame, 
                      group_by: List[str],
                      aggregations: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
        """
        Aggregate data by grouping columns
        
        Args:
            df: Input DataFrame
            group_by: List of columns to group by
            aggregations: Dictionary of column names and aggregation functions
            
        Returns:
            Aggregated DataFrame
        """
        try:
            result_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten column names if multiple aggregations
            if isinstance(result_df.columns, pd.MultiIndex):
                result_df.columns = ['_'.join(col).strip() if col[1] else col[0] 
                                   for col in result_df.columns.values]
            
            logger.info(f"Data aggregation completed. Shape: {df.shape} -> {result_df.shape}")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in data aggregation: {str(e)}")
            raise
    
    def add_timestamp(self, df: pd.DataFrame, 
                      column_name: str = "processed_timestamp") -> pd.DataFrame:
        """
        Add a timestamp column to the DataFrame
        
        Args:
            df: Input DataFrame
            column_name: Name of the timestamp column
            
        Returns:
            DataFrame with added timestamp column
        """
        try:
            result_df = df.copy()
            result_df[column_name] = datetime.now()
            logger.info(f"Added timestamp column '{column_name}'")
            return result_df
            
        except Exception as e:
            logger.error(f"Error adding timestamp: {str(e)}")
            raise
