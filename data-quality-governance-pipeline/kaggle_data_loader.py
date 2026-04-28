#!/usr/bin/env python3
"""
Kaggle Dataset Loader for Data Quality Pipeline

This script provides utilities to load and process real-world datasets from Kaggle
for testing and demonstrating the data quality governance pipeline.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import zipfile
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KaggleDataLoader:
    """Utility class for loading and preparing Kaggle datasets for quality testing."""
    
    def __init__(self, data_dir: str = "data/kaggle"):
        """
        Initialize the Kaggle data loader.
        
        Args:
            data_dir: Directory to store downloaded datasets
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Kaggle dataset configurations
        self.datasets = {
            'telco_churn': {
                'name': 'blastchar/telco-customer-churn',
                'files': ['WA_Fn-UseC_-Telco-Customer-Churn.csv'],
                'local_name': 'telco_customer_churn.csv'
            },
            'financial_transactions': {
                'name': 'computingvictor/transactions-fraud-datasets',
                'files': ['credit_card_fraud_transactions.csv'],
                'local_name': 'financial_transactions.csv'
            },
            'bank_churn': {
                'name': 'gauravtopre/bank-customer-churn-dataset',
                'files': ['Bank-Customer-Churn-Prediction.csv'],
                'local_name': 'bank_customer_churn.csv'
            }
        }
    
    def setup_kaggle_api(self):
        """Setup Kaggle API credentials."""
        try:
            import kaggle
            from kaggle.api.kaggle_api_extended import KaggleApi
            # Check if API is authenticated - try a simple API call
            api = KaggleApi()
            api.authenticate()
            api.dataset_list()
            logger.info("Kaggle API is authenticated and ready")
            return True
        except Exception as e:
            logger.error(f"Kaggle API setup failed: {e}")
            logger.info("Please setup Kaggle API:")
            logger.info("1. Install: pip install kaggle")
            logger.info("2. Get API key from: https://www.kaggle.com/account")
            logger.info("3. Place kaggle.json in ~/.kaggle/ directory")
            return False
    
    def download_dataset(self, dataset_key: str, force_download: bool = False) -> bool:
        """
        Download dataset using Kaggle API.
        
        Args:
            dataset_key: Key from self.datasets
            force_download: Force re-download even if file exists
            
        Returns:
            True if download successful, False otherwise
        """
        if dataset_key not in self.datasets:
            logger.error(f"Unknown dataset: {dataset_key}")
            return False
        
        dataset_config = self.datasets[dataset_key]
        local_file = self.data_dir / dataset_config['local_name']
        
        # Check if file already exists
        if local_file.exists() and not force_download:
            logger.info(f"Dataset {dataset_key} already exists locally")
            return True
        
        # Try to download via API
        try:
            import kaggle
            
            logger.info(f"Downloading {dataset_config['name']}...")
            
            # Download dataset
            kaggle.api.dataset_download_files(
                dataset_config['name'],
                path=str(self.data_dir),
                unzip=True,
                force=force_download
            )
            
            # Find and rename the target file
            for file_name in dataset_config['files']:
                source_file = self.data_dir / file_name
                if source_file.exists():
                    shutil.move(str(source_file), str(local_file))
                    logger.info(f"Downloaded and renamed: {local_file}")
                    break
            
            # Clean up zip files
            for zip_file in self.data_dir.glob("*.zip"):
                zip_file.unlink()
            
            return True
            
        except ImportError:
            logger.error("Kaggle package not installed. Run: pip install kaggle")
            return False
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def auto_download_all_datasets(self):
        """Download all configured datasets."""
        logger.info("Starting automatic dataset downloads...")
        
        # Setup API first
        if not self.setup_kaggle_api():
            logger.warning("API setup failed, will use sample data")
            return False
        
        success_count = 0
        for dataset_key in self.datasets.keys():
            if self.download_dataset(dataset_key):
                success_count += 1
        
        logger.info(f"Downloaded {success_count}/{len(self.datasets)} datasets")
        return success_count > 0
    
    def load_telco_churn_data(self, file_path: str = None, auto_download: bool = True) -> pd.DataFrame:
        """
        Load Telco Customer Churn dataset.
        
        Args:
            file_path: Path to the Telco churn CSV file
            auto_download: Automatically download via API if file not found
            
        Returns:
            DataFrame with customer churn data
        """
        if file_path is None:
            file_path = self.data_dir / "telco_customer_churn.csv"
        
        # Try to download if file doesn't exist
        if not file_path.exists() and auto_download:
            logger.info("Telco churn dataset not found, attempting download...")
            self.download_dataset('telco_churn')
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded Telco churn dataset: {len(df)} records, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logger.warning(f"Telco churn dataset not found at {file_path}")
            logger.info("Using sample data for demonstration")
            return self._create_sample_telco_data()
    
    def load_financial_transactions(self, file_path: str = None, auto_download: bool = True) -> pd.DataFrame:
        """
        Load Financial Transactions dataset.
        
        Args:
            file_path: Path to the financial transactions CSV file
            auto_download: Automatically download via API if file not found
            
        Returns:
            DataFrame with transaction data
        """
        if file_path is None:
            file_path = self.data_dir / "financial_transactions.csv"
        
        # Try to download if file doesn't exist
        if not file_path.exists() and auto_download:
            logger.info("Financial transactions dataset not found, attempting download...")
            self.download_dataset('financial_transactions')
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded financial transactions: {len(df)} records, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logger.warning(f"Financial transactions dataset not found at {file_path}")
            logger.info("Using sample data for demonstration")
            return self._create_sample_financial_data()
    
    def load_bank_churn_data(self, file_path: str = None, auto_download: bool = True) -> pd.DataFrame:
        """
        Load Bank Customer Churn dataset.
        
        Args:
            file_path: Path to the bank churn CSV file
            auto_download: Automatically download via API if file not found
            
        Returns:
            DataFrame with bank customer data
        """
        if file_path is None:
            file_path = self.data_dir / "bank_customer_churn.csv"
        
        # Try to download if file doesn't exist
        if not file_path.exists() and auto_download:
            logger.info("Bank churn dataset not found, attempting download...")
            self.download_dataset('bank_churn')
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded bank churn dataset: {len(df)} records, {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            logger.warning(f"Bank churn dataset not found at {file_path}")
            logger.info("Using sample data for demonstration")
            return self._create_sample_bank_data()
    
    def _create_sample_telco_data(self) -> pd.DataFrame:
        """Create sample Telco customer churn data for demonstration."""
        np.random.seed(42)
        n_records = 1000
        
        data = {
            'customerID': [f'CUST_{i:06d}' for i in range(1, n_records + 1)],
            'gender': np.random.choice(['Male', 'Female'], n_records),
            'SeniorCitizen': np.random.choice([0, 1], n_records, p=[0.9, 0.1]),
            'Partner': np.random.choice(['Yes', 'No'], n_records),
            'Dependents': np.random.choice(['Yes', 'No'], n_records, p=[0.3, 0.7]),
            'tenure': np.random.randint(1, 72, n_records),
            'PhoneService': np.random.choice(['Yes', 'No'], n_records, p=[0.9, 0.1]),
            'MultipleLines': np.random.choice(['Yes', 'No', 'No phone service'], n_records),
            'InternetService': np.random.choice(['DSL', 'Fiber optic', 'No'], n_records, p=[0.3, 0.5, 0.2]),
            'OnlineSecurity': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'OnlineBackup': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'DeviceProtection': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'TechSupport': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'StreamingTV': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'StreamingMovies': np.random.choice(['Yes', 'No', 'No internet service'], n_records),
            'Contract': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_records, p=[0.5, 0.3, 0.2]),
            'PaperlessBilling': np.random.choice(['Yes', 'No'], n_records, p=[0.7, 0.3]),
            'PaymentMethod': np.random.choice([
                'Electronic check', 'Mailed check', 'Bank transfer (automatic)', 
                'Credit card (automatic)'
            ], n_records),
            'MonthlyCharges': np.random.uniform(20, 120, n_records),
            'TotalCharges': np.random.uniform(100, 8000, n_records),
            'Churn': np.random.choice(['Yes', 'No'], n_records, p=[0.3, 0.7])
        }
        
        # Add some data quality issues
        df = pd.DataFrame(data)
        
        # Add missing values
        for i in range(50):
            col = np.random.choice(['MonthlyCharges', 'TotalCharges', 'tenure'])
            df.loc[i, col] = np.nan
        
        # Add some outliers
        for i in range(10, 15):
            df.loc[i, 'MonthlyCharges'] = np.random.uniform(200, 300)
        
        return df
    
    def _create_sample_financial_data(self) -> pd.DataFrame:
        """Create sample financial transaction data for demonstration."""
        np.random.seed(42)
        n_records = 2000
        
        data = {
            'transaction_id': [f'TXN_{i:08d}' for i in range(1, n_records + 1)],
            'customer_id': [f'CUST_{np.random.randint(1, 500):06d}' for _ in range(n_records)],
            'transaction_amount': np.random.lognormal(3, 1, n_records),
            'transaction_date': pd.date_range('2023-01-01', periods=n_records, freq='H'),
            'merchant_category': np.random.choice([
                'Retail', 'Restaurant', 'Gas', 'Online', 'Travel', 'Entertainment'
            ], n_records),
            'card_type': np.random.choice(['Credit', 'Debit'], n_records),
            'is_fraud': np.random.choice([0, 1], n_records, p=[0.98, 0.02]),
            'customer_age': np.random.randint(18, 80, n_records),
            'customer_income': np.random.normal(50000, 20000, n_records)
        }
        
        df = pd.DataFrame(data)
        
        # Add data quality issues
        # Missing values
        for i in range(100):
            col = np.random.choice(['customer_age', 'customer_income'])
            df.loc[i, col] = np.nan
        
        # Outliers in transaction amounts
        for i in range(20, 30):
            df.loc[i, 'transaction_amount'] = np.random.uniform(10000, 50000)
        
        # Invalid dates
        for i in range(5, 10):
            df.loc[i, 'transaction_date'] = '2023-13-45'  # Invalid date
        
        return df
    
    def _create_sample_bank_data(self) -> pd.DataFrame:
        """Create sample bank customer churn data for demonstration."""
        np.random.seed(42)
        n_records = 1500
        
        data = {
            'RowNumber': range(1, n_records + 1),
            'CustomerId': [np.random.randint(10000000, 99999999) for _ in range(n_records)],
            'Surname': [f'Customer_{i}' for i in range(n_records)],
            'CreditScore': np.random.randint(350, 850, n_records),
            'Geography': np.random.choice(['France', 'Germany', 'Spain'], n_records),
            'Gender': np.random.choice(['Male', 'Female'], n_records),
            'Age': np.random.randint(18, 92, n_records),
            'Tenure': np.random.randint(0, 10, n_records),
            'Balance': np.random.uniform(0, 250000, n_records),
            'NumOfProducts': np.random.randint(1, 4, n_records),
            'HasCrCard': np.random.choice([0, 1], n_records, p=[0.3, 0.7]),
            'IsActiveMember': np.random.choice([0, 1], n_records, p=[0.5, 0.5]),
            'EstimatedSalary': np.random.normal(100000, 50000, n_records),
            'Exited': np.random.choice([0, 1], n_records, p=[0.8, 0.2])
        }
        
        df = pd.DataFrame(data)
        
        # Add data quality issues
        # Missing credit scores
        for i in range(30):
            df.loc[i, 'CreditScore'] = np.nan
        
        # Negative balances (invalid)
        for i in range(10, 15):
            df.loc[i, 'Balance'] = -np.random.uniform(100, 1000)
        
        # Age outliers
        for i in range(5, 8):
            df.loc[i, 'Age'] = np.random.choice([120, 130, 150])
        
        return df
    
    def get_dataset_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get comprehensive information about a dataset.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with dataset statistics
        """
        info = {
            'shape': df.shape,
            'columns': list(df.columns),
            'data_types': df.dtypes.to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
            'numeric_columns': list(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': list(df.select_dtypes(include=['object']).columns),
            'memory_usage': df.memory_usage(deep=True).sum()
        }
        
        return info


def main():
    """Demonstrate loading Kaggle datasets."""
    loader = KaggleDataLoader()
    
    print("=== Kaggle Dataset Loader Demo ===\n")
    
    # Load Telco churn data
    print("1. Loading Telco Customer Churn Dataset:")
    telco_df = loader.load_telco_churn_data()
    telco_info = loader.get_dataset_info(telco_df)
    print(f"   Records: {telco_info['shape'][0]:,}")
    print(f"   Columns: {telco_info['shape'][1]}")
    print(f"   Missing values: {sum(telco_info['missing_values'].values())}")
    print()
    
    # Load financial transactions
    print("2. Loading Financial Transactions Dataset:")
    fin_df = loader.load_financial_transactions()
    fin_info = loader.get_dataset_info(fin_df)
    print(f"   Records: {fin_info['shape'][0]:,}")
    print(f"   Columns: {fin_info['shape'][1]}")
    print(f"   Missing values: {sum(fin_info['missing_values'].values())}")
    print()
    
    # Load bank churn data
    print("3. Loading Bank Customer Churn Dataset:")
    bank_df = loader.load_bank_churn_data()
    bank_info = loader.get_dataset_info(bank_df)
    print(f"   Records: {bank_info['shape'][0]:,}")
    print(f"   Columns: {bank_info['shape'][1]}")
    print(f"   Missing values: {sum(bank_info['missing_values'].values())}")
    print()
    
    print("=== Ready for Data Quality Pipeline Testing ===")


if __name__ == "__main__":
    main()
