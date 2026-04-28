"""
Tests for the data lake migration pipeline
"""

import unittest
import pandas as pd
import tempfile
import os
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from transform import Transformer
from load import Loader


class TestTransformer(unittest.TestCase):
    """Test cases for the Transformer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = Transformer()
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['John', 'Jane', None, 'Alice', 'Bob'],
            'age': [25, 30, 35, None, 40],
            'salary': [50000, 60000, 70000, 80000, None]
        })
    
    def test_clean_data_remove_nulls(self):
        """Test data cleaning with null removal"""
        result = self.transformer.clean_data(self.sample_data, remove_nulls=True)
        self.assertEqual(len(result), 2)  # Only rows without nulls remain
    
    def test_clean_data_fill_nulls(self):
        """Test data cleaning with null filling"""
        fill_values = {'name': 'Unknown', 'age': 0, 'salary': 0}
        result = self.transformer.clean_data(self.sample_data, remove_nulls=False, fill_nulls=fill_values)
        self.assertEqual(len(result), 5)  # All rows remain
        self.assertEqual(result.loc[2, 'name'], 'Unknown')
        self.assertEqual(result.loc[3, 'age'], 0)
        self.assertEqual(result.loc[4, 'salary'], 0)
    
    def test_transform_data_types(self):
        """Test data type transformation"""
        type_mapping = {'age': 'float64', 'salary': 'float64'}
        result = self.transformer.transform_data_types(self.sample_data, type_mapping)
        self.assertEqual(result['age'].dtype, 'float64')
        self.assertEqual(result['salary'].dtype, 'float64')
    
    def test_add_derived_columns(self):
        """Test adding derived columns"""
        derived_columns = {'salary_age_ratio': 'salary / age'}
        result = self.transformer.add_derived_columns(self.sample_data, derived_columns)
        self.assertIn('salary_age_ratio', result.columns)
    
    def test_filter_data(self):
        """Test data filtering"""
        filters = {'age': {'min': 30}}
        result = self.transformer.filter_data(self.sample_data, filters)
        self.assertTrue(all(result['age'] >= 30))
    
    def test_add_timestamp(self):
        """Test adding timestamp column"""
        result = self.transformer.add_timestamp(self.sample_data)
        self.assertIn('processed_timestamp', result.columns)


class TestLoader(unittest.TestCase):
    """Test cases for the Loader class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'age': [25, 30, 35]
        })
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_load_to_local_csv(self):
        """Test loading to local CSV file"""
        file_path = os.path.join(self.temp_dir, 'test.csv')
        loader = Loader()
        result = loader.load_to_local(self.sample_data, file_path, 'csv')
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(file_path))
        
        # Verify data
        loaded_data = pd.read_csv(file_path)
        self.assertEqual(len(loaded_data), len(self.sample_data))
    
    def test_load_to_local_json(self):
        """Test loading to local JSON file"""
        file_path = os.path.join(self.temp_dir, 'test.json')
        loader = Loader()
        result = loader.load_to_local(self.sample_data, file_path, 'json')
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(file_path))
    
    def test_load_partitioned(self):
        """Test loading partitioned data"""
        base_path = os.path.join(self.temp_dir, 'partitioned')
        partition_columns = ['age']
        
        loader = Loader()
        result = loader.load_partitioned(self.sample_data, base_path, partition_columns, 'csv')
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(base_path))
        
        # Check that partition directories were created
        partition_dirs = [d for d in os.listdir(base_path) if d.startswith('age=')]
        self.assertGreater(len(partition_dirs), 0)


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'age': [25, 30, 35, 28, 32],
            'salary': [50000, 60000, 70000, 55000, 65000]
        })
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_transform_and_load_workflow(self):
        """Test complete transform and load workflow"""
        transformer = Transformer()
        loader = Loader()
        
        # Transform data
        transform_config = {
            'clean': {'remove_nulls': True},
            'data_types': {'age': 'float64', 'salary': 'float64'},
            'derived_columns': {'salary_age_ratio': 'salary / age'}
        }
        
        transformed_data = transformer.run_transformation(self.sample_data, transform_config)
        
        # Load data
        output_path = os.path.join(self.temp_dir, 'output.csv')
        result = loader.load_to_local(transformed_data, output_path, 'csv')
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_path))
        
        # Verify output
        loaded_data = pd.read_csv(output_path)
        self.assertIn('salary_age_ratio', loaded_data.columns)
        self.assertIn('processed_timestamp', loaded_data.columns)
        self.assertEqual(len(loaded_data), len(self.sample_data))


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    unittest.main(verbosity=2)
