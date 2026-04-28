"""
Test suite for data quality validation modules.
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from validation.schema_checks import SchemaValidator
from validation.null_checks import NullValidator
from validation.range_checks import RangeValidator
from monitoring.data_quality_metrics import DataQualityMetrics
from governance.lineage_tracker import LineageTracker
from governance.audit_logger import AuditLogger, AuditEventType


class TestSchemaValidator(unittest.TestCase):
    """Test cases for SchemaValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.expected_schema = {
            'id': 'int',
            'name': 'string',
            'age': 'int',
            'salary': 'float',
            'is_active': 'bool',
            'join_date': 'datetime'
        }
        self.validator = SchemaValidator(self.expected_schema)
        
        # Create test DataFrame
        self.valid_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0],
            'is_active': [True, False, True],
            'join_date': ['2020-01-01', '2021-01-01', '2022-01-01']
        })
        
        self.invalid_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000, 60000, 70000],  # Wrong type
            'is_active': [True, False, True],
            'extra_column': ['x', 'y', 'z']  # Extra column
        })
    
    def test_validate_schema_valid(self):
        """Test schema validation with valid data."""
        result = self.validator.validate_schema(self.valid_df)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(result['total_errors'], 0)
        self.assertEqual(len(result['missing_columns']), 0)
        self.assertEqual(len(result['extra_columns']), 0)
        self.assertEqual(len(result['type_mismatches']), 0)
    
    def test_validate_schema_invalid(self):
        """Test schema validation with invalid data."""
        result = self.validator.validate_schema(self.invalid_df)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['total_errors'], 0)
        self.assertIn('extra_column', result['extra_columns'])
        self.assertGreater(len(result['type_mismatches']), 0)
    
    def test_enforce_schema(self):
        """Test schema enforcement."""
        df_missing = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        enforced_df = self.validator.enforce_schema(df_missing)
        
        # Check that missing columns were added
        self.assertIn('age', enforced_df.columns)
        self.assertIn('salary', enforced_df.columns)
        self.assertIn('is_active', enforced_df.columns)
        self.assertIn('join_date', enforced_df.columns)


class TestNullValidator(unittest.TestCase):
    """Test cases for NullValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = NullValidator(null_threshold=0.2)
        
        # Create test DataFrame with nulls
        self.df_with_nulls = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', 'Bob', None],
            'age': [25, 30, None, 35, 40],
            'salary': [50000.0, 60000.0, 70000.0, None, 90000.0]
        })
        
        self.df_clean = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0]
        })
    
    def test_check_null_values_with_nulls(self):
        """Test null checking with null values present."""
        result = self.validator.check_null_values(self.df_with_nulls)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['overall_null_percentage'], 0)
        self.assertGreater(len(result['columns_above_threshold']), 0)
    
    def test_check_null_values_clean(self):
        """Test null checking with clean data."""
        result = self.validator.check_null_values(self.df_clean)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(result['overall_null_percentage'], 0)
        self.assertEqual(len(result['columns_above_threshold']), 0)
    
    def test_validate_critical_columns(self):
        """Test critical column validation."""
        critical_columns = ['id', 'name']
        
        result = self.validator.validate_critical_columns(self.df_with_nulls, critical_columns)
        
        self.assertFalse(result['is_valid'])
        self.assertIn('name', result['failed_columns'])
        self.assertEqual(result['critical_nulls']['name'], 2)
    
    def test_handle_nulls_drop(self):
        """Test null handling with drop strategy."""
        result_df = self.validator.handle_nulls(self.df_with_nulls, strategy='drop', columns=['name'])
        
        # Should drop rows with null names
        self.assertEqual(len(result_df), 3)
        self.assertTrue(result_df['name'].notna().all())
    
    def test_handle_nulls_fill(self):
        """Test null handling with fill strategy."""
        result_df = self.validator.handle_nulls(self.df_with_nulls, strategy='fill', fill_value='Unknown', columns=['name'])
        
        # Should fill null names with 'Unknown'
        self.assertTrue(result_df['name'].notna().all())
        self.assertEqual(result_df['name'].iloc[1], 'Unknown')


class TestRangeValidator(unittest.TestCase):
    """Test cases for RangeValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = RangeValidator()
        
        # Create test DataFrame
        self.df = pd.DataFrame({
            'age': [25, 30, 35, 70, -5],  # Some values outside range
            'salary': [50000, 60000, 70000, 150000, 10000],  # Some values outside range
            'department': ['Engineering', 'Sales', 'Marketing', 'HR', 'Invalid'],
            'email': ['test@example.com', 'invalid-email', 'user@domain.com', 'bad', 'good@site.com'],
            'join_date': ['2020-01-01', '2021-01-01', '2022-01-01', '2019-01-01', '2025-01-01']
        })
        
        self.range_rules = {
            'age': {'min': 18, 'max': 65, 'inclusive': True},
            'salary': {'min': 20000, 'max': 100000, 'inclusive': True}
        }
        
        self.categorical_rules = {
            'department': {
                'allowed_values': ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'],
                'case_sensitive': False
            }
        }
        
        self.string_rules = {
            'email': {
                'min_length': 5,
                'max_length': 50,
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            }
        }
        
        self.date_rules = {
            'join_date': {
                'start_date': '2020-01-01',
                'end_date': '2024-12-31'
            }
        }
    
    def test_check_numeric_ranges(self):
        """Test numeric range validation."""
        result = self.validator.check_numeric_ranges(self.df, self.range_rules)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['total_violations'], 0)
        self.assertIn('age', result['range_violations'])
        self.assertIn('salary', result['range_violations'])
    
    def test_check_categorical_constraints(self):
        """Test categorical constraint validation."""
        result = self.validator.check_categorical_constraints(self.df, self.categorical_rules)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['total_violations'], 0)
        self.assertIn('department', result['categorical_violations'])
    
    def test_check_string_constraints(self):
        """Test string constraint validation."""
        result = self.validator.check_string_constraints(self.df, self.string_rules)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['total_violations'], 0)
        self.assertIn('email', result['string_violations'])
    
    def test_check_date_ranges(self):
        """Test date range validation."""
        result = self.validator.check_date_ranges(self.df, self.date_rules)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(result['total_violations'], 0)
        self.assertIn('join_date', result['date_violations'])
    
    def test_detect_outliers(self):
        """Test outlier detection."""
        # Create DataFrame with outliers
        df_outliers = pd.DataFrame({
            'normal_col': [10, 12, 11, 13, 9, 14, 8, 15, 100],  # 100 is outlier
            'no_outliers': [10, 12, 11, 13, 9, 14, 8, 15, 11]
        })
        
        result = self.validator.detect_outliers(df_outliers, ['normal_col', 'no_outliers'], method='iqr')
        
        self.assertGreater(result['outliers']['normal_col']['count'], 0)
        self.assertEqual(result['outliers']['no_outliers']['count'], 0)


class TestDataQualityMetrics(unittest.TestCase):
    """Test cases for DataQualityMetrics class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.metrics_calculator = DataQualityMetrics()
        
        # Create test DataFrame
        self.df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', None],
            'age': [25, 30, 35, None, 40],
            'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
            'join_date': ['2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01', '2024-01-01']
        })
        
        self.validation_rules = {
            'id': {'required': True, 'data_type': 'numeric'},
            'name': {'required': True, 'data_type': 'string', 'min_length': 2},
            'age': {'required': False, 'data_type': 'numeric', 'min': 18, 'max': 65}
        }
    
    def test_calculate_completeness_metrics(self):
        """Test completeness metrics calculation."""
        metrics = self.metrics_calculator.calculate_completeness_metrics(self.df)
        
        self.assertIn('overall_completeness', metrics)
        self.assertIn('column_completeness', metrics)
        self.assertIn('row_completeness', metrics)
        self.assertIn('completeness_score', metrics)
        
        # Check that completeness is less than 1 due to nulls
        self.assertLess(metrics['overall_completeness'], 1.0)
    
    def test_calculate_consistency_metrics(self):
        """Test consistency metrics calculation."""
        # Add duplicate rows for testing
        df_with_duplicates = pd.concat([self.df, self.df.iloc[:2]], ignore_index=True)
        
        metrics = self.metrics_calculator.calculate_consistency_metrics(df_with_duplicates)
        
        self.assertIn('consistency_score', metrics)
        self.assertIn('duplicate_rows', metrics)
        self.assertIn('column_consistency', metrics)
        
        # Should detect duplicates
        self.assertGreater(metrics['duplicate_rows'], 0)
    
    def test_calculate_validity_metrics(self):
        """Test validity metrics calculation."""
        metrics = self.metrics_calculator.calculate_validity_metrics(self.df, self.validation_rules)
        
        self.assertIn('validity_score', metrics)
        self.assertIn('total_validations', metrics)
        self.assertIn('passed_validations', metrics)
        self.assertIn('failed_validations', metrics)
        self.assertIn('column_validity', metrics)
    
    def test_calculate_overall_quality_score(self):
        """Test overall quality score calculation."""
        # Create mock metrics
        mock_metrics = {
            'completeness_metrics': {'completeness_score': 0.9},
            'accuracy_metrics': {'accuracy_score': 0.95},
            'consistency_metrics': {'consistency_score': 0.85},
            'timeliness_metrics': {'timeliness_score': 0.8},
            'validity_metrics': {'validity_score': 0.9}
        }
        
        overall_score = self.metrics_calculator.calculate_overall_quality_score(mock_metrics)
        
        self.assertIn('overall_quality_score', overall_score)
        self.assertIn('individual_scores', overall_score)
        self.assertIn('quality_grade', overall_score)
        
        # Score should be between 0 and 1
        self.assertGreaterEqual(overall_score['overall_quality_score'], 0)
        self.assertLessEqual(overall_score['overall_quality_score'], 1)
    
    def test_track_metrics_over_time(self):
        """Test metrics tracking over time."""
        # Track some metrics
        mock_metrics = {'test_metric': 0.8}
        self.metrics_calculator.track_metrics_over_time('test_dataset', mock_metrics)
        
        # Check that metrics were tracked
        self.assertIn('test_dataset', self.metrics_calculator.metrics_history)
        self.assertEqual(len(self.metrics_calculator.metrics_history['test_dataset']), 1)


class TestLineageTracker(unittest.TestCase):
    """Test cases for LineageTracker class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.tracker = LineageTracker(':memory:')  # Use in-memory database for testing
        
        # Create test DataFrame
        self.df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
    
    def test_register_dataset(self):
        """Test dataset registration."""
        dataset_id = self.tracker.register_dataset(
            df=self.df,
            dataset_name='test_dataset',
            source_path='test.csv'
        )
        
        self.assertIsNotNone(dataset_id)
        
        # Check that dataset was registered
        datasets = self.tracker.get_all_datasets()
        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0]['name'], 'test_dataset')
    
    def test_track_transformation(self):
        """Test transformation tracking."""
        # Register input and output datasets
        input_id = self.tracker.register_dataset(self.df, 'input_dataset')
        output_id = self.tracker.register_dataset(self.df, 'output_dataset')
        
        # Track transformation
        transformation_id = self.tracker.track_transformation(
            input_dataset_id=input_id,
            output_dataset_id=output_id,
            transformation_type='filter',
            parameters={'column': 'age', 'value': 30}
        )
        
        self.assertIsNotNone(transformation_id)
        
        # Check lineage graph
        lineage = self.tracker.get_lineage_graph(output_id)
        self.assertEqual(len(lineage['upstream_transformations']), 1)
    
    def test_track_column_lineage(self):
        """Test column lineage tracking."""
        input_id = self.tracker.register_dataset(self.df, 'input_dataset')
        output_id = self.tracker.register_dataset(self.df, 'output_dataset')
        
        # Track column lineage
        lineage_ids = self.tracker.track_column_lineage(
            input_dataset_id=input_id,
            input_columns=['id', 'name'],
            output_dataset_id=output_id,
            output_columns=['user_id', 'user_name'],
            transformation_logic='Rename columns'
        )
        
        self.assertEqual(len(lineage_ids), 2)
    
    def test_find_upstream_datasets(self):
        """Test finding upstream datasets."""
        # Create chain of datasets
        dataset1_id = self.tracker.register_dataset(self.df, 'dataset1')
        dataset2_id = self.tracker.register_dataset(self.df, 'dataset2')
        dataset3_id = self.tracker.register_dataset(self.df, 'dataset3')
        
        # Create transformations
        self.tracker.track_transformation(dataset1_id, dataset2_id, 'transform', {})
        self.tracker.track_transformation(dataset2_id, dataset3_id, 'transform', {})
        
        # Find upstream of dataset3
        upstream = self.tracker.find_upstream_datasets(dataset3_id)
        
        self.assertEqual(len(upstream), 2)  # dataset1 and dataset2


class TestAuditLogger(unittest.TestCase):
    """Test cases for AuditLogger class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.logger = AuditLogger(':memory:')  # Use in-memory database for testing
    
    def test_log_event(self):
        """Test basic event logging."""
        event_id = self.logger.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            user_id='test_user',
            resource_type='dataset',
            resource_id='test_dataset',
            action='read',
            details={'rows': 100}
        )
        
        self.assertIsNotNone(event_id)
        
        # Check that event was logged
        audit_trail = self.logger.get_audit_trail()
        self.assertEqual(len(audit_trail), 1)
        self.assertEqual(audit_trail[0]['event_type'], 'data_access')
    
    def test_log_data_access(self):
        """Test data access logging."""
        event_id = self.logger.log_data_access(
            user_id='test_user',
            dataset_id='test_dataset',
            access_type='read',
            details={'rows': 100}
        )
        
        self.assertIsNotNone(event_id)
        
        # Check that event was logged with correct type
        audit_trail = self.logger.get_audit_trail()
        self.assertEqual(audit_trail[0]['event_type'], 'data_access')
    
    def test_log_validation_execution(self):
        """Test validation execution logging."""
        validation_results = {
            'is_valid': True,
            'details': {'column': 'id', 'result': 'passed'}
        }
        
        event_id = self.logger.log_validation_execution(
            user_id='test_user',
            validation_id='schema_check',
            dataset_id='test_dataset',
            validation_results=validation_results
        )
        
        self.assertIsNotNone(event_id)
        
        # Check that event was logged
        audit_trail = self.logger.get_audit_trail()
        self.assertEqual(audit_trail[0]['event_type'], 'validation_executed')
    
    def test_log_quality_check(self):
        """Test quality check logging."""
        quality_metrics = {
            'overall_quality_score': {'overall_quality_score': 0.85}
        }
        
        event_id = self.logger.log_quality_check(
            user_id='test_user',
            check_type='comprehensive',
            dataset_id='test_dataset',
            quality_metrics=quality_metrics
        )
        
        self.assertIsNotNone(event_id)
        
        # Check that event was logged
        audit_trail = self.logger.get_audit_trail()
        self.assertIn(audit_trail[0]['event_type'], ['data_processing', 'quality_check_failed'])
    
    def test_get_audit_trail_with_filters(self):
        """Test audit trail filtering."""
        # Log multiple events
        self.logger.log_event(AuditEventType.DATA_ACCESS, 'user1', 'dataset', 'ds1', 'read')
        self.logger.log_event(AuditEventType.DATA_MODIFICATION, 'user2', 'dataset', 'ds2', 'write')
        self.logger.log_event(AuditEventType.DATA_ACCESS, 'user1', 'dataset', 'ds3', 'read')
        
        # Filter by user
        user1_events = self.logger.get_audit_trail(user_id='user1')
        self.assertEqual(len(user1_events), 2)
        
        # Filter by event type
        access_events = self.logger.get_audit_trail(event_type='data_access')
        self.assertEqual(len(access_events), 2)
    
    def test_get_activity_summary(self):
        """Test activity summary generation."""
        # Log some events
        self.logger.log_event(AuditEventType.DATA_ACCESS, 'user1', 'dataset', 'ds1', 'read', success=True)
        self.logger.log_event(AuditEventType.DATA_MODIFICATION, 'user2', 'dataset', 'ds2', 'write', success=False)
        
        summary = self.logger.get_activity_summary()
        
        self.assertIn('total_events', summary)
        self.assertIn('successful_events', summary)
        self.assertIn('failed_events', summary)
        self.assertIn('event_types', summary)
        
        self.assertEqual(summary['total_events'], 2)
        self.assertEqual(summary['successful_events'], 1)
        self.assertEqual(summary['failed_events'], 1)


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestSchemaValidator,
        TestNullValidator,
        TestRangeValidator,
        TestDataQualityMetrics,
        TestLineageTracker,
        TestAuditLogger
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\nTest Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    # Exit with appropriate code
    if result.failures or result.errors:
        sys.exit(1)
    else:
        sys.exit(0)
