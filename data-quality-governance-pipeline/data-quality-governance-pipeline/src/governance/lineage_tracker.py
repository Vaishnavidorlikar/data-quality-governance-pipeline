"""
Data lineage tracking module for data governance.
"""

import pandas as pd
import json
import hashlib
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
import logging
from pathlib import Path
import sqlite3
import uuid

logger = logging.getLogger(__name__)


class LineageTracker:
    """Tracks data lineage and transformations throughout the pipeline."""
    
    def __init__(self, db_path: str = "lineage.db"):
        """
        Initialize lineage tracker.
        
        Args:
            db_path: Path to SQLite database for storing lineage information
        """
        self.db_path = db_path
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the lineage database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create datasets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_path TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                schema_hash TEXT,
                row_count INTEGER,
                file_size INTEGER,
                metadata TEXT
            )
        ''')
        
        # Create transformations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transformations (
                id TEXT PRIMARY KEY,
                input_dataset_id TEXT,
                output_dataset_id TEXT,
                transformation_type TEXT,
                parameters TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (input_dataset_id) REFERENCES datasets (id),
                FOREIGN KEY (output_dataset_id) REFERENCES datasets (id)
            )
        ''')
        
        # Create column_lineage table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS column_lineage (
                id TEXT PRIMARY KEY,
                input_dataset_id TEXT,
                input_column TEXT,
                output_dataset_id TEXT,
                output_column TEXT,
                transformation_logic TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (input_dataset_id) REFERENCES datasets (id),
                FOREIGN KEY (output_dataset_id) REFERENCES datasets (id)
            )
        ''')
        
        # Create data_quality_events table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_events (
                id TEXT PRIMARY KEY,
                dataset_id TEXT,
                event_type TEXT,
                description TEXT,
                metrics TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (dataset_id) REFERENCES datasets (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def register_dataset(self, df: pd.DataFrame, dataset_name: str, 
                        source_path: Optional[str] = None, 
                        metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Register a dataset in the lineage tracker.
        
        Args:
            df: DataFrame to register
            dataset_name: Name of the dataset
            source_path: Original source path of the data
            metadata: Additional metadata about the dataset
            
        Returns:
            Dataset ID
        """
        dataset_id = str(uuid.uuid4())
        schema_hash = self._calculate_schema_hash(df)
        
        # Calculate file size if source path provided
        file_size = None
        if source_path and Path(source_path).exists():
            file_size = Path(source_path).stat().st_size
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO datasets 
            (id, name, source_path, created_at, updated_at, schema_hash, row_count, file_size, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dataset_id,
            dataset_name,
            source_path,
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            schema_hash,
            len(df),
            file_size,
            json.dumps(metadata) if metadata else None
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Registered dataset: {dataset_name} with ID: {dataset_id}")
        return dataset_id
    
    def track_transformation(self, input_dataset_id: str, output_dataset_id: str,
                           transformation_type: str, parameters: Dict[str, Any]) -> str:
        """
        Track a data transformation operation.
        
        Args:
            input_dataset_id: ID of the input dataset
            output_dataset_id: ID of the output dataset
            transformation_type: Type of transformation (e.g., 'filter', 'join', 'aggregate')
            parameters: Parameters used in the transformation
            
        Returns:
            Transformation ID
        """
        transformation_id = str(uuid.uuid4())
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO transformations 
            (id, input_dataset_id, output_dataset_id, transformation_type, parameters, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            transformation_id,
            input_dataset_id,
            output_dataset_id,
            transformation_type,
            json.dumps(parameters),
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Tracked transformation: {transformation_type} from {input_dataset_id} to {output_dataset_id}")
        return transformation_id
    
    def track_column_lineage(self, input_dataset_id: str, input_columns: List[str],
                           output_dataset_id: str, output_columns: List[str],
                           transformation_logic: str) -> List[str]:
        """
        Track lineage relationships between columns.
        
        Args:
            input_dataset_id: ID of the input dataset
            input_columns: List of input column names
            output_dataset_id: ID of the output dataset
            output_columns: List of output column names
            transformation_logic: Description of how columns were transformed
            
        Returns:
            List of column lineage IDs
        """
        lineage_ids = []
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create mappings for each column relationship
        for i, (input_col, output_col) in enumerate(zip(input_columns, output_columns)):
            lineage_id = str(uuid.uuid4())
            
            cursor.execute('''
                INSERT INTO column_lineage 
                (id, input_dataset_id, input_column, output_dataset_id, output_column, transformation_logic, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                lineage_id,
                input_dataset_id,
                input_col,
                output_dataset_id,
                output_col,
                transformation_logic,
                datetime.now().isoformat()
            ))
            
            lineage_ids.append(lineage_id)
        
        conn.commit()
        conn.close()
        
        logger.info(f"Tracked column lineage for {len(lineage_ids)} column relationships")
        return lineage_ids
    
    def track_data_quality_event(self, dataset_id: str, event_type: str,
                               description: str, metrics: Dict[str, Any]) -> str:
        """
        Track a data quality event for a dataset.
        
        Args:
            dataset_id: ID of the dataset
            event_type: Type of event (e.g., 'validation_passed', 'validation_failed', 'quality_degradation')
            description: Description of the event
            metrics: Quality metrics associated with the event
            
        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO data_quality_events 
            (id, dataset_id, event_type, description, metrics, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            event_id,
            dataset_id,
            event_type,
            description,
            json.dumps(metrics),
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Tracked data quality event: {event_type} for dataset {dataset_id}")
        return event_id
    
    def get_lineage_graph(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get the complete lineage graph for a dataset.
        
        Args:
            dataset_id: ID of the dataset to analyze
            
        Returns:
            Dictionary containing the lineage graph
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get dataset information
        cursor.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,))
        dataset_row = cursor.fetchone()
        
        if not dataset_row:
            conn.close()
            return {'error': 'Dataset not found'}
        
        # Get upstream transformations
        cursor.execute('''
            SELECT t.*, d.name as input_name 
            FROM transformations t
            JOIN datasets d ON t.input_dataset_id = d.id
            WHERE t.output_dataset_id = ?
        ''', (dataset_id,))
        upstream_transformations = cursor.fetchall()
        
        # Get downstream transformations
        cursor.execute('''
            SELECT t.*, d.name as output_name 
            FROM transformations t
            JOIN datasets d ON t.output_dataset_id = d.id
            WHERE t.input_dataset_id = ?
        ''', (dataset_id,))
        downstream_transformations = cursor.fetchall()
        
        # Get column lineage
        cursor.execute('''
            SELECT cl.*, di.name as input_dataset_name, do.name as output_dataset_name
            FROM column_lineage cl
            JOIN datasets di ON cl.input_dataset_id = di.id
            JOIN datasets do ON cl.output_dataset_id = do.id
            WHERE cl.input_dataset_id = ? OR cl.output_dataset_id = ?
        ''', (dataset_id, dataset_id))
        column_lineage = cursor.fetchall()
        
        # Get data quality events
        cursor.execute('SELECT * FROM data_quality_events WHERE dataset_id = ?', (dataset_id,))
        quality_events = cursor.fetchall()
        
        conn.close()
        
        return {
            'dataset': {
                'id': dataset_row[0],
                'name': dataset_row[1],
                'source_path': dataset_row[2],
                'created_at': dataset_row[3],
                'updated_at': dataset_row[4],
                'schema_hash': dataset_row[5],
                'row_count': dataset_row[6],
                'file_size': dataset_row[7],
                'metadata': json.loads(dataset_row[8]) if dataset_row[8] else None
            },
            'upstream_transformations': [
                {
                    'id': row[0],
                    'input_dataset_id': row[1],
                    'output_dataset_id': row[2],
                    'transformation_type': row[3],
                    'parameters': json.loads(row[4]) if row[4] else None,
                    'created_at': row[5],
                    'input_name': row[6]
                }
                for row in upstream_transformations
            ],
            'downstream_transformations': [
                {
                    'id': row[0],
                    'input_dataset_id': row[1],
                    'output_dataset_id': row[2],
                    'transformation_type': row[3],
                    'parameters': json.loads(row[4]) if row[4] else None,
                    'created_at': row[5],
                    'output_name': row[6]
                }
                for row in downstream_transformations
            ],
            'column_lineage': [
                {
                    'id': row[0],
                    'input_dataset_id': row[1],
                    'input_column': row[2],
                    'output_dataset_id': row[3],
                    'output_column': row[4],
                    'transformation_logic': row[5],
                    'created_at': row[6],
                    'input_dataset_name': row[7],
                    'output_dataset_name': row[8]
                }
                for row in column_lineage
            ],
            'quality_events': [
                {
                    'id': row[0],
                    'dataset_id': row[1],
                    'event_type': row[2],
                    'description': row[3],
                    'metrics': json.loads(row[4]) if row[4] else None,
                    'created_at': row[5]
                }
                for row in quality_events
            ]
        }
    
    def get_dataset_history(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get the complete history of a dataset.
        
        Args:
            dataset_id: ID of the dataset
            
        Returns:
            List of historical events
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all transformations involving this dataset
        cursor.execute('''
            SELECT t.*, 
                   CASE WHEN t.input_dataset_id = ? THEN 'output' ELSE 'input' END as role,
                   d.name as other_dataset_name
            FROM transformations t
            JOIN datasets d ON 
                CASE WHEN t.input_dataset_id = ? THEN t.output_dataset_id ELSE t.input_dataset_id END = d.id
            WHERE t.input_dataset_id = ? OR t.output_dataset_id = ?
            ORDER BY t.created_at
        ''', (dataset_id, dataset_id, dataset_id, dataset_id))
        
        transformations = cursor.fetchall()
        
        # Get quality events
        cursor.execute('SELECT * FROM data_quality_events WHERE dataset_id = ? ORDER BY created_at', (dataset_id,))
        quality_events = cursor.fetchall()
        
        conn.close()
        
        # Combine and sort events by timestamp
        events = []
        
        for trans in transformations:
            events.append({
                'type': 'transformation',
                'timestamp': trans[5],
                'role': trans[6],
                'transformation_type': trans[3],
                'parameters': json.loads(trans[4]) if trans[4] else None,
                'other_dataset': trans[7]
            })
        
        for event in quality_events:
            events.append({
                'type': 'quality_event',
                'timestamp': event[5],
                'event_type': event[2],
                'description': event[3],
                'metrics': json.loads(event[4]) if event[4] else None
            })
        
        # Sort by timestamp
        events.sort(key=lambda x: x['timestamp'])
        
        return events
    
    def find_upstream_datasets(self, dataset_id: str, max_depth: int = 5) -> List[Dict[str, Any]]:
        """
        Find all upstream datasets recursively.
        
        Args:
            dataset_id: Starting dataset ID
            max_depth: Maximum depth to traverse
            
        Returns:
            List of upstream datasets with their depth
        """
        upstream_datasets = []
        visited = set()
        
        def _find_recursive(current_id: str, depth: int):
            if depth > max_depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT d.id, d.name, d.created_at, t.transformation_type, t.created_at
                FROM transformations t
                JOIN datasets d ON t.input_dataset_id = d.id
                WHERE t.output_dataset_id = ?
            ''', (current_id,))
            
            results = cursor.fetchall()
            conn.close()
            
            for row in results:
                upstream_datasets.append({
                    'id': row[0],
                    'name': row[1],
                    'created_at': row[2],
                    'transformation_type': row[3],
                    'transformation_date': row[4],
                    'depth': depth
                })
                
                # Recursively find upstream of this dataset
                _find_recursive(row[0], depth + 1)
        
        _find_recursive(dataset_id, 1)
        return upstream_datasets
    
    def find_downstream_datasets(self, dataset_id: str, max_depth: int = 5) -> List[Dict[str, Any]]:
        """
        Find all downstream datasets recursively.
        
        Args:
            dataset_id: Starting dataset ID
            max_depth: Maximum depth to traverse
            
        Returns:
            List of downstream datasets with their depth
        """
        downstream_datasets = []
        visited = set()
        
        def _find_recursive(current_id: str, depth: int):
            if depth > max_depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT d.id, d.name, d.created_at, t.transformation_type, t.created_at
                FROM transformations t
                JOIN datasets d ON t.output_dataset_id = d.id
                WHERE t.input_dataset_id = ?
            ''', (current_id,))
            
            results = cursor.fetchall()
            conn.close()
            
            for row in results:
                downstream_datasets.append({
                    'id': row[0],
                    'name': row[1],
                    'created_at': row[2],
                    'transformation_type': row[3],
                    'transformation_date': row[4],
                    'depth': depth
                })
                
                # Recursively find downstream of this dataset
                _find_recursive(row[0], depth + 1)
        
        _find_recursive(dataset_id, 1)
        return downstream_datasets
    
    def _calculate_schema_hash(self, df: pd.DataFrame) -> str:
        """Calculate a hash of the DataFrame schema."""
        schema_info = {
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'shape': df.shape
        }
        
        schema_json = json.dumps(schema_info, sort_keys=True)
        return hashlib.md5(schema_json.encode()).hexdigest()
    
    def export_lineage_report(self, dataset_id: str, output_path: str) -> None:
        """
        Export a comprehensive lineage report for a dataset.
        
        Args:
            dataset_id: ID of the dataset
            output_path: Path to save the report
        """
        lineage_graph = self.get_lineage_graph(dataset_id)
        dataset_history = self.get_dataset_history(dataset_id)
        upstream = self.find_upstream_datasets(dataset_id)
        downstream = self.find_downstream_datasets(dataset_id)
        
        report = {
            'dataset': lineage_graph['dataset'],
            'lineage_graph': lineage_graph,
            'history': dataset_history,
            'upstream_datasets': upstream,
            'downstream_datasets': downstream,
            'generated_at': datetime.now().isoformat()
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Lineage report exported to {output_path}")
    
    def get_all_datasets(self) -> List[Dict[str, Any]]:
        """
        Get all registered datasets.
        
        Returns:
            List of all datasets
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM datasets ORDER BY created_at DESC')
        datasets = cursor.fetchall()
        
        conn.close()
        
        return [
            {
                'id': row[0],
                'name': row[1],
                'source_path': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'schema_hash': row[5],
                'row_count': row[6],
                'file_size': row[7],
                'metadata': json.loads(row[8]) if row[8] else None
            }
            for row in datasets
        ]
