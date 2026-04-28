"""Data lineage tracking for data quality governance."""

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class LineageTracker:
    """Tracks data lineage and provenance throughout the pipeline."""
    
    def __init__(self, storage_path: str = "data/lineage/"):
        """
        Initialize lineage tracker.
        
        Args:
            storage_path: Path to store lineage data
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.lineage_data = []
    
    def track_data_lineage(self, dataset_name: str, source_path: str, user_id: str, 
                          timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Track data lineage information.
        
        Args:
            dataset_name: Name/identifier for the dataset
            source_path: Path to original data source
            user_id: User who processed the data
            timestamp: Processing timestamp
            
        Returns:
            Dictionary containing lineage information
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        lineage_entry = {
            'dataset_name': dataset_name,
            'source_path': source_path,
            'user_id': user_id,
            'timestamp': timestamp.isoformat(),
            'processing_steps': [],
            'data_hash': self._calculate_data_hash(source_path)
        }
        
        self.lineage_data.append(lineage_entry)
        
        # Save lineage data
        self._save_lineage_data()
        
        logger.info(f"Data lineage tracked: {dataset_name} by {user_id}")
        
        return lineage_entry
    
    def add_processing_step(self, dataset_name: str, step_name: str, 
                        step_details: Dict[str, Any]) -> None:
        """
        Add a processing step to the lineage tracking.
        
        Args:
            dataset_name: Name of the dataset
            step_name: Name of the processing step
            step_details: Details about the processing step
        """
        # Find the lineage entry
        for entry in self.lineage_data:
            if entry['dataset_name'] == dataset_name:
                step_entry = {
                    'step_name': step_name,
                    'timestamp': datetime.now().isoformat(),
                    'details': step_details
                }
                entry['processing_steps'].append(step_entry)
                break
        
        self._save_lineage_data()
        logger.info(f"Processing step added to {dataset_name}: {step_name}")
    
    def get_lineage_history(self, dataset_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get lineage history for a specific dataset or all datasets.
        
        Args:
            dataset_name: Optional dataset name to filter by
            
        Returns:
            List of lineage entries
        """
        if dataset_name:
            return [entry for entry in self.lineage_data if entry['dataset_name'] == dataset_name]
        else:
            return self.lineage_data
    
    def get_data_provenance(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get complete data provenance information for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary containing provenance information
        """
        lineage_entries = self.get_lineage_history(dataset_name)
        
        if not lineage_entries:
            return {'error': f"No lineage data found for dataset: {dataset_name}"}
        
        latest_entry = lineage_entries[-1]  # Get most recent entry
        
        return {
            'dataset_name': latest_entry['dataset_name'],
            'source_path': latest_entry['source_path'],
            'data_hash': latest_entry['data_hash'],
            'processing_history': latest_entry['processing_steps'],
            'last_modified': latest_entry['timestamp'],
            'total_processing_steps': len(latest_entry['processing_steps']),
            'users_involved': list(set([entry['user_id'] for entry in lineage_entries]))
        }
    
    def _calculate_data_hash(self, file_path: str) -> str:
        """
        Calculate a simple hash of the data file for integrity checking.
        
        Args:
            file_path: Path to the data file
            
        Returns:
            String hash of the file
        """
        try:
            import hashlib
            with open(file_path, 'rb') as f:
                file_content = f.read()
                return hashlib.md5(file_content).hexdigest()
        except Exception as e:
            logger.warning(f"Could not calculate hash for {file_path}: {e}")
            return "unknown"
    
    def _save_lineage_data(self) -> None:
        """Save lineage data to storage."""
        try:
            lineage_file = self.storage_path / "lineage_data.json"
            with open(lineage_file, 'w') as f:
                json.dump(self.lineage_data, f, indent=2, default=str)
            logger.info(f"Lineage data saved to {lineage_file}")
        except Exception as e:
            logger.error(f"Failed to save lineage data: {e}")
    
    def load_lineage_data(self) -> None:
        """Load existing lineage data from storage."""
        try:
            lineage_file = self.storage_path / "lineage_data.json"
            if lineage_file.exists():
                with open(lineage_file, 'r') as f:
                    self.lineage_data = json.load(f)
                logger.info(f"Lineage data loaded from {lineage_file}")
        except Exception as e:
            logger.warning(f"Could not load lineage data: {e}")
    
    def export_lineage_report(self, output_path: str, format: str = 'json') -> str:
        """
        Export lineage data in specified format.
        
        Args:
            output_path: Path for the output file
            format: Export format ('json', 'csv', 'html')
            
        Returns:
            Path to the exported file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == 'json':
            with open(output_file.with_suffix('.json'), 'w') as f:
                json.dump(self.lineage_data, f, indent=2, default=str)
        elif format.lower() == 'csv':
            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(self.lineage_data)
            df.to_csv(output_file.with_suffix('.csv'), index=False)
        elif format.lower() == 'html':
            html_content = self._generate_html_report()
            with open(output_file.with_suffix('.html'), 'w') as f:
                f.write(html_content)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Lineage report exported: {output_file}")
        return str(output_file)
    
    def _generate_html_report(self) -> str:
        """Generate HTML report for lineage data."""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Lineage Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .lineage-entry { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }
                .processing-step { background-color: #f8f9fa; padding: 10px; margin: 5px 0; border-radius: 3px; }
                .timestamp { color: #666; font-size: 0.9em; }
            </style>
        </head>
        <body>
            <h1>Data Lineage Report</h1>
        """
        
        for entry in self.lineage_data:
            html_content += f"""
            <div class="lineage-entry">
                <h3>{entry['dataset_name']}</h3>
                <p><strong>Source:</strong> {entry['source_path']}</p>
                <p><strong>User:</strong> {entry['user_id']}</p>
                <p><strong>Timestamp:</strong> <span class="timestamp">{entry['timestamp']}</span></p>
                <p><strong>Processing Steps:</strong></p>
                <ul>
            """
            
            for step in entry['processing_steps']:
                html_content += f"""
                    <li class="processing-step">
                        <strong>{step['step_name']}</strong> - {step['timestamp']}
                        <br>{step.get('details', {})}
                    </li>
                """
            
            html_content += """
                </ul>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        return html_content
