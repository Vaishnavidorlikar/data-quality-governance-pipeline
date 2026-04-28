"""Audit logging system for data quality governance."""

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Enumeration of audit event types."""
    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    VALIDATION_START = "validation_start"
    VALIDATION_END = "validation_end"
    QUALITY_ISSUE = "quality_issue"
    CONFIG_CHANGE = "config_change"
    USER_ACTION = "user_action"
    SYSTEM_ERROR = "system_error"


class AuditLogger:
    """Comprehensive audit logging for data quality governance."""
    
    def __init__(self, storage_path: str = "data/audit/"):
        """
        Initialize audit logger.
        
        Args:
            storage_path: Path to store audit logs
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.audit_log = []
    
    def log_pipeline_execution(self, dataset_name: str, user_id: str, results: Dict[str, Any]) -> None:
        """
        Log pipeline execution with comprehensive details.
        
        Args:
            dataset_name: Name of the dataset
            user_id: User who executed the pipeline
            results: Pipeline execution results
        """
        audit_entry = {
            'event_type': AuditEventType.PIPELINE_START,
            'timestamp': datetime.now().isoformat(),
            'dataset_name': dataset_name,
            'user_id': user_id,
            'details': {
                'action': 'pipeline_execution',
                'status': 'started',
                'results_summary': {
                    'total_records': results.get('total_records', 0),
                    'validation_count': len(results.get('validation_results', {})),
                    'quality_score': results.get('quality_metrics', {}).get('overall_score', 0)
                }
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.info(f"Pipeline execution started: {dataset_name} by {user_id}")
    
    def log_pipeline_completion(self, dataset_name: str, user_id: str, results: Dict[str, Any]) -> None:
        """
        Log pipeline completion with comprehensive details.
        
        Args:
            dataset_name: Name of the dataset
            user_id: User who executed the pipeline
            results: Pipeline execution results
        """
        audit_entry = {
            'event_type': AuditEventType.PIPELINE_END,
            'timestamp': datetime.now().isoformat(),
            'dataset_name': dataset_name,
            'user_id': user_id,
            'details': {
                'action': 'pipeline_completion',
                'status': 'completed',
                'results_summary': {
                    'total_records': results.get('total_records', 0),
                    'validation_count': len(results.get('validation_results', {})),
                    'quality_score': results.get('quality_metrics', {}).get('overall_score', 0),
                    'issues_detected': self._count_issues(results.get('validation_results', {}))
                }
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.info(f"Pipeline execution completed: {dataset_name} by {user_id}")
    
    def log_quality_issue(self, dataset_name: str, issue_type: str, severity: str, 
                        details: Dict[str, Any]) -> None:
        """
        Log quality issues with severity levels.
        
        Args:
            dataset_name: Name of the dataset
            issue_type: Type of quality issue
            severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)
            details: Additional details about the issue
        """
        audit_entry = {
            'event_type': AuditEventType.QUALITY_ISSUE,
            'timestamp': datetime.now().isoformat(),
            'dataset_name': dataset_name,
            'details': {
                'issue_type': issue_type,
                'severity': severity,
                'issue_details': details
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.warning(f"Quality issue detected in {dataset_name}: {issue_type} ({severity})")
    
    def log_configuration_change(self, user_id: str, config_path: str, 
                             old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
        """
        Log configuration changes for audit trail.
        
        Args:
            user_id: User who made the change
            config_path: Path to configuration file
            old_config: Previous configuration
            new_config: New configuration
        """
        audit_entry = {
            'event_type': AuditEventType.CONFIG_CHANGE,
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'details': {
                'action': 'configuration_change',
                'config_path': config_path,
                'old_config': old_config,
                'new_config': new_config
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.info(f"Configuration changed by {user_id}: {config_path}")
    
    def log_user_action(self, user_id: str, action: str, 
                     details: Dict[str, Any]) -> None:
        """
        Log user actions for accountability.
        
        Args:
            user_id: User who performed the action
            action: Description of the action
            details: Additional details about the action
        """
        audit_entry = {
            'event_type': AuditEventType.USER_ACTION,
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'details': {
                'action': action,
                'action_details': details
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.info(f"User action logged: {action} by {user_id}")
    
    def log_system_error(self, error_type: str, error_message: str, 
                       context: Dict[str, Any]) -> None:
        """
        Log system errors for troubleshooting.
        
        Args:
            error_type: Type of system error
            error_message: Error message
            context: Context information
        """
        audit_entry = {
            'event_type': AuditEventType.SYSTEM_ERROR,
            'timestamp': datetime.now().isoformat(),
            'details': {
                'error_type': error_type,
                'error_message': error_message,
                'context': context
            }
        }
        
        self._add_audit_entry(audit_entry)
        logger.error(f"System error logged: {error_type} - {error_message}")
    
    def _add_audit_entry(self, audit_entry: Dict[str, Any]) -> None:
        """
        Add an audit entry to the log.
        
        Args:
            audit_entry: Audit entry dictionary
        """
        self.audit_log.append(audit_entry)
        self._save_audit_log()
    
    def _count_issues(self, validation_results: Dict[str, Any]) -> int:
        """
        Count total issues across all validation results.
        
        Args:
            validation_results: Results from validation checks
            
        Returns:
            Total number of issues detected
        """
        total_issues = 0
        
        for validation_type, result in validation_results.items():
            if isinstance(result, dict):
                # Count violations
                if result.get('violations'):
                    total_issues += len(result['violations'])
                
                # Count missing columns
                if result.get('missing_columns'):
                    total_issues += len(result['missing_columns'])
                
                # Count type mismatches
                if result.get('type_mismatches'):
                    total_issues += len(result['type_mismatches'])
        
        return total_issues
    
    def _save_audit_log(self) -> None:
        """Save audit log to storage."""
        try:
            audit_file = self.storage_path / f"audit_log_{datetime.now().strftime('%Y%m%d')}.json"
            with open(audit_file, 'w') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            logger.info(f"Audit log saved to {audit_file}")
        except Exception as e:
            logger.error(f"Failed to save audit log: {e}")
    
    def get_audit_summary(self, start_date: Optional[str] = None, 
                        end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary of audit activities within date range.
        
        Args:
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            
        Returns:
            Dictionary containing audit summary
        """
        filtered_log = self.audit_log
        
        # Filter by date range if provided
        if start_date or end_date:
            filtered_log = []
            for entry in self.audit_log:
                entry_date = entry['timestamp'][:10]  # Extract YYYY-MM-DD
                if start_date and entry_date < start_date:
                    continue
                if end_date and entry_date > end_date:
                    continue
                filtered_log.append(entry)
        else:
            filtered_log = self.audit_log
        
        # Generate summary statistics
        summary = {
            'total_entries': len(filtered_log),
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'event_counts': {},
            'user_activity': {},
            'quality_issues': 0
        }
        
        # Count events by type
        for entry in filtered_log:
            event_type = entry.get('event_type', 'unknown')
            summary['event_counts'][event_type] = summary['event_counts'].get(event_type, 0) + 1
            
            # Count user activity
            if 'user_id' in entry:
                user_id = entry['user_id']
                summary['user_activity'][user_id] = summary['user_activity'].get(user_id, 0) + 1
            
            # Count quality issues
            if entry.get('event_type') == AuditEventType.QUALITY_ISSUE:
                summary['quality_issues'] += 1
        
        return summary
    
    def export_audit_report(self, output_path: str, format: str = 'json') -> str:
        """
        Export audit report in specified format.
        
        Args:
            output_path: Path for output file
            format: Export format ('json', 'csv', 'html')
            
        Returns:
            Path to exported file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == 'json':
            with open(output_file.with_suffix('.json'), 'w') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
        elif format.lower() == 'csv':
            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(self.audit_log)
            df.to_csv(output_file.with_suffix('.csv'), index=False)
        elif format.lower() == 'html':
            html_content = self._generate_html_report()
            with open(output_file.with_suffix('.html'), 'w') as f:
                f.write(html_content)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Audit report exported: {output_file}")
        return str(output_file)
    
    def _generate_html_report(self) -> str:
        """Generate HTML audit report."""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Audit Log Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .audit-entry { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }
                .event-type { font-weight: bold; color: #333; }
                .timestamp { color: #666; font-size: 0.9em; }
                .details { margin-top: 10px; }
            </style>
        </head>
        <body>
            <h1>Audit Log Report</h1>
        """
        
        for entry in self.audit_log:
            html_content += f"""
            <div class="audit-entry">
                <div class="event-type">Event: {entry.get('event_type', 'unknown')}</div>
                <div class="timestamp">Timestamp: {entry.get('timestamp', 'N/A')}</div>
                <div class="details">Details: {json.dumps(entry.get('details', {}), indent=2)}</div>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        return html_content
