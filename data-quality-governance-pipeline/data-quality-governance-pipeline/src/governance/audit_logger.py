"""
Audit logging module for data governance and compliance.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import sqlite3
import uuid
from enum import Enum

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Enumeration of audit event types."""
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    DATA_PROCESSING = "data_processing"
    VALIDATION_EXECUTED = "validation_executed"
    QUALITY_CHECK_FAILED = "quality_check_failed"
    SCHEMA_CHANGE = "schema_change"
    TRANSFORMATION_APPLIED = "transformation_applied"
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    PERMISSION_CHANGE = "permission_change"
    SYSTEM_ERROR = "system_error"
    COMPLIANCE_CHECK = "compliance_check"


class AuditLogger:
    """Comprehensive audit logging system for data governance."""
    
    def __init__(self, db_path: str = "audit.db", log_file_path: Optional[str] = None):
        """
        Initialize audit logger.
        
        Args:
            db_path: Path to SQLite database for storing audit logs
            log_file_path: Optional path to text log file
        """
        self.db_path = db_path
        self.log_file_path = log_file_path
        self._initialize_database()
        
        # Configure file logging if path provided
        if log_file_path:
            self._configure_file_logging()
    
    def _initialize_database(self):
        """Initialize the audit database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create audit_logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                event_type TEXT,
                user_id TEXT,
                session_id TEXT,
                resource_type TEXT,
                resource_id TEXT,
                action TEXT,
                details TEXT,
                ip_address TEXT,
                user_agent TEXT,
                success BOOLEAN,
                error_message TEXT
            )
        ''')
        
        # Create audit_summary table for quick reporting
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_summary (
                id TEXT PRIMARY KEY,
                date TEXT,
                event_type TEXT,
                event_count INTEGER,
                success_count INTEGER,
                failure_count INTEGER,
                unique_users INTEGER,
                last_updated TIMESTAMP
            )
        ''')
        
        # Create compliance_events table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS compliance_events (
                id TEXT PRIMARY KEY,
                timestamp TIMESTAMP,
                compliance_type TEXT,
                requirement_id TEXT,
                status TEXT,
                details TEXT,
                evidence TEXT,
                reviewed_by TEXT,
                review_timestamp TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def _configure_file_logging(self):
        """Configure file-based audit logging."""
        audit_handler = logging.FileHandler(self.log_file_path)
        audit_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        audit_handler.setFormatter(formatter)
        
        # Create a dedicated logger for audit events
        self.file_logger = logging.getLogger('audit_file')
        self.file_logger.addHandler(audit_handler)
        self.file_logger.setLevel(logging.INFO)
    
    def log_event(self, event_type: AuditEventType, user_id: Optional[str] = None,
                  session_id: Optional[str] = None, resource_type: Optional[str] = None,
                  resource_id: Optional[str] = None, action: Optional[str] = None,
                  details: Optional[Dict[str, Any]] = None, ip_address: Optional[str] = None,
                  user_agent: Optional[str] = None, success: bool = True,
                  error_message: Optional[str] = None) -> str:
        """
        Log an audit event.
        
        Args:
            event_type: Type of audit event
            user_id: ID of the user performing the action
            session_id: Session identifier
            resource_type: Type of resource being accessed/modified
            resource_id: ID of the specific resource
            action: Action being performed
            details: Additional details about the event
            ip_address: IP address of the user
            user_agent: User agent string
            success: Whether the operation was successful
            error_message: Error message if operation failed
            
        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        # Create log entry
        log_entry = {
            'id': event_id,
            'timestamp': timestamp,
            'event_type': event_type.value,
            'user_id': user_id,
            'session_id': session_id,
            'resource_type': resource_type,
            'resource_id': resource_id,
            'action': action,
            'details': json.dumps(details) if details else None,
            'ip_address': ip_address,
            'user_agent': user_agent,
            'success': success,
            'error_message': error_message
        }
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO audit_logs 
            (id, timestamp, event_type, user_id, session_id, resource_type, 
             resource_id, action, details, ip_address, user_agent, success, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event_id, timestamp, event_type.value, user_id, session_id,
            resource_type, resource_id, action, json.dumps(details) if details else None,
            ip_address, user_agent, success, error_message
        ))
        
        conn.commit()
        conn.close()
        
        # Log to file if configured
        if hasattr(self, 'file_logger'):
            log_message = f"{event_type.value}: {action} by {user_id} on {resource_type}:{resource_id}"
            if not success:
                log_message += f" - FAILED: {error_message}"
            self.file_logger.info(log_message)
        
        # Update summary statistics
        self._update_summary_stats(event_type.value, success)
        
        logger.info(f"Audit event logged: {event_type.value} - {action}")
        return event_id
    
    def log_data_access(self, user_id: str, dataset_id: str, access_type: str,
                       details: Optional[Dict[str, Any]] = None, **kwargs) -> str:
        """
        Log data access event.
        
        Args:
            user_id: ID of the user accessing data
            dataset_id: ID of the dataset being accessed
            access_type: Type of access (read, write, delete)
            details: Additional details
            **kwargs: Additional parameters for log_event
            
        Returns:
            Event ID
        """
        return self.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            user_id=user_id,
            resource_type="dataset",
            resource_id=dataset_id,
            action=f"access_{access_type}",
            details=details,
            **kwargs
        )
    
    def log_data_processing(self, user_id: str, pipeline_id: str, processing_type: str,
                          details: Optional[Dict[str, Any]] = None, **kwargs) -> str:
        """
        Log data processing event.
        
        Args:
            user_id: ID of the user initiating processing
            pipeline_id: ID of the pipeline being executed
            processing_type: Type of processing (validation, transformation, etc.)
            details: Additional details
            **kwargs: Additional parameters for log_event
            
        Returns:
            Event ID
        """
        return self.log_event(
            event_type=AuditEventType.DATA_PROCESSING,
            user_id=user_id,
            resource_type="pipeline",
            resource_id=pipeline_id,
            action=f"process_{processing_type}",
            details=details,
            **kwargs
        )
    
    def log_validation_execution(self, user_id: str, validation_id: str, dataset_id: str,
                                validation_results: Dict[str, Any], **kwargs) -> str:
        """
        Log validation execution event.
        
        Args:
            user_id: ID of the user running validation
            validation_id: ID of the validation rule
            dataset_id: ID of the dataset being validated
            validation_results: Results of the validation
            **kwargs: Additional parameters for log_event
            
        Returns:
            Event ID
        """
        success = validation_results.get('is_valid', True)
        
        return self.log_event(
            event_type=AuditEventType.VALIDATION_EXECUTED,
            user_id=user_id,
            resource_type="validation",
            resource_id=validation_id,
            action="execute_validation",
            details={
                'dataset_id': dataset_id,
                'validation_results': validation_results
            },
            success=success,
            error_message=None if success else "Validation failed",
            **kwargs
        )
    
    def log_quality_check(self, user_id: str, check_type: str, dataset_id: str,
                         quality_metrics: Dict[str, Any], **kwargs) -> str:
        """
        Log quality check event.
        
        Args:
            user_id: ID of the user running quality check
            check_type: Type of quality check
            dataset_id: ID of the dataset being checked
            quality_metrics: Quality metrics results
            **kwargs: Additional parameters for log_event
            
        Returns:
            Event ID
        """
        # Determine if quality check passed based on overall score
        overall_score = quality_metrics.get('overall_quality_score', {}).get('overall_quality_score', 1.0)
        success = overall_score >= 0.8  # 80% threshold
        
        event_type = AuditEventType.QUALITY_CHECK_FAILED if not success else AuditEventType.DATA_PROCESSING
        
        return self.log_event(
            event_type=event_type,
            user_id=user_id,
            resource_type="quality_check",
            resource_id=f"{check_type}_{dataset_id}",
            action=f"quality_check_{check_type}",
            details={
                'dataset_id': dataset_id,
                'quality_metrics': quality_metrics
            },
            success=success,
            error_message=None if success else f"Quality score {overall_score:.3f} below threshold",
            **kwargs
        )
    
    def log_transformation(self, user_id: str, transformation_id: str, input_dataset_id: str,
                          output_dataset_id: str, transformation_details: Dict[str, Any], **kwargs) -> str:
        """
        Log data transformation event.
        
        Args:
            user_id: ID of the user applying transformation
            transformation_id: ID of the transformation
            input_dataset_id: ID of the input dataset
            output_dataset_id: ID of the output dataset
            transformation_details: Details about the transformation
            **kwargs: Additional parameters for log_event
            
        Returns:
            Event ID
        """
        return self.log_event(
            event_type=AuditEventType.TRANSFORMATION_APPLIED,
            user_id=user_id,
            resource_type="transformation",
            resource_id=transformation_id,
            action="apply_transformation",
            details={
                'input_dataset_id': input_dataset_id,
                'output_dataset_id': output_dataset_id,
                'transformation_details': transformation_details
            },
            **kwargs
        )
    
    def log_compliance_event(self, compliance_type: str, requirement_id: str,
                           status: str, details: Dict[str, Any],
                           evidence: Optional[Dict[str, Any]] = None,
                           reviewed_by: Optional[str] = None) -> str:
        """
        Log compliance-related event.
        
        Args:
            compliance_type: Type of compliance (GDPR, HIPAA, SOX, etc.)
            requirement_id: ID of the compliance requirement
            status: Status of compliance (compliant, non_compliant, pending)
            details: Details about the compliance check
            evidence: Evidence supporting the compliance status
            reviewed_by: Person who reviewed the compliance
            
        Returns:
            Event ID
        """
        event_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO compliance_events 
            (id, timestamp, compliance_type, requirement_id, status, details, evidence, reviewed_by, review_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event_id, timestamp, compliance_type, requirement_id, status,
            json.dumps(details), json.dumps(evidence) if evidence else None,
            reviewed_by, timestamp if reviewed_by else None
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Compliance event logged: {compliance_type} - {requirement_id} - {status}")
        return event_id
    
    def _update_summary_stats(self, event_type: str, success: bool):
        """Update daily summary statistics."""
        today = datetime.now().strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if summary exists for today
        cursor.execute('''
            SELECT event_count, success_count, failure_count, unique_users
            FROM audit_summary 
            WHERE date = ? AND event_type = ?
        ''', (today, event_type))
        
        result = cursor.fetchone()
        
        if result:
            # Update existing summary
            event_count, success_count, failure_count, unique_users = result
            
            new_event_count = event_count + 1
            new_success_count = success_count + (1 if success else 0)
            new_failure_count = failure_count + (0 if success else 1)
            
            cursor.execute('''
                UPDATE audit_summary 
                SET event_count = ?, success_count = ?, failure_count = ?, last_updated = ?
                WHERE date = ? AND event_type = ?
            ''', (new_event_count, new_success_count, new_failure_count, datetime.now().isoformat(), today, event_type))
        else:
            # Create new summary
            cursor.execute('''
                INSERT INTO audit_summary 
                (id, date, event_type, event_count, success_count, failure_count, unique_users, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(uuid.uuid4()), today, event_type, 1, 1 if success else 0, 0 if success else 1,
                1, datetime.now().isoformat()
            ))
        
        conn.commit()
        conn.close()
    
    def get_audit_trail(self, resource_id: Optional[str] = None, resource_type: Optional[str] = None,
                       user_id: Optional[str] = None, event_type: Optional[str] = None,
                       start_date: Optional[str] = None, end_date: Optional[str] = None,
                       limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve audit trail with filtering options.
        
        Args:
            resource_id: Filter by specific resource ID
            resource_type: Filter by resource type
            user_id: Filter by user ID
            event_type: Filter by event type
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            limit: Maximum number of records to return
            
        Returns:
            List of audit events
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with filters
        query = "SELECT * FROM audit_logs WHERE 1=1"
        params = []
        
        if resource_id:
            query += " AND resource_id = ?"
            params.append(resource_id)
        
        if resource_type:
            query += " AND resource_type = ?"
            params.append(resource_type)
        
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(f"{start_date} 00:00:00")
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(f"{end_date} 23:59:59")
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        conn.close()
        
        return [
            {
                'id': row[0],
                'timestamp': row[1],
                'event_type': row[2],
                'user_id': row[3],
                'session_id': row[4],
                'resource_type': row[5],
                'resource_id': row[6],
                'action': row[7],
                'details': json.loads(row[8]) if row[8] else None,
                'ip_address': row[9],
                'user_agent': row[10],
                'success': bool(row[11]),
                'error_message': row[12]
            }
            for row in results
        ]
    
    def get_compliance_report(self, compliance_type: Optional[str] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Generate compliance report.
        
        Args:
            compliance_type: Filter by compliance type
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            List of compliance events
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM compliance_events WHERE 1=1"
        params = []
        
        if compliance_type:
            query += " AND compliance_type = ?"
            params.append(compliance_type)
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(f"{start_date} 00:00:00")
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(f"{end_date} 23:59:59")
        
        query += " ORDER BY timestamp DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        conn.close()
        
        return [
            {
                'id': row[0],
                'timestamp': row[1],
                'compliance_type': row[2],
                'requirement_id': row[3],
                'status': row[4],
                'details': json.loads(row[5]) if row[5] else None,
                'evidence': json.loads(row[6]) if row[6] else None,
                'reviewed_by': row[7],
                'review_timestamp': row[8]
            }
            for row in results
        ]
    
    def get_activity_summary(self, start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get activity summary statistics.
        
        Args:
            start_date: Start date for summary
            end_date: End date for summary
            
        Returns:
            Dictionary containing activity summary
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM audit_summary WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        conn.close()
        
        summary = {
            'total_events': 0,
            'successful_events': 0,
            'failed_events': 0,
            'unique_users': set(),
            'event_types': {},
            'daily_breakdown': {}
        }
        
        for row in results:
            event_count, success_count, failure_count, unique_users = row[4], row[5], row[6], row[7]
            event_type = row[2]
            date = row[1]
            
            summary['total_events'] += event_count
            summary['successful_events'] += success_count
            summary['failed_events'] += failure_count
            summary['unique_users'].add(unique_users)
            
            if event_type not in summary['event_types']:
                summary['event_types'][event_type] = {
                    'total': 0, 'success': 0, 'failure': 0
                }
            
            summary['event_types'][event_type]['total'] += event_count
            summary['event_types'][event_type]['success'] += success_count
            summary['event_types'][event_type]['failure'] += failure_count
            
            summary['daily_breakdown'][date] = {
                'total': event_count,
                'success': success_count,
                'failure': failure_count
            }
        
        summary['unique_users'] = len(summary['unique_users'])
        
        return summary
    
    def export_audit_logs(self, output_path: str, format_type: str = 'json',
                         filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Export audit logs to file.
        
        Args:
            output_path: Path to save the export
            format_type: Export format ('json', 'csv')
            filters: Optional filters to apply
        """
        filters = filters or {}
        
        # Get audit logs with filters
        logs = self.get_audit_trail(**filters)
        
        if format_type.lower() == 'json':
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'total_records': len(logs),
                'filters_applied': filters,
                'audit_logs': logs
            }
            
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
        
        elif format_type.lower() == 'csv':
            import pandas as pd
            
            if logs:
                df = pd.DataFrame(logs)
                df.to_csv(output_path, index=False)
            else:
                # Create empty CSV with headers
                pd.DataFrame(columns=['id', 'timestamp', 'event_type', 'user_id', 'session_id',
                                   'resource_type', 'resource_id', 'action', 'details',
                                   'ip_address', 'user_agent', 'success', 'error_message']).to_csv(output_path, index=False)
        
        logger.info(f"Audit logs exported to {output_path} in {format_type} format")
    
    def cleanup_old_logs(self, retention_days: int = 90) -> int:
        """
        Clean up old audit logs based on retention policy.
        
        Args:
            retention_days: Number of days to retain logs
            
        Returns:
            Number of records deleted
        """
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d %H:%M:%S')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Delete old audit logs
        cursor.execute('DELETE FROM audit_logs WHERE timestamp < ?', (cutoff_date,))
        deleted_count = cursor.rowcount
        
        # Delete old compliance events
        cursor.execute('DELETE FROM compliance_events WHERE timestamp < ?', (cutoff_date,))
        deleted_count += cursor.rowcount
        
        # Delete old summary records
        cursor.execute('DELETE FROM audit_summary WHERE date < ?', (cutoff_date.split()[0],))
        deleted_count += cursor.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"Cleaned up {deleted_count} old audit records older than {retention_days} days")
        return deleted_count
