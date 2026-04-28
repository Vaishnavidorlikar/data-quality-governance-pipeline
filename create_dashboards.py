#!/usr/bin/env python3
"""
Automated Dashboard Creation Script
Creates Cloud Monitoring dashboards for all projects
"""

import os
import json
from pathlib import Path
from google.cloud import monitoring_dashboard_v1

def load_env():
    """Load environment variables from .env file"""
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

def create_realtime_dashboard(project_id):
    """Create real-time pipeline dashboard"""
    client = monitoring_dashboard_v1.DashboardsServiceClient()
    
    dashboard_config = {
        "displayName": "Real-time Streaming Pipeline",
        "gridLayout": {
            "columns": "2",
            "widgets": [
                {
                    "title": "Pub/Sub Message Rate",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "pubsub.googleapis.com/topic/message_count"
                                }
                            },
                            "plotType": "LINE",
                            "legendTemplate": "Messages/sec"
                        }],
                        "timeshiftDuration": "0s",
                        "yAxis": {"scale": "LINEAR"}
                    }
                },
                {
                    "title": "Publish Request Count",
                    "scorecard": {
                        "gaugeView": {"upperBound": 100, "lowerBound": 0},
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "pubsub.googleapis.com/topic/publish_request_count"
                                }
                            }
                        }]
                    }
                }
            ]
        }
    }
    
    # Create dashboard
    parent = f"projects/{project_id}"
    dashboard = monitoring_dashboard_v1.Dashboard(**dashboard_config)
    
    try:
        response = client.create_dashboard(request={"parent": parent, "dashboard": dashboard})
        print(f"✅ Created Real-time Dashboard: {response.name}")
        return response
    except Exception as e:
        print(f"❌ Error creating real-time dashboard: {e}")
        return None

def create_automation_dashboard(project_id):
    """Create AI automation dashboard"""
    client = monitoring_dashboard_v1.DashboardsServiceClient()
    
    dashboard_config = {
        "displayName": "AI Automation Workflows",
        "gridLayout": {
            "columns": "2",
            "widgets": [
                {
                    "title": "API Request Count",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "serviceruntime.googleapis.com/api/request_count"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                },
                {
                    "title": "API Response Latency",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "serviceruntime.googleapis.com/api/request_latencies"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                }
            ]
        }
    }
    
    parent = f"projects/{project_id}"
    dashboard = monitoring_dashboard_v1.Dashboard(**dashboard_config)
    
    try:
        response = client.create_dashboard(request={"parent": parent, "dashboard": dashboard})
        print(f"✅ Created Automation Dashboard: {response.name}")
        return response
    except Exception as e:
        print(f"❌ Error creating automation dashboard: {e}")
        return None

def create_quality_dashboard(project_id):
    """Create data quality dashboard"""
    client = monitoring_dashboard_v1.DashboardsServiceClient()
    
    dashboard_config = {
        "displayName": "Data Quality Governance",
        "gridLayout": {
            "columns": "2",
            "widgets": [
                {
                    "title": "BigQuery Query Count",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "bigquery.googleapis.com/query/count"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                },
                {
                    "title": "BigQuery Query Execution Time",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "bigquery.googleapis.com/query/execution_time"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                }
            ]
        }
    }
    
    parent = f"projects/{project_id}"
    dashboard = monitoring_dashboard_v1.Dashboard(**dashboard_config)
    
    try:
        response = client.create_dashboard(request={"parent": parent, "dashboard": dashboard})
        print(f"✅ Created Quality Dashboard: {response.name}")
        return response
    except Exception as e:
        print(f"❌ Error creating quality dashboard: {e}")
        return None

def create_migration_dashboard(project_id):
    """Create data lake migration dashboard"""
    client = monitoring_dashboard_v1.DashboardsServiceClient()
    
    dashboard_config = {
        "displayName": "Data Lake Migration",
        "gridLayout": {
            "columns": "2",
            "widgets": [
                {
                    "title": "Cloud Storage Request Count",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "storage.googleapis.com/api/request_count"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                },
                {
                    "title": "Cloud Storage Bytes Downloaded",
                    "xyChart": {
                        "dataSets": [{
                            "timeSeriesQuery": {
                                "prometheusQuerySource": {
                                    "prometheusQuery": "storage.googleapis.com/api/network/received_bytes_count"
                                }
                            },
                            "plotType": "LINE"
                        }],
                        "yAxis": {"scale": "LINEAR"}
                    }
                }
            ]
        }
    }
    
    parent = f"projects/{project_id}"
    dashboard = monitoring_dashboard_v1.Dashboard(**dashboard_config)
    
    try:
        response = client.create_dashboard(request={"parent": parent, "dashboard": dashboard})
        print(f"✅ Created Migration Dashboard: {response.name}")
        return response
    except Exception as e:
        print(f"❌ Error creating migration dashboard: {e}")
        return None

def main():
    """Main function to create all dashboards"""
    # Load environment variables
    load_env()
    
    project_id = os.getenv('GCP_PROJECT_ID', 'leafy-tractor-277020')
    
    print(f"🚀 Creating Cloud Monitoring dashboards for project: {project_id}")
    print("=" * 60)
    
    # Create dashboards
    dashboards = []
    
    # Real-time Pipeline Dashboard
    realtime = create_realtime_dashboard(project_id)
    if realtime:
        dashboards.append(realtime)
    
    # AI Automation Dashboard
    automation = create_automation_dashboard(project_id)
    if automation:
        dashboards.append(automation)
    
    # Data Quality Dashboard
    quality = create_quality_dashboard(project_id)
    if quality:
        dashboards.append(quality)
    
    # Data Lake Migration Dashboard
    migration = create_migration_dashboard(project_id)
    if migration:
        dashboards.append(migration)
    
    print("=" * 60)
    print(f"✅ Successfully created {len(dashboards)} dashboards")
    
    # Provide dashboard URLs
    print("\n📊 Dashboard URLs:")
    base_url = f"https://console.cloud.google.com/monitoring/dashboards?project={project_id}"
    print(f"Main Dashboard List: {base_url}")
    
    for i, dashboard in enumerate(dashboards):
        dashboard_id = dashboard.name.split('/')[-1]
        print(f"Dashboard {i+1}: {base_url}&dashboard={dashboard_id}")
    
    print(f"\n🎯 Access your dashboards at: {base_url}")

if __name__ == "__main__":
    main()
