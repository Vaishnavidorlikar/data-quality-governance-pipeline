#!/usr/bin/env python3
"""
Simple Dashboard Creation Script
Creates basic Cloud Monitoring dashboards using gcloud commands
"""

import os
import subprocess
from pathlib import Path

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

def run_command(command):
    """Run shell command and return result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Success: {command}")
            return True, result.stdout
        else:
            print(f"❌ Error: {command}")
            print(f"   {result.stderr}")
            return False, result.stderr
    except Exception as e:
        print(f"❌ Exception: {command} - {e}")
        return False, str(e)

def create_simple_dashboard(project_id, dashboard_name, metrics):
    """Create a simple dashboard using gcloud commands"""
    print(f"\n📊 Creating dashboard: {dashboard_name}")
    
    # Create a simple chart for each metric
    for i, metric in enumerate(metrics):
        chart_name = f"{dashboard_name}_chart_{i+1}"
        
        # Create a simple monitoring chart
        command = f"""
        gcloud monitoring dashboards create --project={project_id} --config-from-file=-
        """
        
        config = f"""
        displayName: {dashboard_name}
        gridLayout:
          columns: "2"
          widgets:
          - title: {metric['title']}
            xyChart:
              dataSets:
              - timeSeriesQuery:
                  prometheusQuerySource:
                    prometheusQuery: "{metric['query']}"
                plotType: LINE
              timeshiftDuration: "0s"
              yAxis:
                scale: LINEAR
        """
        
        # Write config to temp file and create dashboard
        temp_file = f"/tmp/{chart_name}.yaml"
        with open(temp_file, 'w') as f:
            f.write(config)
        
        success, output = run_command(f"gcloud monitoring dashboards create --project={project_id} --config-file={temp_file}")
        
        # Clean up temp file
        os.remove(temp_file)
        
        if success:
            print(f"   ✅ Created chart: {metric['title']}")
        else:
            print(f"   ❌ Failed to create chart: {metric['title']}")

def main():
    """Main function to create all dashboards"""
    # Load environment variables
    load_env()
    
    project_id = os.getenv('GCP_PROJECT_ID', 'leafy-tractor-277020')
    
    print(f"🚀 Creating Cloud Monitoring dashboards for project: {project_id}")
    print("=" * 60)
    
    # Define dashboard configurations
    dashboards = [
        {
            "name": "Real-time Streaming Pipeline",
            "metrics": [
                {
                    "title": "Pub/Sub Message Count",
                    "query": "pubsub.googleapis.com/topic/message_count"
                },
                {
                    "title": "Pub/Sub Publish Request Count", 
                    "query": "pubsub.googleapis.com/topic/publish_request_count"
                }
            ]
        },
        {
            "name": "AI Automation Workflows",
            "metrics": [
                {
                    "title": "API Request Count",
                    "query": "serviceruntime.googleapis.com/api/request_count"
                },
                {
                    "title": "API Response Latency",
                    "query": "serviceruntime.googleapis.com/api/request_latencies"
                }
            ]
        },
        {
            "name": "Data Quality Governance",
            "metrics": [
                {
                    "title": "BigQuery Query Count",
                    "query": "bigquery.googleapis.com/query/count"
                },
                {
                    "title": "BigQuery Execution Time",
                    "query": "bigquery.googleapis.com/query/execution_time"
                }
            ]
        },
        {
            "name": "Data Lake Migration",
            "metrics": [
                {
                    "title": "Cloud Storage Request Count",
                    "query": "storage.googleapis.com/api/request_count"
                },
                {
                    "title": "Cloud Storage Bytes",
                    "query": "storage.googleapis.com/api/network/received_bytes_count"
                }
            ]
        }
    ]
    
    # Create each dashboard
    for dashboard in dashboards:
        create_simple_dashboard(project_id, dashboard["name"], dashboard["metrics"])
    
    print("=" * 60)
    print("✅ Dashboard creation completed!")
    
    # Provide dashboard URLs
    dashboard_url = f"https://console.cloud.google.com/monitoring/dashboards?project={project_id}"
    print(f"\n📊 Access your dashboards at: {dashboard_url}")
    
    # Show existing dashboards
    print("\n📋 Current dashboards:")
    success, output = run_command(f"gcloud monitoring dashboards list --project={project_id}")
    if success:
        print(output)

if __name__ == "__main__":
    main()
