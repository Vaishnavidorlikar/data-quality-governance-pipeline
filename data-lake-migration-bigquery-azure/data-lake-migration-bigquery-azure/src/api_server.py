"""
API Server - Data API for dashboards
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncio
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Local imports
from .live_pipeline import LiveDataPipeline

logger = logging.getLogger(__name__)

# API models
class DataSource(BaseModel):
    name: str
    type: str
    config: Dict[str, Any]

class DashboardRequest(BaseModel):
    dashboard_id: str
    filters: Optional[Dict[str, Any]] = {}
    time_range: Optional[str] = "24h"

class AlertConfig(BaseModel):
    metric: str
    threshold: float
    operator: str
    notification_type: str

# FastAPI application
app = FastAPI(
    title="Data Lake Migration API",
    description="Live data migration analytics API",
    version="1.0.0"
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Set specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
pipeline_instance = None
dashboard_cache = {}
alert_rules = []

@app.on_event("startup")
async def startup_event():
    """API server startup"""
    global pipeline_instance
    
    # Initialize live pipeline
    config = {
        'ingestion_interval': 30,
        'processing_interval': 10,
        'broadcast_interval': 5,
        'monitoring_interval': 60
    }
    
    pipeline_instance = LiveDataPipeline(config)
    
    # Start pipeline in background
    asyncio.create_task(pipeline_instance.start_pipeline())
    
    logger.info(" API server and live pipeline started")

@app.on_event("shutdown")
async def shutdown_event():
    """API server shutdown"""
    global pipeline_instance
    if pipeline_instance:
        await pipeline_instance.stop_pipeline()
        logger.info(" API server and live pipeline stopped")

# Dashboard API endpoints

@app.get("/api/v1/dashboard/overview")
async def get_dashboard_overview():
    """Overview dashboard data"""
    try:
        if not pipeline_instance:
            raise HTTPException(status_code=503, detail="Pipeline not available")
        
        # Get latest metrics
        metrics = await pipeline_instance.get_latest_metrics()
        
        # Prepare dashboard data
        dashboard_data = {
            "dashboard_id": "overview",
            "title": " Data Lake Migration Overview",
            "last_updated": metrics['timestamp'],
            "metrics": {
                "total_processed": metrics['metrics']['processed_records'],
                "error_rate": metrics['metrics']['errors'],
                "throughput": metrics['metrics']['throughput'],
                "buffer_size": metrics['buffer_size']
            },
            "charts": [
                {
                    "id": "processing_trend",
                    "type": "line",
                    "data": generate_trend_data(),
                    "title": "Processing Trend"
                },
                {
                    "id": "source_distribution",
                    "type": "pie",
                    "data": generate_source_data(),
                    "title": "Data Source Distribution"
                },
                {
                    "id": "performance_metrics",
                    "type": "gauge",
                    "data": metrics['metrics'],
                    "title": "Performance Metrics"
                }
            ],
            "alerts": get_active_alerts()
        }
        
        return JSONResponse(content=dashboard_data)
        
    except Exception as e:
        logger.error(f"Overview dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching dashboard data")

@app.get("/api/v1/dashboard/performance")
async def get_performance_dashboard():
    """Performance dashboard data"""
    try:
        # Performance metrics
        performance_data = {
            "dashboard_id": "performance",
            "title": "System Performance",
            "last_updated": datetime.now().isoformat(),
            "metrics": {
                "cpu_usage": np.random.uniform(20, 80),
                "memory_usage": np.random.uniform(30, 70),
                "disk_io": np.random.uniform(10, 100),
                "network_io": np.random.uniform(5, 50)
            },
            "charts": [
                {
                    "id": "cpu_timeline",
                    "type": "area",
                    "data": generate_timeline_data("cpu", 24),
                    "title": " CPU Utilization"
                },
                {
                    "id": "memory_timeline",
                    "type": "area",
                    "data": generate_timeline_data("memory", 24),
                    "title": " Memory Utilization"
                },
                {
                    "id": "throughput_chart",
                    "type": "bar",
                    "data": generate_throughput_data(),
                    "title": " Throughput Metrics"
                }
            ]
        }
        
        return JSONResponse(content=performance_data)
        
    except Exception as e:
        logger.error(f"Performance dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching performance data")

@app.get("/api/v1/dashboard/data-quality")
async def get_data_quality_dashboard():
    """Data quality dashboard"""
    try:
        quality_data = {
            "dashboard_id": "data_quality",
            "title": " Data Quality Analytics",
            "last_updated": datetime.now().isoformat(),
            "metrics": {
                "valid_records": np.random.randint(8000, 10000),
                "invalid_records": np.random.randint(0, 500),
                "duplicate_records": np.random.randint(0, 100),
                "missing_values": np.random.randint(50, 200)
            },
            "charts": [
                {
                    "id": "quality_trend",
                    "type": "line",
                    "data": generate_quality_trend(),
                    "title": " Data Quality Trend"
                },
                {
                    "id": "error_breakdown",
                    "type": "donut",
                    "data": generate_error_breakdown(),
                    "title": " Error Breakdown"
                }
            ]
        }
        
        return JSONResponse(content=quality_data)
        
    except Exception as e:
        logger.error(f"Data quality dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data quality data")

# Real-time data endpoints

@app.get("/api/v1/live/metrics")
async def get_live_metrics():
    """Real-time metrics"""
    try:
        if pipeline_instance:
            return await pipeline_instance.get_latest_metrics()
        else:
            return {"error": "Pipeline not running"}
    except Exception as e:
        logger.error(f"Live metrics error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching metrics")

@app.get("/api/v1/live/status")
async def get_pipeline_status():
    """Pipeline status"""
    try:
        status = {
            "pipeline_running": pipeline_instance.is_running if pipeline_instance else False,
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - getattr(pipeline_instance, 'start_time', time.time()) if pipeline_instance else 0,
            "buffer_size": len(pipeline_instance.data_buffer) if pipeline_instance else 0,
            "subscribers": len(pipeline_instance.subscribers) if pipeline_instance else 0
        }
        return status
    except Exception as e:
        logger.error(f"Status error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching status")

# Alert endpoints

@app.post("/api/v1/alerts/configure")
async def configure_alert(alert_config: AlertConfig):
    """Configure alert"""
    try:
        alert_rule = {
            "id": len(alert_rules) + 1,
            "metric": alert_config.metric,
            "threshold": alert_config.threshold,
            "operator": alert_config.operator,
            "notification_type": alert_config.notification_type,
            "enabled": True,
            "created_at": datetime.now().isoformat()
        }
        
        alert_rules.append(alert_rule)
        logger.info(f"New alert rule configured: {alert_rule}")
        
        return {"message": "Alert configured successfully", "alert_id": alert_rule["id"]}
        
    except Exception as e:
        logger.error(f"Alert configuration error: {e}")
        raise HTTPException(status_code=500, detail="Error configuring alert")

@app.get("/api/v1/alerts")
async def get_alerts():
    """Get alerts"""
    try:
        active_alerts = get_active_alerts()
        return {
            "total_alerts": len(alert_rules),
            "active_alerts": len(active_alerts),
            "alerts": active_alerts
        }
    except Exception as e:
        logger.error(f"Alerts error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching alerts")

# Analytics endpoints

@app.get("/api/v1/analytics/trends")
async def get_analytics_trends(time_range: str = "24h"):
    """Analytics trends"""
    try:
        trends_data = {
            "time_range": time_range,
            "data_points": generate_trend_data(),
            "summary": {
                "trend_direction": "upward",
                "growth_rate": "+15.3%",
                "peak_value": 1250,
                "average_value": 890
            }
        }
        return trends_data
    except Exception as e:
        logger.error(f"Trends error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching trends")

@app.get("/api/v1/analytics/summary")
async def get_analytics_summary():
    """Analytics summary"""
    try:
        summary = {
            "period": "last_24_hours",
            "key_metrics": {
                "total_records_processed": np.random.randint(50000, 100000),
                "data_volume_gb": np.random.uniform(10, 50),
                "processing_time_avg": np.random.uniform(2.5, 5.0),
                "success_rate": np.random.uniform(95, 99.5)
            },
            "top_sources": [
                {"source": "BigQuery", "percentage": 45},
                {"source": "Azure Storage", "percentage": 30},
                {"source": "API", "percentage": 25}
            ],
            "performance_indicators": {
                "throughput": "good",
                "latency": "excellent",
                "error_rate": "low"
            }
        }
        return summary
    except Exception as e:
        logger.error(f"Summary error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching summary")

# Utility functions

def generate_trend_data():
    """Generate trend data"""
    import time
    current_time = time.time()
    data = []
    
    for i in range(24):  # 24 hours of data
        timestamp = current_time - (23 - i) * 3600
        value = np.random.randint(800, 1200) + np.sin(i / 4) * 100
        data.append({
            "timestamp": datetime.fromtimestamp(timestamp).isoformat(),
            "value": value
        })
    
    return data

def generate_source_data():
    """Generate source data"""
    return [
        {"source": "BigQuery", "value": 45, "color": "#4285F4"},
        {"source": "Azure Storage", "value": 30, "color": "#0078D4"},
        {"source": "API", "value": 25, "color": "#FF6B6B"}
    ]

def generate_timeline_data(metric_type: str, hours: int):
    """Generate timeline data"""
    data = []
    current_time = datetime.now()
    
    for i in range(hours):
        date = current_time - timedelta(hours=hours - i - 1)
        if metric_type == "cpu":
            value = np.random.uniform(20, 80)
        elif metric_type == "memory":
            value = np.random.uniform(30, 70)
        else:
            value = np.random.uniform(10, 100)
        
        data.append({
            "timestamp": date.isoformat(),
            "value": value
        })
    
    return data

def generate_throughput_data():
    """Generate throughput data"""
    return [
        {"period": "00:00", "records": 850},
        {"period": "04:00", "records": 620},
        {"period": "08:00", "records": 1200},
        {"period": "12:00", "records": 980},
        {"period": "16:00", "records": 1100},
        {"period": "20:00", "records": 750}
    ]

def generate_quality_trend():
    """Generate quality trend data"""
    data = []
    for i in range(7):  # 7 days
        date = datetime.now() - timedelta(days=6 - i)
        quality_score = np.random.uniform(85, 99)
        data.append({
            "date": date.strftime("%Y-%m-%d"),
            "quality_score": quality_score
        })
    return data

def generate_error_breakdown():
    """Generate error breakdown data"""
    return [
        {"error_type": "Missing Values", "count": 45},
        {"error_type": "Invalid Format", "count": 23},
        {"error_type": "Duplicates", "count": 12},
        {"error_type": "Other", "count": 8}
    ]

def get_active_alerts():
    """Get active alerts"""
    active_alerts = []
    
    for alert in alert_rules:
        if alert.get("enabled", False):
            # Simulated alert check
            if np.random.random() > 0.8:  # 20% chance alert is active
                active_alerts.append({
                    "id": alert["id"],
                    "metric": alert["metric"],
                    "message": f"Alert: {alert['metric']} threshold exceeded",
                    "severity": "warning",
                    "timestamp": datetime.now().isoformat()
                })
    
    return active_alerts

# Application runner
if __name__ == "__main__":
    import uvicorn
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run server
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
