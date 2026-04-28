"""
Live Data Pipeline - Real-time data processing for dashboard analytics
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Third-party libraries
try:
    import aiohttp
    import websockets
    from fastapi import FastAPI, WebSocket
    from fastapi.responses import HTMLResponse
    import uvicorn
except ImportError:
    print("Install additional libraries for live pipeline: pip install aiohttp websockets fastapi uvicorn")

logger = logging.getLogger(__name__)


class LiveDataPipeline:
    """
    Real-time data processing pipeline for live dashboard updates
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the live data pipeline
        
        Args:
            config: Pipeline configuration dictionary
        """
        self.config = config
        self.is_running = False
        self.data_buffer = []
        self.subscribers = []
        self.metrics = {
            'processed_records': 0,
            'errors': 0,
            'last_update': None,
            'throughput': 0
        }
        
    async def start_pipeline(self):
        """Start the live data pipeline"""
        self.is_running = True
        logger.info("Live data pipeline started")
        
        # Run multiple tasks in parallel
        tasks = [
            self.data_ingestion(),
            self.data_processing(),
            self.data_broadcasting(),
            self.monitoring()
        ]
        
        await asyncio.gather(*tasks)
    
    async def stop_pipeline(self):
        """Stop the live data pipeline"""
        self.is_running = False
        logger.info("Live data pipeline stopped")
    
    async def data_ingestion(self):
        """Data ingestion from multiple sources"""
        while self.is_running:
            try:
                # Multiple data sources
                data_sources = [
                    self.fetch_bigquery_data(),
                    self.fetch_azure_data(),
                    self.fetch_api_data(),
                    self.generate_sample_data()
                ]
                
                # Parallel data fetching
                results = await asyncio.gather(*data_sources, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Data fetching error: {result}")
                        self.metrics['errors'] += 1
                    elif result is not None:
                        self.data_buffer.append(result)
                
                await asyncio.sleep(self.config.get('ingestion_interval', 30))
                
            except Exception as e:
                logger.error(f"Ingestion error: {e}")
                await asyncio.sleep(5)
    
    async def data_processing(self):
        """Data processing - Real-time processing"""
        while self.is_running:
            try:
                if self.data_buffer:
                    # Batch processing
                    batch_size = min(100, len(self.data_buffer))
                    batch = self.data_buffer[:batch_size]
                    self.data_buffer = self.data_buffer[batch_size:]
                    
                    processed_data = await self.process_batch(batch)
                    
                    # Store processed data
                    await self.store_processed_data(processed_data)
                    
                    self.metrics['processed_records'] += len(processed_data)
                    self.metrics['last_update'] = datetime.now()
                
                await asyncio.sleep(self.config.get('processing_interval', 10))
                
            except Exception as e:
                logger.error(f"Processing error: {e}")
                await asyncio.sleep(5)
    
    async def data_broadcasting(self):
        """Data broadcasting - Send data to dashboards"""
        while self.is_running:
            try:
                if self.subscribers:
                    latest_data = await self.get_latest_metrics()
                    
                    # Send data to all subscribers
                    for subscriber in self.subscribers:
                        try:
                            await subscriber.send(json.dumps(latest_data, default=str))
                        except Exception as e:
                            logger.warning(f"Error sending data to subscriber: {e}")
                
                await asyncio.sleep(self.config.get('broadcast_interval', 5))
                
            except Exception as e:
                logger.error(f"Broadcasting error: {e}")
                await asyncio.sleep(5)
    
    async def monitoring(self):
        """Monitoring - Pipeline health check"""
        while self.is_running:
            try:
                # Calculate throughput
                if self.metrics['last_update']:
                    time_diff = (datetime.now() - self.metrics['last_update']).total_seconds()
                    if time_diff > 0:
                        self.metrics['throughput'] = self.metrics['processed_records'] / time_diff
                
                # Health check
                health_status = {
                    'status': 'healthy' if self.metrics['errors'] < 10 else 'degraded',
                    'buffer_size': len(self.data_buffer),
                    'subscribers': len(self.subscribers),
                    'uptime': time.time() - getattr(self, 'start_time', time.time())
                }
                
                logger.info(f"Pipeline health: {health_status}")
                
                await asyncio.sleep(self.config.get('monitoring_interval', 60))
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(30)
    
    async def fetch_bigquery_data(self) -> Optional[Dict]:
        """Fetch data from BigQuery"""
        try:
            # Sample implementation
            # In real-time, you can run BigQuery queries
            data = {
                'source': 'bigquery',
                'timestamp': datetime.now(),
                'users': np.random.randint(100, 1000),
                'revenue': np.random.uniform(5000, 50000),
                'conversion_rate': np.random.uniform(0.01, 0.05)
            }
            return data
        except Exception as e:
            logger.error(f"BigQuery error: {e}")
            return None
    
    async def fetch_azure_data(self) -> Optional[Dict]:
        """Fetch data from Azure"""
        try:
            # Sample implementation
            # In real-time, you can fetch data from Azure Blob Storage
            data = {
                'source': 'azure',
                'timestamp': datetime.now(),
                'storage_used': np.random.uniform(100, 1000),  # GB
                'api_calls': np.random.randint(1000, 10000),
                'cost': np.random.uniform(10, 100)  # USD
            }
            return data
        except Exception as e:
            logger.error(f"Azure error: {e}")
            return None
    
    async def fetch_api_data(self) -> Optional[Dict]:
        """Fetch data from API"""
        try:
            # Sample implementation
            # In real-time, you can make external API calls
            data = {
                'source': 'api',
                'timestamp': datetime.now(),
                'active_sessions': np.random.randint(50, 500),
                'server_response_time': np.random.uniform(100, 500),  # ms
                'error_rate': np.random.uniform(0.001, 0.01)
            }
            return data
        except Exception as e:
            logger.error(f"API error: {e}")
            return None
    
    async def generate_sample_data(self) -> Optional[Dict]:
        """Generate sample data"""
        try:
            data = {
                'source': 'sample',
                'timestamp': datetime.now(),
                'random_metric': np.random.uniform(0, 100),
                'trend_value': np.sin(time.time() / 3600) * 50 + 50,
                'category': np.random.choice(['A', 'B', 'C'])
            }
            return data
        except Exception as e:
            logger.error(f"Sample data error: {e}")
            return None
    
    async def process_batch(self, batch: List[Dict]) -> List[Dict]:
        """Batch processing"""
        processed = []
        
        for item in batch:
            try:
                # Data validation
                if self.validate_data(item):
                    # Data transformation
                    processed_item = self.transform_data(item)
                    processed.append(processed_item)
            except Exception as e:
                logger.error(f"Data processing error: {e}")
                self.metrics['errors'] += 1
        
        return processed
    
    def validate_data(self, data: Dict) -> bool:
        """Data validation"""
        required_fields = ['source', 'timestamp', 'users']
        return all(field in data for field in required_fields)
    
    def transform_data(self, data: Dict) -> Dict:
        """Data transformation"""
        # Add additional metrics
        data['processed_at'] = datetime.now()
        data['data_quality_score'] = np.random.uniform(0.8, 1.0)
        
        # Timestamp formatting
        if isinstance(data['timestamp'], datetime):
            data['timestamp'] = data['timestamp'].isoformat()
            
        return data
    
    async def fetch_azure_data(self) -> Optional[Dict]:
        """Fetch data from Azure"""
        try:
            # Sample implementation
            # In real-time, you can fetch data from Azure Blob Storage
            data = {
                'source': 'azure',
                'timestamp': datetime.now(),
                'storage_used': np.random.uniform(100, 1000),  # GB
                'api_calls': np.random.randint(1000, 10000),
                'cost': np.random.uniform(10, 100)  # USD
            }
            return data
        except Exception as e:
            logger.error(f"Azure error: {e}")
            return None
    
    async def fetch_api_data(self) -> Optional[Dict]:
        """Fetch data from API"""
        try:
            # Sample implementation
            # In real-time, you can make external API calls
            data = {
                'source': 'api',
                'timestamp': datetime.now(),
                'active_sessions': np.random.randint(50, 500),
                'server_response_time': np.random.uniform(100, 500),  # ms
                'error_rate': np.random.uniform(0.001, 0.01)
            }
            return data
        except Exception as e:
            logger.error(f"API error: {e}")
            return None
    
    async def generate_sample_data(self) -> Optional[Dict]:
        """Generate sample data"""
        try:
            data = {
                'source': 'sample',
                'timestamp': datetime.now(),
                'random_metric': np.random.uniform(0, 100),
                'trend_value': np.sin(time.time() / 3600) * 50 + 50,
                'category': np.random.choice(['A', 'B', 'C'])
            }
            return data
        except Exception as e:
            logger.error(f"Sample data error: {e}")
            return None
    
    async def store_processed_data(self, data: List[Dict]):
        """Store processed data"""
        try:
            # In real-time, you can store data in Azure/BigQuery
            # Currently just logging
            logger.info(f"{len(data)} records stored")
        except Exception as e:
            logger.error(f"Storage error: {e}")
    
    async def get_latest_metrics(self) -> Dict:
        """Get latest metrics"""
        return {
            'metrics': self.metrics,
            'buffer_size': len(self.data_buffer),
            'subscribers': len(self.subscribers),
            'timestamp': datetime.now().isoformat()
        }
    
    def add_subscriber(self, websocket):
        """Add WebSocket subscriber"""
        self.subscribers.append(websocket)
        logger.info(f"New subscriber added. Total: {len(self.subscribers)}")
    
    def remove_subscriber(self, websocket):
        """Remove WebSocket subscriber"""
        if websocket in self.subscribers:
            self.subscribers.remove(websocket)
            logger.info(f"Subscriber removed. Total: {len(self.subscribers)}")


# FastAPI application
app = FastAPI(title="Live Data Pipeline API")

# Global pipeline instance
pipeline = None

@app.on_event("startup")
async def startup_event():
    """Application startup"""
    global pipeline
    config = {
        'ingestion_interval': 30,
        'processing_interval': 10,
        'broadcast_interval': 5,
        'monitoring_interval': 60
    }
    pipeline = LiveDataPipeline(config)
    
    # Start pipeline in background
    asyncio.create_task(pipeline.start_pipeline())

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown"""
    global pipeline
    if pipeline:
        await pipeline.stop_pipeline()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint"""
    await websocket.accept()
    
    if pipeline:
        pipeline.add_subscriber(websocket)
        
        try:
            while True:
                # Read message from client
                data = await websocket.receive_text()
                logger.info(f"Client message: {data}")
                
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            pipeline.remove_subscriber(websocket)

@app.get("/metrics")
async def get_metrics():
    """Metrics API endpoint"""
    if pipeline:
        return await pipeline.get_latest_metrics()
    return {"error": "Pipeline not running"}

@app.get("/health")
async def health_check():
    """Health check API"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "pipeline_running": pipeline.is_running if pipeline else False
    }

if __name__ == "__main__":
    # Development server
    uvicorn.run(app, host="0.0.0.0", port=8000)
