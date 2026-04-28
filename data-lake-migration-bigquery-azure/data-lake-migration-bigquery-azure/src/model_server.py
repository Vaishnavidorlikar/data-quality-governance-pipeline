"""
Model Server API for serving ML predictions
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging
import asyncio
import json
import pandas as pd
from datetime import datetime
import os

from .models.house_price_models import HousePricePredictor

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="House Price Prediction API",
    description="AI-powered house price prediction service",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
predictor = None
model_loaded = False


# Pydantic models for API
class HouseFeatures(BaseModel):
    """House features for prediction"""
    OverallQual: int = Field(..., ge=1, le=10, description="Overall material and finish quality")
    GrLivArea: int = Field(..., ge=0, description="Above ground living area in square feet")
    TotalBsmtSF: int = Field(..., ge=0, description="Total square feet of basement area")
    FullBath: int = Field(..., ge=0, description="Number of full bathrooms")
    HalfBath: int = Field(..., ge=0, description="Number of half bathrooms")
    TotRmsAbvGrd: int = Field(..., ge=0, description="Total rooms above ground")
    YearBuilt: int = Field(..., ge=1800, le=2024, description="Original construction year")
    YearRemodAdd: int = Field(..., ge=1800, le=2024, description="Remodel date")
    GarageCars: float = Field(..., ge=0, description="Size of garage in car capacity")
    GarageArea: int = Field(..., ge=0, description="Size of garage in square feet")
    Neighborhood: str = Field(..., description="Neighborhood name")
    
    class Config:
        schema_extra = {
            "example": {
                "OverallQual": 7,
                "GrLivArea": 1800,
                "TotalBsmtSF": 1200,
                "FullBath": 2,
                "HalfBath": 1,
                "TotRmsAbvGrd": 7,
                "YearBuilt": 2005,
                "YearRemodAdd": 2010,
                "GarageCars": 2,
                "GarageArea": 500,
                "Neighborhood": "NAmes"
            }
        }


class PredictionRequest(BaseModel):
    """Prediction request with multiple houses"""
    houses: List[HouseFeatures] = Field(..., description="List of houses to predict")
    
    class Config:
        schema_extra = {
            "example": {
                "houses": [
                    {
                        "OverallQual": 7,
                        "GrLivArea": 1800,
                        "TotalBsmtSF": 1200,
                        "FullBath": 2,
                        "HalfBath": 1,
                        "TotRmsAbvGrd": 7,
                        "YearBuilt": 2005,
                        "YearRemodAdd": 2010,
                        "GarageCars": 2,
                        "GarageArea": 500,
                        "Neighborhood": "NAmes"
                    }
                ]
            }
        }


class PredictionResponse(BaseModel):
    """Prediction response"""
    predictions: List[Dict[str, Any]] = Field(..., description="Prediction results")
    model_info: Dict[str, Any] = Field(..., description="Model information")
    timestamp: str = Field(..., description="Prediction timestamp")


class ModelInfo(BaseModel):
    """Model information"""
    model_name: str
    model_type: str
    r2_score: float
    rmse: float
    feature_count: int
    training_date: str
    last_updated: str


class FeatureImportance(BaseModel):
    """Feature importance information"""
    feature: str
    importance: float
    impact_level: str


# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the predictor on startup"""
    global predictor, model_loaded
    
    try:
        # Try to load pre-trained models
        model_dir = "models/house_price"
        if os.path.exists(model_dir):
            predictor = HousePricePredictor()
            predictor.ml_pipeline.load_models(model_dir)
            model_loaded = True
            logger.info("Pre-trained models loaded successfully")
        else:
            logger.warning("No pre-trained models found. Use /train endpoint to train models.")
            
    except Exception as e:
        logger.error(f"Error loading models: {str(e)}")
        model_loaded = False


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "House Price Prediction API",
        "version": "1.0.0",
        "status": "ready" if model_loaded else "models_not_loaded",
        "endpoints": [
            "/predict",
            "/predict/batch",
            "/train",
            "/model/info",
            "/model/feature_importance",
            "/health"
        ]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "model_loaded": model_loaded,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/predict", response_model=Dict[str, Any])
async def predict_single(house: HouseFeatures):
    """
    Predict price for a single house
    
    Args:
        house: House features
        
    Returns:
        Prediction result
    """
    global predictor, model_loaded
    
    if not model_loaded or not predictor:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Please train models first using /train endpoint."
        )
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame([house.dict()])
        
        # Get prediction
        result = predictor.predict_price(df)
        
        # Get detailed insights
        insights = predictor.get_property_insights(df)
        
        return {
            "success": True,
            "prediction": result,
            "insights": insights,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in prediction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.post("/predict/batch", response_model=PredictionResponse)
async def predict_batch(request: PredictionRequest):
    """
    Predict prices for multiple houses
    
    Args:
        request: Batch prediction request
        
    Returns:
        Batch prediction results
    """
    global predictor, model_loaded
    
    if not model_loaded or not predictor:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Please train models first using /train endpoint."
        )
    
    try:
        # Convert to DataFrame
        houses_data = [house.dict() for house in request.houses]
        df = pd.DataFrame(houses_data)
        
        # Get predictions for all houses
        predictions = []
        for i, house_data in enumerate(houses_data):
            house_df = pd.DataFrame([house_data])
            
            # Get prediction
            result = predictor.predict_price(house_df)
            
            # Get insights
            insights = predictor.get_property_insights(house_df)
            
            predictions.append({
                "house_index": i,
                "prediction": result,
                "insights": insights
            })
        
        # Get model info
        model_info = await get_model_info_internal()
        
        return PredictionResponse(
            predictions=predictions,
            model_info=model_info,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in batch prediction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch prediction error: {str(e)}")


@app.post("/train")
async def train_models(background_tasks: BackgroundTasks):
    """
    Train house price prediction models
    
    Args:
        background_tasks: FastAPI background tasks
        
    Returns:
        Training job information
    """
    global predictor, model_loaded
    
    try:
        # Start training in background
        job_id = f"train_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        background_tasks.add_task(train_models_background, job_id)
        
        return {
            "success": True,
            "job_id": job_id,
            "message": "Model training started in background",
            "status_endpoint": f"/train/status/{job_id}",
            "estimated_time": "5-10 minutes"
        }
        
    except Exception as e:
        logger.error(f"Error starting training: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Training error: {str(e)}")


async def train_models_background(job_id: str):
    """Background task for model training"""
    global predictor, model_loaded
    
    try:
        # This would typically load data from BigQuery
        # For now, we'll simulate training
        logger.info(f"Starting training job {job_id}")
        
        # Create predictor and train models
        predictor = HousePricePredictor()
        
        # Load training data (this would come from BigQuery)
        # For demo purposes, we'll create dummy data
        training_data = create_sample_training_data()
        
        # Train models
        results = predictor.train_house_price_models(training_data)
        
        # Save models
        os.makedirs("models/house_price", exist_ok=True)
        predictor.ml_pipeline.save_models("models/house_price")
        
        model_loaded = True
        
        logger.info(f"Training job {job_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Error in training job {job_id}: {str(e)}")


def create_sample_training_data() -> pd.DataFrame:
    """Create sample training data for demonstration"""
    import numpy as np
    
    np.random.seed(42)
    n_samples = 1000
    
    data = {
        'OverallQual': np.random.randint(1, 11, n_samples),
        'GrLivArea': np.random.randint(800, 4000, n_samples),
        'TotalBsmtSF': np.random.randint(500, 2500, n_samples),
        'FullBath': np.random.randint(0, 4, n_samples),
        'HalfBath': np.random.randint(0, 3, n_samples),
        'TotRmsAbvGrd': np.random.randint(2, 12, n_samples),
        'YearBuilt': np.random.randint(1950, 2024, n_samples),
        'YearRemodAdd': np.random.randint(1950, 2024, n_samples),
        'GarageCars': np.random.randint(0, 4, n_samples),
        'GarageArea': np.random.randint(0, 1000, n_samples),
        'Neighborhood': np.random.choice(['NAmes', 'CollgCr', 'OldTown', 'Edwards', 'Somerst'], n_samples),
        'SalePrice': np.random.randint(100000, 500000, n_samples)
    }
    
    return pd.DataFrame(data)


@app.get("/train/status/{job_id}")
async def get_training_status(job_id: str):
    """Get training job status"""
    # This would typically check a database or file for status
    # For now, we'll return a simple status
    return {
        "job_id": job_id,
        "status": "completed" if model_loaded else "in_progress",
        "message": "Training completed successfully" if model_loaded else "Training in progress...",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/model/info", response_model=ModelInfo)
async def get_model_info():
    """Get model information"""
    return await get_model_info_internal()


async def get_model_info_internal() -> Dict[str, Any]:
    """Internal method to get model info"""
    global predictor, model_loaded
    
    if not model_loaded or not predictor:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Please train models first using /train endpoint."
        )
    
    try:
        best_model = predictor.best_model
        metrics = best_model['metrics']
        
        return {
            "model_name": predictor.best_model_name,
            "model_type": "ensemble",
            "r2_score": metrics['r2'],
            "rmse": metrics['rmse'],
            "feature_count": len(predictor.ml_pipeline.scalers['scaler'].feature_names_in_),
            "training_date": datetime.now().strftime("%Y-%m-%d"),
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting model info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting model info: {str(e)}")


@app.get("/model/feature_importance", response_model=List[FeatureImportance])
async def get_feature_importance():
    """Get feature importance"""
    global predictor, model_loaded
    
    if not model_loaded or not predictor:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Please train models first using /train endpoint."
        )
    
    try:
        importance_df = predictor.get_feature_importance()
        
        importance_list = []
        for _, row in importance_df.iterrows():
            importance = row['importance'] if 'importance' in row else row['shap_importance']
            
            if importance > 0.1:
                impact_level = "High"
            elif importance > 0.05:
                impact_level = "Medium"
            else:
                impact_level = "Low"
            
            importance_list.append(FeatureImportance(
                feature=row['feature'],
                importance=importance,
                impact_level=impact_level
            ))
        
        return importance_list
        
    except Exception as e:
        logger.error(f"Error getting feature importance: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting feature importance: {str(e)}")


@app.get("/model/explain/{feature_name}")
async def explain_feature(feature_name: str):
    """Explain a specific feature's impact"""
    global predictor, model_loaded
    
    if not model_loaded or not predictor:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Please train models first using /train endpoint."
        )
    
    try:
        importance_df = predictor.get_feature_importance()
        
        if feature_name not in importance_df['feature'].values:
            raise HTTPException(status_code=404, detail=f"Feature {feature_name} not found")
        
        feature_data = importance_df[importance_df['feature'] == feature_name].iloc[0]
        
        return {
            "feature": feature_name,
            "importance": feature_data['importance'] if 'importance' in feature_data else feature_data['shap_importance'],
            "description": get_feature_description(feature_name),
            "impact_level": "High" if feature_data['importance'] > 0.1 else "Medium" if feature_data['importance'] > 0.05 else "Low"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error explaining feature: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error explaining feature: {str(e)}")


def get_feature_description(feature_name: str) -> str:
    """Get description for a feature"""
    descriptions = {
        "OverallQual": "Overall material and finish quality (1-10 scale)",
        "GrLivArea": "Above ground living area in square feet",
        "TotalBsmtSF": "Total square feet of basement area",
        "FullBath": "Number of full bathrooms above ground",
        "HalfBath": "Number of half bathrooms above ground",
        "TotRmsAbvGrd": "Total rooms above ground (does not include bathrooms)",
        "YearBuilt": "Original construction year",
        "YearRemodAdd": "Remodel date (same as construction year if no remodeling)",
        "GarageCars": "Size of garage in car capacity",
        "GarageArea": "Size of garage in square feet",
        "Neighborhood": "Physical location within Ames city limits"
    }
    
    return descriptions.get(feature_name, "Feature description not available")


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "Endpoint not found", "message": str(exc)}
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "message": str(exc)}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
