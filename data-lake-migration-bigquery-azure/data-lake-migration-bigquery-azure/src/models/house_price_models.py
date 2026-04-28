"""
House Price Prediction Models
Specialized ML models for real estate price prediction
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime

from ..ml_pipeline import MLPipeline

logger = logging.getLogger(__name__)


class HousePricePredictor:
    """
    Specialized house price prediction with domain-specific features
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize house price predictor
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.ml_pipeline = MLPipeline(config.get('ml', {}))
        self.feature_engineering = HousePriceFeatureEngineering()
        self.models = {}
        self.best_model = None
        self.best_model_name = None
        
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare house price specific features
        
        Args:
            df: Raw house price data
            
        Returns:
            DataFrame with engineered features
        """
        try:
            logger.info("Starting house price feature engineering")
            
            df_engineered = self.feature_engineering.engineer_features(df)
            
            logger.info(f"Feature engineering completed. Features: {df_engineered.shape[1]}")
            return df_engineered
            
        except Exception as e:
            logger.error(f"Error in feature engineering: {str(e)}")
            raise
    
    def train_house_price_models(self, df: pd.DataFrame, target_column: str = 'SalePrice') -> Dict[str, Any]:
        """
        Train specialized house price prediction models
        
        Args:
            df: House price dataset
            target_column: Name of target column
            
        Returns:
            Dictionary of trained models and metrics
        """
        try:
            logger.info("Starting house price model training")
            
            # Prepare features
            df_features = self.prepare_features(df)
            
            # Preprocess data
            X, y = self.ml_pipeline.preprocess_data(df_features, target_column)
            
            # Train models with house price specific configurations
            model_configs = self._get_house_price_model_configs()
            
            results = {}
            for model_name, config in model_configs.items():
                logger.info(f"Training {model_name} model")
                
                # Train model with specific configuration
                model_results = self.ml_pipeline.train_models(
                    X, y, model_types=[model_name]
                )
                
                results[model_name] = model_results[model_name]
            
            # Select best model
            self.best_model_name, self.best_model = self.ml_pipeline.get_best_model(results, 'r2')
            
            # Generate explanations for best model
            self.ml_pipeline.explain_model(self.best_model_name, X)
            
            logger.info(f"Best model: {self.best_model_name} with R²: {self.best_model['metrics']['r2']:.4f}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in house price model training: {str(e)}")
            raise
    
    def _get_house_price_model_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get house price specific model configurations
        
        Returns:
            Dictionary of model configurations
        """
        return {
            'linear': {},
            'ridge': {'alpha': 1.0},
            'lasso': {'alpha': 1.0},
            'random_forest': {
                'n_estimators': 200,
                'max_depth': 10,
                'min_samples_split': 5,
                'min_samples_leaf': 2,
                'random_state': 42
            },
            'xgboost': {
                'n_estimators': 200,
                'learning_rate': 0.05,
                'max_depth': 6,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42
            },
            'lightgbm': {
                'n_estimators': 200,
                'learning_rate': 0.05,
                'max_depth': 6,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42
            }
        }
    
    def predict_price(self, property_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Predict house price for given property data
        
        Args:
            property_data: Property features
            
        Returns:
            Dictionary with prediction and confidence
        """
        try:
            if not self.best_model:
                raise ValueError("No trained model available. Call train_house_price_models first.")
            
            # Prepare features
            property_features = self.prepare_features(property_data)
            
            # Make prediction
            prediction = self.ml_pipeline.predict(self.best_model_name, property_features)
            
            # Get prediction confidence (using model's performance metrics)
            r2_score = self.best_model['metrics']['r2']
            rmse = self.best_model['metrics']['rmse']
            
            result = {
                'predicted_price': float(prediction[0]),
                'model_used': self.best_model_name,
                'confidence_score': r2_score,
                'rmse': rmse,
                'prediction_date': datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error in price prediction: {str(e)}")
            raise
    
    def get_feature_importance(self, model_name: Optional[str] = None) -> pd.DataFrame:
        """
        Get feature importance for model
        
        Args:
            model_name: Name of model (default: best model)
            
        Returns:
            DataFrame with feature importance
        """
        try:
            if model_name is None:
                model_name = self.best_model_name
            
            if model_name in self.ml_pipeline.feature_importance:
                return self.ml_pipeline.feature_importance[model_name]
            elif model_name in self.ml_pipeline.explainers:
                return self.ml_pipeline.explainers[model_name]['feature_importance']
            else:
                raise ValueError(f"No feature importance available for model: {model_name}")
                
        except Exception as e:
            logger.error(f"Error getting feature importance: {str(e)}")
            raise
    
    def get_property_insights(self, property_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate insights for a specific property
        
        Args:
            property_data: Property features
            
        Returns:
            Dictionary with property insights
        """
        try:
            # Get prediction
            prediction_result = self.predict_price(property_data)
            predicted_price = prediction_result['predicted_price']
            
            # Get feature importance
            feature_importance = self.get_feature_importance()
            
            # Get SHAP values for this property
            property_features = self.prepare_features(property_data)
            explainer_data = self.ml_pipeline.explain_model(self.best_model_name, property_features)
            
            # Generate insights
            insights = {
                'prediction': prediction_result,
                'key_factors': self._get_key_factors(property_data, feature_importance),
                'price_range': self._estimate_price_range(predicted_price, prediction_result['rmse']),
                'market_position': self._assess_market_position(predicted_price, property_data),
                'recommendations': self._generate_recommendations(property_data, feature_importance)
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating property insights: {str(e)}")
            raise
    
    def _get_key_factors(self, property_data: pd.DataFrame, feature_importance: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Get key factors affecting the property price
        
        Args:
            property_data: Property features
            feature_importance: Feature importance DataFrame
            
        Returns:
            List of key factors
        """
        top_features = feature_importance.head(10)
        key_factors = []
        
        for _, row in top_features.iterrows():
            feature_name = row['feature']
            importance = row['importance'] if 'importance' in row else row['shap_importance']
            
            if feature_name in property_data.columns:
                feature_value = property_data[feature_name].iloc[0]
                key_factors.append({
                    'feature': feature_name,
                    'value': feature_value,
                    'importance': importance,
                    'impact': self._assess_feature_impact(feature_name, feature_value, importance)
                })
        
        return key_factors
    
    def _estimate_price_range(self, predicted_price: float, rmse: float) -> Dict[str, float]:
        """
        Estimate price range based on model uncertainty
        
        Args:
            predicted_price: Predicted price
            rmse: Root mean square error
            
        Returns:
            Dictionary with price range
        """
        confidence_factor = 1.96  # 95% confidence interval
        
        lower_bound = predicted_price - (confidence_factor * rmse)
        upper_bound = predicted_price + (confidence_factor * rmse)
        
        return {
            'predicted': predicted_price,
            'lower_bound': max(0, lower_bound),
            'upper_bound': upper_bound,
            'range_width': upper_bound - lower_bound
        }
    
    def _assess_market_position(self, predicted_price: float, property_data: pd.DataFrame) -> str:
        """
        Assess property's market position
        
        Args:
            predicted_price: Predicted price
            property_data: Property features
            
        Returns:
            Market position assessment
        """
        # This is a simplified assessment - in practice, you'd compare with market data
        if 'OverallQual' in property_data.columns:
            quality = property_data['OverallQual'].iloc[0]
            if quality >= 8:
                return "Premium - Above average pricing expected"
            elif quality >= 6:
                return "Standard - Market appropriate pricing"
            else:
                return "Budget - Below average pricing"
        
        return "Standard - Market appropriate pricing"
    
    def _generate_recommendations(self, property_data: pd.DataFrame, feature_importance: pd.DataFrame) -> List[str]:
        """
        Generate recommendations based on property features and importance
        
        Args:
            property_data: Property features
            feature_importance: Feature importance DataFrame
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Check for improvement opportunities based on important features
        top_features = feature_importance.head(5)['feature'].tolist()
        
        if 'OverallQual' in top_features and 'OverallQual' in property_data.columns:
            quality = property_data['OverallQual'].iloc[0]
            if quality < 7:
                recommendations.append("Consider improving overall quality to increase property value")
        
        if 'GrLivArea' in top_features and 'GrLivArea' in property_data.columns:
            area = property_data['GrLivArea'].iloc[0]
            if area < 1500:
                recommendations.append("Property could benefit from additional living space")
        
        if 'YearBuilt' in top_features and 'YearBuilt' in property_data.columns:
            year = property_data['YearBuilt'].iloc[0]
            if year < 2000:
                recommendations.append("Consider renovations to modernize older property")
        
        if len(recommendations) == 0:
            recommendations.append("Property appears to be well-optimized for current market")
        
        return recommendations
    
    def _assess_feature_impact(self, feature_name: str, feature_value: Any, importance: float) -> str:
        """
        Assess the impact of a feature value
        
        Args:
            feature_name: Name of feature
            feature_value: Value of feature
            importance: Importance score
            
        Returns:
            Impact assessment
        """
        if importance > 0.1:
            return "High Impact"
        elif importance > 0.05:
            return "Medium Impact"
        else:
            return "Low Impact"


class HousePriceFeatureEngineering:
    """
    Feature engineering specific to house price prediction
    """
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer house price specific features
        
        Args:
            df: Raw house price data
            
        Returns:
            DataFrame with engineered features
        """
        try:
            df_engineered = df.copy()
            
            # Age features
            if 'YearBuilt' in df_engineered.columns:
                df_engineered['HouseAge'] = 2024 - df_engineered['YearBuilt']
            
            if 'YearRemodAdd' in df_engineered.columns:
                df_engineered['YearsSinceRemodel'] = 2024 - df_engineered['YearRemodAdd']
            
            # Size features
            if 'TotalBsmtSF' in df_engineered.columns and 'GrLivArea' in df_engineered.columns:
                df_engineered['TotalSF'] = df_engineered['TotalBsmtSF'] + df_engineered['GrLivArea']
            
            # Bathroom features
            bathroom_cols = ['FullBath', 'HalfBath', 'BsmtFullBath', 'BsmtHalfBath']
            if all(col in df_engineered.columns for col in bathroom_cols):
                df_engineered['TotalBathrooms'] = (
                    df_engineered['FullBath'] + 
                    df_engineered['HalfBath'] * 0.5 +
                    df_engineered['BsmtFullBath'] + 
                    df_engineered['BsmtHalfBath'] * 0.5
                )
            
            # Room features
            if 'TotRmsAbvGrd' in df_engineered.columns and 'TotalBathrooms' in df_engineered.columns:
                df_engineered['RoomsPerBathroom'] = df_engineered['TotRmsAbvGrd'] / df_engineered['TotalBathrooms']
            
            # Quality indicators
            if 'OverallQual' in df_engineered.columns and 'OverallCond' in df_engineered.columns:
                df_engineered['QualCondRatio'] = df_engineered['OverallQual'] / df_engineered['OverallCond']
            
            # Neighborhood encoding (if exists)
            if 'Neighborhood' in df_engineered.columns:
                # This would typically be replaced with more sophisticated encoding
                neighborhood_means = df_engineered.groupby('Neighborhood')['SalePrice'].mean() if 'SalePrice' in df_engineered.columns else {}
                if neighborhood_means:
                    df_engineered['NeighborhoodPrice'] = df_engineered['Neighborhood'].map(neighborhood_means)
            
            # Drop original columns that have been transformed
            columns_to_drop = ['YearBuilt', 'YearRemodAdd', 'TotalBsmtSF', 'GrLivArea']
            df_engineered = df_engineered.drop(columns=[col for col in columns_to_drop if col in df_engineered.columns])
            
            return df_engineered
            
        except Exception as e:
            logger.error(f"Error in feature engineering: {str(e)}")
            raise
