"""
ML Pipeline module for machine learning model training and deployment
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
import joblib
import json
from datetime import datetime
import os

# ML Libraries
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import xgboost as xgb
import lightgbm as lgb
import shap

# MLOps
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import mlflow.lightgbm

logger = logging.getLogger(__name__)


class MLPipeline:
    """
    Machine Learning pipeline for training, evaluating, and deploying models
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize ML pipeline
        
        Args:
            config: ML configuration dictionary
        """
        self.config = config or {}
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self.feature_importance = {}
        self.explainers = {}
        
        # Initialize MLflow
        mlflow.set_tracking_uri(self.config.get('mlflow_uri', 'http://localhost:5000'))
        self.experiment_name = self.config.get('experiment_name', 'house_price_prediction')
    
    def preprocess_data(self, df: pd.DataFrame, target_column: str) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Preprocess data for ML training
        
        Args:
            df: Input dataframe
            target_column: Name of target column
            
        Returns:
            Tuple of (X, y) preprocessed features and target
        """
        try:
            logger.info("Starting data preprocessing")
            
            # Make a copy to avoid modifying original data
            df_processed = df.copy()
            
            # Handle missing values
            numeric_columns = df_processed.select_dtypes(include=[np.number]).columns
            categorical_columns = df_processed.select_dtypes(include=['object']).columns
            
            # Fill missing numeric values with median
            for col in numeric_columns:
                if col != target_column:
                    df_processed[col] = df_processed[col].fillna(df_processed[col].median())
            
            # Fill missing categorical values with mode
            for col in categorical_columns:
                df_processed[col] = df_processed[col].fillna(df_processed[col].mode()[0] if not df_processed[col].mode().empty else 'Unknown')
            
            # Encode categorical variables
            for col in categorical_columns:
                if col not in self.encoders:
                    self.encoders[col] = LabelEncoder()
                    df_processed[col] = self.encoders[col].fit_transform(df_processed[col])
                else:
                    df_processed[col] = self.encoders[col].transform(df_processed[col])
            
            # Separate features and target
            X = df_processed.drop(columns=[target_column])
            y = df_processed[target_column]
            
            # Scale features
            if 'scaler' not in self.scalers:
                self.scalers['scaler'] = StandardScaler()
                X_scaled = self.scalers['scaler'].fit_transform(X)
            else:
                X_scaled = self.scalers['scaler'].transform(X)
            
            X_scaled = pd.DataFrame(X_scaled, columns=X.columns)
            
            logger.info(f"Preprocessing completed. Features: {X_scaled.shape[1]}, Samples: {X_scaled.shape[0]}")
            
            return X_scaled, y
            
        except Exception as e:
            logger.error(f"Error in data preprocessing: {str(e)}")
            raise
    
    def train_models(self, X: pd.DataFrame, y: pd.Series, model_types: List[str] = None) -> Dict[str, Any]:
        """
        Train multiple ML models
        
        Args:
            X: Features dataframe
            y: Target series
            model_types: List of model types to train
            
        Returns:
            Dictionary of trained models and metrics
        """
        try:
            logger.info("Starting model training")
            
            if model_types is None:
                model_types = ['linear', 'ridge', 'lasso', 'random_forest', 'xgboost', 'lightgbm']
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            results = {}
            
            with mlflow.start_run(experiment_id=self.experiment_name) as run:
                mlflow.log_param("model_types", model_types)
                mlflow.log_param("train_size", len(X_train))
                mlflow.log_param("test_size", len(X_test))
                
                # Linear Regression
                if 'linear' in model_types:
                    logger.info("Training Linear Regression")
                    model = LinearRegression()
                    model.fit(X_train, y_train)
                    self.models['linear'] = model
                    
                    # Evaluate
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['linear'] = {'model': model, 'metrics': metrics}
                    mlflow.log_metrics({f"linear_{k}": v for k, v in metrics.items()})
                    mlflow.sklearn.log_model(model, "linear_model")
                
                # Ridge Regression
                if 'ridge' in model_types:
                    logger.info("Training Ridge Regression")
                    model = Ridge(alpha=1.0)
                    model.fit(X_train, y_train)
                    self.models['ridge'] = model
                    
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['ridge'] = {'model': model, 'metrics': metrics}
                    mlflow.log_metrics({f"ridge_{k}": v for k, v in metrics.items()})
                    mlflow.sklearn.log_model(model, "ridge_model")
                
                # Lasso Regression
                if 'lasso' in model_types:
                    logger.info("Training Lasso Regression")
                    model = Lasso(alpha=1.0)
                    model.fit(X_train, y_train)
                    self.models['lasso'] = model
                    
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['lasso'] = {'model': model, 'metrics': metrics}
                    mlflow.log_metrics({f"lasso_{k}": v for k, v in metrics.items()})
                    mlflow.sklearn.log_model(model, "lasso_model")
                
                # Random Forest
                if 'random_forest' in model_types:
                    logger.info("Training Random Forest")
                    model = RandomForestRegressor(n_estimators=100, random_state=42)
                    model.fit(X_train, y_train)
                    self.models['random_forest'] = model
                    
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['random_forest'] = {'model': model, 'metrics': metrics}
                    
                    # Feature importance
                    feature_importance = pd.DataFrame({
                        'feature': X.columns,
                        'importance': model.feature_importances_
                    }).sort_values('importance', ascending=False)
                    self.feature_importance['random_forest'] = feature_importance
                    
                    mlflow.log_metrics({f"rf_{k}": v for k, v in metrics.items()})
                    mlflow.sklearn.log_model(model, "random_forest_model")
                    mlflow.log_dict(feature_importance.to_dict(), "rf_feature_importance")
                
                # XGBoost
                if 'xgboost' in model_types:
                    logger.info("Training XGBoost")
                    model = xgb.XGBRegressor(
                        n_estimators=100,
                        learning_rate=0.1,
                        max_depth=6,
                        random_state=42
                    )
                    model.fit(X_train, y_train)
                    self.models['xgboost'] = model
                    
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['xgboost'] = {'model': model, 'metrics': metrics}
                    
                    # Feature importance
                    feature_importance = pd.DataFrame({
                        'feature': X.columns,
                        'importance': model.feature_importances_
                    }).sort_values('importance', ascending=False)
                    self.feature_importance['xgboost'] = feature_importance
                    
                    mlflow.log_metrics({f"xgb_{k}": v for k, v in metrics.items()})
                    mlflow.xgboost.log_model(model, "xgboost_model")
                    mlflow.log_dict(feature_importance.to_dict(), "xgb_feature_importance")
                
                # LightGBM
                if 'lightgbm' in model_types:
                    logger.info("Training LightGBM")
                    model = lgb.LGBMRegressor(
                        n_estimators=100,
                        learning_rate=0.1,
                        max_depth=6,
                        random_state=42
                    )
                    model.fit(X_train, y_train)
                    self.models['lightgbm'] = model
                    
                    y_pred = model.predict(X_test)
                    metrics = self._calculate_metrics(y_test, y_pred)
                    results['lightgbm'] = {'model': model, 'metrics': metrics}
                    
                    # Feature importance
                    feature_importance = pd.DataFrame({
                        'feature': X.columns,
                        'importance': model.feature_importances_
                    }).sort_values('importance', ascending=False)
                    self.feature_importance['lightgbm'] = feature_importance
                    
                    mlflow.log_metrics({f"lgb_{k}": v for k, v in metrics.items()})
                    mlflow.lightgbm.log_model(model, "lightgbm_model")
                    mlflow.log_dict(feature_importance.to_dict(), "lgb_feature_importance")
            
            logger.info("Model training completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in model training: {str(e)}")
            raise
    
    def _calculate_metrics(self, y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
        """
        Calculate evaluation metrics
        
        Args:
            y_true: True values
            y_pred: Predicted values
            
        Returns:
            Dictionary of metrics
        """
        return {
            'mse': mean_squared_error(y_true, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
            'mae': mean_absolute_error(y_true, y_pred),
            'r2': r2_score(y_true, y_pred)
        }
    
    def get_best_model(self, results: Dict[str, Any], metric: str = 'r2') -> Tuple[str, Any]:
        """
        Get the best performing model based on specified metric
        
        Args:
            results: Dictionary of model results
            metric: Metric to optimize (higher is better for r2, lower for others)
            
        Returns:
            Tuple of (best_model_name, best_model_results)
        """
        best_model = None
        best_score = None
        
        for model_name, model_results in results.items():
            score = model_results['metrics'][metric]
            
            if best_score is None:
                best_score = score
                best_model = model_name
            elif metric in ['r2'] and score > best_score:
                best_score = score
                best_model = model_name
            elif metric in ['mse', 'rmse', 'mae'] and score < best_score:
                best_score = score
                best_model = model_name
        
        return best_model, results[best_model]
    
    def explain_model(self, model_name: str, X: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate model explanations using SHAP
        
        Args:
            model_name: Name of the model to explain
            X: Feature data
            
        Returns:
            Dictionary containing SHAP explanations
        """
        try:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            
            model = self.models[model_name]
            
            # Create SHAP explainer
            if model_name in ['linear', 'ridge', 'lasso']:
                explainer = shap.LinearExplainer(model, X)
            else:
                explainer = shap.TreeExplainer(model)
            
            # Calculate SHAP values
            shap_values = explainer.shap_values(X)
            
            # Feature importance based on SHAP
            feature_importance = pd.DataFrame({
                'feature': X.columns,
                'shap_importance': np.abs(shap_values).mean(axis=0)
            }).sort_values('shap_importance', ascending=False)
            
            self.explainers[model_name] = {
                'explainer': explainer,
                'shap_values': shap_values,
                'feature_importance': feature_importance
            }
            
            return self.explainers[model_name]
            
        except Exception as e:
            logger.error(f"Error in model explanation: {str(e)}")
            raise
    
    def predict(self, model_name: str, X: pd.DataFrame) -> np.ndarray:
        """
        Make predictions using trained model
        
        Args:
            model_name: Name of the model to use
            X: Feature data
            
        Returns:
            Predictions array
        """
        try:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            
            model = self.models[model_name]
            
            # Preprocess input data
            X_processed = X.copy()
            
            # Handle categorical encoding
            for col, encoder in self.encoders.items():
                if col in X_processed.columns:
                    X_processed[col] = encoder.transform(X_processed[col])
            
            # Scale features
            X_scaled = self.scalers['scaler'].transform(X_processed)
            X_scaled = pd.DataFrame(X_scaled, columns=X_processed.columns)
            
            # Make predictions
            predictions = model.predict(X_scaled)
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error in prediction: {str(e)}")
            raise
    
    def save_models(self, model_dir: str) -> None:
        """
        Save trained models and preprocessing objects
        
        Args:
            model_dir: Directory to save models
        """
        try:
            os.makedirs(model_dir, exist_ok=True)
            
            # Save models
            for model_name, model in self.models.items():
                joblib.dump(model, os.path.join(model_dir, f"{model_name}_model.pkl"))
            
            # Save scalers and encoders
            joblib.dump(self.scalers, os.path.join(model_dir, "scalers.pkl"))
            joblib.dump(self.encoders, os.path.join(model_dir, "encoders.pkl"))
            
            # Save feature importance
            if self.feature_importance:
                for model_name, importance in self.feature_importance.items():
                    importance.to_csv(os.path.join(model_dir, f"{model_name}_feature_importance.csv"), index=False)
            
            # Save SHAP explanations
            if self.explainers:
                for model_name, explainer_data in self.explainers.items():
                    explainer_data['feature_importance'].to_csv(
                        os.path.join(model_dir, f"{model_name}_shap_importance.csv"), index=False
                    )
            
            logger.info(f"Models saved to {model_dir}")
            
        except Exception as e:
            logger.error(f"Error saving models: {str(e)}")
            raise
    
    def load_models(self, model_dir: str) -> None:
        """
        Load trained models and preprocessing objects
        
        Args:
            model_dir: Directory containing saved models
        """
        try:
            # Load models
            model_files = [f for f in os.listdir(model_dir) if f.endswith('_model.pkl')]
            for model_file in model_files:
                model_name = model_file.replace('_model.pkl', '')
                self.models[model_name] = joblib.load(os.path.join(model_dir, model_file))
            
            # Load scalers and encoders
            self.scalers = joblib.load(os.path.join(model_dir, "scalers.pkl"))
            self.encoders = joblib.load(os.path.join(model_dir, "encoders.pkl"))
            
            logger.info(f"Models loaded from {model_dir}")
            
        except Exception as e:
            logger.error(f"Error loading models: {str(e)}")
            raise
