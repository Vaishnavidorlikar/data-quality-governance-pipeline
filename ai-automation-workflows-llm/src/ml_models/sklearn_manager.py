"""
Scikit-learn Machine Learning Manager for comprehensive ML operations.
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder
from sklearn.feature_selection import SelectKBest, f_classif, RFE
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge, Lasso
from sklearn.svm import SVC, SVR
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.naive_bayes import GaussianNB
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.decomposition import PCA, FastICA
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
    mean_squared_error, mean_absolute_error, r2_score,
    silhouette_score, calinski_harabasz_score, davies_bouldin_score,
    classification_report, confusion_matrix
)
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from typing import Dict, List, Tuple, Optional, Any, Union
import json
import os
from pathlib import Path
from datetime import datetime
import pickle


class SklearnManager:
    """
    Comprehensive Scikit-learn machine learning manager.
    """
    
    def __init__(self, models_dir: str = "models/sklearn/"):
        """
        Initialize ML manager.
        
        Args:
            models_dir: Directory to save models
        """
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # Model storage
        self.trained_models = {}
        self.model_metadata = {}
        self.preprocessors = {}
        
        # Available models
        self.classification_models = {
            'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
            'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000),
            'SVM': SVC(random_state=42, probability=True),
            'KNN': KNeighborsClassifier(),
            'Decision Tree': DecisionTreeClassifier(random_state=42),
            'Naive Bayes': GaussianNB(),
            'Gradient Boosting': GradientBoostingClassifier(random_state=42)
        }
        
        self.regression_models = {
            'Random Forest': RandomForestRegressor(n_estimators=100, random_state=42),
            'Linear Regression': LinearRegression(),
            'Ridge': Ridge(random_state=42),
            'Lasso': Lasso(random_state=42),
            'SVR': SVR(),
            'KNN': KNeighborsRegressor(),
            'Decision Tree': DecisionTreeRegressor(random_state=42)
        }
        
        self.clustering_models = {
            'KMeans': KMeans(random_state=42),
            'DBSCAN': DBSCAN(),
            'Agglomerative': AgglomerativeClustering()
        }
    
    def prepare_data(self, X: pd.DataFrame, y: pd.Series = None,
                    test_size: float = 0.2,
                    scale_data: bool = True,
                    encoding_method: str = 'label') -> Dict:
        """
        Prepare data for machine learning.
        
        Args:
            X: Features DataFrame
            y: Target Series (optional)
            test_size: Test set proportion
            scale_data: Whether to scale features
            encoding_method: 'label' or 'onehot' for categorical encoding
            
        Returns:
            Prepared data dictionary
        """
        result = {'X_processed': X.copy()}
        
        # Handle categorical variables
        categorical_cols = X.select_dtypes(include=['object']).columns
        
        if len(categorical_cols) > 0:
            if encoding_method == 'label':
                encoders = {}
                for col in categorical_cols:
                    le = LabelEncoder()
                    result['X_processed'][col] = le.fit_transform(result['X_processed'][col].astype(str))
                    encoders[col] = le
                
                self.preprocessors['label_encoders'] = encoders
                
            elif encoding_method == 'onehot':
                encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
                encoded_data = encoder.fit_transform(result['X_processed'][categorical_cols])
                
                # Create new DataFrame with encoded columns
                encoded_cols = []
                for i, col in enumerate(categorical_cols):
                    categories = encoder.categories_[i]
                    encoded_cols.extend([f"{col}_{cat}" for cat in categories])
                
                encoded_df = pd.DataFrame(encoded_data, columns=encoded_cols, index=result['X_processed'].index)
                
                # Drop original categorical columns and add encoded ones
                result['X_processed'] = result['X_processed'].drop(columns=categorical_cols)
                result['X_processed'] = pd.concat([result['X_processed'], encoded_df], axis=1)
                
                self.preprocessors['onehot_encoder'] = encoder
        
        # Scale data if requested
        if scale_data:
            scaler = StandardScaler()
            numeric_cols = result['X_processed'].select_dtypes(include=[np.number]).columns
            result['X_processed'][numeric_cols] = scaler.fit_transform(result['X_processed'][numeric_cols])
            
            self.preprocessors['scaler'] = scaler
        
        # Split data if target provided
        if y is not None:
            # Encode target if categorical
            if y.dtype == 'object':
                target_encoder = LabelEncoder()
                y_encoded = target_encoder.fit_transform(y)
                self.preprocessors['target_encoder'] = target_encoder
            else:
                y_encoded = y
            
            X_train, X_test, y_train, y_test = train_test_split(
                result['X_processed'], y_encoded, test_size=test_size, random_state=42
            )
            
            result.update({
                'X_train': X_train,
                'X_test': X_test,
                'y_train': y_train,
                'y_test': y_test
            })
        
        return result
    
    def train_classification_models(self, X_train: np.ndarray, y_train: np.ndarray,
                                 cv_folds: int = 5) -> Dict:
        """
        Train multiple classification models with cross-validation.
        
        Args:
            X_train: Training features
            y_train: Training labels
            cv_folds: Number of cross-validation folds
            
        Returns:
            Training results
        """
        results = {}
        
        for name, model in self.classification_models.items():
            try:
                # Cross-validation
                cv_scores = cross_val_score(model, X_train, y_train, cv=cv_folds, scoring='accuracy')
                
                # Train on full training set
                model.fit(X_train, y_train)
                
                # Store model
                self.trained_models[f"classification_{name}"] = model
                
                results[name] = {
                    'cv_scores': cv_scores.tolist(),
                    'cv_mean': cv_scores.mean(),
                    'cv_std': cv_scores.std(),
                    'model_type': 'classification'
                }
                
                self.logger.info(f"Trained {name}: CV Accuracy = {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")
                
            except Exception as e:
                self.logger.error(f"Error training {name}: {str(e)}")
                results[name] = {'error': str(e)}
        
        return results
    
    def train_regression_models(self, X_train: np.ndarray, y_train: np.ndarray,
                              cv_folds: int = 5) -> Dict:
        """
        Train multiple regression models with cross-validation.
        
        Args:
            X_train: Training features
            y_train: Training labels
            cv_folds: Number of cross-validation folds
            
        Returns:
            Training results
        """
        results = {}
        
        for name, model in self.regression_models.items():
            try:
                # Cross-validation
                cv_scores = cross_val_score(model, X_train, y_train, cv=cv_folds, scoring='r2')
                
                # Train on full training set
                model.fit(X_train, y_train)
                
                # Store model
                self.trained_models[f"regression_{name}"] = model
                
                results[name] = {
                    'cv_scores': cv_scores.tolist(),
                    'cv_mean': cv_scores.mean(),
                    'cv_std': cv_scores.std(),
                    'model_type': 'regression'
                }
                
                self.logger.info(f"Trained {name}: CV R² = {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")
                
            except Exception as e:
                self.logger.error(f"Error training {name}: {str(e)}")
                results[name] = {'error': str(e)}
        
        return results
    
    def evaluate_classification_models(self, X_test: np.ndarray, y_test: np.ndarray) -> Dict:
        """
        Evaluate trained classification models.
        
        Args:
            X_test: Test features
            y_test: Test labels
            
        Returns:
            Evaluation results
        """
        results = {}
        
        for model_name, model in self.trained_models.items():
            if 'classification' not in model_name:
                continue
            
            try:
                # Predictions
                y_pred = model.predict(X_test)
                y_proba = model.predict_proba(X_test) if hasattr(model, 'predict_proba') else None
                
                # Metrics
                accuracy = accuracy_score(y_test, y_pred)
                precision = precision_score(y_test, y_pred, average='weighted', zero_division=0)
                recall = recall_score(y_test, y_pred, average='weighted', zero_division=0)
                f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)
                
                metrics = {
                    'accuracy': accuracy,
                    'precision': precision,
                    'recall': recall,
                    'f1_score': f1
                }
                
                # Add AUC if binary classification
                if len(np.unique(y_test)) == 2 and y_proba is not None:
                    auc = roc_auc_score(y_test, y_proba[:, 1])
                    metrics['auc'] = auc
                
                # Classification report
                report = classification_report(y_test, y_pred, output_dict=True)
                
                # Confusion matrix
                cm = confusion_matrix(y_test, y_pred)
                
                results[model_name] = {
                    'metrics': metrics,
                    'classification_report': report,
                    'confusion_matrix': cm.tolist()
                }
                
            except Exception as e:
                self.logger.error(f"Error evaluating {model_name}: {str(e)}")
                results[model_name] = {'error': str(e)}
        
        return results
    
    def evaluate_regression_models(self, X_test: np.ndarray, y_test: np.ndarray) -> Dict:
        """
        Evaluate trained regression models.
        
        Args:
            X_test: Test features
            y_test: Test labels
            
        Returns:
            Evaluation results
        """
        results = {}
        
        for model_name, model in self.trained_models.items():
            if 'regression' not in model_name:
                continue
            
            try:
                # Predictions
                y_pred = model.predict(X_test)
                
                # Metrics
                mse = mean_squared_error(y_test, y_pred)
                mae = mean_absolute_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                rmse = np.sqrt(mse)
                
                results[model_name] = {
                    'mse': mse,
                    'mae': mae,
                    'r2_score': r2,
                    'rmse': rmse
                }
                
            except Exception as e:
                self.logger.error(f"Error evaluating {model_name}: {str(e)}")
                results[model_name] = {'error': str(e)}
        
        return results
    
    def perform_clustering(self, X: pd.DataFrame, n_clusters: int = None) -> Dict:
        """
        Perform clustering analysis.
        
        Args:
            X: Features DataFrame
            n_clusters: Number of clusters (optional)
            
        Returns:
            Clustering results
        """
        results = {}
        
        # Determine optimal number of clusters for KMeans if not provided
        if n_clusters is None:
            n_clusters = self._find_optimal_clusters(X, max_clusters=10)
        
        for name, model in self.clustering_models.items():
            try:
                if name == 'KMeans':
                    model.set_params(n_clusters=n_clusters)
                
                # Fit model
                cluster_labels = model.fit_predict(X)
                
                # Calculate metrics
                if len(set(cluster_labels)) > 1:  # More than one cluster
                    silhouette = silhouette_score(X, cluster_labels)
                    calinski = calinski_harabasz_score(X, cluster_labels)
                    davies = davies_bouldin_score(X, cluster_labels)
                else:
                    silhouette = calinski = davies = 0
                
                results[name] = {
                    'cluster_labels': cluster_labels.tolist(),
                    'n_clusters': len(set(cluster_labels)),
                    'silhouette_score': silhouette,
                    'calinski_harabasz_score': calinski,
                    'davies_bouldin_score': davies
                }
                
                # Store model
                self.trained_models[f"clustering_{name}"] = model
                
            except Exception as e:
                self.logger.error(f"Error in {name} clustering: {str(e)}")
                results[name] = {'error': str(e)}
        
        return results
    
    def _find_optimal_clusters(self, X: pd.DataFrame, max_clusters: int = 10) -> int:
        """Find optimal number of clusters using elbow method."""
        inertias = []
        K_range = range(2, max_clusters + 1)
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(X)
            inertias.append(kmeans.inertia_)
        
        # Simple elbow detection (find point of maximum curvature)
        if len(inertias) < 3:
            return 3
        
        # Calculate differences
        diffs = np.diff(inertias)
        diffs2 = np.diff(diffs)
        
        # Find elbow point
        elbow_idx = np.argmax(diffs2) + 2  # +2 because we start from k=2
        
        return min(elbow_idx + 2, max_clusters)
    
    def feature_selection(self, X: pd.DataFrame, y: pd.Series,
                         method: str = 'selectkbest',
                         k: int = 10) -> Dict:
        """
        Perform feature selection.
        
        Args:
            X: Features DataFrame
            y: Target Series
            method: Selection method ('selectkbest', 'rfe')
            k: Number of features to select
            
        Returns:
            Feature selection results
        """
        results = {}
        
        if method == 'selectkbest':
            selector = SelectKBest(score_func=f_classif, k=k)
            X_selected = selector.fit_transform(X, y)
            
            # Get selected feature names
            selected_features = X.columns[selector.get_support()].tolist()
            feature_scores = selector.scores_[selector.get_support()]
            
            results = {
                'selected_features': selected_features,
                'feature_scores': dict(zip(selected_features, feature_scores)),
                'X_selected': X_selected
            }
            
        elif method == 'rfe':
            # Use a simple estimator for RFE
            estimator = RandomForestClassifier(n_estimators=50, random_state=42)
            selector = RFE(estimator, n_features_to_select=k)
            X_selected = selector.fit_transform(X, y)
            
            selected_features = X.columns[selector.get_support()].tolist()
            feature_rankings = selector.ranking_
            
            results = {
                'selected_features': selected_features,
                'feature_rankings': dict(zip(X.columns, feature_rankings)),
                'X_selected': X_selected
            }
        
        # Store selector
        self.preprocessors[f'feature_selector_{method}'] = selector
        
        return results
    
    def hyperparameter_tuning(self, X_train: np.ndarray, y_train: np.ndarray,
                           model_type: str = 'classification') -> Dict:
        """
        Perform hyperparameter tuning using GridSearchCV.
        
        Args:
            X_train: Training features
            y_train: Training labels
            model_type: 'classification' or 'regression'
            
        Returns:
            Tuning results
        """
        results = {}
        
        # Define parameter grids
        if model_type == 'classification':
            param_grids = {
                'Random Forest': {
                    'n_estimators': [50, 100, 200],
                    'max_depth': [None, 10, 20],
                    'min_samples_split': [2, 5, 10]
                },
                'SVM': {
                    'C': [0.1, 1, 10],
                    'kernel': ['rbf', 'linear'],
                    'gamma': ['scale', 'auto']
                },
                'Logistic Regression': {
                    'C': [0.1, 1, 10],
                    'penalty': ['l1', 'l2'],
                    'solver': ['liblinear']
                }
            }
            models_to_tune = self.classification_models
            scoring = 'accuracy'
            
        else:  # regression
            param_grids = {
                'Random Forest': {
                    'n_estimators': [50, 100, 200],
                    'max_depth': [None, 10, 20],
                    'min_samples_split': [2, 5, 10]
                },
                'Ridge': {
                    'alpha': [0.1, 1.0, 10.0],
                    'solver': ['auto', 'svd']
                },
                'SVR': {
                    'C': [0.1, 1, 10],
                    'kernel': ['rbf', 'linear'],
                    'gamma': ['scale', 'auto']
                }
            }
            models_to_tune = self.regression_models
            scoring = 'r2'
        
        for model_name, param_grid in param_grids.items():
            if model_name in models_to_tune:
                try:
                    model = models_to_tune[model_name]
                    
                    grid_search = GridSearchCV(
                        model, param_grid, cv=3, scoring=scoring, n_jobs=-1
                    )
                    grid_search.fit(X_train, y_train)
                    
                    results[model_name] = {
                        'best_params': grid_search.best_params_,
                        'best_score': grid_search.best_score_,
                        'cv_results': grid_search.cv_results_
                    }
                    
                    # Store best model
                    best_model_name = f"best_{model_type}_{model_name}"
                    self.trained_models[best_model_name] = grid_search.best_estimator_
                    
                    self.logger.info(f"Best params for {model_name}: {grid_search.best_params_}")
                    
                except Exception as e:
                    self.logger.error(f"Error tuning {model_name}: {str(e)}")
                    results[model_name] = {'error': str(e)}
        
        return results
    
    def predict(self, model_name: str, X: np.ndarray) -> np.ndarray:
        """
        Make predictions using a trained model.
        
        Args:
            model_name: Name of the trained model
            X: Features to predict on
            
        Returns:
            Predictions
        """
        if model_name not in self.trained_models:
            raise ValueError(f"Model {model_name} not found in trained models")
        
        model = self.trained_models[model_name]
        return model.predict(X)
    
    def predict_proba(self, model_name: str, X: np.ndarray) -> np.ndarray:
        """
        Get prediction probabilities for classification models.
        
        Args:
            model_name: Name of the trained model
            X: Features to predict on
            
        Returns:
            Prediction probabilities
        """
        if model_name not in self.trained_models:
            raise ValueError(f"Model {model_name} not found in trained models")
        
        model = self.trained_models[model_name]
        
        if not hasattr(model, 'predict_proba'):
            raise ValueError(f"Model {model_name} does not support probability prediction")
        
        return model.predict_proba(X)
    
    def save_model(self, model_name: str, file_path: str = None) -> str:
        """
        Save a trained model to disk.
        
        Args:
            model_name: Name of the model to save
            file_path: Path to save model (optional)
            
        Returns:
            Path to saved model
        """
        if model_name not in self.trained_models:
            raise ValueError(f"Model {model_name} not found")
        
        if file_path is None:
            file_path = self.models_dir / f"{model_name}.pkl"
        
        with open(file_path, 'wb') as f:
            pickle.dump(self.trained_models[model_name], f)
        
        self.logger.info(f"Model saved: {file_path}")
        return str(file_path)
    
    def load_model(self, model_name: str, file_path: str) -> None:
        """
        Load a trained model from disk.
        
        Args:
            model_name: Name to assign to the loaded model
            file_path: Path to model file
        """
        with open(file_path, 'rb') as f:
            self.trained_models[model_name] = pickle.load(f)
        
        self.logger.info(f"Model loaded: {model_name}")
    
    def create_model_comparison_plot(self, results: Dict, metric: str = 'accuracy',
                                  save_path: str = None) -> str:
        """
        Create comparison plot of model performances.
        
        Args:
            results: Evaluation results
            metric: Metric to plot
            save_path: Path to save plot
            
        Returns:
            Path to saved plot
        """
        plt.figure(figsize=(12, 6))
        
        # Extract model names and scores
        model_names = []
        scores = []
        
        for model_name, result in results.items():
            if 'error' not in result and metric in result:
                model_names.append(model_name.replace('classification_', '').replace('regression_', ''))
                scores.append(result[metric])
        
        if not scores:
            self.logger.warning(f"No scores found for metric: {metric}")
            return None
        
        # Create bar plot
        bars = plt.bar(model_names, scores)
        plt.title(f'Model Comparison - {metric.title()}')
        plt.xlabel('Models')
        plt.ylabel(metric.title())
        plt.xticks(rotation=45)
        
        # Add value labels on bars
        for bar, score in zip(bars, scores):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{score:.3f}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = self.models_dir / f"model_comparison_{metric}.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(save_path)
    
    def create_feature_importance_plot(self, model_name: str, feature_names: List[str],
                                     save_path: str = None) -> str:
        """
        Create feature importance plot.
        
        Args:
            model_name: Name of the trained model
            feature_names: List of feature names
            save_path: Path to save plot
            
        Returns:
            Path to saved plot
        """
        if model_name not in self.trained_models:
            raise ValueError(f"Model {model_name} not found")
        
        model = self.trained_models[model_name]
        
        if not hasattr(model, 'feature_importances_'):
            self.logger.warning(f"Model {model_name} does not have feature importances")
            return None
        
        # Get feature importances
        importances = model.feature_importances_
        
        # Create DataFrame for plotting
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importances
        }).sort_values('importance', ascending=False)
        
        # Plot
        plt.figure(figsize=(10, 8))
        sns.barplot(data=importance_df.head(15), x='importance', y='feature')
        plt.title(f'Feature Importance - {model_name}')
        plt.xlabel('Importance')
        plt.ylabel('Features')
        
        if save_path is None:
            save_path = self.models_dir / f"feature_importance_{model_name}.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(save_path)
    
    def get_model_summary(self) -> Dict:
        """
        Get summary of all trained models.
        
        Returns:
            Model summary dictionary
        """
        summary = {
            'total_models': len(self.trained_models),
            'classification_models': len([name for name in self.trained_models if 'classification' in name]),
            'regression_models': len([name for name in self.trained_models if 'regression' in name]),
            'clustering_models': len([name for name in self.trained_models if 'clustering' in name]),
            'preprocessors': list(self.preprocessors.keys()),
            'model_names': list(self.trained_models.keys())
        }
        
        return summary
    
    def export_results(self, results: Dict, file_path: str, format: str = 'json'):
        """
        Export results to file.
        
        Args:
            results: Results dictionary
            file_path: Path to save file
            format: Export format ('json', 'csv')
        """
        if format == 'json':
            # Convert numpy arrays to lists for JSON serialization
            def convert_numpy(obj):
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                return obj
            
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=2, default=convert_numpy)
        
        elif format == 'csv':
            # Convert to DataFrame and save as CSV
            if isinstance(results, dict):
                df = pd.DataFrame.from_dict(results, orient='index')
                df.to_csv(file_path)
        
        self.logger.info(f"Results exported to {file_path}")
