"""
Comprehensive Data Analysis component using NumPy, Pandas, and visualization.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import logging
from typing import Dict, List, Tuple, Optional, Any, Union
import json
import os
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class DataAnalyzer:
    """
    Comprehensive data analysis and visualization toolkit.
    """
    
    def __init__(self, data_dir: str = "data/analysis/"):
        """
        Initialize data analyzer.
        
        Args:
            data_dir: Directory for data storage
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # Data storage
        self.datasets = {}
        self.analysis_results = {}
        
        # Initialize scalers
        self.scalers = {
            'standard': StandardScaler(),
            'minmax': MinMaxScaler()
        }
    
    def load_data(self, file_path: str, data_name: str = None, **kwargs) -> pd.DataFrame:
        """
        Load data from various file formats.
        
        Args:
            file_path: Path to data file
            data_name: Name to store the dataset
            **kwargs: Additional arguments for pandas read functions
            
        Returns:
            Loaded DataFrame
        """
        try:
            file_path = Path(file_path)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Determine file type and load accordingly
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path, **kwargs)
            elif file_path.suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, **kwargs)
            elif file_path.suffix == '.json':
                df = pd.read_json(file_path, **kwargs)
            elif file_path.suffix == '.parquet':
                df = pd.read_parquet(file_path, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            # Store dataset
            if data_name is None:
                data_name = file_path.stem
            
            self.datasets[data_name] = df
            
            self.logger.info(f"Data loaded: {data_name} ({df.shape[0]} rows, {df.shape[1]} columns)")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def generate_sample_data(self, data_type: str = "classification", 
                          num_samples: int = 1000,
                          num_features: int = 10,
                          num_classes: int = 3) -> pd.DataFrame:
        """
        Generate sample datasets for testing and demonstration.
        
        Args:
            data_type: Type of data ('classification', 'regression', 'clustering', 'timeseries')
            num_samples: Number of samples
            num_features: Number of features
            num_classes: Number of classes (for classification)
            
        Returns:
            Generated DataFrame
        """
        np.random.seed(42)
        
        if data_type == "classification":
            # Generate classification data
            X = np.random.randn(num_samples, num_features)
            
            # Create class boundaries
            class_centers = np.random.randn(num_classes, num_features)
            y = np.zeros(num_samples)
            
            for i in range(num_samples):
                distances = [np.linalg.norm(X[i] - center) for center in class_centers]
                y[i] = np.argmin(distances)
            
            # Create DataFrame
            feature_names = [f"feature_{i}" for i in range(num_features)]
            df = pd.DataFrame(X, columns=feature_names)
            df['target'] = y.astype(int)
            
        elif data_type == "regression":
            # Generate regression data
            X = np.random.randn(num_samples, num_features)
            # Create linear relationship with noise
            coefficients = np.random.randn(num_features)
            y = X @ coefficients + np.random.randn(num_samples) * 0.5
            
            feature_names = [f"feature_{i}" for i in range(num_features)]
            df = pd.DataFrame(X, columns=feature_names)
            df['target'] = y
            
        elif data_type == "clustering":
            # Generate clustering data with multiple centers
            num_clusters = 3
            cluster_centers = np.random.randn(num_clusters, num_features) * 3
            cluster_sizes = [num_samples // num_clusters] * num_clusters
            cluster_sizes[-1] = num_samples - sum(cluster_sizes[:-1])
            
            X_list = []
            for i, (center, size) in enumerate(zip(cluster_centers, cluster_sizes)):
                cluster_data = np.random.randn(size, num_features) + center
                X_list.append(cluster_data)
            
            X = np.vstack(X_list)
            feature_names = [f"feature_{i}" for i in range(num_features)]
            df = pd.DataFrame(X, columns=feature_names)
            
        elif data_type == "timeseries":
            # Generate time series data
            dates = pd.date_range(start='2020-01-01', periods=num_samples, freq='D')
            
            # Create trend, seasonality, and noise
            trend = np.linspace(0, 10, num_samples)
            seasonal = 5 * np.sin(2 * np.pi * np.arange(num_samples) / 365.25)
            noise = np.random.randn(num_samples) * 2
            
            y = trend + seasonal + noise
            
            df = pd.DataFrame({
                'date': dates,
                'value': y,
                'trend': trend,
                'seasonal': seasonal,
                'noise': noise
            })
            df.set_index('date', inplace=True)
        
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
        
        # Store dataset
        self.datasets[f"sample_{data_type}"] = df
        
        self.logger.info(f"Generated sample {data_type} data: {df.shape}")
        return df
    
    def get_data_info(self, data_name: str) -> Dict:
        """
        Get comprehensive information about a dataset.
        
        Args:
            data_name: Name of the dataset
            
        Returns:
            Dictionary with dataset information
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name]
        
        info = {
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
            'categorical_columns': df.select_dtypes(include=['object']).columns.tolist(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'description': df.describe().to_dict()
        }
        
        return info
    
    def clean_data(self, data_name: str, 
                   remove_missing: bool = True,
                   fill_missing: str = None,
                   remove_duplicates: bool = True,
                   outlier_method: str = 'iqr') -> pd.DataFrame:
        """
        Clean and preprocess data.
        
        Args:
            data_name: Name of the dataset
            remove_missing: Whether to remove rows with missing values
            fill_missing: Method to fill missing values ('mean', 'median', 'mode', 'forward', 'backward')
            remove_duplicates: Whether to remove duplicate rows
            outlier_method: Method to handle outliers ('iqr', 'zscore', 'none')
            
        Returns:
            Cleaned DataFrame
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name].copy()
        original_shape = df.shape
        
        # Handle missing values
        if fill_missing:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            categorical_cols = df.select_dtypes(include=['object']).columns
            
            if fill_missing == 'mean':
                df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
            elif fill_missing == 'median':
                df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
            elif fill_missing == 'mode':
                df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mode().iloc[0])
                df[categorical_cols] = df[categorical_cols].fillna(df[categorical_cols].mode().iloc[0])
            elif fill_missing == 'forward':
                df = df.fillna(method='ffill')
            elif fill_missing == 'backward':
                df = df.fillna(method='bfill')
        
        elif remove_missing:
            df = df.dropna()
        
        # Remove duplicates
        if remove_duplicates:
            df = df.drop_duplicates()
        
        # Handle outliers
        if outlier_method != 'none':
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if outlier_method == 'iqr':
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
                
                elif outlier_method == 'zscore':
                    z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                    df = df[z_scores < 3]
        
        cleaned_name = f"{data_name}_cleaned"
        self.datasets[cleaned_name] = df
        
        self.logger.info(f"Data cleaned: {original_shape} -> {df.shape}")
        return df
    
    def analyze_data(self, data_name: str) -> Dict:
        """
        Perform comprehensive data analysis.
        
        Args:
            data_name: Name of the dataset
            
        Returns:
            Analysis results
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name]
        
        analysis = {
            'basic_stats': df.describe().to_dict(),
            'correlation_matrix': df.corr(numeric_only=True).to_dict(),
            'data_quality': {
                'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
                'duplicate_rows': df.duplicated().sum(),
                'data_types': df.dtypes.value_counts().to_dict()
            }
        }
        
        # Numeric analysis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            analysis['numeric_analysis'] = {
                'skewness': df[numeric_cols].skew().to_dict(),
                'kurtosis': df[numeric_cols].kurtosis().to_dict(),
                'variance': df[numeric_cols].var().to_dict()
            }
        
        # Categorical analysis
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            analysis['categorical_analysis'] = {}
            for col in categorical_cols:
                analysis['categorical_analysis'][col] = {
                    'unique_values': df[col].nunique(),
                    'most_frequent': df[col].mode().iloc[0] if not df[col].mode().empty else None,
                    'value_counts': df[col].value_counts().head(10).to_dict()
                }
        
        # Generate summary
        analysis['summary'] = self._generate_analysis_summary(analysis)
        
        # Store results
        self.analysis_results[data_name] = analysis
        
        return analysis
    
    def _generate_analysis_summary(self, analysis: Dict) -> str:
        """Generate a human-readable summary of the analysis."""
        summary_parts = []
        
        # Data quality summary
        quality = analysis['data_quality']
        missing_cols = [col for col, pct in quality['missing_percentage'].items() if pct > 0]
        
        if missing_cols:
            summary_parts.append(f"Found missing values in {len(missing_cols)} columns")
        
        if quality['duplicate_rows'] > 0:
            summary_parts.append(f"Found {quality['duplicate_rows']} duplicate rows")
        
        # Correlation summary
        corr_matrix = analysis['correlation_matrix']
        if isinstance(corr_matrix, dict) and corr_matrix:
            high_corr_pairs = []
            for i, (col1, correlations) in enumerate(corr_matrix.items()):
                for j, (col2, corr_val) in enumerate(correlations.items()):
                    if i < j and abs(corr_val) > 0.7:  # High correlation threshold
                        high_corr_pairs.append(f"{col1}-{col2}: {corr_val:.2f}")
            
            if high_corr_pairs:
                summary_parts.append(f"Found {len(high_corr_pairs)} highly correlated feature pairs")
        
        return "; ".join(summary_parts) if summary_parts else "Data appears to be clean"
    
    def create_visualization(self, data_name: str, 
                           plot_type: str = "auto",
                           save_path: str = None,
                           **kwargs) -> str:
        """
        Create data visualizations.
        
        Args:
            data_name: Name of the dataset
            plot_type: Type of plot ('auto', 'histogram', 'scatter', 'box', 'heatmap', 'pairplot')
            save_path: Path to save the plot
            **kwargs: Additional plotting parameters
            
        Returns:
            Path to saved plot
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name]
        
        # Determine plot type if auto
        if plot_type == "auto":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) >= 2:
                plot_type = "pairplot" if len(numeric_cols) <= 5 else "heatmap"
            else:
                plot_type = "histogram"
        
        # Create plot
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if plot_type == "histogram":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                df[numeric_cols[0]].hist(bins=30, ax=ax)
                ax.set_title(f"Distribution of {numeric_cols[0]}")
        
        elif plot_type == "scatter":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) >= 2:
                ax.scatter(df[numeric_cols[0]], df[numeric_cols[1]], alpha=0.6)
                ax.set_xlabel(numeric_cols[0])
                ax.set_ylabel(numeric_cols[1])
                ax.set_title(f"{numeric_cols[0]} vs {numeric_cols[1]}")
        
        elif plot_type == "box":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                df[numeric_cols].boxplot(ax=ax)
                ax.set_title("Box Plot of Numeric Features")
                plt.xticks(rotation=45)
        
        elif plot_type == "heatmap":
            numeric_df = df.select_dtypes(include=[np.number])
            if not numeric_df.empty:
                sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm', center=0, ax=ax)
                ax.set_title("Correlation Heatmap")
        
        elif plot_type == "pairplot":
            numeric_df = df.select_dtypes(include=[np.number])
            if len(numeric_df.columns) <= 5:
                sns.pairplot(numeric_df)
                plt.suptitle("Pair Plot of Numeric Features", y=1.02)
        
        plt.tight_layout()
        
        # Save plot
        if save_path is None:
            save_path = self.data_dir / f"{data_name}_{plot_type}.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info(f"Visualization saved: {save_path}")
        return str(save_path)
    
    def build_ml_model(self, data_name: str,
                      target_column: str,
                      model_type: str = "auto",
                      test_size: float = 0.2,
                      random_state: int = 42) -> Dict:
        """
        Build and evaluate machine learning models.
        
        Args:
            data_name: Name of the dataset
            target_column: Name of target column
            model_type: Type of model ('auto', 'classification', 'regression', 'clustering')
            test_size: Proportion of data for testing
            random_state: Random state for reproducibility
            
        Returns:
            Model evaluation results
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name]
        
        if target_column not in df.columns:
            raise ValueError(f"Target column {target_column} not found")
        
        # Prepare data
        X = df.drop(columns=[target_column])
        y = df[target_column]
        
        # Handle categorical variables
        categorical_cols = X.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            le = LabelEncoder()
            for col in categorical_cols:
                X[col] = le.fit_transform(X[col].astype(str))
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Determine task type
        if model_type == "auto":
            if y.dtype == 'object' or y.nunique() < 20:
                model_type = "classification"
            else:
                model_type = "regression"
        
        results = {}
        
        if model_type == "classification":
            # Classification models
            models = {
                'Random Forest': RandomForestClassifier(n_estimators=100, random_state=random_state),
                'Logistic Regression': LogisticRegression(random_state=random_state, max_iter=1000)
            }
            
            for name, model in models.items():
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                
                results[name] = {
                    'accuracy': model.score(X_test, y_test),
                    'classification_report': classification_report(y_test, y_pred, output_dict=True),
                    'confusion_matrix': confusion_matrix(y_test, y_pred).tolist()
                }
        
        elif model_type == "regression":
            # Regression models
            models = {
                'Random Forest': RandomForestRegressor(n_estimators=100, random_state=random_state),
                'Linear Regression': LinearRegression()
            }
            
            for name, model in models.items():
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                
                results[name] = {
                    'r2_score': r2_score(y_test, y_pred),
                    'mse': mean_squared_error(y_test, y_pred),
                    'rmse': np.sqrt(mean_squared_error(y_test, y_pred))
                }
        
        elif model_type == "clustering":
            # Clustering (unsupervised)
            n_clusters = kwargs.get('n_clusters', 3)
            kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
            cluster_labels = kmeans.fit_predict(X)
            
            results['KMeans'] = {
                'n_clusters': n_clusters,
                'inertia': kmeans.inertia_,
                'silhouette_score': self._calculate_silhouette_score(X, cluster_labels)
            }
        
        # Store results
        self.analysis_results[f"{data_name}_ml"] = results
        
        self.logger.info(f"ML models built for {data_name}")
        return results
    
    def _calculate_silhouette_score(self, X: pd.DataFrame, labels: np.ndarray) -> float:
        """Calculate silhouette score for clustering."""
        try:
            from sklearn.metrics import silhouette_score
            return silhouette_score(X, labels)
        except ImportError:
            return 0.0
    
    def perform_pca(self, data_name: str, 
                   n_components: int = 2,
                   scale_data: bool = True) -> Dict:
        """
        Perform Principal Component Analysis.
        
        Args:
            data_name: Name of the dataset
            n_components: Number of principal components
            scale_data: Whether to scale data before PCA
            
        Returns:
            PCA results
        """
        if data_name not in self.datasets:
            raise ValueError(f"Dataset {data_name} not found")
        
        df = self.datasets[data_name]
        numeric_df = df.select_dtypes(include=[np.number])
        
        if scale_data:
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(numeric_df)
        else:
            X_scaled = numeric_df.values
        
        # Perform PCA
        pca = PCA(n_components=n_components)
        X_pca = pca.fit_transform(X_scaled)
        
        results = {
            'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
            'cumulative_variance_ratio': np.cumsum(pca.explained_variance_ratio_).tolist(),
            'components': pca.components_.tolist(),
            'transformed_data': X_pca.tolist(),
            'feature_names': numeric_df.columns.tolist()
        }
        
        # Create visualization
        if n_components >= 2:
            plt.figure(figsize=(8, 6))
            plt.scatter(X_pca[:, 0], X_pca[:, 1], alpha=0.6)
            plt.xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)')
            plt.ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)')
            plt.title('PCA Visualization')
            
            save_path = self.data_dir / f"{data_name}_pca.png"
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            results['plot_path'] = str(save_path)
        
        # Store results
        self.analysis_results[f"{data_name}_pca"] = results
        
        self.logger.info(f"PCA performed for {data_name}")
        return results
    
    def export_analysis(self, data_name: str, format: str = 'json') -> str:
        """
        Export analysis results to file.
        
        Args:
            data_name: Name of the dataset
            format: Export format ('json', 'csv', 'excel')
            
        Returns:
            Path to exported file
        """
        if data_name not in self.analysis_results:
            raise ValueError(f"No analysis results found for {data_name}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format == 'json':
            export_path = self.data_dir / f"{data_name}_analysis_{timestamp}.json"
            with open(export_path, 'w') as f:
                json.dump(self.analysis_results[data_name], f, indent=2, default=str)
        
        elif format == 'csv':
            if data_name in self.datasets:
                export_path = self.data_dir / f"{data_name}_data_{timestamp}.csv"
                self.datasets[data_name].to_csv(export_path, index=False)
            else:
                raise ValueError("No dataset found to export")
        
        elif format == 'excel':
            if data_name in self.datasets:
                export_path = self.data_dir / f"{data_name}_analysis_{timestamp}.xlsx"
                with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
                    self.datasets[data_name].to_excel(writer, sheet_name='Data', index=False)
                    
                    # Add analysis sheets if available
                    analysis = self.analysis_results[data_name]
                    for key, value in analysis.items():
                        if isinstance(value, dict):
                            pd.DataFrame([value]).to_excel(writer, sheet_name=key[:30], index=False)
        
        self.logger.info(f"Analysis exported: {export_path}")
        return str(export_path)
    
    def list_datasets(self) -> Dict[str, Dict]:
        """List all available datasets with their basic info."""
        datasets_info = {}
        
        for name, df in self.datasets.items():
            datasets_info[name] = {
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': df.dtypes.value_counts().to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum()
            }
        
        return datasets_info
    
    def get_analysis_summary(self) -> str:
        """Get a summary of all performed analyses."""
        summary_parts = []
        
        summary_parts.append(f"Datasets loaded: {len(self.datasets)}")
        summary_parts.append(f"Analyses performed: {len(self.analysis_results)}")
        
        if self.datasets:
            total_rows = sum(df.shape[0] for df in self.datasets.values())
            total_cols = sum(df.shape[1] for df in self.datasets.values())
            summary_parts.append(f"Total data points: {total_rows:,} rows, {total_cols:,} columns")
        
        return "\n".join(summary_parts)
