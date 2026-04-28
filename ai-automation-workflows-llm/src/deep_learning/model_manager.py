"""
Deep Learning Model Manager with TensorFlow/Keras for various AI tasks.
"""

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers, models, optimizers, callbacks
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import os
import logging
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import json
from datetime import datetime


class ModelManager:
    """
    Comprehensive deep learning model manager for various AI tasks.
    """
    
    def __init__(self, models_dir: str = "models/"):
        """
        Initialize model manager.
        
        Args:
            models_dir: Directory to save/load models
        """
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.loaded_models = {}
        self.model_metadata = {}
        
        # Load existing models metadata
        self._load_model_metadata()
    
    def _load_model_metadata(self):
        """Load metadata for existing models."""
        metadata_file = self.models_dir / "model_metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    self.model_metadata = json.load(f)
            except Exception as e:
                self.logger.error(f"Failed to load model metadata: {str(e)}")
                self.model_metadata = {}
    
    def _save_model_metadata(self):
        """Save model metadata."""
        metadata_file = self.models_dir / "model_metadata.json"
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self.model_metadata, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save model metadata: {str(e)}")
    
    def create_image_classifier(self, input_shape: Tuple[int, int, int], 
                               num_classes: int, 
                               model_name: str = "image_classifier") -> keras.Model:
        """
        Create a CNN image classifier.
        
        Args:
            input_shape: Input image shape (height, width, channels)
            num_classes: Number of output classes
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        model = models.Sequential([
            layers.Conv2D(32, (3, 3), activation='relu', input_shape=input_shape),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.MaxPooling2D((2, 2)),
            layers.Conv2D(64, (3, 3), activation='relu'),
            layers.Flatten(),
            layers.Dense(64, activation='relu'),
            layers.Dropout(0.5),
            layers.Dense(num_classes, activation='softmax')
        ])
        
        model.compile(optimizer='adam',
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'image_classifier',
            'input_shape': input_shape,
            'num_classes': num_classes,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created image classifier: {model_name}")
        return model
    
    def create_text_classifier(self, vocab_size: int, 
                              max_length: int,
                              num_classes: int,
                              embedding_dim: int = 128,
                              model_name: str = "text_classifier") -> keras.Model:
        """
        Create a text classification model using LSTM.
        
        Args:
            vocab_size: Size of vocabulary
            max_length: Maximum sequence length
            num_classes: Number of output classes
            embedding_dim: Embedding dimension
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        model = models.Sequential([
            layers.Embedding(vocab_size, embedding_dim, input_length=max_length),
            layers.LSTM(128, return_sequences=True),
            layers.Dropout(0.2),
            layers.LSTM(64),
            layers.Dense(32, activation='relu'),
            layers.Dropout(0.5),
            layers.Dense(num_classes, activation='softmax')
        ])
        
        model.compile(optimizer='adam',
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'text_classifier',
            'vocab_size': vocab_size,
            'max_length': max_length,
            'num_classes': num_classes,
            'embedding_dim': embedding_dim,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created text classifier: {model_name}")
        return model
    
    def create_regression_model(self, input_dim: int,
                              hidden_layers: List[int] = [128, 64, 32],
                              model_name: str = "regression_model") -> keras.Model:
        """
        Create a regression model.
        
        Args:
            input_dim: Input feature dimension
            hidden_layers: List of hidden layer sizes
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        model = models.Sequential()
        
        # Input layer
        model.add(layers.Dense(hidden_layers[0], activation='relu', input_shape=(input_dim,)))
        model.add(layers.Dropout(0.2))
        
        # Hidden layers
        for units in hidden_layers[1:]:
            model.add(layers.Dense(units, activation='relu'))
            model.add(layers.Dropout(0.2))
        
        # Output layer
        model.add(layers.Dense(1, activation='linear'))
        
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'regression_model',
            'input_dim': input_dim,
            'hidden_layers': hidden_layers,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created regression model: {model_name}")
        return model
    
    def create_autoencoder(self, input_shape: Tuple[int, ...],
                          encoding_dim: int = 32,
                          model_name: str = "autoencoder") -> keras.Model:
        """
        Create an autoencoder for dimensionality reduction.
        
        Args:
            input_shape: Input data shape
            encoding_dim: Encoding dimension
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        # Encoder
        input_layer = layers.Input(shape=input_shape)
        encoded = layers.Dense(128, activation='relu')(input_layer)
        encoded = layers.Dense(64, activation='relu')(encoded)
        encoded = layers.Dense(encoding_dim, activation='relu')(encoded)
        
        # Decoder
        decoded = layers.Dense(64, activation='relu')(encoded)
        decoded = layers.Dense(128, activation='relu')(decoded)
        decoded = layers.Dense(np.prod(input_shape), activation='sigmoid')(decoded)
        
        # Autoencoder model
        autoencoder = keras.Model(input_layer, decoded)
        
        # Encoder model (for encoding)
        encoder = keras.Model(input_layer, encoded)
        
        autoencoder.compile(optimizer='adam', loss='mse')
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'autoencoder',
            'input_shape': input_shape,
            'encoding_dim': encoding_dim,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created autoencoder: {model_name}")
        return autoencoder
    
    def create_gan_generator(self, latent_dim: int = 100,
                           output_shape: Tuple[int, int, int] = (28, 28, 1),
                           model_name: str = "gan_generator") -> keras.Model:
        """
        Create a GAN generator model.
        
        Args:
            latent_dim: Latent dimension
            output_shape: Output image shape
            model_name: Name for the model
            
        Returns:
            Keras model
        """
        model = models.Sequential([
            layers.Dense(128, activation='relu', input_dim=latent_dim),
            layers.BatchNormalization(),
            layers.LeakyReLU(alpha=0.2),
            layers.Dense(256),
            layers.BatchNormalization(),
            layers.LeakyReLU(alpha=0.2),
            layers.Dense(512),
            layers.BatchNormalization(),
            layers.LeakyReLU(alpha=0.2),
            layers.Dense(np.prod(output_shape), activation='tanh'),
            layers.Reshape(output_shape)
        ])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'gan_generator',
            'latent_dim': latent_dim,
            'output_shape': output_shape,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created GAN generator: {model_name}")
        return model
    
    def create_gan_discriminator(self, input_shape: Tuple[int, int, int] = (28, 28, 1),
                               model_name: str = "gan_discriminator") -> keras.Model:
        """
        Create a GAN discriminator model.
        
        Args:
            input_shape: Input image shape
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        model = models.Sequential([
            layers.Flatten(input_shape=input_shape),
            layers.Dense(512),
            layers.LeakyReLU(alpha=0.2),
            layers.Dense(256),
            layers.LeakyReLU(alpha=0.2),
            layers.Dense(1, activation='sigmoid')
        ])
        
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'gan_discriminator',
            'input_shape': input_shape,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created GAN discriminator: {model_name}")
        return model
    
    def train_model(self, model: keras.Model, 
                   X_train: np.ndarray, 
                   y_train: np.ndarray,
                   X_val: np.ndarray = None,
                   y_val: np.ndarray = None,
                   epochs: int = 10,
                   batch_size: int = 32,
                   model_name: str = "trained_model") -> Dict:
        """
        Train a model and save it.
        
        Args:
            model: Keras model to train
            X_train: Training features
            y_train: Training labels
            X_val: Validation features
            y_val: Validation labels
            epochs: Number of epochs
            batch_size: Batch size
            model_name: Name to save the model
            
        Returns:
            Training history
        """
        # Create callbacks
        model_path = self.models_dir / f"{model_name}.h5"
        callbacks_list = [
            callbacks.ModelCheckpoint(str(model_path), save_best_only=True),
            callbacks.EarlyStopping(patience=5, restore_best_weights=True),
            callbacks.ReduceLROnPlateau(factor=0.2, patience=3)
        ]
        
        # Prepare validation data
        validation_data = None
        if X_val is not None and y_val is not None:
            validation_data = (X_val, y_val)
        
        # Train model
        history = model.fit(
            X_train, y_train,
            validation_data=validation_data,
            epochs=epochs,
            batch_size=batch_size,
            callbacks=callbacks_list,
            verbose=1
        )
        
        # Save training history
        history_data = {key: values.tolist() for key, values in history.history.items()}
        history_path = self.models_dir / f"{model_name}_history.json"
        with open(history_path, 'w') as f:
            json.dump(history_data, f, indent=2)
        
        # Update metadata
        self.model_metadata[model_name].update({
            'trained_at': datetime.now().isoformat(),
            'epochs': epochs,
            'final_loss': history.history['loss'][-1],
            'final_accuracy': history.history.get('accuracy', [0])[-1]
        })
        self._save_model_metadata()
        
        # Load model for immediate use
        self.loaded_models[model_name] = model
        
        self.logger.info(f"Model trained and saved: {model_name}")
        return history.history
    
    def load_model(self, model_name: str) -> Optional[keras.Model]:
        """
        Load a saved model.
        
        Args:
            model_name: Name of the model to load
            
        Returns:
            Loaded Keras model or None
        """
        if model_name in self.loaded_models:
            return self.loaded_models[model_name]
        
        model_path = self.models_dir / f"{model_name}.h5"
        if model_path.exists():
            try:
                model = keras.models.load_model(str(model_path))
                self.loaded_models[model_name] = model
                self.logger.info(f"Model loaded: {model_name}")
                return model
            except Exception as e:
                self.logger.error(f"Failed to load model {model_name}: {str(e)}")
        
        return None
    
    def predict(self, model_name: str, data: np.ndarray) -> np.ndarray:
        """
        Make predictions using a loaded model.
        
        Args:
            model_name: Name of the model
            data: Input data
            
        Returns:
            Predictions
        """
        model = self.load_model(model_name)
        if model is None:
            raise ValueError(f"Model {model_name} not found")
        
        return model.predict(data)
    
    def evaluate_model(self, model_name: str, 
                      X_test: np.ndarray, 
                      y_test: np.ndarray) -> Dict:
        """
        Evaluate a model on test data.
        
        Args:
            model_name: Name of the model
            X_test: Test features
            y_test: Test labels
            
        Returns:
            Evaluation metrics
        """
        model = self.load_model(model_name)
        if model is None:
            raise ValueError(f"Model {model_name} not found")
        
        results = model.evaluate(X_test, y_test, verbose=0)
        
        # Create metrics dictionary
        metrics = {}
        for i, metric_name in enumerate(model.metrics_names):
            metrics[metric_name] = float(results[i])
        
        self.logger.info(f"Model {model_name} evaluated: {metrics}")
        return metrics
    
    def get_model_summary(self, model_name: str) -> str:
        """
        Get model architecture summary.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Model summary string
        """
        model = self.load_model(model_name)
        if model is None:
            return f"Model {model_name} not found"
        
        # Capture summary as string
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            model.summary()
            summary = buffer.getvalue()
        finally:
            sys.stdout = old_stdout
        
        return summary
    
    def list_models(self) -> Dict[str, Dict]:
        """
        List all available models with their metadata.
        
        Returns:
            Dictionary of model metadata
        """
        return self.model_metadata.copy()
    
    def delete_model(self, model_name: str) -> bool:
        """
        Delete a model and its files.
        
        Args:
            model_name: Name of the model to delete
            
        Returns:
            True if deleted successfully
        """
        try:
            # Remove model file
            model_path = self.models_dir / f"{model_name}.h5"
            if model_path.exists():
                model_path.unlink()
            
            # Remove history file
            history_path = self.models_dir / f"{model_name}_history.json"
            if history_path.exists():
                history_path.unlink()
            
            # Remove from loaded models
            if model_name in self.loaded_models:
                del self.loaded_models[model_name]
            
            # Remove from metadata
            if model_name in self.model_metadata:
                del self.model_metadata[model_name]
                self._save_model_metadata()
            
            self.logger.info(f"Model deleted: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete model {model_name}: {str(e)}")
            return False
    
    def create_transfer_learning_model(self, base_model_name: str,
                                    num_classes: int,
                                    trainable_layers: int = 10,
                                    model_name: str = "transfer_model") -> keras.Model:
        """
        Create a transfer learning model using pre-trained base.
        
        Args:
            base_model_name: Name of pre-trained model ('VGG16', 'ResNet50', etc.)
            num_classes: Number of output classes
            trainable_layers: Number of layers to make trainable
            model_name: Name for the model
            
        Returns:
            Compiled Keras model
        """
        # Load pre-trained model
        if base_model_name == 'VGG16':
            base_model = keras.applications.VGG16(weights='imagenet', include_top=False, input_shape=(224, 224, 3))
        elif base_model_name == 'ResNet50':
            base_model = keras.applications.ResNet50(weights='imagenet', include_top=False, input_shape=(224, 224, 3))
        elif base_model_name == 'MobileNet':
            base_model = keras.applications.MobileNet(weights='imagenet', include_top=False, input_shape=(224, 224, 3))
        else:
            raise ValueError(f"Unsupported base model: {base_model_name}")
        
        # Freeze base model layers
        base_model.trainable = False
        
        # Make last few layers trainable
        for layer in base_model.layers[-trainable_layers:]:
            layer.trainable = True
        
        # Add custom head
        inputs = keras.Input(shape=(224, 224, 3))
        x = keras.applications.vgg16.preprocess_input(inputs)
        x = base_model(x, training=False)
        x = layers.GlobalAveragePooling2D()(x)
        x = layers.Dense(128, activation='relu')(x)
        x = layers.Dropout(0.5)(x)
        outputs = layers.Dense(num_classes, activation='softmax')(x)
        
        model = keras.Model(inputs, outputs)
        
        model.compile(optimizer='adam',
                     loss='sparse_categorical_crossentropy',
                     metrics=['accuracy'])
        
        # Save metadata
        self.model_metadata[model_name] = {
            'type': 'transfer_learning',
            'base_model': base_model_name,
            'num_classes': num_classes,
            'trainable_layers': trainable_layers,
            'created_at': datetime.now().isoformat()
        }
        self._save_model_metadata()
        
        self.logger.info(f"Created transfer learning model: {model_name}")
        return model
    
    def plot_training_history(self, model_name: str, save_path: str = None):
        """
        Plot training history for a model.
        
        Args:
            model_name: Name of the model
            save_path: Path to save the plot
        """
        history_path = self.models_dir / f"{model_name}_history.json"
        
        if not history_path.exists():
            self.logger.warning(f"No training history found for {model_name}")
            return
        
        with open(history_path, 'r') as f:
            history = json.load(f)
        
        # Create plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        
        # Loss plot
        ax1.plot(history['loss'], label='Training Loss')
        if 'val_loss' in history:
            ax1.plot(history['val_loss'], label='Validation Loss')
        ax1.set_title('Model Loss')
        ax1.set_xlabel('Epoch')
        ax1.set_ylabel('Loss')
        ax1.legend()
        
        # Accuracy plot
        if 'accuracy' in history:
            ax2.plot(history['accuracy'], label='Training Accuracy')
            if 'val_accuracy' in history:
                ax2.plot(history['val_accuracy'], label='Validation Accuracy')
            ax2.set_title('Model Accuracy')
            ax2.set_xlabel('Epoch')
            ax2.set_ylabel('Accuracy')
            ax2.legend()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()
        
        plt.close()
    
    def export_model(self, model_name: str, export_format: str = 'tflite') -> str:
        """
        Export model to different formats.
        
        Args:
            model_name: Name of the model
            export_format: Export format ('tflite', 'h5', 'savedmodel')
            
        Returns:
            Path to exported model
        """
        model = self.load_model(model_name)
        if model is None:
            raise ValueError(f"Model {model_name} not found")
        
        export_path = self.models_dir / f"{model_name}_exported"
        
        if export_format == 'tflite':
            converter = tf.lite.TFLiteConverter.from_keras_model(model)
            tflite_model = converter.convert()
            
            tflite_path = export_path.with_suffix('.tflite')
            with open(tflite_path, 'wb') as f:
                f.write(tflite_model)
            
            return str(tflite_path)
        
        elif export_format == 'h5':
            h5_path = export_path.with_suffix('.h5')
            model.save(h5_path)
            return str(h5_path)
        
        elif export_format == 'savedmodel':
            savedmodel_path = export_path
            model.save(savedmodel_path)
            return str(savedmodel_path)
        
        else:
            raise ValueError(f"Unsupported export format: {export_format}")
