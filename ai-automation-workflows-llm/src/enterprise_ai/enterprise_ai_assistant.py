"""
Enterprise AI-like AI Assistant with voice interaction and comprehensive AI capabilities.
"""

import speech_recognition as sr
import pyttsx3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
import cv2
import time
import threading
import queue
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import json
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.llm_client import LLMClient
from src.aiml.aiml_processor import AIMLProcessor
from src.gesture_recognition.gesture_detector import GestureDetector
from src.data_analysis.data_analyzer import DataAnalyzer


class Enterprise AIAssistant:
    """
    Enterprise AI-like AI Assistant with voice, gesture, and comprehensive AI capabilities.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize Enterprise AI Assistant.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or self._load_default_config()
        self.logger = logging.getLogger(__name__)
        
        # Initialize voice components
        self.recognizer = sr.Recognizer()
        self.microphone = sr.Microphone()
        self.engine = pyttsx3.init()
        
        # Initialize AI components
        self.llm_client = LLMClient(self.config.get('llm_provider', 'mock'), self.config.get('llm_config', {}))
        self.aiml_processor = AIMLProcessor()
        self.gesture_detector = GestureDetector()
        self.data_analyzer = DataAnalyzer()
        
        # Initialize deep learning models
        self.models = self._load_models()
        
        # Voice settings
        self._setup_voice()
        
        # Command queue for asynchronous processing
        self.command_queue = queue.Queue()
        self.response_queue = queue.Queue()
        
        # Status tracking
        self.is_listening = False
        self.is_processing = False
        self.gesture_mode = False
        
        self.logger.info("Enterprise AI Assistant initialized successfully")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration."""
        return {
            'llm_provider': 'mock',
            'llm_config': {'mock_response_delay': 0.1},
            'voice': {
                'rate': 200,
                'volume': 0.9,
                'voice_id': None
            },
            'models': {
                'gesture_model': 'models/gesture_recognition.h5',
                'speech_model': 'models/speech_recognition.h5'
            },
            'data': {
                'storage_path': 'data/enterprise_ai_data/',
                'log_file': 'logs/enterprise_ai.log'
            }
        }
    
    def _setup_voice(self):
        """Setup voice engine settings."""
        voice_config = self.config.get('voice', {})
        
        # Set speech rate
        if 'rate' in voice_config:
            self.engine.setProperty('rate', voice_config['rate'])
        
        # Set volume
        if 'volume' in voice_config:
            self.engine.setProperty('volume', voice_config['volume'])
        
        # Set voice (if specified)
        if 'voice_id' in voice_config and voice_config['voice_id']:
            voices = self.engine.getProperty('voices')
            for voice in voices:
                if voice.id == voice_config['voice_id']:
                    self.engine.setProperty('voice', voice)
                    break
    
    def _load_models(self) -> Dict:
        """Load deep learning models."""
        models = {}
        
        try:
            # Load gesture recognition model
            gesture_model_path = self.config['models']['gesture_model']
            if os.path.exists(gesture_model_path):
                models['gesture'] = tf.keras.models.load_model(gesture_model_path)
                self.logger.info("Gesture recognition model loaded")
            
            # Load speech enhancement model
            speech_model_path = self.config['models']['speech_model']
            if os.path.exists(speech_model_path):
                models['speech'] = tf.keras.models.load_model(speech_model_path)
                self.logger.info("Speech enhancement model loaded")
                
        except Exception as e:
            self.logger.warning(f"Could not load models: {str(e)}")
        
        return models
    
    def speak(self, text: str, async_mode: bool = True):
        """
        Speak text using text-to-speech.
        
        Args:
            text: Text to speak
            async_mode: Whether to speak asynchronously
        """
        def speak_worker():
            try:
                self.engine.say(text)
                self.engine.runAndWait()
            except Exception as e:
                self.logger.error(f"Speech error: {str(e)}")
        
        if async_mode:
            threading.Thread(target=speak_worker, daemon=True).start()
        else:
            speak_worker()
    
    def listen_for_command(self, timeout: int = 5) -> Optional[str]:
        """
        Listen for voice command.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Recognized text or None
        """
        try:
            with self.microphone as source:
                self.logger.info("Listening for command...")
                self.recognizer.adjust_for_ambient_noise(source, duration=1)
                audio = self.recognizer.listen(source, timeout=timeout)
            
            # Recognize speech using Google Speech Recognition
            text = self.recognizer.recognize_google(audio)
            self.logger.info(f"Recognized: {text}")
            return text.lower()
            
        except sr.WaitTimeoutError:
            self.logger.info("Listening timeout")
            return None
        except sr.UnknownValueError:
            self.logger.error("Could not understand audio")
            return None
        except sr.RequestError as e:
            self.logger.error(f"Speech recognition error: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Listening error: {str(e)}")
            return None
    
    def process_command(self, command: str) -> str:
        """
        Process voice command and generate response.
        
        Args:
            command: Voice command text
            
        Returns:
            Response text
        """
        try:
            self.is_processing = True
            
            # Check for wake word
            if self._is_wake_word(command):
                return self._handle_wake_word(command)
            
            # Process with AIML first for basic responses
            aiml_response = self.aiml_processor.respond(command)
            if aiml_response and aiml_response != "I don't understand.":
                return aiml_response
            
            # Use LLM for complex commands
            llm_response = self.llm_client.generate_response(
                f"As Enterprise AI AI assistant, respond to: {command}"
            )
            
            # Check for special commands
            if any(keyword in command.lower() for keyword in ['analyze', 'data', 'plot', 'graph']):
                return self._handle_data_command(command)
            elif any(keyword in command.lower() for keyword in ['gesture', 'hand', 'wave']):
                return self._handle_gesture_command(command)
            elif any(keyword in command.lower() for keyword in ['learn', 'train', 'model']):
                return self._handle_ml_command(command)
            
            return llm_response
            
        except Exception as e:
            self.logger.error(f"Command processing error: {str(e)}")
            return "I apologize, but I encountered an error processing your request."
        finally:
            self.is_processing = False
    
    def _is_wake_word(self, command: str) -> bool:
        """Check if command contains wake word."""
        wake_words = ['enterprise_ai', 'hey enterprise_ai', 'ok enterprise_ai', 'assistant']
        return any(word in command.lower() for word in wake_words)
    
    def _handle_wake_word(self, command: str) -> str:
        """Handle wake word activation."""
        self.speak("Yes, I'm listening. How can I help you?")
        return "Enterprise AI activated and ready to assist."
    
    def _handle_data_command(self, command: str) -> str:
        """Handle data analysis commands."""
        try:
            # Extract data analysis request
            if 'analyze' in command.lower():
                # Generate sample data for demonstration
                data = self.data_analyzer.generate_sample_data()
                analysis_result = self.data_analyzer.analyze_data(data)
                
                # Create visualization
                self.data_analyzer.create_visualization(data, 'analysis_plot.png')
                
                response = f"I've analyzed the data. Key insights: {analysis_result['summary']}"
                self.speak(response)
                return response
            
            elif 'plot' in command.lower() or 'graph' in command.lower():
                # Create sample plot
                data = self.data_analyzer.generate_sample_data()
                self.data_analyzer.create_visualization(data, 'command_plot.png')
                
                response = "I've created the visualization for you."
                self.speak(response)
                return response
                
        except Exception as e:
            self.logger.error(f"Data command error: {str(e)}")
            return "I encountered an error processing your data request."
    
    def _handle_gesture_command(self, command: str) -> str:
        """Handle gesture recognition commands."""
        try:
            if 'start' in command.lower() and 'gesture' in command.lower():
                self.gesture_mode = True
                self.speak("Gesture recognition activated. Show me your hand gestures.")
                return "Gesture recognition mode activated."
            
            elif 'stop' in command.lower() and 'gesture' in command.lower():
                self.gesture_mode = False
                self.speak("Gesture recognition deactivated.")
                return "Gesture recognition mode deactivated."
            
            elif self.gesture_mode:
                # Perform gesture detection
                gesture = self.gesture_detector.detect_gesture()
                if gesture:
                    response = f"I detected gesture: {gesture}"
                    self.speak(response)
                    return response
                else:
                    return "No gesture detected."
                    
        except Exception as e:
            self.logger.error(f"Gesture command error: {str(e)}")
            return "I encountered an error with gesture recognition."
    
    def _handle_ml_command(self, command: str) -> str:
        """Handle machine learning commands."""
        try:
            if 'train' in command.lower():
                # Simulate training a model
                self.speak("Training model in progress. This may take a moment.")
                
                # Create and train a simple model
                model = self._create_sample_model()
                training_history = self._train_sample_model(model)
                
                response = f"Model training completed. Final accuracy: {training_history['accuracy']:.2f}"
                self.speak(response)
                return response
            
            elif 'predict' in command.lower():
                # Simulate making predictions
                prediction = self._make_sample_prediction()
                response = f"Based on the data, I predict: {prediction}"
                self.speak(response)
                return response
                
        except Exception as e:
            self.logger.error(f"ML command error: {str(e)}")
            return "I encountered an error with the machine learning operation."
    
    def _create_sample_model(self) -> keras.Model:
        """Create a sample neural network model."""
        model = keras.Sequential([
            keras.layers.Dense(64, activation='relu', input_shape=(10,)),
            keras.layers.Dropout(0.2),
            keras.layers.Dense(32, activation='relu'),
            keras.layers.Dropout(0.2),
            keras.layers.Dense(1, activation='sigmoid')
        ])
        
        model.compile(optimizer='adam',
                     loss='binary_crossentropy',
                     metrics=['accuracy'])
        
        return model
    
    def _train_sample_model(self, model: keras.Model) -> Dict:
        """Train the sample model."""
        # Generate sample data
        X_train = np.random.random((1000, 10))
        y_train = np.random.randint(0, 2, (1000, 1))
        
        # Train model
        history = model.fit(X_train, y_train, epochs=10, batch_size=32, verbose=0)
        
        return {
            'accuracy': max(history.history['accuracy']),
            'loss': min(history.history['loss'])
        }
    
    def _make_sample_prediction(self) -> str:
        """Make a sample prediction."""
        # Generate sample data for prediction
        sample_data = np.random.random((1, 10))
        
        # For demonstration, return a simple prediction
        prediction = np.random.choice(['positive', 'negative'], p=[0.7, 0.3])
        confidence = np.random.uniform(0.8, 0.95)
        
        return f"{prediction} with {confidence:.1%} confidence"
    
    def start_continuous_listening(self):
        """Start continuous listening mode."""
        self.is_listening = True
        
        def listening_worker():
            while self.is_listening:
                command = self.listen_for_command(timeout=3)
                if command:
                    response = self.process_command(command)
                    if response:
                        self.speak(response)
                
                time.sleep(0.1)  # Small delay to prevent excessive CPU usage
        
        # Start listening in separate thread
        listening_thread = threading.Thread(target=listening_worker, daemon=True)
        listening_thread.start()
        
        self.logger.info("Continuous listening started")
    
    def stop_continuous_listening(self):
        """Stop continuous listening mode."""
        self.is_listening = False
        self.logger.info("Continuous listening stopped")
    
    def start_gesture_monitoring(self):
        """Start continuous gesture monitoring."""
        def gesture_worker():
            while self.gesture_mode:
                try:
                    gesture = self.gesture_detector.detect_gesture()
                    if gesture:
                        response = f"Gesture detected: {gesture}"
                        self.speak(response)
                    
                    time.sleep(0.1)  # Small delay
                    
                except Exception as e:
                    self.logger.error(f"Gesture monitoring error: {str(e)}")
                    time.sleep(1)  # Longer delay on error
        
        gesture_thread = threading.Thread(target=gesture_worker, daemon=True)
        gesture_thread.start()
        
        self.logger.info("Gesture monitoring started")
    
    def get_status(self) -> Dict:
        """Get current status of Enterprise AI assistant."""
        return {
            'is_listening': self.is_listening,
            'is_processing': self.is_processing,
            'gesture_mode': self.gesture_mode,
            'models_loaded': list(self.models.keys()),
            'timestamp': datetime.now().isoformat()
        }
    
    def shutdown(self):
        """Shutdown Enterprise AI assistant."""
        self.speak("Shutting down systems. Goodbye!")
        self.stop_continuous_listening()
        self.gesture_mode = False
        self.logger.info("Enterprise AI Assistant shutdown")
