"""
Hand Gesture Recognition using Computer Vision and Deep Learning.
"""

import cv2
import numpy as np
import mediapipe as mp
import tensorflow as tf
from tensorflow import keras
import time
import logging
from typing import Dict, List, Optional, Tuple
import pickle
import os
from pathlib import Path


class GestureDetector:
    """
    Hand gesture recognition using MediaPipe and deep learning.
    """
    
    def __init__(self, model_path: str = None):
        """
        Initialize gesture detector.
        
        Args:
            model_path: Path to trained gesture classification model
        """
        self.logger = logging.getLogger(__name__)
        
        # Initialize MediaPipe
        self.mp_hands = mp.solutions.hands
        self.mp_drawing = mp.solutions.drawing_utils
        self.mp_drawing_styles = mp.solutions.drawing_styles
        
        # Hands detection
        self.hands = self.mp_hands.Hands(
            static_image_mode=False,
            max_num_hands=2,
            min_detection_confidence=0.5,
            min_tracking_confidence=0.5
        )
        
        # Gesture classification model
        self.model = None
        self.gesture_labels = {
            0: 'thumbs_up',
            1: 'thumbs_down', 
            2: 'peace',
            3: 'rock',
            4: 'paper',
            5: 'scissors',
            6: 'ok',
            7: 'point',
            8: 'open_palm',
            9: 'fist'
        }
        
        # Load model if path provided
        if model_path and os.path.exists(model_path):
            self.load_model(model_path)
        else:
            self.logger.info("No gesture model provided, using rule-based detection")
        
        # Camera setup
        self.cap = None
        self.is_running = False
        
        # Gesture history for smoothing
        self.gesture_history = []
        self.history_size = 5
    
    def load_model(self, model_path: str):
        """Load pre-trained gesture classification model."""
        try:
            self.model = tf.keras.models.load_model(model_path)
            self.logger.info(f"Gesture model loaded from {model_path}")
        except Exception as e:
            self.logger.error(f"Failed to load gesture model: {str(e)}")
    
    def extract_hand_landmarks(self, image: np.ndarray) -> Optional[List[Dict]]:
        """
        Extract hand landmarks from image.
        
        Args:
            image: Input image
            
        Returns:
            List of hand landmarks or None
        """
        # Convert BGR to RGB
        rgb_image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        
        # Process the image
        results = self.hands.process(rgb_image)
        
        if results.multi_hand_landmarks:
            landmarks_list = []
            
            for hand_idx, hand_landmarks in enumerate(results.multi_hand_landmarks):
                # Extract landmark coordinates
                landmarks = []
                for lm in hand_landmarks.landmark:
                    landmarks.extend([lm.x, lm.y, lm.z])
                
                landmarks_list.append({
                    'landmarks': landmarks,
                    'handedness': results.multi_handedness[hand_idx].classification[0].label
                })
            
            return landmarks_list
        
        return None
    
    def extract_features(self, landmarks: List[float]) -> np.ndarray:
        """
        Extract features from hand landmarks for classification.
        
        Args:
            landmarks: Normalized landmark coordinates
            
        Returns:
            Feature array
        """
        landmarks_array = np.array(landmarks)
        
        # Calculate distances between key points
        features = []
        
        # Wrist to fingertips distances
        wrist = landmarks_array[:3]
        fingertips = [
            landmarks_array[12:15],  # Thumb tip
            landmarks_array[24:27],  # Index tip
            landmarks_array[36:39],  # Middle tip
            landmarks_array[48:51],  # Ring tip
            landmarks_array[60:63]   # Pinky tip
        ]
        
        for fingertip in fingertips:
            distance = np.linalg.norm(wrist - fingertip)
            features.append(distance)
        
        # Angles between fingers
        for i in range(len(fingertips) - 1):
            angle = self._calculate_angle(wrist, fingertips[i], fingertips[i+1])
            features.append(angle)
        
        # Finger states (extended or not)
        finger_states = self._get_finger_states(landmarks_array)
        features.extend(finger_states)
        
        return np.array(features)
    
    def _calculate_angle(self, p1: np.ndarray, p2: np.ndarray, p3: np.ndarray) -> float:
        """Calculate angle between three points."""
        v1 = p2 - p1
        v2 = p3 - p2
        
        cosine_angle = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        angle = np.arccos(np.clip(cosine_angle, -1.0, 1.0))
        
        return np.degrees(angle)
    
    def _get_finger_states(self, landmarks: np.ndarray) -> List[int]:
        """
        Determine if fingers are extended (1) or curled (0).
        
        Args:
            landmarks: Hand landmark coordinates
            
        Returns:
            List of finger states
        """
        finger_states = []
        
        # Finger tip and pip landmark indices
        finger_indices = [
            (4, 3),   # Thumb
            (8, 6),   # Index
            (12, 10), # Middle
            (16, 14), # Ring
            (20, 18)  # Pinky
        ]
        
        # Wrist landmark
        wrist = landmarks[:3]
        
        for tip_idx, pip_idx in finger_indices:
            tip = landmarks[tip_idx*3:(tip_idx+1)*3]
            pip = landmarks[pip_idx*3:(pip_idx+1)*3]
            
            # Check if finger is extended (tip is further from wrist than pip)
            tip_distance = np.linalg.norm(tip - wrist)
            pip_distance = np.linalg.norm(pip - wrist)
            
            finger_states.append(1 if tip_distance > pip_distance else 0)
        
        return finger_states
    
    def classify_gesture(self, features: np.ndarray) -> str:
        """
        Classify gesture using trained model or rule-based approach.
        
        Args:
            features: Extracted features
            
        Returns:
            Gesture label
        """
        if self.model is not None:
            # Use deep learning model
            features_reshaped = features.reshape(1, -1)
            prediction = self.model.predict(features_reshaped, verbose=0)
            gesture_idx = np.argmax(prediction[0])
            
            return self.gesture_labels.get(gesture_idx, 'unknown')
        else:
            # Rule-based gesture recognition
            return self._rule_based_classification(features)
    
    def _rule_based_classification(self, features: np.ndarray) -> str:
        """
        Rule-based gesture classification.
        
        Args:
            features: Extracted features
            
        Returns:
            Gesture label
        """
        # This is a simplified rule-based approach
        # In practice, you'd want more sophisticated rules
        
        # Extract finger states (last 5 features)
        finger_states = features[-5:] if len(features) >= 5 else [0, 0, 0, 0, 0]
        
        # Simple gesture rules
        if finger_states == [1, 0, 0, 0, 0]:
            return 'thumbs_up'
        elif finger_states == [0, 1, 1, 0, 0]:
            return 'peace'
        elif finger_states == [0, 0, 0, 0, 0]:
            return 'fist'
        elif finger_states == [1, 1, 1, 1, 1]:
            return 'open_palm'
        elif finger_states == [0, 1, 0, 0, 0]:
            return 'point'
        else:
            return 'unknown'
    
    def detect_gesture(self, image: np.ndarray = None) -> Optional[str]:
        """
        Detect gesture from image or camera feed.
        
        Args:
            image: Input image (if None, uses camera)
            
        Returns:
            Detected gesture or None
        """
        try:
            if image is None:
                # Use camera
                if self.cap is None:
                    self.cap = cv2.VideoCapture(0)
                
                ret, frame = self.cap.read()
                if not ret:
                    return None
                
                image = frame
            
            # Extract hand landmarks
            landmarks_data = self.extract_hand_landmarks(image)
            
            if landmarks_data:
                # Use first detected hand
                landmarks = landmarks_data[0]['landmarks']
                
                # Extract features
                features = self.extract_features(landmarks)
                
                # Classify gesture
                gesture = self.classify_gesture(features)
                
                # Add to history for smoothing
                self.gesture_history.append(gesture)
                if len(self.gesture_history) > self.history_size:
                    self.gesture_history.pop(0)
                
                # Return most common gesture in history
                if len(self.gesture_history) >= 3:
                    gesture_counts = {}
                    for g in self.gesture_history:
                        gesture_counts[g] = gesture_counts.get(g, 0) + 1
                    
                    most_common = max(gesture_counts.items(), key=lambda x: x[1])
                    return most_common[0] if most_common[1] >= 2 else gesture
                
                return gesture
            
            return None
            
        except Exception as e:
            self.logger.error(f"Gesture detection error: {str(e)}")
            return None
    
    def start_camera_detection(self, window_name: str = "Gesture Detection"):
        """
        Start real-time gesture detection from camera.
        
        Args:
            window_name: Name of the display window
        """
        self.is_running = True
        self.cap = cv2.VideoCapture(0)
        
        if not self.cap.isOpened():
            self.logger.error("Could not open camera")
            return
        
        try:
            while self.is_running:
                ret, frame = self.cap.read()
                if not ret:
                    break
                
                # Flip frame horizontally for mirror effect
                frame = cv2.flip(frame, 1)
                
                # Detect gesture
                gesture = self.detect_gesture(frame)
                
                # Draw hand landmarks
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                results = self.hands.process(rgb_frame)
                
                if results.multi_hand_landmarks:
                    for hand_landmarks in results.multi_hand_landmarks:
                        self.mp_drawing.draw_landmarks(
                            frame, hand_landmarks, self.mp_hands.HAND_CONNECTIONS,
                            self.mp_drawing_styles.get_default_hand_landmarks_style(),
                            self.mp_drawing_styles.get_default_hand_connections_style()
                        )
                
                # Display gesture
                if gesture:
                    cv2.putText(frame, f"Gesture: {gesture}", (10, 30),
                               cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
                
                # Display frame
                cv2.imshow(window_name, frame)
                
                # Exit on 'q' key
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
                
                time.sleep(0.03)  # ~30 FPS
                
        except KeyboardInterrupt:
            self.logger.info("Gesture detection interrupted")
        finally:
            self.stop_camera_detection()
    
    def stop_camera_detection(self):
        """Stop camera detection."""
        self.is_running = False
        if self.cap:
            self.cap.release()
        cv2.destroyAllWindows()
    
    def train_gesture_model(self, data_path: str, save_path: str):
        """
        Train a gesture classification model.
        
        Args:
            data_path: Path to training data
            save_path: Path to save trained model
        """
        try:
            # Load training data
            X_train, y_train = self._load_training_data(data_path)
            
            # Create model
            model = keras.Sequential([
                keras.layers.Dense(128, activation='relu', input_shape=(X_train.shape[1],)),
                keras.layers.Dropout(0.3),
                keras.layers.Dense(64, activation='relu'),
                keras.layers.Dropout(0.3),
                keras.layers.Dense(32, activation='relu'),
                keras.layers.Dense(len(self.gesture_labels), activation='softmax')
            ])
            
            model.compile(optimizer='adam',
                         loss='sparse_categorical_crossentropy',
                         metrics=['accuracy'])
            
            # Train model
            history = model.fit(X_train, y_train, epochs=50, batch_size=32, 
                              validation_split=0.2, verbose=1)
            
            # Save model
            model.save(save_path)
            self.logger.info(f"Model saved to {save_path}")
            
            return model, history
            
        except Exception as e:
            self.logger.error(f"Training error: {str(e)}")
            return None, None
    
    def _load_training_data(self, data_path: str) -> Tuple[np.ndarray, np.ndarray]:
        """
        Load training data for gesture classification.
        
        Args:
            data_path: Path to training data
            
        Returns:
            Features and labels arrays
        """
        # This is a placeholder - in practice, you'd load real training data
        # For demonstration, generate synthetic data
        
        num_samples = 1000
        num_features = 20  # Adjust based on your feature extraction
        
        X_train = np.random.random((num_samples, num_features))
        y_train = np.random.randint(0, len(self.gesture_labels), num_samples)
        
        return X_train, y_train
    
    def get_gesture_statistics(self) -> Dict:
        """
        Get statistics about detected gestures.
        
        Returns:
            Dictionary with gesture statistics
        """
        if not self.gesture_history:
            return {'total_detections': 0}
        
        gesture_counts = {}
        for gesture in self.gesture_history:
            gesture_counts[gesture] = gesture_counts.get(gesture, 0) + 1
        
        return {
            'total_detections': len(self.gesture_history),
            'gesture_counts': gesture_counts,
            'most_common': max(gesture_counts.items(), key=lambda x: x[1]) if gesture_counts else None,
            'accuracy': self._calculate_detection_accuracy()
        }
    
    def _calculate_detection_accuracy(self) -> float:
        """
        Calculate detection confidence based on history consistency.
        
        Returns:
            Accuracy score (0-1)
        """
        if len(self.gesture_history) < 3:
            return 0.5
        
        # Calculate consistency in recent detections
        recent_gestures = self.gesture_history[-5:]
        gesture_counts = {}
        for gesture in recent_gestures:
            gesture_counts[gesture] = gesture_counts.get(gesture, 0) + 1
        
        if not gesture_counts:
            return 0.5
        
        # Accuracy based on most common gesture frequency
        most_common_count = max(gesture_counts.values())
        return most_common_count / len(recent_gestures)
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        self.stop_camera_detection()
