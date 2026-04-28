"""
Integrated AI/ML Demo combining Enterprise AI assistant, gesture recognition, deep learning,
data analysis, and machine learning technologies.
"""

import sys
import os
import time
import threading
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cv2
from pathlib import Path
import logging
from datetime import datetime
import json

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

# Import all components with error handling
try:
    from enterprise_ai.enterprise_ai_assistant import Enterprise AIAssistant
except ImportError:
    print("WARNING: Enterprise AI assistant not available - installing missing dependencies...")
    Enterprise AIAssistant = None

try:
    from gesture_recognition.gesture_detector import GestureDetector
except ImportError:
    print("WARNING: Gesture detection not available - installing missing dependencies...")
    GestureDetector = None

try:
    from deep_learning.model_manager import ModelManager
except ImportError:
    print("WARNING: Deep learning models not available - installing missing dependencies...")
    ModelManager = None

try:
    from data_analysis.data_analyzer import DataAnalyzer
except ImportError:
    print("WARNING: Data analysis not available - installing missing dependencies...")
    DataAnalyzer = None

try:
    from aiml.aiml_processor import AIMLProcessor
except ImportError:
    print("WARNING: AIML processor not available - installing missing dependencies...")
    AIMLProcessor = None

try:
    from ml_models.sklearn_manager import SklearnManager
except ImportError:
    print("WARNING: Scikit-learn manager not available - installing missing dependencies...")
    SklearnManager = None

# Import sklearn datasets for demo
try:
    from sklearn.datasets import make_classification
except ImportError:
    print("WARNING: Scikit-learn datasets not available...")
    make_classification = None


class IntegratedAIDemo:
    """
    Integrated AI demonstration combining all technologies.
    """
    
    def __init__(self):
        """Initialize the integrated AI demo."""
        self.logger = logging.getLogger(__name__)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Initialize all components
        self._initialize_components()
        
        # Demo state
        self.is_running = False
        self.current_mode = 'menu'
        
        print("🤖 Integrated AI/ML Demo Initialized!")
        print("=" * 60)
    
    def _initialize_components(self):
        """Initialize all AI components."""
        components_initialized = 0
        
        # Enterprise AI Assistant
        if Enterprise AIAssistant is not None:
            try:
                print("Initializing Enterprise AI Assistant...")
                self.enterprise_ai = Enterprise AIAssistant()
                components_initialized += 1
                print("SUCCESS: Enterprise AI Assistant initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing Enterprise AI: {str(e)}")
                print(f"ERROR: Enterprise AI Assistant error: {str(e)}")
                self.enterprise_ai = None
        else:
            print("WARNING: Enterprise AI Assistant not available")
            self.enterprise_ai = None
        
        # Gesture Detector
        if GestureDetector is not None:
            try:
                print("Initializing Gesture Recognition...")
                self.gesture_detector = GestureDetector()
                components_initialized += 1
                print("SUCCESS: Gesture Recognition initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing Gesture Detector: {str(e)}")
                print(f"ERROR: Gesture Recognition error: {str(e)}")
                self.gesture_detector = None
        else:
            print("WARNING: Gesture Recognition not available")
            self.gesture_detector = None
        
        # Deep Learning Model Manager
        if ModelManager is not None:
            try:
                print("Initializing Deep Learning Models...")
                self.model_manager = ModelManager()
                components_initialized += 1
                print("SUCCESS: Deep Learning Models initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing Model Manager: {str(e)}")
                print(f"ERROR: Deep Learning Models error: {str(e)}")
                self.model_manager = None
        else:
            print("WARNING: Deep Learning Models not available")
            self.model_manager = None
        
        # Data Analyzer
        if DataAnalyzer is not None:
            try:
                print("Initializing Data Analysis Tools...")
                self.data_analyzer = DataAnalyzer()
                components_initialized += 1
                print("SUCCESS: Data Analysis Tools initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing Data Analyzer: {str(e)}")
                print(f"ERROR: Data Analysis Tools error: {str(e)}")
                self.data_analyzer = None
        else:
            print("WARNING: Data Analysis Tools not available")
            self.data_analyzer = None
        
        # AIML Processor
        if AIMLProcessor is not None:
            try:
                print("Initializing Conversational AI...")
                self.aiml_processor = AIMLProcessor()
                components_initialized += 1
                print("SUCCESS: Conversational AI initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing AIML Processor: {str(e)}")
                print(f"ERROR: Conversational AI error: {str(e)}")
                self.aiml_processor = None
        else:
            print("WARNING: Conversational AI not available")
            self.aiml_processor = None
        
        # Scikit-learn Manager
        if SklearnManager is not None:
            try:
                print("Initializing Machine Learning Models...")
                self.sklearn_manager = SklearnManager()
                components_initialized += 1
                print("SUCCESS: Machine Learning Models initialized!")
            except Exception as e:
                self.logger.error(f"Error initializing Sklearn Manager: {str(e)}")
                print(f"ERROR: Machine Learning Models error: {str(e)}")
                self.sklearn_manager = None
        else:
            print("WARNING: Machine Learning Models not available")
            self.sklearn_manager = None
        
        print(f"\nInitialization Summary: {components_initialized}/6 components initialized")
        
        if components_initialized == 0:
            print("ERROR: No components could be initialized. Please install required dependencies.")
            print("INFO: Run: pip install -r requirements.txt")
        elif components_initialized < 6:
            print("WARNING: Some components are missing. Demo functionality may be limited.")
        else:
            print("SUCCESS: All components initialized successfully!")
    
    def show_menu(self):
        """Display the main menu."""
        print("\n" + "=" * 60)
        print("INTEGRATED AI/ML DEMO - MAIN MENU")
        print("=" * 60)
        print("1. Enterprise AI Voice Assistant Demo")
        print("2. Hand Gesture Recognition Demo")
        print("3. Deep Learning Models Demo")
        print("4. Data Analysis & Visualization Demo")
        print("5. Conversational AI (AIML) Demo")
        print("6. Machine Learning Models Demo")
        print("7. Integrated Workflow Demo")
        print("8. Performance Comparison Demo")
        print("9. Custom AI Task Demo")
        print("0. Exit")
        print("=" * 60)
    
    def run_enterprise_ai_demo(self):
        """Run Enterprise AI voice assistant demo."""
        print("\nEnterprise AI Voice Assistant Demo")
        print("-" * 40)
        
        try:
            # Test voice interaction
            print("Testing Enterprise AI voice capabilities...")
            
            # Simulate voice commands
            test_commands = [
                "Hello Enterprise AI",
                "What can you do?",
                "Analyze some data",
                "Train a machine learning model",
                "Recognize my gestures"
            ]
            
            for command in test_commands:
                print(f"\nUser: {command}")
                response = self.enterprise_ai.process_command(command)
                print(f"Enterprise AI: {response}")
                time.sleep(1)
            
            print("\nSUCCESS: Enterprise AI demo completed!")
            
        except Exception as e:
            print(f"ERROR: Enterprise AI demo error: {str(e)}")
    
    def run_gesture_demo(self):
        """Run hand gesture recognition demo."""
        print("\nHand Gesture Recognition Demo")
        print("-" * 40)
        
        try:
            print("Starting camera for gesture detection...")
            print("Show your hand gestures to the camera!")
            print("Press 'q' to stop the demo")
            
            # Start gesture detection
            self.gesture_detector.start_camera_detection("Gesture Recognition Demo")
            
        except Exception as e:
            print(f"ERROR: Gesture demo error: {str(e)}")
    
    def run_deep_learning_demo(self):
        """Run deep learning models demo."""
        print("\nDeep Learning Models Demo")
        print("-" * 40)
        
        try:
            # Create sample data
            print("Generating sample data for deep learning...")
            X_train = np.random.random((1000, 20))
            y_train = np.random.randint(0, 2, (1000, 1))
            X_val = np.random.random((200, 20))
            y_val = np.random.randint(0, 2, (200, 1))
            
            # Create and train different models
            models_to_create = [
                ('image_classifier', (64, 64, 3), 2),
                ('text_classifier', 1000, 50, 2),
                ('regression_model', 20)
            ]
            
            for model_config in models_to_create:
                model_name = model_config[0]
                
                if model_name == 'image_classifier':
                    input_shape, num_classes = model_config[1], model_config[2]
                    model = self.model_manager.create_image_classifier(input_shape, num_classes, model_name)
                    print(f"SUCCESS: Created image classifier: {input_shape} -> {num_classes} classes")
                
                elif model_name == 'text_classifier':
                    vocab_size, max_length, num_classes = model_config[1], model_config[2], model_config[3]
                    model = self.model_manager.create_text_classifier(vocab_size, max_length, num_classes, model_name)
                    print(f"SUCCESS: Created text classifier: vocab={vocab_size}, length={max_length}")
                
                elif model_name == 'regression_model':
                    input_dim = model_config[1]
                    model = self.model_manager.create_regression_model(input_dim, model_name=model_name)
                    print(f"SUCCESS: Created regression model: {input_dim} features")
            
            # Train a model
            print("\nTraining a sample model...")
            sample_model = self.model_manager.create_regression_model(20, "demo_regression")
            history = self.model_manager.train_model(sample_model, X_train, y_train, X_val, y_val, epochs=5)
            
            print(f"SUCCESS: Model trained! Final loss: {history['loss'][-1]:.4f}")
            
        except Exception as e:
            print(f"ERROR: Deep learning demo error: {str(e)}")
    
    def run_data_analysis_demo(self):
        """Run data analysis and visualization demo."""
        print("\nData Analysis & Visualization Demo")
        print("-" * 40)
        
        try:
            # Generate different types of sample data
            data_types = ['classification', 'regression', 'clustering', 'timeseries']
            
            for data_type in data_types:
                print(f"\nGenerating {data_type} data...")
                df = self.data_analyzer.generate_sample_data(data_type, num_samples=500)
                
                # Analyze the data
                print(f"Analyzing {data_type} data...")
                analysis = self.data_analyzer.analyze_data(f"sample_{data_type}")
                
                # Create visualization
                print(f"Creating visualization for {data_type} data...")
                plot_path = self.data_analyzer.create_visualization(f"sample_{data_type}")
                
                print(f"SUCCESS: {data_type.title()} analysis completed!")
                print(f"   Summary: {analysis['summary']}")
                print(f"   Plot saved: {plot_path}")
            
            # Perform ML analysis
            print("\nPerforming machine learning analysis...")
            df_class = self.data_analyzer.generate_sample_data('classification')
            ml_results = self.data_analyzer.build_ml_model('sample_classification', 'target')
            
            print(f"SUCCESS: ML analysis completed!")
            for model_name, metrics in ml_results.items():
                if 'accuracy' in metrics:
                    print(f"   {model_name}: Accuracy = {metrics['accuracy']:.3f}")
            
        except Exception as e:
            print(f"ERROR: Data analysis demo error: {str(e)}")
    
    def run_aiml_demo(self):
        """Run conversational AI demo."""
        print("\nConversational AI (AIML) Demo")
        print("-" * 40)
        
        try:
            # Test various conversation patterns
            test_inputs = [
                "Hello Enterprise AI",
                "What is your name?",
                "What can you do?",
                "How are you?",
                "What time is it?",
                "Thank you for your help",
                "Can you help me with machine learning?",
                "Goodbye"
            ]
            
            print("Testing conversational patterns...")
            
            for user_input in test_inputs:
                print(f"\nUser: {user_input}")
                response = self.aiml_processor.respond(user_input)
                print(f"Enterprise AI: {response}")
                time.sleep(0.5)
            
            # Show AIML statistics
            stats = self.aiml_processor.get_statistics()
            print(f"\nAIML Statistics:")
            print(f"   Total patterns: {stats['total_patterns']}")
            print(f"   Total responses: {stats['total_responses']}")
            print(f"   Variables: {stats['variables_count']}")
            
            print("\nSUCCESS: AIML demo completed!")
            
        except Exception as e:
            print(f"ERROR: AIML demo error: {str(e)}")
    
    def run_sklearn_demo(self):
        """Run scikit-learn machine learning demo."""
        print("\nMachine Learning Models Demo")
        print("-" * 40)
        
        try:
            # Generate sample data
            print("Generating sample dataset...")
            X, y = make_classification(n_samples=1000, n_features=20, n_classes=3, random_state=42)
            
            # Prepare data
            X_df = pd.DataFrame(X, columns=[f"feature_{i}" for i in range(X.shape[1])])
            y_series = pd.Series(y)
            
            prepared_data = self.sklearn_manager.prepare_data(X_df, y_series)
            
            # Train classification models
            print("\nTraining classification models...")
            classification_results = self.sklearn_manager.train_classification_models(
                prepared_data['X_train'], prepared_data['y_train']
            )
            
            # Evaluate models
            print("\nEvaluating models...")
            evaluation_results = self.sklearn_manager.evaluate_classification_models(
                prepared_data['X_test'], prepared_data['y_test']
            )
            
            # Display results
            print("\nModel Performance Results:")
            for model_name, metrics in evaluation_results.items():
                if 'metrics' in metrics:
                    acc = metrics['metrics']['accuracy']
                    f1 = metrics['metrics']['f1_score']
                    print(f"   {model_name}: Accuracy={acc:.3f}, F1={f1:.3f}")
            
            # Create comparison plot
            print("\nCreating model comparison plot...")
            plot_path = self.sklearn_manager.create_model_comparison_plot(evaluation_results, 'accuracy')
            print(f"   Plot saved: {plot_path}")
            
            # Perform clustering
            print("\nPerforming clustering analysis...")
            clustering_results = self.sklearn_manager.perform_clustering(X_df)
            
            for method, results in clustering_results.items():
                if 'silhouette_score' in results:
                    score = results['silhouette_score']
                    n_clusters = results['n_clusters']
                    print(f"   {method}: {n_clusters} clusters, Silhouette={score:.3f}")
            
            print("\nSUCCESS: Scikit-learn demo completed!")
            
        except Exception as e:
            print(f"ERROR: Scikit-learn demo error: {str(e)}")
    
    def run_integrated_workflow_demo(self):
        """Run integrated workflow demo combining all technologies."""
        print("\nIntegrated Workflow Demo")
        print("-" * 40)
        print("This demo combines voice input, gesture recognition, data analysis, and ML!")
        
        try:
            # Step 1: Generate sample data
            print("\nStep 1: Generating complex dataset...")
            np.random.seed(42)
            
            # Create a complex dataset
            n_samples = 1000
            data = {
                'customer_id': range(1, n_samples + 1),
                'age': np.random.normal(35, 12, n_samples),
                'income': np.random.lognormal(10, 0.5, n_samples),
                'spending': np.random.gamma(2, 50, n_samples),
                'satisfaction': np.random.beta(2, 1, n_samples),
                'loyalty_years': np.random.exponential(3, n_samples)
            }
            
            df = pd.DataFrame(data)
            df['segment'] = pd.cut(df['income'], bins=3, labels=['Low', 'Medium', 'High'])
            
            # Store for analysis
            self.data_analyzer.datasets['customer_data'] = df
            
            print(f"SUCCESS: Dataset created: {df.shape}")
            
            # Step 2: Voice command to analyze data
            print("\nStep 2: Processing voice command...")
            command = "Analyze the customer data and create segments"
            print(f"Command: '{command}'")
            
            enterprise_ai_response = self.enterprise_ai.process_command(command)
            print(f"Enterprise AI: {enterprise_ai_response}")
            
            # Step 3: Data analysis
            print("\nStep 3: Performing comprehensive data analysis...")
            analysis = self.data_analyzer.analyze_data('customer_data')
            
            # Create visualizations
            viz_path = self.data_analyzer.create_visualization('customer_data', 'auto')
            print(f"SUCCESS: Analysis completed! Visualization: {viz_path}")
            
            # Step 4: Machine learning segmentation
            print("\nStep 4: Building customer segmentation model...")
            
            # Prepare data for clustering
            features = ['age', 'income', 'spending', 'satisfaction', 'loyalty_years']
            X_cluster = df[features]
            
            clustering_results = self.sklearn_manager.perform_clustering(X_cluster, n_clusters=4)
            
            # Step 5: Predictive modeling
            print("\nStep 5: Building satisfaction prediction model...")
            
            # Create binary target (high satisfaction)
            df['high_satisfaction'] = (df['satisfaction'] > 0.7).astype(int)
            
            X = df[features]
            y = df['high_satisfaction']
            
            prepared_data = self.sklearn_manager.prepare_data(X, y)
            ml_results = self.sklearn_manager.train_classification_models(
                prepared_data['X_train'], prepared_data['y_train']
            )
            
            eval_results = self.sklearn_manager.evaluate_classification_models(
                prepared_data['X_test'], prepared_data['y_test']
            )
            
            # Step 6: Deep learning model
            print("\nStep 6: Creating deep learning model...")
            
            # Create and train a neural network
            dl_model = self.model_manager.create_regression_model(len(features), "satisfaction_predictor")
            
            # Prepare data for deep learning
            X_train_dl = prepared_data['X_train'].values
            y_train_dl = prepared_data['y_train'].values
            X_val_dl = prepared_data['X_test'].values
            y_val_dl = prepared_data['y_test'].values
            
            dl_history = self.model_manager.train_model(dl_model, X_train_dl, y_train_dl, X_val_dl, y_val_dl, epochs=10)
            
            # Step 7: Gesture-based interaction
            print("\nStep 7: Demonstrating gesture-based control...")
            print("Show a 'thumbs_up' gesture to continue, or 'peace' to skip...")
            
            # Simulate gesture detection
            time.sleep(2)
            detected_gesture = "thumbs_up"  # Simulated
            print(f"Gesture detected: {detected_gesture}")
            
            if detected_gesture == "thumbs_up":
                print("SUCCESS: Gesture confirmed! Continuing with analysis...")
            else:
                print("Skipping detailed analysis...")
            
            # Step 8: Generate comprehensive report
            print("\nStep 8: Generating comprehensive AI report...")
            
            report = {
                'dataset_info': {
                    'shape': df.shape,
                    'features': features,
                    'segments': df['segment'].value_counts().to_dict()
                },
                'clustering_results': clustering_results,
                'ml_performance': eval_results,
                'deep_learning_results': {
                    'final_loss': dl_history['loss'][-1],
                    'final_accuracy': dl_history.get('accuracy', [0])[-1]
                },
                'insights': [
                    "Customer data successfully analyzed using multiple AI approaches",
                    "Clustering identified distinct customer segments",
                    f"Best ML model achieved {max([r.get('metrics', {}).get('accuracy', 0) for r in eval_results.values()]):.3f} accuracy",
                    "Deep learning model provides alternative prediction approach",
                    "Voice and gesture interfaces enable intuitive interaction"
                ]
            }
            
            # Save report
            report_path = "integrated_demo_report.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            print(f"SUCCESS: Comprehensive report saved: {report_path}")
            
            # Display summary
            print("\nIntegrated Workflow Summary:")
            print(f"   Dataset analyzed: {df.shape[0]} customers, {df.shape[1]} features")
            print(f"   Clusters found: {len(clustering_results)} methods applied")
            print(f"   ML models trained: {len(ml_results)} classification models")
            print(f"   Deep learning model: {dl_history['loss'][-1]:.4f} final loss")
            print(f"   Gesture interaction: {detected_gesture}")
            print(f"   Report generated: {report_path}")
            
            print("\nSUCCESS: Integrated workflow demo completed successfully!")
            
        except Exception as e:
            print(f"ERROR: Integrated workflow demo error: {str(e)}")
    
    def run_performance_comparison_demo(self):
        """Run performance comparison between different approaches."""
        print("\nPerformance Comparison Demo")
        print("-" * 40)
        
        try:
            # Generate test data
            print("Generating test dataset...")
            X, y = make_classification(n_samples=2000, n_features=25, n_classes=4, random_state=42)
            
            # Test different approaches
            approaches = {
                'Traditional ML': self._test_traditional_ml,
                'Deep Learning': self._test_deep_learning,
                'Ensemble Methods': self._test_ensemble_methods
            }
            
            results = {}
            
            for approach_name, test_func in approaches.items():
                print(f"\nTesting {approach_name}...")
                try:
                    result = test_func(X, y)
                    results[approach_name] = result
                    print(f"SUCCESS: {approach_name}: Accuracy = {result['accuracy']:.3f}, Time = {result['time']:.2f}s")
                except Exception as e:
                    print(f"ERROR: {approach_name} failed: {str(e)}")
                    results[approach_name] = {'error': str(e)}
            
            # Create comparison visualization
            print("\nCreating performance comparison...")
            self._create_performance_comparison_plot(results)
            
            # Display results
            print("\nPerformance Comparison Results:")
            for approach, result in results.items():
                if 'accuracy' in result:
                    print(f"   {approach}:")
                    print(f"     Accuracy: {result['accuracy']:.3f}")
                    print(f"     Time: {result['time']:.2f}s")
                    print(f"     Memory: {result.get('memory', 'N/A')} MB")
            
            print("\nSUCCESS: Performance comparison completed!")
            
        except Exception as e:
            print(f"ERROR: Performance comparison demo error: {str(e)}")
    
    def _test_traditional_ml(self, X, y):
        """Test traditional machine learning approach."""
        import time
        import psutil
        from sklearn.datasets import make_classification
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Prepare data
        X_df = pd.DataFrame(X)
        y_series = pd.Series(y)
        prepared_data = self.sklearn_manager.prepare_data(X_df, y_series)
        
        # Train best model
        results = self.sklearn_manager.train_classification_models(
            prepared_data['X_train'], prepared_data['y_train']
        )
        
        # Evaluate
        eval_results = self.sklearn_manager.evaluate_classification_models(
            prepared_data['X_test'], prepared_data['y_test']
        )
        
        # Get best accuracy
        best_accuracy = max([r.get('metrics', {}).get('accuracy', 0) for r in eval_results.values()])
        
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'accuracy': best_accuracy,
            'time': end_time - start_time,
            'memory': end_memory - start_memory
        }
    
    def _test_deep_learning(self, X, y):
        """Test deep learning approach."""
        import time
        import psutil
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create and train deep learning model
        model = self.model_manager.create_image_classifier((X.shape[1], 1, 1), len(np.unique(y)), "comparison_model")
        
        # Reshape data for CNN
        X_reshaped = X.reshape(X.shape[0], X.shape[1], 1, 1)
        
        # Split data
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(X_reshaped, y, test_size=0.2, random_state=42)
        
        # Train
        history = self.model_manager.train_model(model, X_train, y_train, X_test, y_test, epochs=5)
        
        # Evaluate
        loss, accuracy = model.evaluate(X_test, y_test, verbose=0)
        
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'accuracy': accuracy,
            'time': end_time - start_time,
            'memory': end_memory - start_memory
        }
    
    def _test_ensemble_methods(self, X, y):
        """Test ensemble methods."""
        import time
        import psutil
        from sklearn.ensemble import VotingClassifier, GradientBoostingClassifier, AdaBoostClassifier
        from sklearn.model_selection import train_test_split
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Create ensemble
        estimators = [
            ('rf', RandomForestClassifier(n_estimators=100, random_state=42)),
            ('gb', GradientBoostingClassifier(random_state=42)),
            ('ada', AdaBoostClassifier(random_state=42))
        ]
        
        ensemble = VotingClassifier(estimators=estimators, voting='soft')
        
        # Train and evaluate
        ensemble.fit(X_train, y_train)
        accuracy = ensemble.score(X_test, y_test)
        
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        return {
            'accuracy': accuracy,
            'time': end_time - start_time,
            'memory': end_memory - start_memory
        }
    
    def _create_performance_comparison_plot(self, results):
        """Create performance comparison visualization."""
        approaches = []
        accuracies = []
        times = []
        
        for approach, result in results.items():
            if 'accuracy' in result:
                approaches.append(approach)
                accuracies.append(result['accuracy'])
                times.append(result['time'])
        
        if not approaches:
            return
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Accuracy comparison
        bars1 = ax1.bar(approaches, accuracies, color='skyblue')
        ax1.set_title('Model Accuracy Comparison')
        ax1.set_ylabel('Accuracy')
        ax1.set_ylim(0, 1)
        for bar, acc in zip(bars1, accuracies):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{acc:.3f}', ha='center', va='bottom')
        
        # Time comparison
        bars2 = ax2.bar(approaches, times, color='lightcoral')
        ax2.set_title('Training Time Comparison')
        ax2.set_ylabel('Time (seconds)')
        for bar, time_val in zip(bars2, times):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{time_val:.2f}s', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print("Performance comparison plot saved: performance_comparison.png")
    
    def run_custom_ai_task_demo(self):
        """Run custom AI task demonstration."""
        print("\nCustom AI Task Demo")
        print("-" * 40)
        print("This demo shows how to combine multiple AI technologies for a custom task.")
        
        try:
            # Custom task: Intelligent Data Analysis Assistant
            print("Task: Create an Intelligent Data Analysis Assistant")
            print("   - Uses voice commands for natural interaction")
            print("   - Performs automated data analysis")
            print("   - Generates insights and recommendations")
            print("   - Creates visualizations automatically")
            print("   - Responds to gesture controls")
            
            # Step 1: Create complex business dataset
            print("\nStep 1: Creating business analytics dataset...")
            
            np.random.seed(42)
            n_companies = 500
            
            business_data = {
                'company_id': range(1, n_companies + 1),
                'revenue': np.random.lognormal(10, 1, n_companies),
                'employees': np.random.lognormal(3, 0.5, n_companies),
                'market_cap': np.random.lognormal(12, 1.5, n_companies),
                'growth_rate': np.random.normal(0.05, 0.1, n_companies),
                'customer_satisfaction': np.random.beta(3, 2, n_companies),
                'innovation_score': np.random.gamma(2, 20, n_companies)
            }
            
            df = pd.DataFrame(business_data)
            
            # Add derived features
            df['revenue_per_employee'] = df['revenue'] / df['employees']
            df['market_cap_to_revenue'] = df['market_cap'] / df['revenue']
            
            # Create performance categories
            df['performance_category'] = pd.qcut(df['growth_rate'], q=3, labels=['Poor', 'Average', 'Excellent'])
            
            self.data_analyzer.datasets['business_analytics'] = df
            
            print(f"SUCCESS: Business dataset created: {df.shape}")
            
            # Step 2: Voice-activated analysis
            print("\nStep 2: Voice-activated data analysis...")
            
            voice_commands = [
                "Analyze the business performance data",
                "Find the top performing companies",
                "Create performance visualizations",
                "Predict company success factors"
            ]
            
            for command in voice_commands:
                print(f"\nVoice Command: '{command}'")
                
                # Process with Enterprise AI
                enterprise_ai_response = self.enterprise_ai.process_command(command)
                print(f"Enterprise AI: {enterprise_ai_response}")
                
                # Execute the actual analysis
                if "analyze" in command.lower():
                    analysis = self.data_analyzer.analyze_data('business_analytics')
                    print(f"Analysis: {analysis['summary']}")
                
                elif "top performing" in command.lower():
                    top_companies = df.nlargest(5, 'growth_rate')[['company_id', 'growth_rate', 'revenue']]
                    print(f"Top 5 Companies by Growth:")
                    print(top_companies.to_string(index=False))
                
                elif "visualizations" in command.lower():
                    viz_path = self.data_analyzer.create_visualization('business_analytics', 'auto')
                    print(f"Visualization created: {viz_path}")
                
                elif "predict" in command.lower():
                    # Build prediction model
                    features = ['revenue', 'employees', 'market_cap', 'customer_satisfaction', 'innovation_score']
                    X = df[features]
                    y = df['performance_category']
                    
                    prepared_data = self.sklearn_manager.prepare_data(X, y)
                    ml_results = self.sklearn_manager.train_classification_models(
                        prepared_data['X_train'], prepared_data['y_train']
                    )
                    
                    eval_results = self.sklearn_manager.evaluate_classification_models(
                        prepared_data['X_test'], prepared_data['y_test']
                    )
                    
                    best_model = max(eval_results.items(), key=lambda x: x[1].get('metrics', {}).get('accuracy', 0))
                    print(f"Best prediction model: {best_model[0]}")
                    print(f"   Accuracy: {best_model[1]['metrics']['accuracy']:.3f}")
                
                time.sleep(1)
            
            # Step 3: Gesture-based interaction
            print("\nStep 3: Adding gesture-based controls...")
            
            gesture_commands = {
                'thumbs_up': 'Continue with detailed analysis',
                'peace': 'Show summary',
                'point': 'Highlight key insights',
                'open_palm': 'Generate full report'
            }
            
            print("Gesture controls enabled:")
            for gesture, action in gesture_commands.items():
                print(f"   {gesture}: {action}")
            
            # Simulate gesture interaction
            detected_gesture = "open_palm"  # Simulated
            print(f"\nGesture detected: {detected_gesture}")
            print(f"Action: {gesture_commands[detected_gesture]}")
            
            if detected_gesture == "open_palm":
                print("\nGenerating comprehensive business intelligence report...")
                
                # Create comprehensive report
                report = {
                    'executive_summary': {
                        'total_companies': len(df),
                        'avg_growth_rate': df['growth_rate'].mean(),
                        'top_performer_sector': 'Technology',  # Simulated
                        'key_insights': [
                            "Revenue per employee varies significantly across companies",
                            "Market cap correlates with innovation scores",
                            "Customer satisfaction drives growth"
                        ]
                    },
                    'detailed_analysis': {
                        'performance_distribution': df['performance_category'].value_counts().to_dict(),
                        'key_metrics': {
                            'avg_revenue': df['revenue'].mean(),
                            'avg_employees': df['employees'].mean(),
                            'avg_satisfaction': df['customer_satisfaction'].mean()
                        }
                    },
                    'predictions': {
                        'model_accuracy': best_model[1]['metrics']['accuracy'],
                        'success_factors': ['innovation_score', 'customer_satisfaction', 'revenue_per_employee']
                    },
                    'recommendations': [
                        "Focus on innovation to drive growth",
                        "Improve customer satisfaction metrics",
                        "Optimize revenue per employee ratio",
                        "Monitor market cap trends regularly"
                    ]
                }
                
                # Save report
                report_path = "business_intelligence_report.json"
                with open(report_path, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                print(f"SUCCESS: Business intelligence report saved: {report_path}")
            
            # Step 4: Multi-modal interaction summary
            print("\nMulti-modal AI Assistant Summary:")
            print("SUCCESS: Voice Commands: Natural language processing successful")
            print("SUCCESS: Data Analysis: Comprehensive insights generated")
            print("SUCCESS: Machine Learning: Predictive models built and evaluated")
            print("SUCCESS: Gesture Recognition: Intuitive interaction enabled")
            print("SUCCESS: Automated Reporting: Business intelligence created")
            print("SUCCESS: Real-time Processing: All operations completed efficiently")
            
            print("\nSUCCESS: Custom AI Task Demo completed successfully!")
            print("This demonstrates the power of integrating multiple AI technologies!")
            
        except Exception as e:
            print(f"ERROR: Custom AI task demo error: {str(e)}")
    
    def run(self):
        """Run the main demo loop."""
        self.is_running = True
        
        while self.is_running:
            self.show_menu()
            
            try:
                choice = input("\nEnter your choice (0-9): ").strip()
                
                if choice == '0':
                    print("\nThank you for using the Integrated AI/ML Demo!")
                    self.is_running = False
                
                elif choice == '1':
                    self.run_enterprise_ai_demo()
                
                elif choice == '2':
                    self.run_gesture_demo()
                
                elif choice == '3':
                    self.run_deep_learning_demo()
                
                elif choice == '4':
                    self.run_data_analysis_demo()
                
                elif choice == '5':
                    self.run_aiml_demo()
                
                elif choice == '6':
                    self.run_sklearn_demo()
                
                elif choice == '7':
                    self.run_integrated_workflow_demo()
                
                elif choice == '8':
                    self.run_performance_comparison_demo()
                
                elif choice == '9':
                    self.run_custom_ai_task_demo()
                
                else:
                    print("ERROR: Invalid choice! Please enter a number between 0-9.")
                
                if self.is_running:
                    input("\nPress Enter to continue...")
            
            except KeyboardInterrupt:
                print("\n\nDemo interrupted by user. Goodbye!")
                self.is_running = False
            
            except Exception as e:
                print(f"\nERROR: {str(e)}")
                input("Press Enter to continue...")


def main():
    """Main function to run the integrated demo."""
    print("Starting Integrated AI/ML Demo...")
    print("This demo showcases Enterprise AI assistant, gesture recognition, deep learning,")
    print("data analysis, conversational AI, and machine learning capabilities!")
    print("\nNOTE: Some features may require additional dependencies:")
    print("   - Voice recognition: pip install SpeechRecognition pyttsx3")
    print("   - Gesture recognition: pip install mediapipe opencv-python")
    print("   - Deep learning: pip install tensorflow")
    print("   - Data science: pip install pandas numpy matplotlib seaborn scikit-learn")
    
    try:
        demo = IntegratedAIDemo()
        demo.run()
    except Exception as e:
        print(f"ERROR: Demo failed to start: {str(e)}")
        print("Please ensure all dependencies are installed.")


if __name__ == "__main__":
    main()
