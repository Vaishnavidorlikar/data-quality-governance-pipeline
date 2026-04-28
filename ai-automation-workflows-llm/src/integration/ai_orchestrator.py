"""
AI Orchestrator - Unified integration layer for all AI/ML components.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import threading
from pathlib import Path

# Import all AI/ML components
from src.utils.llm_client import LLMClient
from src.agents.email_agent import EmailAgent
from src.agents.report_agent import ReportAgent
from src.agents.summarizer import SummarizerAgent
from src.workflows.automate_reporting import AutomatedReportingWorkflow
from src.workflows.customer_support_flow import CustomerSupportWorkflow
from src.enterprise_ai.enterprise_ai_assistant import Enterprise AIAssistant
from src.gesture_recognition.gesture_detector import GestureDetector
from src.deep_learning.model_manager import ModelManager
from src.ml_models.sklearn_manager import SklearnManager
from src.data_analysis.data_analyzer import DataAnalyzer
from src.aiml.aiml_processor import AIMLProcessor


class AIOrchestrator:
    """
    Unified orchestrator for all AI/ML components in the project.
    Provides a single interface to access and coordinate all functionalities.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the AI Orchestrator.
        
        Args:
            config: Configuration dictionary containing all component settings
        """
        self.config = config or self._load_default_config()
        self.logger = logging.getLogger(__name__)
        
        # Initialize all components
        self.llm_client = None
        self.agents = {}
        self.workflows = {}
        self.ai_components = {}
        self.ml_components = {}
        
        # Status tracking
        self.is_initialized = False
        self.active_sessions = {}
        
        self.logger.info("AI Orchestrator created")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration."""
        return {
            'llm': {
                'default_provider': 'mock',
                'mock': {'mock_response_delay': 0.1}
            },
            'enterprise_ai': {
                'llm_provider': 'mock',
                'voice': {
                    'rate': 200,
                    'volume': 0.9
                }
            },
            'models_dir': 'models/',
            'data_dir': 'data/',
            'logs_dir': 'logs/'
        }
    
    def initialize_all(self) -> bool:
        """
        Initialize all AI/ML components.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            self.logger.info("Initializing all AI/ML components...")
            
            # Initialize LLM client first
            self._initialize_llm_client()
            
            # Initialize agents
            self._initialize_agents()
            
            # Initialize workflows
            self._initialize_workflows()
            
            # Initialize AI components (Enterprise AI, Gesture, etc.)
            self._initialize_ai_components()
            
            # Initialize ML components
            self._initialize_ml_components()
            
            self.is_initialized = True
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False
    
    def _initialize_llm_client(self):
        """Initialize LLM client."""
        llm_config = self.config.get('llm', {})
        provider = llm_config.get('default_provider', 'mock')
        provider_config = llm_config.get(provider, {})
        
        self.llm_client = LLMClient(provider, provider_config)
        
        if self.llm_client.test_connection():
            self.logger.info(f"LLM client initialized with {provider} provider")
        else:
            self.logger.warning("LLM client connection failed")
    
    def _initialize_agents(self):
        """Initialize all agents."""
        if not self.llm_client:
            raise Exception("LLM client not initialized")
        
        self.agents = {
            'email': EmailAgent(self.llm_client),
            'report': ReportAgent(self.llm_client),
            'summarizer': SummarizerAgent(self.llm_client)
        }
        
        self.logger.info("All agents initialized")
    
    def _initialize_workflows(self):
        """Initialize all workflows."""
        if not self.llm_client:
            raise Exception("LLM client not initialized")
        
        self.workflows = {
            'reporting': AutomatedReportingWorkflow(self.llm_client, self.config),
            'customer_support': CustomerSupportWorkflow(self.llm_client, self.config)
        }
        
        self.logger.info("All workflows initialized")
    
    def _initialize_ai_components(self):
        """Initialize AI components (Enterprise AI, Gesture, etc.)."""
        # Initialize Enterprise AI Assistant
        enterprise_ai_config = self.config.get('enterprise_ai', {})
        self.ai_components['enterprise_ai'] = Enterprise AIAssistant(enterprise_ai_config)
        
        # Initialize Gesture Detector
        self.ai_components['gesture_detector'] = GestureDetector()
        
        # Initialize AIML Processor
        self.ai_components['aiml'] = AIMLProcessor()
        
        self.logger.info("AI components initialized")
    
    def _initialize_ml_components(self):
        """Initialize ML components."""
        models_dir = self.config.get('models_dir', 'models/')
        
        # Initialize Deep Learning Model Manager
        self.ml_components['deep_learning'] = ModelManager(models_dir)
        
        # Initialize Scikit-learn Manager
        self.ml_components['sklearn'] = SklearnManager(f"{models_dir}/sklearn/")
        
        # Initialize Data Analyzer
        self.ml_components['data_analyzer'] = DataAnalyzer()
        
        self.logger.info("ML components initialized")
    
    # Agent Methods
    def process_email(self, content: str, sender: str, subject: str) -> Dict:
        """Process email using email agent."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.agents['email'].process_email(content, sender, subject)
    
    def generate_report(self, data: Dict, report_type: str, title: str) -> Dict:
        """Generate report using report agent."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.agents['report'].generate_report(data, report_type, title)
    
    def summarize_text(self, text: str, summary_type: str = 'brief') -> Dict:
        """Summarize text using summarizer agent."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.agents['summarizer'].summarize_text(text, summary_type)
    
    # Workflow Methods
    def run_reporting_workflow(self, data_sources: List[str], recipients: List[str]) -> Dict:
        """Run automated reporting workflow."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.workflows['reporting'].run_daily_report(data_sources, recipients)
    
    def process_support_ticket(self, ticket: Dict) -> Dict:
        """Process customer support ticket."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.workflows['customer_support'].process_incoming_ticket(ticket)
    
    # AI Component Methods
    def start_enterprise_ai(self) -> Dict:
        """Start Enterprise AI assistant."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        enterprise_ai = self.ai_components['enterprise_ai']
        enterprise_ai.start_continuous_listening()
        
        return {
            'status': 'started',
            'message': 'Enterprise AI assistant is now listening',
            'capabilities': ['voice_commands', 'gesture_control', 'data_analysis']
        }
    
    def process_voice_command(self, command: str) -> str:
        """Process voice command through Enterprise AI."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.ai_components['enterprise_ai'].process_command(command)
    
    def detect_gesture(self) -> Optional[str]:
        """Detect hand gesture."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        return self.ai_components['gesture_detector'].detect_gesture()
    
    def start_gesture_recognition(self, window_name: str = "Gesture Detection"):
        """Start real-time gesture recognition."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        # Run in separate thread to avoid blocking
        gesture_thread = threading.Thread(
            target=self.ai_components['gesture_detector'].start_camera_detection,
            args=(window_name,),
            daemon=True
        )
        gesture_thread.start()
        
        return {'status': 'started', 'message': 'Gesture recognition started'}
    
    # ML Component Methods
    def create_deep_learning_model(self, model_type: str, **kwargs) -> Any:
        """Create deep learning model."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        manager = self.ml_components['deep_learning']
        
        if model_type == 'image_classifier':
            return manager.create_image_classifier(**kwargs)
        elif model_type == 'text_classifier':
            return manager.create_text_classifier(**kwargs)
        elif model_type == 'gan_generator':
            return manager.create_gan_generator(**kwargs)
        elif model_type == 'gan_discriminator':
            return manager.create_gan_discriminator(**kwargs)
        else:
            raise ValueError(f"Unknown model type: {model_type}")
    
    def train_sklearn_models(self, X_train, y_train, model_types: List[str] = None) -> Dict:
        """Train scikit-learn models."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        manager = self.ml_components['sklearn']
        
        if model_types is None:
            model_types = ['random_forest', 'svm', 'logistic_regression', 'knn']
        
        results = {}
        for model_type in model_types:
            try:
                result = manager.train_single_model(model_type, X_train, y_train)
                results[model_type] = result
            except Exception as e:
                self.logger.error(f"Failed to train {model_type}: {str(e)}")
                results[model_type] = {'error': str(e)}
        
        return results
    
    def analyze_data(self, data_source: str) -> Dict:
        """Analyze data using data analyzer."""
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        analyzer = self.ml_components['data_analyzer']
        
        # Load data if path provided
        if isinstance(data_source, str) and data_source.endswith('.csv'):
            analyzer.load_data(data_source, 'analysis_data')
            data_key = 'analysis_data'
        else:
            data_key = data_source
        
        return analyzer.analyze_data(data_key)
    
    # Integrated Workflow Methods
    def run_complete_ai_workflow(self, user_request: str) -> Dict:
        """
        Run complete AI workflow based on user request.
        Integrates multiple components as needed.
        """
        if not self.is_initialized:
            raise Exception("Orchestrator not initialized")
        
        try:
            # Process request through Enterprise AI for understanding
            enterprise_ai_response = self.process_voice_command(user_request)
            
            # Determine workflow based on request content
            request_lower = user_request.lower()
            
            if any(keyword in request_lower for keyword in ['email', 'support', 'ticket']):
                # Customer support workflow
                ticket = {
                    'customer_email': 'user@example.com',
                    'subject': user_request,
                    'message': user_request,
                    'priority': 'normal'
                }
                result = self.process_support_ticket(ticket)
                result['enterprise_ai_response'] = enterprise_ai_response
                
            elif any(keyword in request_lower for keyword in ['report', 'analyze', 'summary']):
                # Reporting/analysis workflow
                # Generate sample data for demonstration
                sample_data = {
                    'sales': [1000, 1500, 2000, 2500, 3000],
                    'customers': [50, 75, 100, 125, 150],
                    'satisfaction': [4.1, 4.3, 4.5, 4.4, 4.6]
                }
                
                if 'report' in request_lower:
                    result = self.generate_report(sample_data, 'automated', 'AI Generated Report')
                elif 'summary' in request_lower:
                    text_to_summarize = f"User request: {user_request}. Data: {sample_data}"
                    result = self.summarize_text(text_to_summarize, 'executive')
                else:
                    result = self.analyze_data('sample_data')
                
                result['enterprise_ai_response'] = enterprise_ai_response
                
            elif any(keyword in request_lower for keyword in ['gesture', 'hand', 'wave']):
                # Gesture recognition workflow
                result = self.start_gesture_recognition()
                result['enterprise_ai_response'] = enterprise_ai_response
                
            else:
                # General LLM response
                result = {
                    'response': enterprise_ai_response,
                    'workflow_type': 'general',
                    'enterprise_ai_response': enterprise_ai_response
                }
            
            result['timestamp'] = datetime.now().isoformat()
            result['request'] = user_request
            
            return result
            
        except Exception as e:
            self.logger.error(f"Workflow error: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'request': user_request
            }
    
    def get_system_status(self) -> Dict:
        """Get status of all components."""
        status = {
            'orchestrator': {
                'initialized': self.is_initialized,
                'active_sessions': len(self.active_sessions)
            },
            'llm_client': {
                'connected': self.llm_client.test_connection() if self.llm_client else False
            },
            'agents': {name: 'loaded' for name in self.agents.keys()},
            'workflows': {name: 'loaded' for name in self.workflows.keys()},
            'ai_components': {},
            'ml_components': {}
        }
        
        # Get AI component status
        if 'enterprise_ai' in self.ai_components:
            status['ai_components']['enterprise_ai'] = self.ai_components['enterprise_ai'].get_status()
        
        # Get ML component status
        for name, component in self.ml_components.items():
            status['ml_components'][name] = 'loaded'
        
        return status
    
    def shutdown(self):
        """Shutdown all components gracefully."""
        self.logger.info("Shutting down AI Orchestrator...")
        
        try:
            # Shutdown Enterprise AI
            if 'enterprise_ai' in self.ai_components:
                self.ai_components['enterprise_ai'].shutdown()
            
            # Stop gesture detection
            if 'gesture_detector' in self.ai_components:
                self.ai_components['gesture_detector'].stop_camera_detection()
            
            # Clear components
            self.agents.clear()
            self.workflows.clear()
            self.ai_components.clear()
            self.ml_components.clear()
            
            self.is_initialized = False
            self.logger.info("AI Orchestrator shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {str(e)}")
