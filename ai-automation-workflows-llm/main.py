"""
Main entry point for AI Automation Workflows application.
"""

import sys
import os
import yaml
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from utils.llm_client import LLMClient
from agents.email_agent import EmailAgent
from agents.report_agent import ReportAgent
from agents.summarizer import SummarizerAgent
from workflows.automate_reporting import AutomatedReportingWorkflow
from workflows.customer_support_flow import CustomerSupportWorkflow
from integration.ai_orchestrator import AIOrchestrator


def setup_logging(config: dict):
    """Set up logging configuration."""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create logs directory if it doesn't exist
    log_file = log_config.get('file')
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        filename=log_file,
        filemode='a'
    )
    
    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)


def load_config(config_path: str = 'config/config.yaml') -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Replace environment variables
        def replace_env_vars(obj):
            if isinstance(obj, dict):
                return {k: replace_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_env_vars(item) for item in obj]
            elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
                env_var = obj[2:-1]
                return os.getenv(env_var, obj)
            else:
                return obj
        
        config = replace_env_vars(config)
        return config
        
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        return {}
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        return {}


def initialize_components(config: dict):
    """Initialize all components with given configuration."""
    print("Initializing AI Orchestrator with all components...")
    
    # Initialize the unified orchestrator
    orchestrator = AIOrchestrator(config)
    
    # Initialize all components
    if orchestrator.initialize_all():
        print("SUCCESS: All components initialized successfully")
        return orchestrator
    else:
        print("ERROR: Failed to initialize components")
        return None

# Legacy initialization function for backward compatibility
def initialize_legacy_components(config: dict):
    """Initialize components using legacy approach."""
    llm_config = config.get('llm', {})
    default_provider = llm_config.get('default_provider', 'mock')
    
    # Get provider-specific config
    provider_config = llm_config.get(default_provider, {})
    
    # Initialize LLM client
    print(f"Initializing LLM client with {default_provider} provider...")
    llm_client = LLMClient(default_provider, provider_config)
    
    # Test connection
    if llm_client.test_connection():
        print("SUCCESS: LLM client connection successful")
    else:
        print("ERROR: LLM client connection failed")
        return None
    
    # Initialize agents
    print("Initializing agents...")
    email_agent = EmailAgent(llm_client)
    report_agent = ReportAgent(llm_client)
    summarizer = SummarizerAgent(llm_client)
    
    # Initialize workflows
    print("Initializing workflows...")
    reporting_workflow = AutomatedReportingWorkflow(llm_client, config)
    support_workflow = CustomerSupportWorkflow(llm_client, config)
    
    return {
        'llm_client': llm_client,
        'email_agent': email_agent,
        'report_agent': report_agent,
        'summarizer': summarizer,
        'reporting_workflow': reporting_workflow,
        'support_workflow': support_workflow
    }


def demo_enterprise_ai_assistant(orchestrator: AIOrchestrator):
    """Demonstrate Enterprise AI assistant capabilities."""
    print("\n" + "="*50)
    print("🤖 Enterprise AI ASSISTANT DEMO")
    print("="*50)
    
    # Start Enterprise AI
    enterprise_ai_status = orchestrator.start_enterprise_ai()
    print(f"Enterprise AI Status: {enterprise_ai_status['message']}")
    
    # Test voice commands
    test_commands = [
        "Hello Enterprise AI, how are you?",
        "Analyze some data for me",
        "What's the weather like?",
        "Thank you Enterprise AI"
    ]
    
    print("\nTesting voice commands:")
    for command in test_commands:
        response = orchestrator.process_voice_command(command)
        print(f"  Command: {command}")
        print(f"  Response: {response[:100]}...")
        print()


def demo_gesture_recognition(orchestrator: AIOrchestrator):
    """Demonstrate gesture recognition capabilities."""
    print("\n" + "="*50)
    print("GESTURE RECOGNITION DEMO")
    print("="*50)
    
    print("Starting gesture recognition...")
    print("(Press 'q' in the gesture window to stop)")
    
    # Start gesture recognition in background
    gesture_status = orchestrator.start_gesture_recognition()
    print(f"Gesture Status: {gesture_status['message']}")
    
    # Test a few gesture detections
    import time
    for i in range(3):
        time.sleep(2)
        gesture = orchestrator.detect_gesture()
        if gesture:
            print(f"  Detected gesture: {gesture}")
        else:
            print(f"  No gesture detected (attempt {i+1})")


def demo_deep_learning(orchestrator: AIOrchestrator):
    """Demonstrate deep learning capabilities."""
    print("\n" + "="*50)
    print("🧠 DEEP LEARNING DEMO")
    print("="*50)
    
    # Create different types of models
    models_created = {}
    
    try:
        # Create image classifier
        cnn_model = orchestrator.create_deep_learning_model(
            'image_classifier', 
            input_shape=(64, 64, 3), 
            num_classes=10,
            model_name='demo_cnn'
        )
        models_created['CNN'] = 'Image Classifier'
        print("SUCCESS: CNN Image Classifier created")
    except Exception as e:
        print(f"ERROR: CNN creation failed: {str(e)}")
    
    try:
        # Create text classifier
        lstm_model = orchestrator.create_deep_learning_model(
            'text_classifier',
            vocab_size=1000,
            embedding_dim=100,
            max_length=50,
            num_classes=5,
            model_name='demo_lstm'
        )
        models_created['LSTM'] = 'Text Classifier'
        print("SUCCESS: LSTM Text Classifier created")
    except Exception as e:
        print(f"ERROR: LSTM creation failed: {str(e)}")
    
    try:
        # Create GAN components
        generator = orchestrator.create_deep_learning_model(
            'gan_generator',
            latent_dim=100,
            model_name='demo_generator'
        )
        
        discriminator = orchestrator.create_deep_learning_model(
            'gan_discriminator',
            model_name='demo_discriminator'
        )
        models_created['GAN'] = 'Generator & Discriminator'
        print("SUCCESS: GAN components created")
    except Exception as e:
        print(f"ERROR: GAN creation failed: {str(e)}")
    
    print(f"\nModels Created: {len(models_created)}")
    for model_type, description in models_created.items():
        print(f"  {model_type}: {description}")


def demo_machine_learning(orchestrator: AIOrchestrator):
    """Demonstrate machine learning capabilities."""
    print("\n" + "="*50)
    print("MACHINE LEARNING DEMO")
    print("="*50)
    
    # Generate sample data
    from sklearn.datasets import make_classification, make_regression
    import numpy as np
    
    # Classification data
    X_class, y_class = make_classification(n_samples=1000, n_features=20, n_classes=3, random_state=42)
    
    # Regression data
    X_reg, y_reg = make_regression(n_samples=1000, n_features=15, n_targets=1, random_state=42)
    
    print("Generated sample datasets:")
    print(f"  Classification: {X_class.shape}, {y_class.shape}")
    print(f"  Regression: {X_reg.shape}, {y_reg.shape}")
    
    # Train classification models
    print("\nTraining classification models...")
    class_results = orchestrator.train_sklearn_models(
        X_class, y_class, 
        ['random_forest', 'svm', 'logistic_regression', 'knn']
    )
    
    print("Classification Results:")
    for model_name, result in class_results.items():
        if 'error' not in result:
            print(f"  {model_name}: Trained successfully")
        else:
            print(f"  {model_name}: ERROR: {result['error']}")
    
    # Train regression models
    print("\nTraining regression models...")
    reg_results = orchestrator.train_sklearn_models(
        X_reg, y_reg,
        ['random_forest', 'linear_regression', 'ridge', 'lasso']
    )
    
    print("Regression Results:")
    for model_name, result in reg_results.items():
        if 'error' not in result:
            print(f"  {model_name}: Trained successfully")
        else:
            print(f"  {model_name}: ERROR: {result['error']}")


def demo_integrated_workflow(orchestrator: AIOrchestrator):
    """Demonstrate integrated AI workflow."""
    print("\n" + "="*50)
    print("INTEGRATED AI WORKFLOW DEMO")
    print("="*50)
    
    # Test different types of requests
    test_requests = [
        "I need help with my account login issue",
        "Generate a monthly sales report",
        "Summarize the latest customer feedback",
        "Show me some gesture recognition",
        "Analyze the quarterly performance data"
    ]
    
    print("Processing integrated requests:")
    for i, request in enumerate(test_requests, 1):
        print(f"\n{i}. Request: {request}")
        result = orchestrator.run_complete_ai_workflow(request)
        
        if 'error' in result:
            print(f"   ERROR: {result['error']}")
        else:
            print(f"   Workflow: {result.get('workflow_type', 'unknown')}")
            if 'enterprise_ai_response' in result:
                print(f"   Enterprise AI: {result['enterprise_ai_response'][:100]}...")
            if 'status' in result:
                print(f"   Status: {result['status']}")


def demo_email_processing_legacy(components: dict):
    """Demonstrate email processing capabilities."""
    print("\n" + "="*50)
    print("EMAIL PROCESSING DEMO")
    print("="*50)
    
    email_agent = components['email_agent']
    
    test_email = {
        'content': '''
        Hi Support Team,
        
        I'm having trouble accessing my account. I've been trying to log in for the past hour 
        but keep getting an "Invalid credentials" error. I'm sure my password is correct 
        because I just used it yesterday. I need to access my account urgently as I have 
        an important deadline today. Can you please help me resolve this issue quickly?
        
        Thank you,
        Sarah Johnson
        ''',
        'sender': 'sarah.johnson@example.com',
        'subject': 'Urgent: Account Login Issue'
    }
    
    print(f"Processing email from {test_email['sender']}")
    print(f"Subject: {test_email['subject']}")
    print(f"Content preview: {test_email['content'][:100]}...")
    
    result = email_agent.process_email(
        test_email['content'],
        test_email['sender'],
        test_email['subject']
    )
    
    print(f"\nResults:")
    print(f"  Category: {result.get('category', 'N/A')}")
    print(f"  Processed: {'Yes' if result.get('processed') else 'No'}")
    print(f"  Summary: {result.get('summary', 'N/A')}")
    print(f"  Response: {result.get('response', 'N/A')[:200]}...")


def demo_report_generation_legacy(components: dict):
    """Demonstrate report generation capabilities."""
    print("\n" + "="*50)
    print("REPORT GENERATION DEMO")
    print("="*50)
    
    report_agent = components['report_agent']
    
    test_data = {
        'sales_performance': {
            'monthly_revenue': [45000, 52000, 48000, 61000, 58000],
            'new_customers': [120, 145, 130, 165, 155],
            'customer_retention': [0.85, 0.87, 0.86, 0.89, 0.88]
        },
        'support_metrics': {
            'total_tickets': 1250,
            'resolved_tickets': 1180,
            'avg_resolution_time': 3.2,
            'customer_satisfaction': 4.3
        },
        'product_usage': {
            'active_users': 2500,
            'daily_logins': 1800,
            'feature_adoption': {
                'dashboard': 0.95,
                'reports': 0.78,
                'automation': 0.42
            }
        }
    }
    
    print("Generating monthly performance report...")
    
    result = report_agent.generate_report(
        test_data,
        'monthly',
        'Monthly Performance Report'
    )
    
    print(f"\nResults:")
    print(f"  Title: {result.get('title', 'N/A')}")
    print(f"  Status: {result.get('status', 'N/A')}")
    print(f"  Executive Summary: {result.get('executive_summary', 'N/A')[:200]}...")
    print(f"  Recommendations: {len(result.get('recommendations', []))} items")
    
    # Show trend analysis
    print("\nTrend Analysis:")
    historical_data = [
        {'timestamp': '2024-01-01', 'revenue': 45000, 'customers': 120},
        {'timestamp': '2024-02-01', 'revenue': 52000, 'customers': 145},
        {'timestamp': '2024-03-01', 'revenue': 48000, 'customers': 130},
        {'timestamp': '2024-04-01', 'revenue': 61000, 'customers': 165},
        {'timestamp': '2024-05-01', 'revenue': 58000, 'customers': 155}
    ]
    
    trend_result = report_agent.generate_trend_analysis(historical_data, 'revenue')
    print(f"  Revenue Trend: {trend_result.get('trend_direction', 'N/A')}")
    print(f"  Change Rate: {trend_result.get('change_rate', 0):.1f}%")


def demo_summarization_legacy(components: dict):
    """Demonstrate text summarization capabilities."""
    print("\n" + "="*50)
    print("TEXT SUMMARIZATION DEMO")
    print("="*50)
    
    summarizer = components['summarizer']
    
    test_text = '''
    Artificial Intelligence (AI) has become an integral part of modern business operations, 
    transforming industries across the globe. Companies are increasingly adopting AI technologies 
    to automate repetitive tasks, analyze vast amounts of data, and provide personalized 
    customer experiences. Machine learning algorithms are being used to predict customer behavior, 
    optimize supply chains, and detect fraudulent activities. Natural Language Processing (NLP) 
    enables businesses to analyze customer feedback, automate customer support, and generate 
    content at scale. Computer vision is revolutionizing quality control, security monitoring, 
    and inventory management. Despite these advancements, organizations face challenges including 
    data privacy concerns, the need for skilled AI professionals, and the ethical implications 
    of automated decision-making. Successful AI implementation requires careful planning, 
    robust data infrastructure, and a clear understanding of business objectives.
    '''
    
    print(f"Original text length: {len(test_text)} characters")
    
    # Generate different types of summaries
    summary_types = ['brief', 'executive', 'detailed']
    
    for summary_type in summary_types:
        print(f"\n{summary_type.title()} Summary:")
        result = summarizer.summarize_text(test_text, summary_type)
        
        print(f"  Length: {result.get('summary_length', 0)} words")
        print(f"  Compression: {result.get('compression_ratio', 0):.2f}x")
        print(f"  Summary: {result.get('summary', 'N/A')}")
        print(f"  Key Points: {len(result.get('key_points', []))} items")


def demo_customer_support_legacy(components: dict):
    """Demonstrate customer support workflow."""
    print("\n" + "="*50)
    print("CUSTOMER SUPPORT WORKFLOW DEMO")
    print("="*50)
    
    support_workflow = components['support_workflow']
    
    test_ticket = {
        'customer_email': 'john.doe@company.com',
        'subject': 'Critical System Issue - Production Down',
        'message': '''
        Our production system has been down for the past 2 hours. We're getting error code 500 
        when trying to access the main dashboard. This is affecting our entire team and we 
        have critical deadlines to meet. We need immediate assistance as this is causing 
        significant business impact. Please escalate this to your senior technical team.
        ''',
        'priority': 'critical'
    }
    
    print(f"Processing ticket from {test_ticket['customer_email']}")
    print(f"Priority: {test_ticket['priority']}")
    print(f"Subject: {test_ticket['subject']}")
    
    result = support_workflow.process_incoming_ticket(test_ticket)
    
    print(f"\nTicket Results:")
    print(f"  Ticket ID: {result.get('ticket_id', 'N/A')}")
    print(f"  Status: {result.get('status', 'N/A')}")
    print(f"  Escalation Needed: {'Yes' if result.get('escalation_needed') else 'No'}")
    print(f"  Auto Response: {result.get('auto_response', 'N/A')[:200]}...")
    print(f"  Similar Tickets: {len(result.get('similar_tickets', []))} found")


def demo_automated_reporting_legacy(components: dict):
    """Demonstrate automated reporting workflow."""
    print("\n" + "="*50)
    print("AUTOMATED REPORTING WORKFLOW DEMO")
    print("="*50)
    
    reporting_workflow = components['reporting_workflow']
    
    data_sources = ['sales_database', 'crm_system', 'support_tickets']
    recipients = ['manager@company.com', 'team@company.com']
    
    print(f"Running daily report for {len(data_sources)} data sources...")
    
    result = reporting_workflow.run_daily_report(data_sources, recipients)
    
    print(f"\nWorkflow Results:")
    print(f"  Status: {result.get('status', 'N/A')}")
    print(f"  Report ID: {result.get('report_id', 'N/A')}")
    print(f"  Recipients: {len(result.get('recipients', []))}")
    print(f"  Summary: {result.get('summary', 'N/A')[:200]}...")


def demo_summarization_legacy(components: dict):
    """Demonstrate text summarization capabilities."""
    print("\n" + "="*50)
    print("TEXT SUMMARIZATION DEMO")
    print("="*50)
    
    summarizer = components['summarizer']
    
    test_text = '''
    Artificial Intelligence (AI) has become an integral part of modern business operations, 
    transforming industries across the globe. Companies are increasingly adopting AI technologies 
    to automate repetitive tasks, analyze vast amounts of data, and provide personalized 
    customer experiences. Machine learning algorithms are being used to predict customer behavior, 
    optimize supply chains, and detect fraudulent activities. Natural Language Processing (NLP) 
    enables businesses to analyze customer feedback, automate customer support, and generate 
    content at scale. Computer vision is revolutionizing quality control, security monitoring, 
    and inventory management. Despite these advancements, organizations face challenges including 
    data privacy concerns, the need for skilled AI professionals, and the ethical implications 
    of automated decision-making. Successful AI implementation requires careful planning, 
    robust data infrastructure, and a clear understanding of business objectives.
    '''
    
    print(f"Original text length: {len(test_text)} characters")
    
    # Generate different types of summaries
    summary_types = ['brief', 'executive', 'detailed']
    
    for summary_type in summary_types:
        print(f"\n{summary_type.title()} Summary:")
        result = summarizer.summarize_text(test_text, summary_type)
        
        print(f"  Length: {result.get('summary_length', 0)} words")
        print(f"  Compression: {result.get('compression_ratio', 0):.2f}x")
        print(f"  Summary: {result.get('summary', 'N/A')}")
        print(f"  Key Points: {len(result.get('key_points', []))} items")


def demo_customer_support_legacy(components: dict):
    """Demonstrate customer support workflow."""
    print("\n" + "="*50)
    print("CUSTOMER SUPPORT WORKFLOW DEMO")
    print("="*50)
    
    support_workflow = components['support_workflow']
    
    test_ticket = {
        'customer_email': 'john.doe@company.com',
        'subject': 'Critical System Issue - Production Down',
        'message': '''
        Our production system has been down for the past 2 hours. We're getting error code 500 
        when trying to access the main dashboard. This is affecting our entire team and we 
        have critical deadlines to meet. We need immediate assistance as this is causing 
        significant business impact. Please escalate this to your senior technical team.
        ''',
        'priority': 'critical'
    }
    
    print(f"Processing ticket from {test_ticket['customer_email']}")
    print(f"Priority: {test_ticket['priority']}")
    print(f"Subject: {test_ticket['subject']}")
    
    result = support_workflow.process_incoming_ticket(test_ticket)
    
    print(f"\nTicket Results:")
    print(f"  Ticket ID: {result.get('ticket_id', 'N/A')}")
    print(f"  Status: {result.get('status', 'N/A')}")
    print(f"  Escalation Needed: {'Yes' if result.get('escalation_needed') else 'No'}")
    print(f"  Auto Response: {result.get('auto_response', 'N/A')[:200]}...")
    print(f"  Similar Tickets: {len(result.get('similar_tickets', []))} found")


def demo_automated_reporting_legacy(components: dict):
    """Demonstrate automated reporting workflow."""
    print("\n" + "="*50)
    print("AUTOMATED REPORTING WORKFLOW DEMO")
    print("="*50)
    
    reporting_workflow = components['reporting_workflow']
    
    data_sources = ['sales_database', 'crm_system', 'support_tickets']
    recipients = ['manager@company.com', 'team@company.com']
    
    print(f"Running daily report for {len(data_sources)} data sources...")
    
    result = reporting_workflow.run_daily_report(data_sources, recipients)
    
    print(f"\nWorkflow Results:")
    print(f"  Status: {result.get('status', 'N/A')}")
    print(f"  Report ID: {result.get('report_id', 'N/A')}")
    print(f"  Recipients: {len(result.get('recipients', []))}")
    print(f"  Summary: {result.get('summary', 'N/A')[:200]}...")


def run_demo(components, demo_type: str = 'all'):
    """Run selected demo or all demos."""
    demos = {
        'email': demo_email_processing,
        'report': demo_report_generation,
        'summarize': demo_summarization,
        'support': demo_customer_support,
        'workflow': demo_automated_reporting
    }
    
    if demo_type == 'all':
        print("Running all demos...")
        
        # Check if using new orchestrator or legacy components
        if isinstance(components, AIOrchestrator):
            # New integrated demos
            demo_enterprise_ai_assistant(components)
            demo_gesture_recognition(components)
            demo_deep_learning(components)
            demo_machine_learning(components)
            demo_integrated_workflow(components)
        else:
            # Legacy demos
            demo_email_processing_legacy(components)
            demo_report_generation_legacy(components)
            demo_summarization_legacy(components)
            demo_customer_support_legacy(components)
            demo_automated_reporting_legacy(components)
            
    elif isinstance(components, AIOrchestrator):
        # New orchestrator demos
        new_demos = {
            'enterprise_ai': demo_enterprise_ai_assistant,
            'gesture': demo_gesture_recognition,
            'deeplearning': demo_deep_learning,
            'ml': demo_machine_learning,
            'integrated': demo_integrated_workflow
        }
        
        if demo_type in new_demos:
            new_demos[demo_type](components)
        else:
            print(f"Unknown demo: {demo_type}")
            print(f"Available demos: {', '.join(list(new_demos.keys()) + ['all'])}")
    else:
        # Legacy demos
        legacy_demos = {
            'email': demo_email_processing_legacy,
            'report': demo_report_generation_legacy,
            'summarize': demo_summarization_legacy,
            'support': demo_customer_support_legacy,
            'workflow': demo_automated_reporting_legacy
        }
        
        if demo_type in legacy_demos:
            legacy_demos[demo_type](components)
        else:
            print(f"Unknown demo: {demo_type}")
            print(f"Available demos: {', '.join(list(legacy_demos.keys()) + ['all'])}")


def start_api_server(config: dict):
    """Start the FastAPI server."""
    print("\nStarting API Server...")
    print("API will be available at: http://localhost:8000")
    print("Interactive docs at: http://localhost:8000/docs")
    
    # Import and run the FastAPI app
    sys.path.append(str(Path(__file__).parent / 'api'))
    from app import app
    import uvicorn
    
    api_config = config.get('api', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8000)
    debug = api_config.get('debug', False)
    
    uvicorn.run(app, host=host, port=port, debug=debug)


def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='AI Automation Workflows')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--demo', 
                       choices=['all', 'email', 'report', 'summarize', 'support', 'workflow', 
                               'enterprise_ai', 'gesture', 'deeplearning', 'ml', 'integrated'], 
                       default='all', help='Demo to run')
    parser.add_argument('--legacy', action='store_true', help='Use legacy component initialization')
    parser.add_argument('--api', action='store_true', help='Start API server')
    parser.add_argument('--test', action='store_true', help='Run tests')
    
    args = parser.parse_args()
    
    print("AI Automation Workflows")
    print("=" * 50)
    
    # Load configuration
    print("Loading configuration...")
    config = load_config(args.config)
    if not config:
        print("ERROR: Failed to load configuration")
        return
    
    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    logger.info("Application starting")
    
    # Initialize components
    if args.legacy:
        print("Using legacy component initialization...")
        components = initialize_legacy_components(config)
    else:
        print("Using new AI Orchestrator...")
        components = initialize_components(config)
        
    if not components:
        print("ERROR: Failed to initialize components")
        return
    
    print("SUCCESS: All components initialized successfully")
    
    # Show system status if using orchestrator
    if isinstance(components, AIOrchestrator):
        status = components.get_system_status()
        print("\nSystem Status:")
        print(f"  Orchestrator: {'Yes' if status['orchestrator']['initialized'] else 'No'}")
        print(f"  LLM Client: {'Yes' if status['llm_client']['connected'] else 'No'}")
        print(f"  Agents: {len(status['agents'])} loaded")
        print(f"  Workflows: {len(status['workflows'])} loaded")
        print(f"  AI Components: {len(status['ai_components'])} loaded")
        print(f"  ML Components: {len(status['ml_components'])} loaded")
    
    try:
        if args.test:
            # Run tests
            print("\nRunning tests...")
            import subprocess
            result = subprocess.run([sys.executable, 'tests/test_agents.py'], 
                                  capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
        
        elif args.api:
            # Start API server
            start_api_server(config)
        
        else:
            # Run demos
            run_demo(components, args.demo)
            
            print("\n" + "="*50)
            print("SUCCESS: Demo completed successfully!")
            print("\nNext steps:")
            print("  • Run with --api to start the REST API server")
            print("  • Visit http://localhost:8000/docs for API documentation")
            print("  • Check notebooks/experimentation.ipynb for more examples")
            print("  • Review config/config.yaml for customization options")
            if args.legacy:
                print("  • Use without --legacy to try the new AI Orchestrator")
            else:
                print("  • Try specific demos: --demo enterprise_ai, --demo gesture, --demo deeplearning")
                print("  • Use --legacy for the original component structure")
    
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
        
        # Shutdown orchestrator if used
        if isinstance(components, AIOrchestrator):
            components.shutdown()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        print(f"ERROR: Error: {str(e)}")
        
        # Shutdown orchestrator on error
        if isinstance(components, AIOrchestrator):
            try:
                components.shutdown()
            except:
                pass
    
    logger.info("Application finished")


if __name__ == '__main__':
    main()
