#!/usr/bin/env python3
"""
Simple test script to verify the AI Orchestrator integration works.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

def test_basic_integration():
    """Test basic integration without complex dependencies."""
    print("Testing Basic AI Integration...")
    
    try:
        # Test 1: Import orchestrator
        print("  Testing imports...")
        from integration.ai_orchestrator import AIOrchestrator
        print("    SUCCESS: AIOrchestrator imported successfully")
        
        # Test 2: Load config
        print("  Testing configuration...")
        import yaml
        
        config_path = Path(__file__).parent / 'config' / 'config.yaml'
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("    SUCCESS: Configuration loaded successfully")
        
        # Test 3: Initialize orchestrator with mock config
        print("  Testing orchestrator initialization...")
        test_config = {
            'llm': {
                'default_provider': 'mock',
                'mock': {'mock_response_delay': 0.1}
            },
            'models_dir': 'models/',
            'data_dir': 'data/',
            'logs_dir': 'logs/'
        }
        
        orchestrator = AIOrchestrator(test_config)
        print("    SUCCESS: AIOrchestrator created successfully")
        
        # Test 4: Initialize components (without voice/gesture)
        print("  Testing component initialization...")
        success = orchestrator._initialize_llm_client()
        if success:
            print("    SUCCESS: LLM client initialized")
        else:
            print("    ERROR: LLM client failed")
            return False
        
        # Test 5: Test basic functionality
        print("  Testing basic functionality...")
        try:
            # Test text processing (should work)
            response = orchestrator.llm_client.generate_response("Hello, test message")
            print(f"    SUCCESS: LLM response: {response[:50]}...")
        except Exception as e:
            print(f"    ERROR: LLM test failed: {str(e)}")
            return False
        
        print("  SUCCESS: Basic integration test PASSED!")
        return True
        
    except ImportError as e:
        print(f"    ERROR: Import error: {str(e)}")
        return False
    except Exception as e:
        print(f"    ERROR: Unexpected error: {str(e)}")
        return False

def test_component_imports():
    """Test individual component imports."""
    print("\nTesting Component Imports...")
    
    components = [
        ("LLM Client", "utils.llm_client", "LLMClient"),
        ("Email Agent", "agents.email_agent", "EmailAgent"),
        ("Report Agent", "agents.report_agent", "ReportAgent"),
        ("Summarizer", "agents.summarizer", "SummarizerAgent"),
        ("Data Analyzer", "data_analysis.data_analyzer", "DataAnalyzer"),
        ("AIML Processor", "aiml.aiml_processor", "AIMLProcessor"),
        ("Sklearn Manager", "ml_models.sklearn_manager", "SklearnManager"),
    ]
    
    results = []
    
    for name, module_path, class_name in components:
        try:
            module = __import__(f"src.{module_path}", fromlist=[class_name])
            cls = getattr(module, class_name)
            results.append((name, True, None))
            print(f"  SUCCESS: {name}")
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"  ERROR: {name}: {str(e)}")
    
    return results

def main():
    """Main test function."""
    print("AI Integration Test Suite")
    print("=" * 50)
    
    # Test component imports
    import_results = test_component_imports()
    
    # Test basic integration
    basic_success = test_basic_integration()
    
    # Summary
    print("\nTest Summary")
    print("=" * 50)
    
    successful_imports = sum(1 for _, success, _ in import_results if success)
    total_imports = len(import_results)
    
    print(f"Component Imports: {successful_imports}/{total_imports} successful")
    print(f"Basic Integration: {'PASSED' if basic_success else 'FAILED'}")
    
    if successful_imports == total_imports and basic_success:
        print("\nSUCCESS: All tests PASSED! Integration is working correctly.")
        return 0
    else:
        print("\nWARNING: Some tests failed. Check the errors above.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
