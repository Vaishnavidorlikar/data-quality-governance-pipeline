"""
Test suite for AI automation agents.
"""

import unittest
import sys
from pathlib import Path
import asyncio
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.append(str(Path('../src').absolute()))

from utils.llm_client import LLMClient, MockProvider
from agents.email_agent import EmailAgent
from agents.report_agent import ReportAgent
from agents.summarizer import SummarizerAgent


class TestLLMClient(unittest.TestCase):
    """Test cases for LLM Client."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {'mock_response_delay': 0}
        self.client = LLMClient('mock', self.config)
    
    def test_initialization(self):
        """Test LLM client initialization."""
        self.assertEqual(self.client.provider_name, 'mock')
        self.assertIsInstance(self.client.provider, MockProvider)
    
    def test_generate_response(self):
        """Test response generation."""
        prompt = "Test prompt"
        response = self.client.generate_response(prompt)
        self.assertIsInstance(response, str)
        self.assertGreater(len(response), 0)
    
    def test_connection_test(self):
        """Test connection testing."""
        result = self.client.test_connection()
        self.assertTrue(result)
    
    def test_provider_info(self):
        """Test provider information retrieval."""
        info = self.client.get_provider_info()
        self.assertIn('provider', info)
        self.assertIn('provider_type', info)
        self.assertEqual(info['provider'], 'mock')


class TestEmailAgent(unittest.TestCase):
    """Test cases for Email Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.llm_client = LLMClient('mock', {'mock_response_delay': 0})
        self.email_agent = EmailAgent(self.llm_client)
        
        self.test_email = {
            'content': 'I am having trouble with my account login.',
            'sender': 'test@example.com',
            'subject': 'Login Issue'
        }
    
    def test_process_email(self):
        """Test email processing."""
        result = self.email_agent.process_email(
            self.test_email['content'],
            self.test_email['sender'],
            self.test_email['subject']
        )
        
        self.assertIn('category', result)
        self.assertIn('summary', result)
        self.assertIn('response', result)
        self.assertIn('processed', result)
        self.assertTrue(result['processed'])
    
    def test_categorize_email(self):
        """Test email categorization."""
        category = self.email_agent._categorize_email(
            self.test_email['content'],
            self.test_email['subject']
        )
        
        self.assertIsInstance(category, str)
        self.assertIn(category, [
            'support_request', 'inquiry', 'complaint',
            'information', 'spam', 'newsletter'
        ])
    
    def test_generate_summary(self):
        """Test email summary generation."""
        summary = self.email_agent._generate_summary(self.test_email['content'])
        self.assertIsInstance(summary, str)
        self.assertGreater(len(summary), 0)
    
    def test_generate_response(self):
        """Test email response generation."""
        response = self.email_agent._generate_response(
            self.test_email['content'],
            self.test_email['sender'],
            'support_request'
        )
        
        self.assertIsInstance(response, str)
        self.assertGreater(len(response), 0)
    
    def test_batch_process_emails(self):
        """Test batch email processing."""
        emails = [
            {
                'content': 'Question about pricing',
                'sender': 'customer1@example.com',
                'subject': 'Pricing'
            },
            {
                'content': 'Bug report',
                'sender': 'customer2@example.com',
                'subject': 'Bug'
            }
        ]
        
        results = self.email_agent.batch_process_emails(emails)
        self.assertEqual(len(results), 2)
        
        for result in results:
            self.assertIn('processed', result)
            self.assertTrue(result['processed'])


class TestReportAgent(unittest.TestCase):
    """Test cases for Report Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.llm_client = LLMClient('mock', {'mock_response_delay': 0})
        self.report_agent = ReportAgent(self.llm_client)
        
        self.test_data = {
            'sales': [
                {'month': 'Jan', 'revenue': 1000},
                {'month': 'Feb', 'revenue': 1500}
            ],
            'customers': 50,
            'satisfaction': 4.2
        }
    
    def test_generate_report(self):
        """Test report generation."""
        result = self.report_agent.generate_report(
            self.test_data,
            'monthly',
            'Test Report'
        )
        
        self.assertIn('title', result)
        self.assertIn('report_type', result)
        self.assertIn('executive_summary', result)
        self.assertIn('content', result)
        self.assertIn('recommendations', result)
        self.assertIn('status', result)
        self.assertEqual(result['status'], 'completed')
    
    def test_analyze_data(self):
        """Test data analysis."""
        analysis = self.report_agent._analyze_data(self.test_data)
        
        self.assertIsInstance(analysis, dict)
        self.assertIn('sales_summary', analysis)
        self.assertIn('customers_keys', analysis)
    
    def test_generate_report_content(self):
        """Test report content generation."""
        content = self.report_agent._generate_report_content(
            self.test_data,
            {'test': 'analysis'},
            'monthly',
            'Test Report'
        )
        
        self.assertIsInstance(content, str)
        self.assertGreater(len(content), 0)
    
    def test_generate_executive_summary(self):
        """Test executive summary generation."""
        summary = self.report_agent._generate_executive_summary(
            {'test': 'analysis'},
            'monthly'
        )
        
        self.assertIsInstance(summary, str)
        self.assertGreater(len(summary), 0)
    
    def test_generate_recommendations(self):
        """Test recommendations generation."""
        recommendations = self.report_agent._generate_recommendations(
            {'test': 'analysis'}
        )
        
        self.assertIsInstance(recommendations, list)
        self.assertGreater(len(recommendations), 0)
    
    def test_generate_trend_analysis(self):
        """Test trend analysis."""
        historical_data = [
            {'timestamp': '2024-01-01', 'sales': 1000},
            {'timestamp': '2024-01-02', 'sales': 1100},
            {'timestamp': '2024-01-03', 'sales': 1200}
        ]
        
        result = self.report_agent.generate_trend_analysis(historical_data, 'sales')
        
        self.assertIn('metric', result)
        self.assertIn('trend_direction', result)
        self.assertIn('change_rate', result)


class TestSummarizerAgent(unittest.TestCase):
    """Test cases for Summarizer Agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.llm_client = LLMClient('mock', {'mock_response_delay': 0})
        self.summarizer = SummarizerAgent(self.llm_client)
        
        self.test_text = """
        Artificial Intelligence is revolutionizing various industries. Machine learning algorithms
        are being used to automate processes, analyze data, and provide insights. Companies are
        investing heavily in AI technologies to improve efficiency and gain competitive advantages.
        However, challenges include data privacy concerns and the need for skilled personnel.
        """
    
    def test_summarize_text(self):
        """Test text summarization."""
        result = self.summarizer.summarize_text(self.test_text, 'general', 50)
        
        self.assertIn('summary', result)
        self.assertIn('key_points', result)
        self.assertIn('summary_type', result)
        self.assertIn('original_length', result)
        self.assertIn('summary_length', result)
        self.assertIn('compression_ratio', result)
        
        self.assertEqual(result['summary_type'], 'general')
        self.assertGreater(result['original_length'], 0)
        self.assertGreater(result['summary_length'], 0)
    
    def test_summarize_meeting(self):
        """Test meeting summarization."""
        transcript = """
        John: We need to discuss the quarterly results.
        Mary: The revenue increased by 15%.
        John: That's great news. What about customer satisfaction?
        Mary: It improved to 4.2 out of 5.
        """
        
        participants = ['John', 'Mary']
        result = self.summarizer.summarize_meeting(transcript, participants)
        
        self.assertIn('meeting_summary', result)
        self.assertIn('participants', result)
        self.assertEqual(result['participants'], participants)
    
    def test_summarize_document(self):
        """Test document summarization."""
        result = self.summarizer.summarize_document(self.test_text, 'article')
        
        self.assertIn('summary', result)
        self.assertIn('document_type', result)
        self.assertIn('metadata', result)
        self.assertEqual(result['document_type'], 'article')
    
    def test_batch_summarize(self):
        """Test batch summarization."""
        texts = [
            "Short text one.",
            "Short text two.",
            "Short text three."
        ]
        
        results = self.summarizer.batch_summarize(texts, 'brief')
        
        self.assertEqual(len(results), 3)
        
        for result in results:
            self.assertIn('summary', result)
            self.assertIn('batch_index', result)
    
    def test_extract_key_points(self):
        """Test key points extraction."""
        key_points = self.summarizer._extract_key_points(self.test_text)
        
        self.assertIsInstance(key_points, list)
        self.assertLessEqual(len(key_points), 5)  # Max 5 points
    
    def test_get_default_length(self):
        """Test default length calculation."""
        executive_length = self.summarizer._get_default_length('executive')
        general_length = self.summarizer._get_default_length('general')
        detailed_length = self.summarizer._get_default_length('detailed')
        
        self.assertEqual(executive_length, 150)
        self.assertEqual(general_length, 250)
        self.assertEqual(detailed_length, 500)


class TestIntegration(unittest.TestCase):
    """Integration tests for agents working together."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.llm_client = LLMClient('mock', {'mock_response_delay': 0})
        self.email_agent = EmailAgent(self.llm_client)
        self.report_agent = ReportAgent(self.llm_client)
        self.summarizer = SummarizerAgent(self.llm_client)
    
    def test_email_to_report_workflow(self):
        """Test workflow from email processing to report generation."""
        # Process email
        email_result = self.email_agent.process_email(
            "I'm experiencing issues with the new feature rollout.",
            "manager@company.com",
            "Feature Rollout Issues"
        )
        
        # Create report data from email
        report_data = {
            'email_analysis': email_result,
            'issue_category': email_result.get('category'),
            'priority': 'high'
        }
        
        # Generate report
        report_result = self.report_agent.generate_report(
            report_data,
            'incident',
            'Feature Rollout Issue Report'
        )
        
        # Verify workflow
        self.assertTrue(email_result['processed'])
        self.assertEqual(report_result['status'], 'completed')
        self.assertIn('content', report_result)
    
    def test_summarization_integration(self):
        """Test summarizer integration with other agents."""
        # Generate report
        test_data = {'sales': [100, 200, 300], 'customers': 50}
        report_result = self.report_agent.generate_report(test_data, 'monthly')
        
        # Summarize report content
        summary_result = self.summarizer.summarize_text(
            report_result.get('content', ''),
            'executive'
        )
        
        # Verify integration
        self.assertEqual(report_result['status'], 'completed')
        self.assertIn('summary', summary_result)
        self.assertGreater(len(summary_result['summary']), 0)


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.llm_client = LLMClient('mock', {'mock_response_delay': 0})
        self.email_agent = EmailAgent(self.llm_client)
        self.report_agent = ReportAgent(self.llm_client)
        self.summarizer = SummarizerAgent(self.llm_client)
    
    def test_empty_email_processing(self):
        """Test processing empty email."""
        result = self.email_agent.process_email('', '', '')
        
        self.assertIn('processed', result)
        self.assertFalse(result['processed'])
    
    def test_empty_text_summarization(self):
        """Test summarizing empty text."""
        result = self.summarizer.summarize_text('', 'general')
        
        self.assertIn('error', result)
    
    def test_invalid_report_data(self):
        """Test report generation with invalid data."""
        result = self.report_agent.generate_report({}, 'monthly')
        
        # Should still generate a report even with minimal data
        self.assertIn('status', result)
    
    def test_missing_llm_client(self):
        """Test behavior when LLM client is not available."""
        # This would be tested with mocking the LLM client to raise exceptions
        pass


def run_tests():
    """Run all tests."""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestLLMClient,
        TestEmailAgent,
        TestReportAgent,
        TestSummarizerAgent,
        TestIntegration,
        TestErrorHandling
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    print("Running AI Automation Agents Test Suite")
    print("=" * 50)
    
    success = run_tests()
    
    if success:
        print("\nSUCCESS: All tests passed!")
    else:
        print("\nERROR: Some tests failed!")
    
    print("=" * 50)
