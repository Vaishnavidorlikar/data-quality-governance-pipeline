"""
Email Agent for automated email processing and responses.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

from src.utils.llm_client import LLMClient
from src.utils.prompt_templates import EMAIL_RESPONSE_TEMPLATE, EMAIL_SUMMARY_TEMPLATE


class EmailAgent:
    """
    Agent for processing emails, generating responses, and categorizing messages.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Email Agent.
        
        Args:
            llm_client: Client for LLM interactions
        """
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
    
    def process_email(self, email_content: str, sender: str, subject: str) -> Dict:
        """
        Process an incoming email and generate appropriate response.
        
        Args:
            email_content: The body of the email
            sender: Email sender
            subject: Email subject
            
        Returns:
            Dictionary containing categorized response
        """
        try:
            # Categorize the email
            category = self._categorize_email(email_content, subject)
            
            # Generate summary
            summary = self._generate_summary(email_content)
            
            # Generate response if needed
            response = None
            if category in ['support_request', 'inquiry', 'complaint']:
                response = self._generate_response(email_content, sender, category)
            
            return {
                'sender': sender,
                'subject': subject,
                'category': category,
                'summary': summary,
                'response': response,
                'timestamp': datetime.now().isoformat(),
                'processed': True
            }
            
        except Exception as e:
            self.logger.error(f"Error processing email: {str(e)}")
            return {
                'sender': sender,
                'subject': subject,
                'error': str(e),
                'processed': False
            }
    
    def _categorize_email(self, content: str, subject: str) -> str:
        """
        Categorize the email based on content and subject.
        
        Args:
            content: Email body
            subject: Email subject
            
        Returns:
            Email category
        """
        categories = [
            'support_request',
            'inquiry', 
            'complaint',
            'information',
            'spam',
            'newsletter'
        ]
        
        prompt = f"""
        Categorize the following email into one of these categories: {', '.join(categories)}
        
        Subject: {subject}
        Content: {content[:500]}...
        
        Return only the category name.
        """
        
        response = self.llm_client.generate_response(prompt)
        return response.strip().lower() if response else 'information'
    
    def _generate_summary(self, content: str) -> str:
        """
        Generate a concise summary of the email content.
        
        Args:
            content: Email body
            
        Returns:
            Email summary
        """
        prompt = EMAIL_SUMMARY_TEMPLATE.format(content=content)
        return self.llm_client.generate_response(prompt)
    
    def _generate_response(self, content: str, sender: str, category: str) -> str:
        """
        Generate an appropriate response based on email category.
        
        Args:
            content: Email body
            sender: Email sender
            category: Email category
            
        Returns:
            Generated response
        """
        prompt = EMAIL_RESPONSE_TEMPLATE.format(
            content=content,
            sender=sender,
            category=category
        )
        return self.llm_client.generate_response(prompt)
    
    def batch_process_emails(self, emails: List[Dict]) -> List[Dict]:
        """
        Process multiple emails in batch.
        
        Args:
            emails: List of email dictionaries
            
        Returns:
            List of processed email results
        """
        results = []
        for email in emails:
            result = self.process_email(
                email.get('content', ''),
                email.get('sender', ''),
                email.get('subject', '')
            )
            results.append(result)
        
        return results
