"""
Customer Support Workflow for automated ticket processing and response generation.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import uuid

from src.agents.email_agent import EmailAgent
from src.agents.summarizer import SummarizerAgent
from src.utils.llm_client import LLMClient


class CustomerSupportWorkflow:
    """
    Workflow for automating customer support ticket processing and responses.
    """
    
    def __init__(self, llm_client: LLMClient, config: Dict = None):
        """
        Initialize the customer support workflow.
        
        Args:
            llm_client: Client for LLM interactions
            config: Configuration dictionary
        """
        self.llm_client = llm_client
        self.config = config or {}
        self.email_agent = EmailAgent(llm_client)
        self.summarizer = SummarizerAgent(llm_client)
        self.logger = logging.getLogger(__name__)
        
        # Initialize ticket storage
        self.tickets = {}
        self.knowledge_base = self._load_knowledge_base()
    
    def process_incoming_ticket(self, ticket_data: Dict) -> Dict:
        """
        Process an incoming customer support ticket.
        
        Args:
            ticket_data: Dictionary containing ticket information
            
        Returns:
            Processing results
        """
        try:
            # Generate unique ticket ID
            ticket_id = str(uuid.uuid4())[:8]
            
            # Extract ticket information
            customer_email = ticket_data.get('customer_email', '')
            subject = ticket_data.get('subject', '')
            message = ticket_data.get('message', '')
            priority = ticket_data.get('priority', 'normal')
            category = ticket_data.get('category', '')
            
            # Create ticket record
            ticket = {
                'ticket_id': ticket_id,
                'customer_email': customer_email,
                'subject': subject,
                'message': message,
                'priority': priority,
                'category': category,
                'status': 'new',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'assigned_to': None,
                'responses': [],
                'tags': []
            }
            
            # Process the ticket
            processed_ticket = self._process_ticket_content(ticket)
            
            # Check for similar tickets
            similar_tickets = self._find_similar_tickets(processed_ticket)
            
            # Generate initial response
            response = self._generate_initial_response(processed_ticket, similar_tickets)
            
            # Update ticket with processing results
            processed_ticket['auto_response'] = response
            processed_ticket['similar_tickets'] = similar_tickets
            
            # Store ticket
            self.tickets[ticket_id] = processed_ticket
            
            # Determine if escalation is needed
            escalation_needed = self._check_escalation_needed(processed_ticket)
            
            return {
                'ticket_id': ticket_id,
                'status': 'processed',
                'auto_response': response,
                'similar_tickets': similar_tickets,
                'escalation_needed': escalation_needed,
                'processed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error processing ticket: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }
    
    def handle_ticket_followup(self, ticket_id: str, 
                              followup_message: str) -> Dict:
        """
        Handle follow-up messages for existing tickets.
        
        Args:
            ticket_id: ID of the existing ticket
            followup_message: Follow-up message from customer
            
        Returns:
            Follow-up processing results
        """
        try:
            if ticket_id not in self.tickets:
                return {'error': 'Ticket not found', 'ticket_id': ticket_id}
            
            ticket = self.tickets[ticket_id]
            
            # Add follow-up to ticket
            followup = {
                'timestamp': datetime.now().isoformat(),
                'message': followup_message,
                'type': 'customer_followup'
            }
            
            ticket['responses'].append(followup)
            ticket['updated_at'] = datetime.now().isoformat()
            
            # Analyze follow-up sentiment and urgency
            sentiment_analysis = self._analyze_sentiment(followup_message)
            urgency_score = self._calculate_urgency(followup_message)
            
            # Generate follow-up response
            response = self._generate_followup_response(
                ticket, followup_message, sentiment_analysis
            )
            
            # Update ticket status if needed
            if urgency_score > 0.8:
                ticket['priority'] = 'high'
                ticket['status'] = 'urgent'
            
            # Add response to ticket
            agent_response = {
                'timestamp': datetime.now().isoformat(),
                'message': response,
                'type': 'agent_response',
                'auto_generated': True
            }
            
            ticket['responses'].append(agent_response)
            
            return {
                'ticket_id': ticket_id,
                'followup_response': response,
                'sentiment': sentiment_analysis,
                'urgency_score': urgency_score,
                'ticket_updated': True,
                'processed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error handling follow-up: {str(e)}")
            return {
                'error': str(e),
                'ticket_id': ticket_id,
                'processed_at': datetime.now().isoformat()
            }
    
    def escalate_ticket(self, ticket_id: str, reason: str, 
                       escalate_to: str = 'senior_agent') -> Dict:
        """
        Escalate a ticket to a higher level of support.
        
        Args:
            ticket_id: ID of the ticket to escalate
            reason: Reason for escalation
            escalate_to: Target escalation level
            
        Returns:
            Escalation results
        """
        try:
            if ticket_id not in self.tickets:
                return {'error': 'Ticket not found', 'ticket_id': ticket_id}
            
            ticket = self.tickets[ticket_id]
            
            # Create escalation record
            escalation = {
                'timestamp': datetime.now().isoformat(),
                'reason': reason,
                'escalated_to': escalate_to,
                'escalated_by': 'system',
                'previous_priority': ticket['priority']
            }
            
            # Update ticket
            ticket['status'] = 'escalated'
            ticket['priority'] = 'critical'
            ticket['escalation'] = escalation
            ticket['updated_at'] = datetime.now().isoformat()
            
            # Generate escalation summary
            escalation_summary = self._generate_escalation_summary(ticket, reason)
            
            # Notify escalation target (placeholder)
            self._notify_escalation_target(ticket, escalate_to, escalation_summary)
            
            return {
                'ticket_id': ticket_id,
                'escalated': True,
                'escalated_to': escalate_to,
                'reason': reason,
                'summary': escalation_summary,
                'escalated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error escalating ticket: {str(e)}")
            return {
                'error': str(e),
                'ticket_id': ticket_id,
                'escalated': False
            }
    
    def generate_ticket_summary(self, ticket_id: str) -> Dict:
        """
        Generate a comprehensive summary of a ticket.
        
        Args:
            ticket_id: ID of the ticket
            
        Returns:
            Ticket summary
        """
        try:
            if ticket_id not in self.tickets:
                return {'error': 'Ticket not found', 'ticket_id': ticket_id}
            
            ticket = self.tickets[ticket_id]
            
            # Compile all messages
            all_messages = [ticket['message']]
            for response in ticket['responses']:
                all_messages.append(response['message'])
            
            combined_text = '\n\n'.join(all_messages)
            
            # Generate summary
            summary_result = self.summarizer.summarize_text(
                combined_text, 
                'detailed'
            )
            
            # Extract key information
            key_info = {
                'ticket_id': ticket_id,
                'customer_email': ticket['customer_email'],
                'subject': ticket['subject'],
                'priority': ticket['priority'],
                'status': ticket['status'],
                'created_at': ticket['created_at'],
                'updated_at': ticket['updated_at'],
                'total_responses': len(ticket['responses']),
                'resolution_time': self._calculate_resolution_time(ticket)
            }
            
            return {
                'ticket_summary': summary_result.get('summary'),
                'key_points': summary_result.get('key_points'),
                'ticket_info': key_info,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating ticket summary: {str(e)}")
            return {
                'error': str(e),
                'ticket_id': ticket_id
            }
    
    def _process_ticket_content(self, ticket: Dict) -> Dict:
        """
        Process and categorize ticket content.
        
        Args:
            ticket: Original ticket data
            
        Returns:
            Processed ticket with additional metadata
        """
        processed_ticket = ticket.copy()
        
        # Analyze email content
        email_result = self.email_agent.process_email(
            ticket['message'],
            ticket['customer_email'],
            ticket['subject']
        )
        
        # Update ticket with email processing results
        processed_ticket['category'] = email_result.get('category', ticket.get('category', ''))
        processed_ticket['summary'] = email_result.get('summary', '')
        
        # Extract keywords and tags
        keywords = self._extract_keywords(ticket['message'])
        processed_ticket['keywords'] = keywords
        
        # Determine priority if not set
        if not ticket.get('priority') or ticket['priority'] == 'normal':
            calculated_priority = self._calculate_priority(ticket)
            processed_ticket['priority'] = calculated_priority
        
        return processed_ticket
    
    def _find_similar_tickets(self, ticket: Dict) -> List[Dict]:
        """
        Find similar historical tickets.
        
        Args:
            ticket: Current ticket
            
        Returns:
            List of similar tickets
        """
        similar_tickets = []
        
        # Search through existing tickets for similarities
        for existing_id, existing_ticket in self.tickets.items():
            if existing_id != ticket.get('ticket_id'):
                similarity_score = self._calculate_similarity(
                    ticket['message'], 
                    existing_ticket['message']
                )
                
                if similarity_score > 0.7:  # Similarity threshold
                    similar_tickets.append({
                        'ticket_id': existing_id,
                        'similarity_score': similarity_score,
                        'subject': existing_ticket['subject'],
                        'category': existing_ticket['category'],
                        'resolution': existing_ticket.get('resolution', 'N/A')
                    })
        
        # Sort by similarity score and return top 3
        similar_tickets.sort(key=lambda x: x['similarity_score'], reverse=True)
        return similar_tickets[:3]
    
    def _generate_initial_response(self, ticket: Dict, 
                                  similar_tickets: List[Dict]) -> str:
        """
        Generate initial automated response for the ticket.
        
        Args:
            ticket: Processed ticket data
            similar_tickets: List of similar tickets
            
        Returns:
            Generated response
        """
        # Check knowledge base first
        kb_response = self._search_knowledge_base(ticket)
        if kb_response:
            return kb_response
        
        # Use similar tickets for context
        context = ""
        if similar_tickets:
            context = f"Similar issues have been resolved in the past. "
        
        # Generate response using email agent
        response = self.email_agent._generate_response(
            ticket['message'],
            ticket['customer_email'],
            ticket['category']
        )
        
        return f"{context}{response}"
    
    def _check_escalation_needed(self, ticket: Dict) -> bool:
        """
        Check if ticket needs escalation.
        
        Args:
            ticket: Processed ticket
            
        Returns:
            True if escalation is needed
        """
        escalation_triggers = [
            ticket['priority'] == 'critical',
            'urgent' in ticket['keywords'],
            'complaint' in ticket['category'],
            len(ticket['message']) > 1000  # Long message might indicate complexity
        ]
        
        return any(escalation_triggers)
    
    def _analyze_sentiment(self, message: str) -> Dict:
        """
        Analyze sentiment of a message.
        
        Args:
            message: Message to analyze
            
        Returns:
            Sentiment analysis results
        """
        # Placeholder for sentiment analysis
        # In practice, you would use a sentiment analysis library or API
        
        positive_words = ['happy', 'satisfied', 'good', 'great', 'excellent', 'thank']
        negative_words = ['angry', 'frustrated', 'disappointed', 'bad', 'terrible', 'unhappy']
        
        message_lower = message.lower()
        
        positive_count = sum(1 for word in positive_words if word in message_lower)
        negative_count = sum(1 for word in negative_words if word in message_lower)
        
        if positive_count > negative_count:
            sentiment = 'positive'
        elif negative_count > positive_count:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return {
            'sentiment': sentiment,
            'positive_score': positive_count,
            'negative_score': negative_count,
            'confidence': 0.8  # Placeholder
        }
    
    def _calculate_urgency(self, message: str) -> float:
        """
        Calculate urgency score of a message.
        
        Args:
            message: Message to analyze
            
        Returns:
            Urgency score between 0 and 1
        """
        urgent_words = ['urgent', 'asap', 'immediately', 'emergency', 'critical', 'as soon as possible']
        message_lower = message.lower()
        
        urgent_count = sum(1 for word in urgent_words if word in message_lower)
        
        # Simple urgency calculation
        base_urgency = min(urgent_count * 0.3, 0.9)
        
        # Add urgency based on message length (longer messages might be more complex)
        length_factor = min(len(message) / 1000, 0.1)
        
        return min(base_urgency + length_factor, 1.0)
    
    def _generate_followup_response(self, ticket: Dict, 
                                   followup_message: str,
                                   sentiment: Dict) -> str:
        """
        Generate response for follow-up message.
        
        Args:
            ticket: Original ticket
            followup_message: Follow-up message
            sentiment: Sentiment analysis
            
        Returns:
            Generated response
        """
        # Adjust response based on sentiment
        if sentiment['sentiment'] == 'negative':
            prefix = "I understand your frustration and I'm here to help resolve this issue. "
        elif sentiment['sentiment'] == 'positive':
            prefix = "I'm glad to hear from you! "
        else:
            prefix = "Thank you for the additional information. "
        
        # Generate contextual response
        prompt = f"""
        Generate a helpful response to this customer follow-up:
        
        Original issue: {ticket['subject']}
        Follow-up message: {followup_message}
        
        {prefix}Please provide a clear, empathetic response that addresses their concern.
        """
        
        return self.llm_client.generate_response(prompt)
    
    def _calculate_resolution_time(self, ticket: Dict) -> str:
        """
        Calculate ticket resolution time.
        
        Args:
            ticket: Ticket data
            
        Returns:
            Resolution time as string
        """
        if ticket['status'] not in ['resolved', 'closed']:
            return 'N/A'
        
        created = datetime.fromisoformat(ticket['created_at'])
        updated = datetime.fromisoformat(ticket['updated_at'])
        
        resolution_time = updated - created
        
        hours = resolution_time.total_seconds() / 3600
        
        if hours < 1:
            return f"{int(hours * 60)} minutes"
        elif hours < 24:
            return f"{int(hours)} hours"
        else:
            return f"{int(hours / 24)} days"
    
    def _extract_keywords(self, text: str) -> List[str]:
        """
        Extract keywords from text.
        
        Args:
            text: Text to analyze
            
        Returns:
            List of keywords
        """
        # Placeholder for keyword extraction
        # In practice, you would use NLP techniques
        common_keywords = [
            'login', 'password', 'account', 'billing', 'payment', 'refund',
            'bug', 'error', 'issue', 'problem', 'help', 'support',
            'urgent', 'asap', 'emergency', 'critical'
        ]
        
        text_lower = text.lower()
        found_keywords = [kw for kw in common_keywords if kw in text_lower]
        
        return found_keywords
    
    def _calculate_priority(self, ticket: Dict) -> str:
        """
        Calculate ticket priority based on content.
        
        Args:
            ticket: Ticket data
            
        Returns:
            Priority level
        """
        message_lower = ticket['message'].lower()
        subject_lower = ticket['subject'].lower()
        
        # Check for high priority indicators
        high_priority_indicators = [
            'urgent', 'emergency', 'critical', 'asap', 'immediately'
        ]
        
        for indicator in high_priority_indicators:
            if indicator in message_lower or indicator in subject_lower:
                return 'high'
        
        # Check for medium priority indicators
        medium_priority_indicators = [
            'problem', 'issue', 'broken', 'not working', 'error'
        ]
        
        for indicator in medium_priority_indicators:
            if indicator in message_lower or indicator in subject_lower:
                return 'medium'
        
        return 'normal'
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate similarity between two texts.
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Similarity score between 0 and 1
        """
        # Simple word overlap similarity
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        if not union:
            return 0.0
        
        return len(intersection) / len(union)
    
    def _search_knowledge_base(self, ticket: Dict) -> Optional[str]:
        """
        Search knowledge base for relevant responses.
        
        Args:
            ticket: Ticket data
            
        Returns:
            Knowledge base response if found
        """
        # Placeholder for knowledge base search
        # In practice, you would search a database or file system
        
        ticket_category = ticket.get('category', '').lower()
        
        if ticket_category in self.knowledge_base:
            return self.knowledge_base[ticket_category]
        
        return None
    
    def _load_knowledge_base(self) -> Dict[str, str]:
        """
        Load knowledge base responses.
        
        Returns:
            Knowledge base dictionary
        """
        # Placeholder knowledge base
        return {
            'login_issue': "I can help you with login issues. Please try resetting your password using the 'Forgot Password' link on the login page. If that doesn't work, please provide your registered email address so I can assist further.",
            'billing_inquiry': "For billing inquiries, I can check your account status and recent transactions. Please provide your account number or the email associated with your account.",
            'technical_support': "I understand you're experiencing technical difficulties. Please provide details about the error message you're seeing and what steps you took before the issue occurred.",
            'account_question': "I can help with account-related questions. What specific information do you need about your account?"
        }
    
    def _generate_escalation_summary(self, ticket: Dict, reason: str) -> str:
        """
        Generate summary for escalated ticket.
        
        Args:
            ticket: Escalated ticket
            reason: Escalation reason
            
        Returns:
            Escalation summary
        """
        return f"""
        Ticket Escalation Summary:
        
        Ticket ID: {ticket['ticket_id']}
        Customer: {ticket['customer_email']}
        Subject: {ticket['subject']}
        Original Priority: {ticket['escalation']['previous_priority']}
        Current Priority: {ticket['priority']}
        
        Escalation Reason: {reason}
        
        Issue Summary: {ticket.get('summary', 'N/A')}
        
        Customer Messages: {len(ticket['responses']) + 1}
        Time Since Creation: {self._calculate_resolution_time(ticket)}
        """
    
    def _notify_escalation_target(self, ticket: Dict, 
                                escalate_to: str, 
                                summary: str) -> bool:
        """
        Notify escalation target about the ticket.
        
        Args:
            ticket: Escalated ticket
            escalate_to: Target escalation level
            summary: Escalation summary
            
        Returns:
            True if notification sent successfully
        """
        # Placeholder for notification system
        # In practice, you would send email, Slack message, etc.
        self.logger.info(f"Escalation notification sent to {escalate_to} for ticket {ticket['ticket_id']}")
        return True
