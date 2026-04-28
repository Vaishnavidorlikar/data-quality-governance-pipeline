"""
Summarizer Agent for generating concise summaries of various content types.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from src.utils.llm_client import LLMClient
from src.utils.prompt_templates import (
    TEXT_SUMMARY_TEMPLATE, 
    MEETING_SUMMARY_TEMPLATE,
    DOCUMENT_SUMMARY_TEMPLATE
)


class SummarizerAgent:
    """
    Agent for generating summaries of various types of content.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Summarizer Agent.
        
        Args:
            llm_client: Client for LLM interactions
        """
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
    
    def summarize_text(self, text: str, summary_type: str = 'general',
                      max_length: int = None) -> Dict:
        """
        Generate a summary of the provided text.
        
        Args:
            text: Text to summarize
            summary_type: Type of summary (general, executive, detailed)
            max_length: Maximum length of summary in words
            
        Returns:
            Dictionary containing summary results
        """
        try:
            if not text or len(text.strip()) < 50:
                return {
                    'error': 'Text too short to summarize',
                    'original_length': len(text),
                    'summary': text
                }
            
            # Determine appropriate summary length
            if max_length is None:
                max_length = self._get_default_length(summary_type)
            
            # Generate summary
            prompt = TEXT_SUMMARY_TEMPLATE.format(
                text=text[:4000],  # Limit input size
                summary_type=summary_type,
                max_length=max_length
            )
            
            summary = self.llm_client.generate_response(prompt)
            
            # Extract key points
            key_points = self._extract_key_points(text)
            
            return {
                'summary': summary,
                'key_points': key_points,
                'summary_type': summary_type,
                'original_length': len(text),
                'summary_length': len(summary.split()),
                'compression_ratio': len(summary.split()) / len(text.split()) if text.split() else 0,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error summarizing text: {str(e)}")
            return {'error': str(e)}
    
    def summarize_meeting(self, meeting_transcript: str, 
                         participants: List[str] = None) -> Dict:
        """
        Generate a meeting summary from transcript.
        
        Args:
            meeting_transcript: Full meeting transcript
            participants: List of meeting participants
            
        Returns:
            Dictionary containing meeting summary
        """
        try:
            prompt = MEETING_SUMMARY_TEMPLATE.format(
                transcript=meeting_transcript[:4000],
                participants=', '.join(participants or [])
            )
            
            response = self.llm_client.generate_response(prompt)
            
            # Parse the structured response
            summary_data = self._parse_meeting_summary(response)
            
            return {
                'meeting_summary': summary_data,
                'participants': participants or [],
                'transcript_length': len(meeting_transcript),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error summarizing meeting: {str(e)}")
            return {'error': str(e)}
    
    def summarize_document(self, document_text: str, 
                          document_type: str = 'general') -> Dict:
        """
        Generate a document summary.
        
        Args:
            document_text: Full document text
            document_type: Type of document (report, article, research, etc.)
            
        Returns:
            Dictionary containing document summary
        """
        try:
            prompt = DOCUMENT_SUMMARY_TEMPLATE.format(
                document_text=document_text[:4000],
                document_type=document_type
            )
            
            response = self.llm_client.generate_response(prompt)
            
            # Extract metadata
            metadata = self._extract_document_metadata(document_text)
            
            return {
                'summary': response,
                'document_type': document_type,
                'metadata': metadata,
                'original_length': len(document_text),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error summarizing document: {str(e)}")
            return {'error': str(e)}
    
    def batch_summarize(self, texts: List[str], 
                       summary_type: str = 'general') -> List[Dict]:
        """
        Summarize multiple texts in batch.
        
        Args:
            texts: List of texts to summarize
            summary_type: Type of summary for all texts
            
        Returns:
            List of summary results
        """
        results = []
        for i, text in enumerate(texts):
            result = self.summarize_text(text, summary_type)
            result['batch_index'] = i
            results.append(result)
        
        return results
    
    def _get_default_length(self, summary_type: str) -> int:
        """
        Get default summary length based on type.
        
        Args:
            summary_type: Type of summary
            
        Returns:
            Default maximum length in words
        """
        length_map = {
            'executive': 150,
            'general': 250,
            'detailed': 500,
            'brief': 100
        }
        return length_map.get(summary_type.lower(), 250)
    
    def _extract_key_points(self, text: str) -> List[str]:
        """
        Extract key points from text.
        
        Args:
            text: Source text
            
        Returns:
            List of key points
        """
        prompt = f"""
        Extract 3-5 key points from the following text:
        
        {text[:2000]}
        
        Format each key point as a clear, concise bullet point.
        """
        
        response = self.llm_client.generate_response(prompt)
        key_points = [point.strip() for point in response.split('\n') if point.strip()]
        return key_points[:5]  # Limit to 5 points
    
    def _parse_meeting_summary(self, response: str) -> Dict:
        """
        Parse structured meeting summary response.
        
        Args:
            response: LLM response for meeting summary
            
        Returns:
            Structured meeting summary data
        """
        # This is a simplified parser - in practice, you might want more sophisticated parsing
        lines = response.split('\n')
        summary_data = {
            'overall_summary': '',
            'key_decisions': [],
            'action_items': [],
            'next_steps': []
        }
        
        current_section = 'overall_summary'
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if 'key decisions:' in line.lower():
                current_section = 'key_decisions'
            elif 'action items:' in line.lower():
                current_section = 'action_items'
            elif 'next steps:' in line.lower():
                current_section = 'next_steps'
            elif line.startswith('-') or line.startswith('•'):
                item = line.lstrip('- •').strip()
                if current_section in summary_data and isinstance(summary_data[current_section], list):
                    summary_data[current_section].append(item)
            else:
                if current_section == 'overall_summary':
                    summary_data[current_section] += line + ' '
        
        return summary_data
    
    def _extract_document_metadata(self, text: str) -> Dict:
        """
        Extract basic metadata from document text.
        
        Args:
            text: Document text
            
        Returns:
            Document metadata
        """
        metadata = {
            'word_count': len(text.split()),
            'character_count': len(text),
            'paragraph_count': len([p for p in text.split('\n\n') if p.strip()]),
            'sentence_count': len([s for s in text.split('.') if s.strip()])
        }
        
        # Try to extract potential title (first line if it's short)
        first_line = text.split('\n')[0].strip()
        if len(first_line) < 100 and first_line:
            metadata['potential_title'] = first_line
        
        return metadata
