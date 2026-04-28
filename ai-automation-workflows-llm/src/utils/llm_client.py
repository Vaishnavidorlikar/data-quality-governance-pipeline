"""
LLM Client for interacting with various language model APIs.
"""

import logging
from typing import Dict, List, Optional, Any
import os
import json
from abc import ABC, abstractmethod

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


class LLMProvider(ABC):
    """
    Abstract base class for LLM providers.
    """
    
    @abstractmethod
    def generate_response(self, prompt: str, **kwargs) -> str:
        """
        Generate a response from the language model.
        
        Args:
            prompt: Input prompt for the model
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        pass


class OpenAIProvider(LLMProvider):
    """
    OpenAI API provider.
    """
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
        """
        Initialize OpenAI provider.
        
        Args:
            api_key: OpenAI API key
            model: Model to use
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI library not installed. Install with: pip install openai")
        
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        self.logger = logging.getLogger(__name__)
    
    def generate_response(self, prompt: str, **kwargs) -> str:
        """
        Generate response using OpenAI API.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        try:
            messages = [{"role": "user", "content": prompt}]
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=kwargs.get('max_tokens', 1000),
                temperature=kwargs.get('temperature', 0.7),
                **{k: v for k, v in kwargs.items() if k not in ['max_tokens', 'temperature']}
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            self.logger.error(f"OpenAI API error: {str(e)}")
            raise


class AnthropicProvider(LLMProvider):
    """
    Anthropic Claude API provider.
    """
    
    def __init__(self, api_key: str, model: str = "claude-3-sonnet-20240229"):
        """
        Initialize Anthropic provider.
        
        Args:
            api_key: Anthropic API key
            model: Model to use
        """
        if not ANTHROPIC_AVAILABLE:
            raise ImportError("Anthropic library not installed. Install with: pip install anthropic")
        
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.logger = logging.getLogger(__name__)
    
    def generate_response(self, prompt: str, **kwargs) -> str:
        """
        Generate response using Anthropic API.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=kwargs.get('max_tokens', 1000),
                temperature=kwargs.get('temperature', 0.7),
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            self.logger.error(f"Anthropic API error: {str(e)}")
            raise


class MockProvider(LLMProvider):
    """
    Mock provider for testing and development.
    """
    
    def __init__(self, response_delay: float = 0.1):
        """
        Initialize mock provider.
        
        Args:
            response_delay: Simulated response delay in seconds
        """
        self.response_delay = response_delay
        self.logger = logging.getLogger(__name__)
    
    def generate_response(self, prompt: str, **kwargs) -> str:
        """
        Generate mock response.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Mock response
        """
        import time
        time.sleep(self.response_delay)
        
        # Generate a simple mock response based on prompt length
        if len(prompt) < 100:
            return "This is a mock response for a short prompt."
        elif len(prompt) < 500:
            return "This is a mock response for a medium-length prompt. The system is processing your request and generating an appropriate response based on the input provided."
        else:
            return "This is a comprehensive mock response for a long and detailed prompt. The system has analyzed the extensive input and is providing a thorough response that addresses the various aspects mentioned in your request."


class LLMClient:
    """
    Main LLM client that manages different providers.
    """
    
    def __init__(self, provider: str = "mock", config: Dict = None):
        """
        Initialize LLM client.
        
        Args:
            provider: Provider name (openai, anthropic, mock)
            config: Configuration dictionary
        """
        self.config = config or {}
        self.provider_name = provider.lower()
        self.logger = logging.getLogger(__name__)
        
        # Initialize the appropriate provider
        self.provider = self._initialize_provider()
    
    def _initialize_provider(self) -> LLMProvider:
        """
        Initialize the appropriate LLM provider.
        
        Returns:
            Initialized provider instance
        """
        if self.provider_name == "openai":
            api_key = self.config.get('openai_api_key') or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not provided")
            
            model = self.config.get('openai_model', 'gpt-3.5-turbo')
            return OpenAIProvider(api_key, model)
        
        elif self.provider_name == "anthropic":
            api_key = self.config.get('anthropic_api_key') or os.getenv('ANTHROPIC_API_KEY')
            if not api_key:
                raise ValueError("Anthropic API key not provided")
            
            model = self.config.get('anthropic_model', 'claude-3-sonnet-20240229')
            return AnthropicProvider(api_key, model)
        
        elif self.provider_name == "mock":
            response_delay = self.config.get('mock_response_delay', 0.1)
            return MockProvider(response_delay)
        
        else:
            raise ValueError(f"Unknown provider: {self.provider_name}")
    
    def generate_response(self, prompt: str, **kwargs) -> str:
        """
        Generate a response using the configured provider.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        try:
            return self.provider.generate_response(prompt, **kwargs)
        except Exception as e:
            self.logger.error(f"Error generating response: {str(e)}")
            raise
    
    def switch_provider(self, new_provider: str, config: Dict = None) -> None:
        """
        Switch to a different provider.
        
        Args:
            new_provider: New provider name
            config: New configuration
        """
        self.provider_name = new_provider.lower()
        if config:
            self.config.update(config)
        
        self.provider = self._initialize_provider()
        self.logger.info(f"Switched to {new_provider} provider")
    
    def get_provider_info(self) -> Dict:
        """
        Get information about the current provider.
        
        Returns:
            Provider information
        """
        return {
            'provider': self.provider_name,
            'provider_type': type(self.provider).__name__,
            'config': self.config
        }
    
    def test_connection(self) -> bool:
        """
        Test connection to the provider.
        
        Returns:
            True if connection successful
        """
        try:
            test_prompt = "Hello, this is a test message."
            response = self.generate_response(test_prompt)
            return len(response) > 0
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False


class LLMClientManager:
    """
    Manager for multiple LLM clients with load balancing and failover.
    """
    
    def __init__(self, clients_config: List[Dict]):
        """
        Initialize client manager.
        
        Args:
            clients_config: List of client configurations
        """
        self.clients = []
        self.current_client_index = 0
        self.logger = logging.getLogger(__name__)
        
        # Initialize all clients
        for config in clients_config:
            try:
                client = LLMClient(config['provider'], config.get('config', {}))
                self.clients.append(client)
                self.logger.info(f"Initialized {config['provider']} client")
            except Exception as e:
                self.logger.error(f"Failed to initialize {config['provider']} client: {str(e)}")
    
    def get_response(self, prompt: str, **kwargs) -> str:
        """
        Get response using available clients with failover.
        
        Args:
            prompt: Input prompt
            **kwargs: Additional parameters
            
        Returns:
            Generated response
        """
        if not self.clients:
            raise ValueError("No clients available")
        
        # Try current client first
        for attempt in range(len(self.clients)):
            client = self.clients[self.current_client_index]
            
            try:
                response = client.generate_response(prompt, **kwargs)
                return response
            except Exception as e:
                self.logger.warning(f"Client {self.current_client_index} failed: {str(e)}")
                # Move to next client
                self.current_client_index = (self.current_client_index + 1) % len(self.clients)
        
        # If all clients failed, raise exception
        raise RuntimeError("All LLM clients failed")
    
    def get_client_status(self) -> List[Dict]:
        """
        Get status of all clients.
        
        Returns:
            List of client status information
        """
        status_list = []
        
        for i, client in enumerate(self.clients):
            status = {
                'index': i,
                'provider': client.provider_name,
                'active': i == self.current_client_index,
                'connection_test': client.test_connection()
            }
            status_list.append(status)
        
        return status_list
