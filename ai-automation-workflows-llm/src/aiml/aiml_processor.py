"""
AIML (Artificial Intelligence Markup Language) Processor for conversational AI.
"""

import re
import logging
from typing import Dict, List, Optional, Any
import json
import os
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime


class AIMLProcessor:
    """
    AIML processor for creating conversational AI with pattern matching.
    """
    
    def __init__(self, aiml_file: str = None):
        """
        Initialize AIML processor.
        
        Args:
            aiml_file: Path to AIML file
        """
        self.logger = logging.getLogger(__name__)
        
        # Pattern-response storage
        self.patterns = {}
        self.categories = []
        self.variables = {}
        self.that_stack = []
        
        # Load default patterns
        self._load_default_patterns()
        
        # Load custom AIML file if provided
        if aiml_file and os.path.exists(aiml_file):
            self.load_aiml_file(aiml_file)
    
    def _load_default_patterns(self):
        """Load default conversational patterns."""
        default_patterns = {
            # Greetings
            r"(?i)^(hello|hi|hey|good morning|good afternoon|good evening)": [
                "Hello! How can I help you today?",
                "Hi there! What can I do for you?",
                "Greetings! How may I assist you?"
            ],
            
            # Farewells
            r"(?i)^(goodbye|bye|see you|farewell|take care)": [
                "Goodbye! Have a great day!",
                "See you later! Take care.",
                "Farewell! It was nice talking to you."
            ],
            
            # How are you
            r"(?i)^(how are you|how do you do|how's it going)": [
                "I'm functioning perfectly, thank you for asking!",
                "I'm operating at optimal capacity! How about you?",
                "All systems are running smoothly! How are you?"
            ],
            
            # Name questions
            r"(?i)^(what is your name|who are you|what should I call you)": [
                "I'm Enterprise AI, your AI assistant. I'm here to help with various tasks.",
                "You can call me Enterprise AI. I'm an advanced AI assistant.",
                "My name is Enterprise AI, and I'm designed to assist you with intelligent automation."
            ],
            
            # Capabilities
            r"(?i)^(what can you do|what are your capabilities|what do you do)": [
                "I can help with voice commands, gesture recognition, data analysis, machine learning, and much more!",
                "I'm equipped with natural language processing, computer vision, deep learning, and data analysis capabilities.",
                "I can process voice commands, recognize hand gestures, analyze data, build ML models, and provide intelligent assistance."
            ],
            
            # Help
            r"(?i)^(help|what help can you provide|assist me)": [
                "I can help you with: voice commands, gesture recognition, data analysis, ML model training, and automation workflows.",
                "Try asking me to: analyze data, recognize gestures, train models, or help with automation tasks.",
                "I'm here to assist with AI-powered tasks. Just tell me what you need help with!"
            ],
            
            # Thanks
            r"(?i)^(thank you|thanks|appreciate it|grateful)": [
                "You're welcome! I'm always here to help.",
                "My pleasure! Is there anything else I can assist you with?",
                "Happy to help! Let me know if you need anything else."
            ],
            
            # Time
            r"(?i)^(what time is it|current time|time now)": [
                lambda: f"The current time is {datetime.now().strftime('%I:%M %p')}.",
                lambda: f"It's {datetime.now().strftime('%H:%M')} hours.",
                lambda: f"Time is {datetime.now().strftime('%I:%M:%S %p')}."
            ],
            
            # Date
            r"(?i)^(what is the date|today's date|current date)": [
                lambda: f"Today is {datetime.now().strftime('%A, %B %d, %Y')}.",
                lambda: f"The date is {datetime.now().strftime('%Y-%m-%d')}.",
                lambda: f"Today's date is {datetime.now().strftime('%d/%m/%Y')}."
            ],
            
            # Weather (placeholder)
            r"(?i)^(what's the weather|how's the weather|weather forecast)": [
                "I can help you check the weather, but I'd need access to weather data for your location.",
                "Weather information requires external API access. Would you like me to help you set that up?",
                "I don't have current weather data, but I can help you integrate weather APIs."
            ],
            
            # Enterprise AI specific
            r"(?i)^(are you enterprise_ai|like enterprise_ai|iron man)": [
                "Yes, I'm inspired by Enterprise AI from Iron Man! I aim to provide intelligent assistance.",
                "I share capabilities with Enterprise AI - voice recognition, data analysis, and intelligent responses.",
                "Like the famous Enterprise AI, I'm here to be your intelligent AI assistant!"
            ],
            
            # System status
            r"(?i)^(system status|how are you working|status check)": [
                "All systems operational! Voice recognition, gesture detection, and AI processing are active.",
                "System status: All green! Ready to assist with any task.",
                "Operating at full capacity! All AI modules are functioning properly."
            ],
            
            # Learning
            r"(?i)^(can you learn|do you learn|machine learning)": [
                "I can help you build and train machine learning models using TensorFlow and scikit-learn!",
                "I have deep learning capabilities and can assist with ML model creation and training.",
                "I can perform data analysis, create ML models, and help with various learning algorithms."
            ],
            
            # Data analysis
            r"(?i)^(analyze data|data analysis|process data)": [
                "I can analyze data using NumPy, Pandas, and create visualizations with Matplotlib!",
                "I have comprehensive data analysis capabilities - statistics, visualization, and ML modeling.",
                    "I can process, analyze, and visualize your data with advanced analytics tools."
            ],
            
            # Gesture recognition
            r"(?i)^(gesture|hand gesture|recognize gesture)": [
                "I can recognize hand gestures using computer vision and MediaPipe!",
                "My gesture recognition system can detect various hand signs and movements.",
                "I'm equipped with advanced hand gesture recognition capabilities."
            ],
            
            # Default responses for unknown patterns
            "default": [
                "I'm not sure I understand. Could you rephrase that?",
                "That's interesting! Could you tell me more?",
                "I'm still learning. Could you explain that differently?",
                "I may not have a response for that. Can I help you with something else?",
                "I'm processing your request. Could you provide more details?"
            ]
        }
        
        # Convert patterns to regex and store
        for pattern, responses in default_patterns.items():
            if pattern != "default":
                self.patterns[re.compile(pattern)] = responses
            else:
                self.patterns["default"] = responses
    
    def load_aiml_file(self, file_path: str):
        """
        Load patterns from AIML file.
        
        Args:
            file_path: Path to AIML XML file
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            for category in root.findall('category'):
                pattern = category.find('pattern').text.strip()
                template = category.find('template').text.strip()
                
                # Convert AIML pattern to regex
                regex_pattern = self._aiml_to_regex(pattern)
                
                if regex_pattern in self.patterns:
                    self.patterns[regex_pattern].append(template)
                else:
                    self.patterns[regex_pattern] = [template]
            
            self.logger.info(f"Loaded AIML patterns from {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error loading AIML file: {str(e)}")
    
    def _aiml_to_regex(self, aiml_pattern: str) -> str:
        """
        Convert AIML pattern to regex.
        
        Args:
            aiml_pattern: AIML pattern string
            
        Returns:
            Regex pattern string
        """
        # Convert AIML wildcards to regex
        pattern = aiml_pattern.lower()
        
        # Handle AIML wildcards
        pattern = pattern.replace('*', '.*')
        pattern = pattern.replace('_', '.+')
        
        # Add word boundaries and make case insensitive
        pattern = f"^{pattern}$"
        
        return pattern
    
    def respond(self, input_text: str, context: Dict = None) -> str:
        """
        Generate response to input text using pattern matching.
        
        Args:
            input_text: Input text to respond to
            context: Additional context for response
            
        Returns:
            Generated response
        """
        if not input_text or not input_text.strip():
            return "I didn't catch that. Could you please repeat?"
        
        # Clean input
        cleaned_input = input_text.strip().lower()
        
        # Try to match patterns
        for pattern, responses in self.patterns.items():
            if pattern == "default":
                continue
                
            try:
                if pattern.match(cleaned_input):
                    # Select response (randomly if multiple)
                    import random
                    response = random.choice(responses)
                    
                    # Process response if it's a callable (for dynamic content)
                    if callable(response):
                        response = response()
                    
                    # Process AIML tags in response
                    response = self._process_response_tags(response, context)
                    
                    # Store in that stack for context
                    self.that_stack.append(response)
                    if len(self.that_stack) > 5:  # Keep only last 5 responses
                        self.that_stack.pop(0)
                    
                    return response
                    
            except Exception as e:
                self.logger.error(f"Pattern matching error: {str(e)}")
                continue
        
        # Return default response
        import random
        default_responses = self.patterns.get("default", ["I'm not sure how to respond to that."])
        return random.choice(default_responses)
    
    def _process_response_tags(self, response: str, context: Dict = None) -> str:
        """
        Process AIML tags in response template.
        
        Args:
            response: Response template
            context: Context for variable substitution
            
        Returns:
            Processed response
        """
        if context is None:
            context = {}
        
        # Process variables
        response = self._substitute_variables(response, context)
        
        # Process conditional tags
        response = self._process_conditionals(response, context)
        
        # Process random tags
        response = self._process_random(response)
        
        return response
    
    def _substitute_variables(self, text: str, context: Dict) -> str:
        """Substitute variables in text."""
        # Substitute user variables
        for key, value in context.items():
            text = text.replace(f"<get name=\"{key}\"/>", str(value))
            text = text.replace(f"<get name='{key}'/>", str(value))
        
        # Substitute system variables
        text = text.replace("<get name=\"name\"/>", "user")
        text = text.replace("<get name='name'/>", "user")
        
        return text
    
    def _process_conditionals(self, text: str, context: Dict) -> str:
        """Process conditional tags in text."""
        # Simple conditional processing
        # In a full implementation, this would handle <condition> tags
        return text
    
    def _process_random(self, text: str) -> str:
        """Process random selection tags."""
        import random
        
        # Handle <random> tags
        random_pattern = r'<random>(.*?)</random>'
        matches = re.findall(random_pattern, text, re.DOTALL)
        
        for match in matches:
            # Split by <li> tags
            options = re.split(r'<li>', match)
            options = [opt.strip() for opt in options if opt.strip()]
            
            if options:
                # Remove closing </li> tags if present
                options = [re.sub(r'</li>', '', opt) for opt in options]
                selected = random.choice(options)
                text = text.replace(f'<random>{match}</random>', selected, 1)
        
        return text
    
    def add_pattern(self, pattern: str, response: str):
        """
        Add a new pattern-response pair.
        
        Args:
            pattern: Pattern string (can include wildcards)
            response: Response string
        """
        try:
            # Convert to regex
            regex_pattern = self._aiml_to_regex(pattern)
            compiled_pattern = re.compile(regex_pattern)
            
            # Add to patterns
            if compiled_pattern in self.patterns:
                self.patterns[compiled_pattern].append(response)
            else:
                self.patterns[compiled_pattern] = [response]
            
            self.logger.info(f"Added pattern: {pattern}")
            
        except Exception as e:
            self.logger.error(f"Error adding pattern: {str(e)}")
    
    def remove_pattern(self, pattern: str) -> bool:
        """
        Remove a pattern.
        
        Args:
            pattern: Pattern to remove
            
        Returns:
            True if removed successfully
        """
        try:
            regex_pattern = self._aiml_to_regex(pattern)
            compiled_pattern = re.compile(regex_pattern)
            
            if compiled_pattern in self.patterns:
                del self.patterns[compiled_pattern]
                self.logger.info(f"Removed pattern: {pattern}")
                return True
            else:
                self.logger.warning(f"Pattern not found: {pattern}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error removing pattern: {str(e)}")
            return False
    
    def learn_from_interaction(self, input_text: str, user_feedback: str = None):
        """
        Learn from user interactions.
        
        Args:
            input_text: User input
            user_feedback: User feedback on the response
        """
        # This is a simplified learning mechanism
        # In a full implementation, this would update the AIML knowledge base
        
        if user_feedback and user_feedback.lower() in ['good', 'correct', 'helpful']:
            self.logger.info(f"Positive feedback for: {input_text}")
        elif user_feedback and user_feedback.lower() in ['bad', 'wrong', 'unhelpful']:
            self.logger.info(f"Negative feedback for: {input_text}")
    
    def save_patterns(self, file_path: str):
        """
        Save current patterns to AIML file.
        
        Args:
            file_path: Path to save AIML file
        """
        try:
            root = ET.Element("aiml")
            
            for pattern, responses in self.patterns.items():
                if pattern == "default":
                    continue
                
                for response in responses:
                    category = ET.SubElement(root, "category")
                    
                    # Convert regex back to AIML pattern (simplified)
                    aiml_pattern = self._regex_to_aiml(pattern.pattern)
                    
                    pattern_elem = ET.SubElement(category, "pattern")
                    pattern_elem.text = aiml_pattern
                    
                    template_elem = ET.SubElement(category, "template")
                    template_elem.text = response
            
            # Write to file
            tree = ET.ElementTree(root)
            tree.write(file_path, encoding='utf-8', xml_declaration=True)
            
            self.logger.info(f"Patterns saved to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving patterns: {str(e)}")
    
    def _regex_to_aiml(self, regex_pattern: str) -> str:
        """
        Convert regex pattern back to AIML pattern (simplified).
        
        Args:
            regex_pattern: Regex pattern
            
        Returns:
            AIML pattern
        """
        # Remove regex anchors
        pattern = regex_pattern.strip('^$')
        
        # Convert regex wildcards back to AIML
        pattern = pattern.replace('.*', '*')
        pattern = pattern.replace('.+', '_')
        
        return pattern.upper()
    
    def get_pattern_count(self) -> int:
        """Get the number of loaded patterns."""
        return len([p for p in self.patterns.keys() if p != "default"])
    
    def get_recent_responses(self, count: int = 5) -> List[str]:
        """
        Get recent responses for context.
        
        Args:
            count: Number of recent responses to return
            
        Returns:
            List of recent responses
        """
        return self.that_stack[-count:] if self.that_stack else []
    
    def clear_context(self):
        """Clear conversation context."""
        self.that_stack.clear()
        self.variables.clear()
    
    def set_variable(self, name: str, value: str):
        """Set a conversation variable."""
        self.variables[name] = value
    
    def get_variable(self, name: str) -> Optional[str]:
        """Get a conversation variable."""
        return self.variables.get(name)
    
    def export_patterns_json(self, file_path: str):
        """
        Export patterns as JSON for easier editing.
        
        Args:
            file_path: Path to save JSON file
        """
        try:
            patterns_data = {}
            
            for pattern, responses in self.patterns.items():
                if pattern == "default":
                    continue
                
                pattern_str = pattern.pattern if hasattr(pattern, 'pattern') else str(pattern)
                patterns_data[pattern_str] = responses
            
            with open(file_path, 'w') as f:
                json.dump(patterns_data, f, indent=2)
            
            self.logger.info(f"Patterns exported to JSON: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error exporting patterns: {str(e)}")
    
    def import_patterns_json(self, file_path: str):
        """
        Import patterns from JSON file.
        
        Args:
            file_path: Path to JSON file
        """
        try:
            with open(file_path, 'r') as f:
                patterns_data = json.load(f)
            
            for pattern_str, responses in patterns_data.items():
                try:
                    compiled_pattern = re.compile(pattern_str)
                    self.patterns[compiled_pattern] = responses
                except Exception as e:
                    self.logger.error(f"Error importing pattern {pattern_str}: {str(e)}")
            
            self.logger.info(f"Patterns imported from JSON: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error importing patterns: {str(e)}")
    
    def get_statistics(self) -> Dict:
        """
        Get statistics about the AIML processor.
        
        Returns:
            Statistics dictionary
        """
        return {
            'total_patterns': self.get_pattern_count(),
            'total_responses': sum(len(responses) for responses in self.patterns.values()),
            'variables_count': len(self.variables),
            'context_depth': len(self.that_stack),
            'last_response': self.that_stack[-1] if self.that_stack else None
        }
