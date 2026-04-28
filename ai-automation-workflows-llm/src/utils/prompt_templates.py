"""
Prompt templates for various LLM interactions.
"""

# Email processing templates
EMAIL_RESPONSE_TEMPLATE = """
You are a helpful customer service agent. Generate a professional and empathetic response to the following email:

From: {sender}
Category: {category}

Email Content:
{content}

Guidelines:
- Be professional and empathetic
- Address the main concern directly
- Provide helpful information or next steps
- Keep the response concise but thorough
- Include a polite closing

Response:
"""

EMAIL_SUMMARY_TEMPLATE = """
Summarize the following email in 2-3 sentences, capturing the main points and any action items:

Email Content:
{content}

Summary:
"""

# Report generation templates
REPORT_GENERATION_TEMPLATE = """
Generate a comprehensive {report_type} report based on the following data and analysis:

Report Title: {title}

Data:
{data}

Analysis:
{analysis}

Please structure the report with:
1. Executive Summary
2. Key Findings
3. Detailed Analysis
4. Recommendations
5. Conclusion

Make the report professional, data-driven, and actionable.
"""

DATA_ANALYSIS_TEMPLATE = """
Analyze the following data and provide key insights:

Data:
{data}

Please provide:
1. Overall trends and patterns
2. Key metrics and their significance
3. Notable anomalies or outliers
4. Business implications
5. Areas requiring attention

Analysis:
"""

# Summarization templates
TEXT_SUMMARY_TEMPLATE = """
Create a {summary_type} summary of the following text. The summary should be approximately {max_length} words.

Text:
{text}

Guidelines:
- Capture the main ideas and key points
- Maintain the original meaning and tone
- Be concise and clear
- Include any important conclusions or recommendations

Summary:
"""

MEETING_SUMMARY_TEMPLATE = """
Generate a comprehensive meeting summary from the following transcript:

Meeting Participants: {participants}

Transcript:
{transcript}

Please structure the summary with:
1. Overall Summary
2. Key Decisions Made
3. Action Items
4. Next Steps
5. Important Discussion Points

Meeting Summary:
"""

DOCUMENT_SUMMARY_TEMPLATE = """
Create a detailed summary of the following {document_type} document:

Document Content:
{document_text}

Please provide:
1. Main purpose and objectives
2. Key findings or arguments
3. Important conclusions
4. Recommendations or implications
5. Target audience and context

Document Summary:
"""

# Customer support templates
TICKET_CATEGORIZATION_TEMPLATE = """
Categorize the following customer support ticket into one of these categories:
- technical_support
- billing_inquiry
- account_management
- product_question
- complaint
- feature_request
- general_inquiry

Ticket Subject: {subject}
Ticket Message: {message}

Category:
"""

TICKET_PRIORITY_TEMPLATE = """
Assess the priority level of the following customer support ticket:

Ticket Subject: {subject}
Ticket Message: {message}

Consider factors like:
- Urgency indicators (urgent, emergency, asap)
- Impact on customer
- Technical complexity
- Business impact

Priority (low/medium/high/critical):
"""

ESCALATION_TEMPLATE = """
Determine if the following customer support ticket should be escalated:

Ticket Details:
- Subject: {subject}
- Message: {message}
- Category: {category}
- Customer History: {customer_history}

Escalation Criteria:
- Complex technical issues
- High-value customer complaints
- Legal or compliance concerns
- Multiple failed resolution attempts

Should this ticket be escalated? (Yes/No)
If yes, provide reason and suggested escalation level.
"""

# Workflow templates
WORKFLOW_STATUS_TEMPLATE = """
Analyze the status of the following workflow and provide recommendations:

Workflow Type: {workflow_type}
Current Status: {status}
Recent Activities: {activities}
Error Messages: {errors}

Please provide:
1. Current state assessment
2. Potential bottlenecks or issues
3. Recommended next steps
4. Optimization suggestions

Analysis:
"""

AUTOMATION_DECISION_TEMPLATE = """
Evaluate whether the following task should be automated:

Task Description: {task_description}
Current Manual Process: {current_process}
Frequency: {frequency}
Complexity: {complexity}
Required Resources: {resources}

Consider:
- Repetitiveness and volume
- Error rates in manual process
- Time and cost savings
- Technical feasibility
- Risk factors

Recommendation (Automate/Don't Automate) with justification:
"""

# Data processing templates
DATA_CLEANING_TEMPLATE = """
Analyze the following dataset and identify data quality issues:

Dataset Sample:
{data_sample}

Schema Information:
{schema}

Please identify:
1. Missing values and patterns
2. Data type inconsistencies
3. Duplicate records
4. Outliers and anomalies
5. Formatting issues
6. Recommended cleaning steps

Data Quality Assessment:
"""

DATA_VALIDATION_TEMPLATE = """
Validate the following data against the specified rules:

Data:
{data}

Validation Rules:
{validation_rules}

For each rule, provide:
- Pass/Fail status
- Specific records that failed
- Recommended corrections

Validation Results:
"""

# Content generation templates
CONTENT_GENERATION_TEMPLATE = """
Generate {content_type} based on the following specifications:

Topic: {topic}
Target Audience: {audience}
Tone: {tone}
Length: {length}
Key Points: {key_points}

Requirements:
{requirements}

Generate professional, engaging content that meets all specifications.
"""

TEMPLATE_CUSTOMIZATION_TEMPLATE = """
Customize the following template for specific use:

Base Template:
{template}

Customization Requirements:
{requirements}

Context:
{context}

Provide the customized template with explanations for changes made.
"""

# Analysis templates
SENTIMENT_ANALYSIS_TEMPLATE = """
Analyze the sentiment of the following text:

Text: {text}

Provide:
1. Overall sentiment (positive/negative/neutral)
2. Confidence score (0-1)
3. Key emotional indicators
4. Sentiment intensity
5. Contextual factors

Sentiment Analysis:
"""

KEYWORD_EXTRACTION_TEMPLATE = """
Extract important keywords and phrases from the following text:

Text: {text}

Please provide:
1. Primary keywords (most important)
2. Secondary keywords (supporting concepts)
3. Named entities (people, organizations, locations)
4. Technical terms or jargon
5. Action words or commands

Keywords and Phrases:
"""

TOPIC_MODELING_TEMPLATE = """
Identify the main topics and themes in the following text:

Text: {text}

Please provide:
1. Primary topic
2. Secondary topics
3. Topic relevance scores
4. Key terms for each topic
5. Topic relationships

Topic Analysis:
"""

# Quality assurance templates
QUALITY_CHECK_TEMPLATE = """
Review the following content for quality and accuracy:

Content Type: {content_type}
Content:
{content}

Quality Criteria:
{quality_criteria}

Please assess:
1. Accuracy and factual correctness
2. Clarity and readability
3. Completeness
4. Formatting and structure
5. Tone and appropriateness
6. Overall quality score (1-10)

Quality Assessment:
"""

ERROR_DETECTION_TEMPLATE = """
Analyze the following content for potential errors or issues:

Content:
{content}

Error Categories to Check:
{error_categories}

Please identify:
1. Spelling and grammar errors
2. Factual inaccuracies
3. Logical inconsistencies
4. Formatting problems
5. Missing information
6. Suggested corrections

Error Analysis:
"""

# Performance templates
PERFORMANCE_ANALYSIS_TEMPLATE = """
Analyze the performance metrics and provide insights:

Metrics Data:
{metrics}

Time Period: {time_period}
Baseline: {baseline}

Please provide:
1. Performance trends
2. Key improvements or declines
3. Factors affecting performance
4. Recommendations for optimization
5. Predictions for future performance

Performance Analysis:
"""

OPTIMIZATION_TEMPLATE = """
Provide optimization recommendations for the following system/process:

System Description:
{system_description}

Current Performance:
{current_performance}

Constraints:
{constraints}

Please suggest:
1. Specific optimization strategies
2. Expected improvements
3. Implementation steps
4. Resource requirements
5. Risk assessment

Optimization Plan:
"""

# Template utility functions
def get_template(template_name: str, **kwargs) -> str:
    """
    Get a template by name and format it with provided kwargs.
    
    Args:
        template_name: Name of the template
        **kwargs: Variables to substitute in template
        
    Returns:
        Formatted template string
    """
    templates = {
        'email_response': EMAIL_RESPONSE_TEMPLATE,
        'email_summary': EMAIL_SUMMARY_TEMPLATE,
        'report_generation': REPORT_GENERATION_TEMPLATE,
        'data_analysis': DATA_ANALYSIS_TEMPLATE,
        'text_summary': TEXT_SUMMARY_TEMPLATE,
        'meeting_summary': MEETING_SUMMARY_TEMPLATE,
        'document_summary': DOCUMENT_SUMMARY_TEMPLATE,
        'ticket_categorization': TICKET_CATEGORIZATION_TEMPLATE,
        'ticket_priority': TICKET_PRIORITY_TEMPLATE,
        'escalation': ESCALATION_TEMPLATE,
        'workflow_status': WORKFLOW_STATUS_TEMPLATE,
        'automation_decision': AUTOMATION_DECISION_TEMPLATE,
        'data_cleaning': DATA_CLEANING_TEMPLATE,
        'data_validation': DATA_VALIDATION_TEMPLATE,
        'content_generation': CONTENT_GENERATION_TEMPLATE,
        'template_customization': TEMPLATE_CUSTOMIZATION_TEMPLATE,
        'sentiment_analysis': SENTIMENT_ANALYSIS_TEMPLATE,
        'keyword_extraction': KEYWORD_EXTRACTION_TEMPLATE,
        'topic_modeling': TOPIC_MODELING_TEMPLATE,
        'quality_check': QUALITY_CHECK_TEMPLATE,
        'error_detection': ERROR_DETECTION_TEMPLATE,
        'performance_analysis': PERFORMANCE_ANALYSIS_TEMPLATE,
        'optimization': OPTIMIZATION_TEMPLATE
    }
    
    template = templates.get(template_name)
    if not template:
        raise ValueError(f"Template '{template_name}' not found")
    
    try:
        return template.format(**kwargs)
    except KeyError as e:
        raise ValueError(f"Missing required template variable: {str(e)}")

def list_templates() -> list:
    """
    List all available template names.
    
    Returns:
        List of template names
    """
    return [
        'email_response',
        'email_summary', 
        'report_generation',
        'data_analysis',
        'text_summary',
        'meeting_summary',
        'document_summary',
        'ticket_categorization',
        'ticket_priority',
        'escalation',
        'workflow_status',
        'automation_decision',
        'data_cleaning',
        'data_validation',
        'content_generation',
        'template_customization',
        'sentiment_analysis',
        'keyword_extraction',
        'topic_modeling',
        'quality_check',
        'error_detection',
        'performance_analysis',
        'optimization'
    ]
