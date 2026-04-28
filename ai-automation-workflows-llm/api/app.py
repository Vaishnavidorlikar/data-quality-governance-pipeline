"""
FastAPI application for AI Automation Workflows.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path('../src').absolute()))

from utils.llm_client import LLMClient, LLMClientManager
from agents.email_agent import EmailAgent
from agents.report_agent import ReportAgent
from agents.summarizer import SummarizerAgent
from workflows.automate_reporting import AutomatedReportingWorkflow
from workflows.customer_support_flow import CustomerSupportWorkflow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="AI Automation Workflows API",
    description="API for AI-powered automation workflows including email processing, reporting, and customer support",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for agents and workflows
llm_client = None
email_agent = None
report_agent = None
summarizer = None
reporting_workflow = None
support_workflow = None

# Pydantic models for request/response
class EmailRequest(BaseModel):
    content: str = Field(..., description="Email content")
    sender: str = Field(..., description="Email sender")
    subject: str = Field(..., description="Email subject")

class EmailResponse(BaseModel):
    category: str
    summary: str
    response: Optional[str]
    processed: bool
    timestamp: str

class ReportRequest(BaseModel):
    data: Dict[str, Any] = Field(..., description="Data for report generation")
    report_type: str = Field(default="custom", description="Type of report")
    title: Optional[str] = Field(None, description="Report title")

class ReportResponse(BaseModel):
    title: str
    report_type: str
    generated_at: str
    executive_summary: str
    content: str
    recommendations: List[str]
    status: str

class SummaryRequest(BaseModel):
    text: str = Field(..., description="Text to summarize")
    summary_type: str = Field(default="general", description="Type of summary")
    max_length: Optional[int] = Field(None, description="Maximum summary length")

class SummaryResponse(BaseModel):
    summary: str
    key_points: List[str]
    summary_type: str
    original_length: int
    summary_length: int
    compression_ratio: float

class TicketRequest(BaseModel):
    customer_email: str = Field(..., description="Customer email")
    subject: str = Field(..., description="Ticket subject")
    message: str = Field(..., description="Ticket message")
    priority: str = Field(default="normal", description="Ticket priority")
    category: Optional[str] = Field(None, description="Ticket category")

class TicketResponse(BaseModel):
    ticket_id: str
    status: str
    auto_response: str
    similar_tickets: List[Dict]
    escalation_needed: bool
    processed_at: str

class WorkflowReportRequest(BaseModel):
    data_sources: List[str] = Field(..., description="Data sources for report")
    report_type: str = Field(default="daily", description="Type of report")
    recipients: Optional[List[str]] = Field(None, description="Report recipients")
    include_trends: bool = Field(default=False, description="Include trend analysis")

class WorkflowReportResponse(BaseModel):
    workflow_type: str
    status: str
    report_id: Optional[str]
    summary: Optional[str]
    trend_analysis: Optional[Dict]
    recipients: List[str]
    generated_at: str

class LLMConfig(BaseModel):
    provider: str = Field(default="mock", description="LLM provider")
    api_key: Optional[str] = Field(None, description="API key for provider")
    model: Optional[str] = Field(None, description="Model name")
    config: Optional[Dict[str, Any]] = Field(None, description="Additional config")

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    components: Dict[str, str]

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    global llm_client, email_agent, report_agent, summarizer, reporting_workflow, support_workflow
    
    try:
        # Initialize LLM client with mock provider by default
        llm_client = LLMClient("mock", {"mock_response_delay": 0.1})
        
        # Initialize agents
        email_agent = EmailAgent(llm_client)
        report_agent = ReportAgent(llm_client)
        summarizer = SummarizerAgent(llm_client)
        
        # Initialize workflows
        reporting_workflow = AutomatedReportingWorkflow(llm_client)
        support_workflow = CustomerSupportWorkflow(llm_client)
        
        logger.info("Application initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}")
        raise

# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Check the health of the API and its components."""
    components = {
        "llm_client": "healthy" if llm_client else "unhealthy",
        "email_agent": "healthy" if email_agent else "unhealthy",
        "report_agent": "healthy" if report_agent else "unhealthy",
        "summarizer": "healthy" if summarizer else "unhealthy",
        "reporting_workflow": "healthy" if reporting_workflow else "unhealthy",
        "support_workflow": "healthy" if support_workflow else "unhealthy"
    }
    
    overall_status = "healthy" if all(status == "healthy" for status in components.values()) else "degraded"
    
    return HealthResponse(
        status=overall_status,
        timestamp=datetime.now().isoformat(),
        components=components
    )

# LLM configuration endpoint
@app.post("/config/llm")
async def configure_llm(config: LLMConfig):
    """Configure the LLM provider."""
    global llm_client, email_agent, report_agent, summarizer, reporting_workflow, support_workflow
    
    try:
        # Prepare configuration
        llm_config = config.config or {}
        
        if config.provider == "openai":
            llm_config["openai_api_key"] = config.api_key
            if config.model:
                llm_config["openai_model"] = config.model
        elif config.provider == "anthropic":
            llm_config["anthropic_api_key"] = config.api_key
            if config.model:
                llm_config["anthropic_model"] = config.model
        
        # Reinitialize LLM client
        llm_client = LLMClient(config.provider, llm_config)
        
        # Test connection
        if not llm_client.test_connection():
            raise HTTPException(status_code=400, detail="Failed to connect to LLM provider")
        
        # Reinitialize agents and workflows with new client
        email_agent = EmailAgent(llm_client)
        report_agent = ReportAgent(llm_client)
        summarizer = SummarizerAgent(llm_client)
        reporting_workflow = AutomatedReportingWorkflow(llm_client)
        support_workflow = CustomerSupportWorkflow(llm_client)
        
        return {"message": f"LLM provider configured successfully", "provider": config.provider}
        
    except Exception as e:
        logger.error(f"Failed to configure LLM: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Email processing endpoints
@app.post("/email/process", response_model=EmailResponse)
async def process_email(request: EmailRequest):
    """Process an email and generate response."""
    try:
        result = email_agent.process_email(request.content, request.sender, request.subject)
        
        return EmailResponse(
            category=result.get("category", "unknown"),
            summary=result.get("summary", ""),
            response=result.get("response"),
            processed=result.get("processed", False),
            timestamp=result.get("timestamp", datetime.now().isoformat())
        )
        
    except Exception as e:
        logger.error(f"Error processing email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/email/batch")
async def batch_process_emails(emails: List[EmailRequest]):
    """Process multiple emails in batch."""
    try:
        email_data = [
            {
                "content": email.content,
                "sender": email.sender,
                "subject": email.subject
            }
            for email in emails
        ]
        
        results = email_agent.batch_process_emails(email_data)
        return {"results": results, "processed_count": len(results)}
        
    except Exception as e:
        logger.error(f"Error in batch email processing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Report generation endpoints
@app.post("/report/generate", response_model=ReportResponse)
async def generate_report(request: ReportRequest):
    """Generate a report from data."""
    try:
        result = report_agent.generate_report(request.data, request.report_type, request.title)
        
        return ReportResponse(
            title=result.get("title", ""),
            report_type=result.get("report_type", ""),
            generated_at=result.get("generated_at", ""),
            executive_summary=result.get("executive_summary", ""),
            content=result.get("content", ""),
            recommendations=result.get("recommendations", []),
            status=result.get("status", "")
        )
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/report/trends")
async def analyze_trends(
    historical_data: List[Dict[str, Any]],
    metric: str
):
    """Analyze trends for a specific metric."""
    try:
        result = report_agent.generate_trend_analysis(historical_data, metric)
        return result
        
    except Exception as e:
        logger.error(f"Error analyzing trends: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Summarization endpoints
@app.post("/summarize/text", response_model=SummaryResponse)
async def summarize_text(request: SummaryRequest):
    """Summarize text content."""
    try:
        result = summarizer.summarize_text(request.text, request.summary_type, request.max_length)
        
        return SummaryResponse(
            summary=result.get("summary", ""),
            key_points=result.get("key_points", []),
            summary_type=result.get("summary_type", ""),
            original_length=result.get("original_length", 0),
            summary_length=result.get("summary_length", 0),
            compression_ratio=result.get("compression_ratio", 0.0)
        )
        
    except Exception as e:
        logger.error(f"Error summarizing text: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/summarize/meeting")
async def summarize_meeting(
    transcript: str,
    participants: Optional[List[str]] = None
):
    """Summarize meeting transcript."""
    try:
        result = summarizer.summarize_meeting(transcript, participants or [])
        return result
        
    except Exception as e:
        logger.error(f"Error summarizing meeting: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/summarize/document")
async def summarize_document(
    document_text: str,
    document_type: str = "general"
):
    """Summarize document content."""
    try:
        result = summarizer.summarize_document(document_text, document_type)
        return result
        
    except Exception as e:
        logger.error(f"Error summarizing document: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Customer support endpoints
@app.post("/support/ticket", response_model=TicketResponse)
async def create_ticket(request: TicketRequest):
    """Create and process a support ticket."""
    try:
        ticket_data = {
            "customer_email": request.customer_email,
            "subject": request.subject,
            "message": request.message,
            "priority": request.priority,
            "category": request.category
        }
        
        result = support_workflow.process_incoming_ticket(ticket_data)
        
        return TicketResponse(
            ticket_id=result.get("ticket_id", ""),
            status=result.get("status", ""),
            auto_response=result.get("auto_response", ""),
            similar_tickets=result.get("similar_tickets", []),
            escalation_needed=result.get("escalation_needed", False),
            processed_at=result.get("processed_at", "")
        )
        
    except Exception as e:
        logger.error(f"Error creating ticket: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/support/ticket/{ticket_id}/followup")
async def handle_followup(
    ticket_id: str,
    followup_message: str
):
    """Handle follow-up message for existing ticket."""
    try:
        result = support_workflow.handle_ticket_followup(ticket_id, followup_message)
        return result
        
    except Exception as e:
        logger.error(f"Error handling follow-up: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/support/ticket/{ticket_id}/escalate")
async def escalate_ticket(
    ticket_id: str,
    reason: str,
    escalate_to: str = "senior_agent"
):
    """Escalate a ticket to higher support level."""
    try:
        result = support_workflow.escalate_ticket(ticket_id, reason, escalate_to)
        return result
        
    except Exception as e:
        logger.error(f"Error escalating ticket: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/support/ticket/{ticket_id}/summary")
async def get_ticket_summary(ticket_id: str):
    """Get comprehensive summary of a ticket."""
    try:
        result = support_workflow.generate_ticket_summary(ticket_id)
        return result
        
    except Exception as e:
        logger.error(f"Error generating ticket summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Workflow endpoints
@app.post("/workflow/report", response_model=WorkflowReportResponse)
async def run_reporting_workflow(request: WorkflowReportRequest):
    """Run automated reporting workflow."""
    try:
        if request.report_type == "daily":
            result = reporting_workflow.run_daily_report(request.data_sources, request.recipients)
        elif request.report_type == "weekly":
            result = reporting_workflow.run_weekly_report(
                request.data_sources, 
                request.include_trends, 
                request.recipients
            )
        else:
            # Custom report
            config = {"type": request.report_type, "include_trends": request.include_trends}
            result = reporting_workflow.run_custom_report(request.data_sources, config, request.recipients)
        
        return WorkflowReportResponse(
            workflow_type=result.get("workflow_type", ""),
            status=result.get("status", ""),
            report_id=result.get("report_id"),
            summary=result.get("summary"),
            trend_analysis=result.get("trend_analysis"),
            recipients=result.get("recipients", []),
            generated_at=result.get("generated_at", "")
        )
        
    except Exception as e:
        logger.error(f"Error running reporting workflow: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Utility endpoints
@app.get("/templates/list")
async def list_templates():
    """List all available prompt templates."""
    try:
        from utils.prompt_templates import list_templates
        return {"templates": list_templates()}
        
    except Exception as e:
        logger.error(f"Error listing templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/info/providers")
async def get_provider_info():
    """Get information about current LLM provider."""
    try:
        if llm_client:
            return llm_client.get_provider_info()
        else:
            return {"error": "LLM client not initialized"}
            
    except Exception as e:
        logger.error(f"Error getting provider info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Background task example
@app.post("/workflow/report/async")
async def run_async_report(
    background_tasks: BackgroundTasks,
    request: WorkflowReportRequest
):
    """Run reporting workflow in background."""
    try:
        task_id = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Add background task
        background_tasks.add_task(
            run_reporting_workflow_background,
            task_id,
            request.data_sources,
            request.report_type,
            request.recipients,
            request.include_trends
        )
        
        return {
            "message": "Report generation started",
            "task_id": task_id,
            "status": "processing"
        }
        
    except Exception as e:
        logger.error(f"Error starting async report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_reporting_workflow_background(
    task_id: str,
    data_sources: List[str],
    report_type: str,
    recipients: Optional[List[str]],
    include_trends: bool
):
    """Background task for report generation."""
    try:
        if report_type == "daily":
            result = reporting_workflow.run_daily_report(data_sources, recipients)
        elif report_type == "weekly":
            result = reporting_workflow.run_weekly_report(data_sources, include_trends, recipients)
        else:
            config = {"type": report_type, "include_trends": include_trends}
            result = reporting_workflow.run_custom_report(data_sources, config, recipients)
        
        # In a real implementation, you would save the result and notify the user
        logger.info(f"Background task {task_id} completed: {result.get('status')}")
        
    except Exception as e:
        logger.error(f"Background task {task_id} failed: {str(e)}")

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
