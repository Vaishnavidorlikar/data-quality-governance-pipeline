"""
Report Agent for automated report generation and data analysis.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd

from src.utils.llm_client import LLMClient
from src.utils.prompt_templates import REPORT_GENERATION_TEMPLATE, DATA_ANALYSIS_TEMPLATE


class ReportAgent:
    """
    Agent for generating automated reports and analyzing data.
    """
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize the Report Agent.
        
        Args:
            llm_client: Client for LLM interactions
        """
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
    
    def generate_report(self, data: Dict[str, Any], report_type: str, 
                       title: str = None) -> Dict:
        """
        Generate a comprehensive report based on provided data.
        
        Args:
            data: Dictionary containing data for the report
            report_type: Type of report (daily, weekly, monthly, custom)
            title: Optional custom title for the report
            
        Returns:
            Dictionary containing generated report
        """
        try:
            # Analyze the data
            analysis = self._analyze_data(data)
            
            # Generate report content
            report_content = self._generate_report_content(
                data, analysis, report_type, title
            )
            
            # Generate executive summary
            executive_summary = self._generate_executive_summary(
                analysis, report_type
            )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(analysis)
            
            return {
                'title': title or f"{report_type.title()} Report",
                'report_type': report_type,
                'generated_at': datetime.now().isoformat(),
                'data_analysis': analysis,
                'executive_summary': executive_summary,
                'content': report_content,
                'recommendations': recommendations,
                'status': 'completed'
            }
            
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            return {
                'error': str(e),
                'status': 'failed',
                'generated_at': datetime.now().isoformat()
            }
    
    def _analyze_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze the provided data and extract key insights.
        
        Args:
            data: Raw data dictionary
            
        Returns:
            Dictionary containing data analysis
        """
        analysis = {}
        
        # Handle different data types
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0:
                # Analyze list data
                if all(isinstance(x, (int, float)) for x in value):
                    analysis[f"{key}_stats"] = {
                        'count': len(value),
                        'sum': sum(value),
                        'average': sum(value) / len(value),
                        'min': min(value),
                        'max': max(value)
                    }
                elif isinstance(value[0], dict):
                    # Analyze list of dictionaries
                    df = pd.DataFrame(value)
                    analysis[f"{key}_summary"] = {
                        'records': len(df),
                        'columns': list(df.columns),
                        'sample_data': df.head(3).to_dict('records')
                    }
            elif isinstance(value, dict):
                analysis[f"{key}_keys"] = list(value.keys())
                analysis[f"{key}_summary"] = f"Dictionary with {len(value)} items"
        
        # Generate insights using LLM
        if data:
            prompt = DATA_ANALYSIS_TEMPLATE.format(data=str(data)[:2000])
            llm_insights = self.llm_client.generate_response(prompt)
            analysis['llm_insights'] = llm_insights
        
        return analysis
    
    def _generate_report_content(self, data: Dict[str, Any], 
                                analysis: Dict[str, Any], 
                                report_type: str, 
                                title: str = None) -> str:
        """
        Generate the main content of the report.
        
        Args:
            data: Raw data
            analysis: Data analysis results
            report_type: Type of report
            title: Report title
            
        Returns:
            Generated report content
        """
        prompt = REPORT_GENERATION_TEMPLATE.format(
            title=title or f"{report_type.title()} Report",
            data=str(data)[:1500],
            analysis=str(analysis)[:1500],
            report_type=report_type
        )
        
        return self.llm_client.generate_response(prompt)
    
    def _generate_executive_summary(self, analysis: Dict[str, Any], 
                                  report_type: str) -> str:
        """
        Generate an executive summary for the report.
        
        Args:
            analysis: Data analysis results
            report_type: Type of report
            
        Returns:
            Executive summary
        """
        prompt = f"""
        Generate a concise executive summary (2-3 paragraphs) for a {report_type} report
        based on the following analysis:
        
        {str(analysis)[:1000]}
        
        Focus on key findings, trends, and business impact.
        """
        
        return self.llm_client.generate_response(prompt)
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """
        Generate actionable recommendations based on data analysis.
        
        Args:
            analysis: Data analysis results
            
        Returns:
            List of recommendations
        """
        prompt = f"""
        Based on the following data analysis, generate 3-5 actionable recommendations:
        
        {str(analysis)[:1000]}
        
        Format each recommendation as a clear, actionable bullet point.
        """
        
        response = self.llm_client.generate_response(prompt)
        # Split into bullet points
        recommendations = [rec.strip() for rec in response.split('\n') if rec.strip()]
        return recommendations
    
    def generate_trend_analysis(self, historical_data: List[Dict], 
                              metric: str) -> Dict:
        """
        Generate trend analysis for a specific metric over time.
        
        Args:
            historical_data: List of historical data points
            metric: Metric to analyze
            
        Returns:
            Trend analysis results
        """
        try:
            # Extract metric values over time
            values = []
            timestamps = []
            
            for data_point in historical_data:
                if metric in data_point:
                    values.append(data_point[metric])
                    timestamps.append(data_point.get('timestamp', datetime.now().isoformat()))
            
            if len(values) < 2:
                return {'error': 'Insufficient data for trend analysis'}
            
            # Calculate basic trend metrics
            trend_direction = 'increasing' if values[-1] > values[0] else 'decreasing'
            change_rate = ((values[-1] - values[0]) / values[0]) * 100 if values[0] != 0 else 0
            
            # Generate trend insights
            prompt = f"""
            Analyze the following trend data for {metric}:
            Values: {values}
            Time period: {timestamps[0]} to {timestamps[-1]}
            
            Provide insights about the trend, including:
            1. Overall pattern
            2. Notable changes
            3. Potential causes
            4. Future outlook
            """
            
            insights = self.llm_client.generate_response(prompt)
            
            return {
                'metric': metric,
                'trend_direction': trend_direction,
                'change_rate': change_rate,
                'data_points': len(values),
                'insights': insights,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error in trend analysis: {str(e)}")
            return {'error': str(e)}
