"""
Data Quality Governance Dashboard
Enterprise Data Quality Monitoring Dashboard
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_quality_data():
    """Generate realistic data quality monitoring data"""
    
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
    
    # Quality Scores
    overall_quality = np.random.normal(93.5, 2, len(dates))
    completeness = np.random.normal(95, 3, len(dates))
    accuracy = np.random.normal(92, 4, len(dates))
    consistency = np.random.normal(94, 2.5, len(dates))
    timeliness = np.random.normal(91, 3.5, len(dates))
    
    # Quality Issues
    issues_detected = np.random.poisson(15, len(dates))
    issues_resolved = np.random.poisson(12, len(dates))
    
    # Validation Metrics
    validation_rules = np.cumsum(np.random.randint(1, 5, len(dates)))
    automated_checks = np.random.normal(85, 10, len(dates))
    
    # Business Impact
    risk_score = np.random.normal(25, 8, len(dates))
    cost_savings = np.cumsum(np.random.normal(1200, 200, len(dates)))
    
    # Compliance Metrics
    compliance_score = np.random.normal(96, 2, len(dates))
    audit_passed = np.random.binomial(1, 0.95, len(dates))
    
    return {
        'dates': dates,
        'overall_quality': overall_quality,
        'completeness': completeness,
        'accuracy': accuracy,
        'consistency': consistency,
        'timeliness': timeliness,
        'issues_detected': issues_detected,
        'issues_resolved': issues_resolved,
        'validation_rules': validation_rules,
        'automated_checks': automated_checks,
        'risk_score': risk_score,
        'cost_savings': cost_savings,
        'compliance_score': compliance_score,
        'audit_passed': audit_passed
    }

def create_quality_dashboard():
    """Create comprehensive data quality dashboard"""
    
    data = generate_quality_data()
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=(
            'Overall Quality Score', 'Quality Dimensions', 'Issues Detected vs Resolved',
            'Validation Performance', 'Risk Assessment', 'Cost Savings',
            'Compliance Score', 'Audit Results', 'Data Trust Index'
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": True}, {"secondary_y": True}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}]
        ]
    )
    
    # Overall Quality Score
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['overall_quality'],
            mode='lines+markers',
            name='Overall Quality',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=4)
        ),
        row=1, col=1
    )
    
    # Quality Dimensions
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['completeness'],
            mode='lines',
            name='Completeness',
            line=dict(color='#2E86AB', width=2)
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['accuracy'],
            mode='lines',
            name='Accuracy',
            line=dict(color='#E71D36', width=2)
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['consistency'],
            mode='lines',
            name='Consistency',
            line=dict(color='#FF9F1C', width=2)
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['timeliness'],
            mode='lines',
            name='Timeliness',
            line=dict(color='#A23B72', width=2)
        ),
        row=1, col=2
    )
    
    # Issues Detected vs Resolved
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['issues_detected'],
            mode='lines+markers',
            name='Issues Detected',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=1, col=3
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['issues_resolved'],
            mode='lines+markers',
            name='Issues Resolved',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=3)
        ),
        row=1, col=3
    )
    
    # Validation Performance
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['validation_rules'],
            mode='lines',
            name='Validation Rules',
            line=dict(color='#A23B72', width=2)
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['automated_checks'],
            mode='lines+markers',
            name='Automated Checks',
            line=dict(color='#FF9F1C', width=2),
            marker=dict(size=3)
        ),
        row=2, col=1, secondary_y=True
    )
    
    # Risk Assessment
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['risk_score'],
            mode='lines+markers',
            name='Risk Score',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=4),
            fill='tonexty'
        ),
        row=2, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=[50] * len(data['dates']),  # Risk threshold
            mode='lines',
            name='Risk Threshold',
            line=dict(color='red', width=2, dash='dash')
        ),
        row=2, col=2, secondary_y=True
    )
    
    # Cost Savings
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['cost_savings'],
            mode='lines+markers',
            name='Cumulative Savings',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=4),
            fill='tonexty'
        ),
        row=2, col=3
    )
    
    # Compliance Score
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['compliance_score'],
            mode='lines+markers',
            name='Compliance Score',
            line=dict(color='#A23B72', width=2),
            marker=dict(size=3)
        ),
        row=3, col=1
    )
    
    # Audit Results
    audit_results = ['Pass' if x == 1 else 'Fail' for x in data['audit_passed']]
    pass_count = [audit_results[i:i+7].count('Pass') for i in range(0, len(audit_results), 7)]
    
    fig.add_trace(
        go.Bar(
            x=data['dates'][::7][:len(pass_count)],
            y=pass_count,
            name='Weekly Audit Passes',
            marker_color='#2E86AB'
        ),
        row=3, col=2
    )
    
    # Data Trust Index
    trust_index = (data['completeness'] + data['accuracy'] + data['consistency'] + data['compliance_score']) / 4
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=trust_index,
            mode='lines+markers',
            name='Data Trust Index',
            line=dict(color='#FF9F1C', width=3),
            marker=dict(size=4)
        ),
        row=3, col=3
    )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Data Quality Governance Dashboard',
            x=0.5,
            font=dict(size=20, color='#1f2937')
        ),
        height=1400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template='plotly_white',
        margin=dict(l=50, r=50, t=80, b=50),
        grid=dict(rows=3, columns=3, pattern="independent"),
        subplot_titles=dict(font=dict(size=12))
    )
    
    # Update axes labels with better spacing
    fig.update_xaxes(title_text="Date", row=3, col=1, tickfont=dict(size=10))
    fig.update_xaxes(title_text="Date", row=3, col=2, tickfont=dict(size=10))
    fig.update_xaxes(title_text="Date", row=3, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Quality Score (%)", row=1, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Quality Score (%)", row=1, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Number of Issues", row=1, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Rules Count", row=2, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Automated Checks (%)", row=2, col=1, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Risk Score", row=2, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Risk Threshold", row=2, col=2, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Cost Savings ($)", row=2, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Compliance (%)", row=3, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Passes per Week", row=3, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Trust Index", row=3, col=3, tickfont=dict(size=10))
    
    return fig

def create_quality_heatmap():
    """Create quality heatmap for different data domains"""
    
    # Sample data for different domains
    domains = ['Customer Data', 'Financial Data', 'Product Data', 'Operations', 'Marketing']
    quality_metrics = ['Completeness', 'Accuracy', 'Consistency', 'Timeliness', 'Validity']
    
    # Generate quality scores matrix
    np.random.seed(42)
    quality_matrix = np.random.uniform(85, 98, (len(quality_metrics), len(domains)))
    
    fig = go.Figure(data=go.Heatmap(
        z=quality_matrix,
        x=domains,
        y=quality_metrics,
        colorscale='RdYlGn',
        colorbar=dict(title="Quality Score (%)"),
        text=np.round(quality_matrix, 1),
        texttemplate="%{text}%",
        textfont={"size": 10},
        hoverongaps=False
    ))
    
    fig.update_layout(
        title='Data Quality Heatmap by Domain',
        xaxis_title='Data Domains',
        yaxis_title='Quality Metrics',
        height=500,
        template='plotly_white'
    )
    
    return fig

# Main execution
if __name__ == "__main__":
    # Create main quality dashboard
    quality_dashboard = create_quality_dashboard()
    quality_dashboard.show()
    
    # Create quality heatmap
    heatmap_dashboard = create_quality_heatmap()
    heatmap_dashboard.show()
    
    # Save dashboards
    quality_dashboard.write_html("data_quality_dashboard.html")
    heatmap_dashboard.write_html("quality_heatmap.html")
    
    print("Data Quality Governance Dashboard created successfully!")
    print("Files saved: data_quality_dashboard.html, quality_heatmap.html")
