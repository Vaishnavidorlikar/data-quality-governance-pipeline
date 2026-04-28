"""
Data Lake Migration Dashboard
Multi-Cloud Migration Performance Dashboard
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_migration_data():
    """Generate realistic data migration dashboard data"""
    
    dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')
    
    # Migration Progress
    data_migrated = np.cumsum(np.random.normal(8.5, 2, len(dates)))
    total_data_size = 500  # GB
    
    # Cost Metrics
    bigquery_costs = np.random.normal(2500, 300, len(dates))
    azure_costs = np.random.normal(1625, 200, len(dates))
    total_costs = bigquery_costs + azure_costs
    
    # Performance Metrics
    migration_speed = np.random.normal(8.5, 1.5, len(dates))
    data_accuracy = np.random.normal(99.9, 0.1, len(dates))
    
    # Processing Times
    etl_time = np.random.normal(45, 10, len(dates))
    validation_time = np.random.normal(15, 5, len(dates))
    
    # Cloud Resources
    cpu_utilization = np.random.normal(65, 15, len(dates))
    storage_utilization = np.cumsum(np.random.normal(1.6, 0.3, len(dates)))
    
    return {
        'dates': dates,
        'data_migrated': data_migrated,
        'total_data_size': total_data_size,
        'bigquery_costs': bigquery_costs,
        'azure_costs': azure_costs,
        'total_costs': total_costs,
        'migration_speed': migration_speed,
        'data_accuracy': data_accuracy,
        'etl_time': etl_time,
        'validation_time': validation_time,
        'cpu_utilization': cpu_utilization,
        'storage_utilization': storage_utilization
    }

def create_migration_dashboard():
    """Create comprehensive data migration dashboard"""
    
    data = generate_migration_data()
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=(
            'Data Migration Progress', 'Cost Comparison', 'Migration Speed',
            'Data Accuracy', 'Processing Times', 'Resource Utilization',
            'Cloud Storage Usage', 'Daily Migration Volume', 'Error Rate'
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": True}, {"secondary_y": True}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}]
        ]
    )
    
    # Data Migration Progress
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['data_migrated'],
            mode='lines+markers',
            name='Data Migrated',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=4),
            fill='tonexty'
        ),
        row=1, col=1
    )
    
    # Cost Comparison
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['bigquery_costs'],
            mode='lines',
            name='BigQuery Costs',
            line=dict(color='#E71D36', width=2)
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['azure_costs'],
            mode='lines',
            name='Azure Costs',
            line=dict(color='#FF9F1C', width=2)
        ),
        row=1, col=2
    )
    
    # Migration Speed
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['migration_speed'],
            mode='lines+markers',
            name='Migration Speed',
            line=dict(color='#A23B72', width=2),
            marker=dict(size=4)
        ),
        row=1, col=3
    )
    
    # Data Accuracy
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['data_accuracy'],
            mode='lines+markers',
            name='Data Accuracy',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=3)
        ),
        row=2, col=1
    )
    
    # Processing Times
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['etl_time'],
            mode='lines+markers',
            name='ETL Time',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=2, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['validation_time'],
            mode='lines+markers',
            name='Validation Time',
            line=dict(color='#FF9F1C', width=2),
            marker=dict(size=3)
        ),
        row=2, col=2, secondary_y=True
    )
    
    # Resource Utilization
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['cpu_utilization'],
            mode='lines',
            name='CPU Utilization',
            line=dict(color='#A23B72', width=2),
            fill='tonexty'
        ),
        row=2, col=3
    )
    
    # Cloud Storage Usage
    fig.add_trace(
        go.Bar(
            x=data['dates'],
            y=data['storage_utilization'],
            name='Storage Used',
            marker_color='#2E86AB'
        ),
        row=3, col=1
    )
    
    # Daily Migration Volume
    daily_volume = np.diff(data['data_migrated'], prepend=data['data_migrated'][0])
    fig.add_trace(
        go.Bar(
            x=data['dates'],
            y=daily_volume,
            name='Daily Volume',
            marker_color='#C73E1D'
        ),
        row=3, col=2
    )
    
    # Error Rate
    error_rate = np.random.exponential(0.1, len(data['dates']))
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=error_rate,
            mode='lines+markers',
            name='Error Rate',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=3, col=3
    )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Multi-Cloud Data Migration Dashboard',
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
    
    fig.update_yaxes(title_text="Data Migrated (GB)", row=1, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Cost ($)", row=1, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Speed (GB/hr)", row=1, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Accuracy (%)", row=2, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="ETL Time (min)", row=2, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Validation Time (min)", row=2, col=2, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="CPU Utilization (%)", row=2, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Storage (TB)", row=3, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Volume (GB/day)", row=3, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Error Rate (%)", row=3, col=3, tickfont=dict(size=10))
    
    return fig

def create_cost_savings_dashboard():
    """Create cost savings analysis dashboard"""
    
    data = generate_migration_data()
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Monthly Cost Comparison', 'Cumulative Savings', 'Cost Breakdown', 'ROI Analysis'),
        specs=[[{"type": "bar"}, {"type": "scatter"}],
               [{"type": "pie"}, {"type": "scatter"}]]
    )
    
    # Monthly Cost Comparison
    monthly_costs = data['total_costs'].reshape(-1, 30).mean(axis=1)
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
    
    fig.add_trace(
        go.Bar(
            x=months,
            y=monthly_costs[:6],
            name='Monthly Costs',
            marker_color='#2E86AB'
        ),
        row=1, col=1
    )
    
    # Cumulative Savings
    baseline_costs = np.full_like(data['total_costs'], 5000)
    cumulative_savings = np.cumsum(baseline_costs - data['total_costs'])
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=cumulative_savings,
            mode='lines+markers',
            name='Cumulative Savings',
            line=dict(color='#A23B72', width=3),
            marker=dict(size=4),
            fill='tonexty'
        ),
        row=1, col=2
    )
    
    # Cost Breakdown
    cost_breakdown = {
        'BigQuery': data['bigquery_costs'][-1],
        'Azure': data['azure_costs'][-1],
        'Processing': 500,
        'Storage': 300
    }
    
    fig.add_trace(
        go.Pie(
            labels=list(cost_breakdown.keys()),
            values=list(cost_breakdown.values()),
            name="Cost Breakdown"
        ),
        row=2, col=1
    )
    
    # ROI Analysis
    roi = (cumulative_savings / (data['total_costs'].cumsum())) * 100
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=roi,
            mode='lines+markers',
            name='ROI %',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=2, col=2
    )
    
    fig.update_layout(
        title=dict(
            text='Cost Analysis & ROI Dashboard',
            x=0.5,
            font=dict(size=18, color='#1f2937')
        ),
        height=800,
        showlegend=False,
        template='plotly_white'
    )
    
    return fig

# Main execution
if __name__ == "__main__":
    # Create main migration dashboard
    migration_dashboard = create_migration_dashboard()
    migration_dashboard.show()
    
    # Create cost savings dashboard
    cost_dashboard = create_cost_savings_dashboard()
    cost_dashboard.show()
    
    # Save dashboards
    migration_dashboard.write_html("data_lake_migration_dashboard.html")
    cost_dashboard.write_html("cost_analysis_dashboard.html")
    
    print("Data Lake Migration Dashboard created successfully!")
    print("Files saved: data_lake_migration_dashboard.html, cost_analysis_dashboard.html")
