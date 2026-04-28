"""
Real-time Pipeline Dashboard
Google Cloud Streaming Performance Dashboard
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_realtime_data():
    """Generate realistic real-time streaming data"""
    
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='H')
    
    # Streaming Metrics
    events_per_second = np.random.normal(100000, 15000, len(dates))
    throughput = np.random.normal(95, 5, len(dates))
    latency = np.random.normal(0.8, 0.2, len(dates))
    
    # System Performance
    cpu_usage = np.random.normal(45, 10, len(dates))
    memory_usage = np.random.normal(60, 8, len(dates))
    network_io = np.random.normal(75, 12, len(dates))
    
    # Data Processing
    processed_events = np.cumsum(np.random.normal(350000, 50000, len(dates)))
    failed_events = np.random.poisson(50, len(dates))
    error_rate = (failed_events / events_per_second) * 100
    
    # Pipeline Health
    uptime = np.random.normal(99.9, 0.1, len(dates))
    alert_count = np.random.poisson(3, len(dates))
    
    # Business Metrics
    trading_decisions = np.random.normal(1500, 200, len(dates))
    fraud_detected = np.random.poisson(25, len(dates))
    
    return {
        'dates': dates,
        'events_per_second': events_per_second,
        'throughput': throughput,
        'latency': latency,
        'cpu_usage': cpu_usage,
        'memory_usage': memory_usage,
        'network_io': network_io,
        'processed_events': processed_events,
        'failed_events': failed_events,
        'error_rate': error_rate,
        'uptime': uptime,
        'alert_count': alert_count,
        'trading_decisions': trading_decisions,
        'fraud_detected': fraud_detected
    }

def create_realtime_dashboard():
    """Create comprehensive real-time pipeline dashboard"""
    
    data = generate_realtime_data()
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=(
            'Events per Second', 'Processing Latency', 'System Throughput',
            'Resource Utilization', 'Error Rate', 'Cumulative Events',
            'Business Impact', 'Pipeline Health', 'Alert Summary'
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": True}, {"secondary_y": True}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}, {"secondary_y": False}]
        ]
    )
    
    # Events per Second
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['events_per_second'],
            mode='lines+markers',
            name='Events/sec',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=3)
        ),
        row=1, col=1
    )
    
    # Processing Latency
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['latency'],
            mode='lines+markers',
            name='Latency',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=1, col=2
    )
    
    # System Throughput
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['throughput'],
            mode='lines+markers',
            name='Throughput',
            line=dict(color='#A23B72', width=2),
            marker=dict(size=3)
        ),
        row=1, col=3
    )
    
    # Resource Utilization
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['cpu_usage'],
            mode='lines',
            name='CPU Usage',
            line=dict(color='#2E86AB', width=2)
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['memory_usage'],
            mode='lines',
            name='Memory Usage',
            line=dict(color='#FF9F1C', width=2)
        ),
        row=2, col=1, secondary_y=True
    )
    
    # Error Rate
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['error_rate'],
            mode='lines+markers',
            name='Error Rate',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=2, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=[0.1] * len(data['dates']),  # Error threshold
            mode='lines',
            name='Error Threshold',
            line=dict(color='red', width=2, dash='dash')
        ),
        row=2, col=2, secondary_y=True
    )
    
    # Cumulative Events
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['processed_events'],
            mode='lines',
            name='Processed Events',
            line=dict(color='#2E86AB', width=3),
            fill='tonexty'
        ),
        row=2, col=3
    )
    
    # Business Impact
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['trading_decisions'],
            mode='lines+markers',
            name='Trading Decisions',
            line=dict(color='#A23B72', width=2),
            marker=dict(size=3)
        ),
        row=3, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['fraud_detected'],
            mode='lines+markers',
            name='Fraud Detected',
            line=dict(color='#E71D36', width=2),
            marker=dict(size=3)
        ),
        row=3, col=1, secondary_y=True
    )
    
    # Pipeline Health
    fig.add_trace(
        go.Scatter(
            x=data['dates'],
            y=data['uptime'],
            mode='lines+markers',
            name='Uptime',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=3)
        ),
        row=3, col=2
    )
    
    # Alert Summary
    daily_alerts = data['alert_count'].reshape(-1, 24).sum(axis=1)
    alert_dates = data['dates'][::24][:len(daily_alerts)]
    
    fig.add_trace(
        go.Bar(
            x=alert_dates,
            y=daily_alerts,
            name='Daily Alerts',
            marker_color='#E71D36'
        ),
        row=3, col=3
    )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Real-time Streaming Pipeline Dashboard',
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
    
    fig.update_yaxes(title_text="Events/sec", row=1, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Latency (sec)", row=1, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Throughput (%)", row=1, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="CPU Usage (%)", row=2, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Memory Usage (%)", row=2, col=1, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Error Threshold (%)", row=2, col=2, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Total Events", row=2, col=3, tickfont=dict(size=10))
    
    fig.update_yaxes(title_text="Trading Decisions", row=3, col=1, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Fraud Cases", row=3, col=1, secondary_y=True, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Uptime (%)", row=3, col=2, tickfont=dict(size=10))
    fig.update_yaxes(title_text="Alert Count", row=3, col=3, tickfont=dict(size=10))
    
    return fig

def create_performance_gauge():
    """Create performance gauge charts"""
    
    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "indicator"}, {"type": "indicator"}],
               [{"type": "indicator"}, {"type": "indicator"}]],
        subplot_titles=('Current Throughput', 'Average Latency', 'System Health', 'Error Rate')
    )
    
    # Current Throughput Gauge
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=95.2,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Throughput (%)"},
            delta={'reference': 90},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "#2E86AB"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ),
        row=1, col=1
    )
    
    # Average Latency Gauge
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=0.8,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Latency (sec)"},
            delta={'reference': 1.0},
            gauge={
                'axis': {'range': [None, 2]},
                'bar': {'color': "#E71D36"},
                'steps': [
                    {'range': [0, 0.5], 'color': "lightgreen"},
                    {'range': [0.5, 1.0], 'color': "yellow"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 1.0
                }
            }
        ),
        row=1, col=2
    )
    
    # System Health Gauge
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=99.9,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "System Health (%)"},
            delta={'reference': 99.0},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "#A23B72"},
                'steps': [
                    {'range': [0, 90], 'color': "lightgray"},
                    {'range': [90, 98], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "green", 'width': 4},
                    'thickness': 0.75,
                    'value': 98
                }
            }
        ),
        row=2, col=1
    )
    
    # Error Rate Gauge
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=0.05,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Error Rate (%)"},
            delta={'reference': 0.1},
            gauge={
                'axis': {'range': [None, 0.5]},
                'bar': {'color': "#FF9F1C"},
                'steps': [
                    {'range': [0, 0.1], 'color': "lightgreen"},
                    {'range': [0.1, 0.3], 'color': "yellow"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 0.1
                }
            }
        ),
        row=2, col=2
    )
    
    fig.update_layout(
        title=dict(
            text='Real-time Performance Gauges',
            x=0.5,
            font=dict(size=18, color='#1f2937')
        ),
        height=600,
        template='plotly_white'
    )
    
    return fig

# Main execution
if __name__ == "__main__":
    # Create main realtime dashboard
    realtime_dashboard = create_realtime_dashboard()
    realtime_dashboard.show()
    
    # Create performance gauges
    gauge_dashboard = create_performance_gauge()
    gauge_dashboard.show()
    
    # Save dashboards
    realtime_dashboard.write_html("realtime_pipeline_dashboard.html")
    gauge_dashboard.write_html("performance_gauges.html")
    
    print("Real-time Pipeline Dashboard created successfully!")
    print("Files saved: realtime_pipeline_dashboard.html, performance_gauges.html")
