#!/usr/bin/env python3
"""
AI Automation Workflows Dashboard
Interactive web dashboard for monitoring and controlling all AI components.
"""

import streamlit as st
import sys
import os
from pathlib import Path
import yaml
import json
import time
import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from src.integration.ai_orchestrator import AIOrchestrator

# Page configuration
st.set_page_config(
    page_title="AI Automation Dashboard",
    page_icon="settings",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .status-success { color: #28a745; }
    .status-warning { color: #ffc107; }
    .status-error { color: #dc3545; }
</style>
""", unsafe_allow_html=True)

def load_config():
    """Load configuration from file."""
    try:
        config_path = Path(__file__).parent / 'config' / 'config.yaml'
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

def initialize_orchestrator():
    """Initialize AI Orchestrator if not already done."""
    if 'orchestrator' not in st.session_state:
        config = load_config()
        st.session_state.orchestrator = AIOrchestrator(config)
        return st.session_state.orchestrator
    return st.session_state.orchestrator

def create_status_badge(status, text):
    """Create status badge with color."""
    color_map = {
        'success': '#28a745',
        'warning': '#ffc107', 
        'error': '#dc3545',
        'info': '#17a2b8'
    }
    color = color_map.get(status, '#6c757d')
    return f'<span style="background-color: {color}; color: white; padding: 0.25rem 0.5rem; border-radius: 0.25rem; font-size: 0.875rem;">{text}</span>'

def create_enterprise_dashboard():
    """Create Enterprise Performance Dashboard with corrected subplot types."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('API Response Times', 'Success Rate', 'Request Volume', 'System Health'),
        specs=[[{"type": "scatter"}, {"type": "indicator"}],
                [{"type": "bar"}, {"type": "indicator"}]]  # FIXED: "gauge" -> "indicator"
    )

    # Simulate time series data
    time_points = pd.date_range('2024-01-01', periods=24, freq='H')
    response_times = np.random.normal(0.15, 0.05, 24)
    request_volume = np.random.poisson(50, 24)

    # Response times
    fig.add_trace(
        go.Scatter(x=time_points, y=response_times, name='Response Time', line=dict(color='#1f77b4')),
        row=1, col=1
    )

    # Success rate indicator
    fig.add_trace(
        go.Indicator(
            mode="number+gauge+delta",
            value=98.5,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Success Rate (%)"},
            delta={'reference': 95},
            gauge={'axis': {'range': [None, 100]}, 'bar': {'color': '#2ca02c'}, 'steps': [
                {'range': [0, 50], 'color': 'lightgray'},
                {'range': [50, 80], 'color': 'yellow'},
                {'range': [80, 100], 'color': 'lightgreen'}
            ], 'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 90}}
        ),
        row=1, col=2
    )

    # Request volume
    fig.add_trace(
        go.Bar(x=time_points[:12], y=request_volume[:12], name='Request Volume', marker_color='#ff7f0e'),
        row=2, col=1
    )

    # System health indicator
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=99.9,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "System Health (%)"},
            delta={'reference': 99},
            gauge={'axis': {'range': [None, 100]}, 'bar': {'color': '#9467bd'}, 'steps': [
                {'range': [0, 50], 'color': 'lightgray'},
                {'range': [50, 80], 'color': 'yellow'},
                {'range': [80, 100], 'color': 'lightgreen'}
            ], 'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 95}}
        ),
        row=2, col=2
    )

    fig.update_layout(
        height=800,
        title_text="Enterprise Performance Dashboard",
        showlegend=False
    )

    return fig

def main():
    """Main dashboard application."""
    # Header
    st.markdown('<h1 class="main-header">AI Automation Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize orchestrator
    orchestrator = initialize_orchestrator()
    
    # Sidebar
    with st.sidebar:
        st.header("Control Panel")
        
        # System Status
        st.subheader("System Status")
        if st.button("Refresh Status", key="refresh_status"):
            if hasattr(orchestrator, 'get_system_status'):
                st.session_state.system_status = orchestrator.get_system_status()
            else:
                st.session_state.system_status = {"error": "Orchestrator not fully initialized"}
        
        # Initialize Components
        if st.button("Initialize Components", key="init_components"):
            with st.spinner("Initializing components..."):
                try:
                    if orchestrator.initialize_all():
                        st.success("All components initialized successfully!")
                        st.session_state.system_status = orchestrator.get_system_status()
                    else:
                        st.error("Failed to initialize components")
                except Exception as e:
                    st.error(f"Initialization error: {str(e)}")
        
        st.divider()
        
        # Quick Actions
        st.subheader("Quick Actions")
        
        if st.button("Test Enterprise AI", key="test_enterprise_ai"):
            st.session_state.test_enterprise_ai = True
        
        if st.button("Test Gesture", key="test_gesture"):
            st.session_state.test_gesture = True
            
        if st.button("Train ML Model", key="train_ml"):
            st.session_state.train_ml = True
            
        if st.button("Analyze Data", key="analyze_data"):
            st.session_state.analyze_data = True
        
        st.divider()
        
        # Configuration
        st.subheader("Configuration")
        
        # LLM Provider
        llm_provider = st.selectbox(
            "LLM Provider",
            ["mock", "openai", "anthropic"],
            index=0,
            key="llm_provider"
        )
        
        # Voice Settings
        st.subheader("Voice Settings")
        voice_rate = st.slider("Speech Rate", 100, 300, 200, key="voice_rate")
        voice_volume = st.slider("Volume", 0.0, 1.0, 0.9, key="voice_volume")
        
        # Camera Settings
        st.subheader("Camera Settings")
        camera_device = st.number_input("Camera Device", 0, 10, 0, key="camera_device")
        
        st.divider()
        
        # Logs
        st.subheader("Logs")
        if st.button("View Logs", key="view_logs"):
            st.session_state.show_logs = True

    # Main content area
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Dashboard", "Enterprise AI", "ML Models", 
        "Data Analysis", "Settings"
    ])

    with tab1:
        st.header("System Dashboard")
        
        # System Status Overview
        if 'system_status' in st.session_state:
            status = st.session_state.system_status
            
            # Status cards
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                orchestrator_status = status.get('orchestrator', {}).get('initialized', False)
                st.metric(
                    "Orchestrator",
                    "Active" if orchestrator_status else "Inactive",
                    None if orchestrator_status else "ERROR"
                )
            
            with col2:
                llm_status = status.get('llm_client', {}).get('connected', False)
                st.metric(
                    "LLM Client",
                    "Connected" if llm_status else "Disconnected",
                    None if llm_status else "ERROR"
                )
            
            with col3:
                agents_count = len(status.get('agents', {}))
                st.metric("Agents", f"{agents_count} Loaded")
            
            with col4:
                workflows_count = len(status.get('workflows', {}))
                st.metric("Workflows", f"{workflows_count} Loaded")
        
        # Performance Metrics
        st.subheader("Performance Metrics")
        
        # Sample performance data
        metrics_data = {
            'Email Processing': [1.2, 1.5, 1.1, 1.8, 1.3],
            'Report Generation': [3.2, 3.8, 2.9, 4.1, 3.5],
            'Text Summarization': [0.8, 1.2, 0.9, 1.5, 1.1],
            'API Response': [0.05, 0.08, 0.06, 0.09, 0.07]
        }
        
        # Create performance chart
        fig = go.Figure()
        
        for i, (metric, values) in enumerate(metrics_data.items()):
            fig.add_trace(go.Scatter(
                x=list(range(len(values))),
                y=values,
                mode='lines+markers',
                name=metric,
                line=dict(width=2),
                marker=dict(size=6)
            ))
        
        fig.update_layout(
            title="Response Time (seconds)",
            xaxis_title="Test Run",
            yaxis_title="Time (seconds)",
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Enterprise Performance Dashboard
        st.subheader("Enterprise Performance Dashboard")
        enterprise_fig = create_enterprise_dashboard()
        st.plotly_chart(enterprise_fig, use_container_width=True)
        
        # Activity Log
        st.subheader("Recent Activity")
        
        # Sample activity data
        activity_data = [
            {"timestamp": datetime.now() - timedelta(minutes=5), "component": "Email Agent", "action": "Processed 3 emails", "status": "success"},
            {"timestamp": datetime.now() - timedelta(minutes=10), "component": "Enterprise AI", "action": "Voice command processed", "status": "success"},
            {"timestamp": datetime.now() - timedelta(minutes=15), "component": "ML Manager", "action": "Trained Random Forest model", "status": "success"},
            {"timestamp": datetime.now() - timedelta(minutes=20), "component": "Report Agent", "action": "Generated monthly report", "status": "success"},
            {"timestamp": datetime.now() - timedelta(minutes=25), "component": "Gesture Detector", "action": "Detected 'thumbs_up' gesture", "status": "success"},
        ]
        
        activity_df = pd.DataFrame(activity_data)
        
        for _, row in activity_df.iterrows():
            status_icon = "SUCCESS" if row['status'] == 'success' else "ERROR"
            st.markdown(f"""
            <div style="padding: 0.5rem; margin-bottom: 0.5rem; border-left: 3px solid #28a745; background: #f8f9fa;">
                <strong>{row['timestamp'].strftime('%H:%M:%S')}</strong> - {row['component']}<br>
                {row['action']} {status_icon}
            </div>
            """, unsafe_allow_html=True)

    with tab2:
        st.header("Enterprise AI Assistant")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Voice Commands")
            
            # Command input
            command = st.text_input("Enter voice command:", placeholder="e.g., 'Analyze sales data'")
            
            if st.button("Process Command", key="process_command"):
                if command and hasattr(orchestrator, 'process_voice_command'):
                    with st.spinner("Processing command..."):
                        try:
                            response = orchestrator.process_voice_command(command)
                            st.success("Command processed successfully!")
                            st.write(f"**Enterprise AI Response:** {response}")
                        except Exception as e:
                            st.error(f"Error processing command: {str(e)}")
                else:
                    st.warning("Enterprise AI not available")
            
            # Command history
            st.subheader("Command History")
            if 'command_history' not in st.session_state:
                st.session_state.command_history = []
            
            for cmd in st.session_state.command_history[-5:]:
                st.write(f"• {cmd}")
        
        with col2:
            st.subheader("Gesture Control")
            
            # Gesture status
            if hasattr(orchestrator, 'detect_gesture'):
                if st.button("Detect Gesture", key="detect_gesture"):
                    with st.spinner("Detecting gesture..."):
                        try:
                            gesture = orchestrator.detect_gesture()
                            if gesture:
                                st.success(f"Gesture detected: **{gesture}**")
                            else:
                                st.info("No gesture detected")
                        except Exception as e:
                            st.error(f"Error detecting gesture: {str(e)}")
                
                # Gesture statistics
                st.subheader("Gesture Statistics")
                
                gesture_stats = {
                    'Thumbs Up': 15,
                    'Peace': 8,
                    'Rock': 12,
                    'Paper': 6,
                    'Scissors': 4,
                    'OK': 9,
                    'Point': 11,
                    'Open Palm': 7,
                    'Fist': 3
                }
                
                fig = px.pie(
                    values=list(gesture_stats.values()),
                    names=list(gesture_stats.keys()),
                    title="Detected Gestures Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Gesture detection not available")

    with tab3:
        st.header("Machine Learning Models")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Model Training")
            
            # Model type selection
            model_type = st.selectbox(
                "Model Type",
                ["Classification", "Regression", "Clustering"],
                key="model_type"
            )
            
            # Algorithm selection
            algorithms = st.multiselect(
                "Algorithms",
                ["Random Forest", "SVM", "Logistic Regression", "KNN", "Neural Network"],
                default=["Random Forest"],
                key="algorithms"
            )
            
            # Training parameters
            st.subheader("Training Parameters")
            
            test_size = st.slider("Test Size", 0.1, 0.5, 0.2, key="test_size")
            cv_folds = st.slider("CV Folds", 3, 10, 5, key="cv_folds")
            
            if st.button("Start Training", key="start_training"):
                st.success("Training started! (Demo mode)")
                st.info("In production, this would train models with your data")
        
        with col2:
            st.subheader("Model Performance")
            
            # Sample model performance data
            performance_data = {
                'Model': ['Random Forest', 'SVM', 'Logistic Regression', 'KNN'],
                'Accuracy': [0.92, 0.89, 0.87, 0.85],
                'Precision': [0.91, 0.88, 0.86, 0.84],
                'Recall': [0.93, 0.90, 0.88, 0.86],
                'F1-Score': [0.92, 0.89, 0.87, 0.85]
            }
            
            perf_df = pd.DataFrame(performance_data)
            
            # Performance chart
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Accuracy',
                x=perf_df['Model'],
                y=perf_df['Accuracy'],
                marker_color='#1f77b4'
            ))
            
            fig.add_trace(go.Bar(
                name='F1-Score',
                x=perf_df['Model'],
                y=perf_df['F1-Score'],
                marker_color='#ff7f0e'
            ))
            
            fig.update_layout(
                title="Model Performance Comparison",
                xaxis_title="Model",
                yaxis_title="Score",
                barmode='group',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Performance table
            st.subheader("Performance Details")
            st.dataframe(perf_df, use_container_width=True)

    with tab4:
        st.header("Data Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Data Upload & Analysis")
            
            # File upload
            uploaded_file = st.file_uploader(
                "Upload CSV file for analysis",
                type=['csv'],
                key="data_upload"
            )
            
            if uploaded_file:
                try:
                    data = pd.read_csv(uploaded_file)
                    st.success(f"File loaded: {uploaded_file.name}")
                    st.info(f"Dataset shape: {data.shape}")
                    
                    # Basic statistics
                    st.subheader("Data Statistics")
                    st.write(data.describe())
                    
                    # Analysis options
                    analysis_type = st.selectbox(
                        "Analysis Type",
                        ["Summary Statistics", "Correlation Analysis", "Distribution Analysis"],
                        key="analysis_type"
                    )
                    
                    if st.button("Analyze Data", key="analyze_uploaded"):
                        st.success("Analysis completed! (Demo mode)")
                        
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
            
            # Sample data generation
            st.subheader("Generate Sample Data")
            
            sample_type = st.selectbox(
                "Sample Data Type",
                ["Classification", "Regression", "Time Series"],
                key="sample_type"
            )
            
            n_samples = st.number_input(
                "Number of Samples",
                min_value=100,
                max_value=10000,
                value=1000,
                key="n_samples"
            )
            
            if st.button("Generate Data", key="generate_sample"):
                st.success(f"Generated {n_samples} samples for {sample_type}!")
                st.info("Sample data ready for analysis")
        
        with col2:
            st.subheader("Visualizations")
            
            # Visualization type
            viz_type = st.selectbox(
                "Visualization Type",
                ["Line Chart", "Bar Chart", "Scatter Plot", "Heatmap", "Histogram"],
                key="viz_type"
            )
            
            if st.button("Create Visualization", key="create_viz"):
                # Sample visualization
                if viz_type == "Line Chart":
                    fig = px.line(
                        x=list(range(100)),
                        y=[i*2 + (i%10) for i in range(100)],
                        title="Sample Time Series"
                    )
                elif viz_type == "Bar Chart":
                    fig = px.bar(
                        x=['A', 'B', 'C', 'D', 'E'],
                        y=[20, 35, 30, 35, 27],
                        title="Sample Bar Chart"
                    )
                elif viz_type == "Scatter Plot":
                    fig = px.scatter(
                        x=[i + (i%5) for i in range(50)],
                        y=[i*1.5 + (i%7) for i in range(50)],
                        title="Sample Scatter Plot"
                    )
                elif viz_type == "Heatmap":
                    import numpy as np
                    data = np.random.randn(10, 10)
                    fig = px.imshow(
                        data,
                        title="Sample Heatmap"
                    )
                else:  # Histogram
                    fig = px.histogram(
                        x=[i + (i%3) for i in range(100)],
                        title="Sample Histogram"
                    )
                
                st.plotly_chart(fig, use_container_width=True)

    with tab5:
        st.header("System Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Configuration")
            
            # Current config display
            config = load_config()
            
            st.write("**Current Configuration:**")
            st.json(config)
            
            # Configuration reload
            if st.button("Reload Configuration", key="reload_config"):
                st.success("Configuration reloaded!")
                st.rerun()
        
        with col2:
            st.subheader("System Logs")
            
            # Log level selection
            log_level = st.selectbox(
                "Log Level",
                ["DEBUG", "INFO", "WARNING", "ERROR"],
                key="log_level"
            )
            
            # Sample logs
            sample_logs = [
                {"timestamp": datetime.now() - timedelta(minutes=1), "level": "INFO", "message": "AI Orchestrator initialized successfully"},
                {"timestamp": datetime.now() - timedelta(minutes=2), "level": "INFO", "message": "Enterprise AI assistant ready"},
                {"timestamp": datetime.now() - timedelta(minutes=3), "level": "WARNING", "message": "Camera not found, using mock data"},
                {"timestamp": datetime.now() - timedelta(minutes=4), "level": "ERROR", "message": "Failed to connect to OpenAI API"},
                {"timestamp": datetime.now() - timedelta(minutes=5), "level": "INFO", "message": "Model training completed"},
            ]
            
            for log in sample_logs:
                level_color = {
                    "INFO": "#28a745",
                    "WARNING": "#ffc107", 
                    "ERROR": "#dc3545",
                    "DEBUG": "#6c757d"
                }.get(log['level'], "#6c757d")
                
                st.markdown(f"""
                <div style="padding: 0.5rem; margin-bottom: 0.5rem; border-left: 3px solid {level_color}; background: #f8f9fa;">
                    <strong>{log['timestamp'].strftime('%H:%M:%S')}</strong> - 
                    <span style="color: {level_color}; font-weight: bold;">{log['level']}</span><br>
                    {log['message']}
                </div>
                """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <strong>AI Automation Workflows Dashboard</strong> | 
        Built with Streamlit | 
        <a href='https://github.com/Vaishnavidorlikar/ai-automation-workflows-llm' target='_blank'>GitHub</a>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
