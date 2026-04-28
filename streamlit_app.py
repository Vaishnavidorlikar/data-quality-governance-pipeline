import streamlit as st
import pandas as pd
import json
import sys
import os
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.pipeline import DataQualityPipeline

# Page configuration
st.set_page_config(
    page_title="Data Quality Governance Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 0.25rem solid #1f77b4;
    }
    .status-good {
        color: #28a745;
        font-weight: bold;
    }
    .status-warning {
        color: #ffc107;
        font-weight: bold;
    }
    .status-critical {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.markdown('<h1 class="main-header">📊 Data Quality Governance Pipeline</h1>', unsafe_allow_html=True)
    st.markdown("---")

    # Sidebar
    st.sidebar.title("🔧 Controls")

    # Data source selection
    data_sources = [
        "data/kaggle/Bank Customer Churn Prediction.csv",
        "data/kaggle/cards_data.csv",
        "data/kaggle/telco_customer_churn.csv",
        "data/kaggle/users_data.csv"
    ]

    selected_data_source = st.sidebar.selectbox(
        "Select Data Source",
        data_sources,
        help="Choose the dataset to analyze"
    )

    dataset_name = st.sidebar.text_input(
        "Dataset Name",
        value=Path(selected_data_source).stem,
        help="Name for the dataset analysis"
    )

    user_id = st.sidebar.text_input(
        "User ID",
        value="streamlit_user",
        help="User identifier for audit logging"
    )

    # Configuration
    config_path = st.sidebar.text_input(
        "Config Path",
        value="configs/validation_rules.yaml",
        help="Path to validation rules configuration"
    )

    # Run Pipeline Button
    if st.sidebar.button("🚀 Run Data Quality Pipeline", type="primary"):
        run_pipeline_analysis(selected_data_source, dataset_name, user_id, config_path)

    # Display sample data if file exists
    if os.path.exists(selected_data_source):
        st.markdown("### 📋 Sample Data Preview")
        try:
            df = pd.read_csv(selected_data_source)
            st.dataframe(df.head(10), use_container_width=True)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Data Types", len(df.dtypes.unique()))
        except Exception as e:
            st.error(f"Error loading data: {e}")

def run_pipeline_analysis(data_source, dataset_name, user_id, config_path):
    """Run the data quality pipeline and display results."""
    try:
        with st.spinner("🔄 Running Data Quality Pipeline..."):
            # Initialize pipeline
            pipeline = DataQualityPipeline(config_path=config_path)

            # Run pipeline
            results = pipeline.run_pipeline(
                data_source=data_source,
                dataset_name=dataset_name,
                user_id=user_id
            )

            # Get summary
            summary = pipeline.get_pipeline_summary()

        st.success("✅ Pipeline completed successfully!")

        # Display summary metrics
        display_summary_metrics(summary)

        # Display detailed results
        display_detailed_results(results)

        # Display visualizations
        display_quality_visualizations(results, summary)

    except Exception as e:
        st.error(f"❌ Error running pipeline: {e}")
        st.exception(e)

def display_summary_metrics(summary):
    """Display key summary metrics in cards."""
    st.markdown("### 📈 Quality Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        status_class = "status-good" if summary['overall_status'] == 'PASS' else "status-warning" if summary['overall_status'] == 'WARNING' else "status-critical"
        st.markdown(f"""
        <div class="metric-card">
            <h4>Overall Status</h4>
            <p class="{status_class}">{summary['overall_status']}</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h4>Quality Score</h4>
            <p style="font-size: 1.5rem; font-weight: bold;">{summary['quality_score']:.3f}</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        grade_color = "#28a745" if summary['overall_grade'] in ['A', 'B'] else "#ffc107" if summary['overall_grade'] == 'C' else "#dc3545"
        st.markdown(f"""
        <div class="metric-card">
            <h4>Grade</h4>
            <p style="font-size: 1.5rem; font-weight: bold; color: {grade_color};">{summary['overall_grade']}</p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h4>Total Issues</h4>
            <p style="font-size: 1.5rem; font-weight: bold;">{summary['total_issues']}</p>
        </div>
        """, unsafe_allow_html=True)

def display_detailed_results(results):
    """Display detailed validation results."""
    st.markdown("### 🔍 Detailed Results")

    # Validation Results
    if 'validation_results' in results:
        st.markdown("#### Validation Results")
        validation_df = pd.DataFrame(results['validation_results'])
        st.dataframe(validation_df, use_container_width=True)

    # Metrics
    if 'metrics' in results:
        st.markdown("#### Quality Metrics")
        metrics_df = pd.DataFrame([results['metrics']])
        st.dataframe(metrics_df, use_container_width=True)

    # Issues
    if 'issues' in results:
        st.markdown("#### Detected Issues")
        issues_df = pd.DataFrame(results['issues'])
        st.dataframe(issues_df, use_container_width=True)

def display_quality_visualizations(results, summary):
    """Display quality visualizations using Plotly."""
    st.markdown("### 📊 Quality Visualizations")

    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Quality Dimensions', 'Issue Distribution', 'Validation Status', 'Trend Analysis'),
        specs=[[{'type': 'radar'}, {'type': 'pie'}],
               [{'type': 'bar'}, {'type': 'scatter'}]]
    )

    # Radar chart for quality dimensions
    if 'metrics' in results:
        metrics = results['metrics']
        categories = ['Completeness', 'Accuracy', 'Consistency', 'Timeliness', 'Validity']
        values = [
            metrics.get('completeness_score', 0) * 100,
            metrics.get('accuracy_score', 0) * 100,
            metrics.get('consistency_score', 0) * 100,
            metrics.get('timeliness_score', 0) * 100,
            metrics.get('validity_score', 0) * 100
        ]

        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself',
                name='Quality Score'
            ),
            row=1, col=1
        )

    # Pie chart for issue distribution
    if 'issues' in results and results['issues']:
        issue_types = {}
        for issue in results['issues']:
            issue_type = issue.get('type', 'Unknown')
            issue_types[issue_type] = issue_types.get(issue_type, 0) + 1

        fig.add_trace(
            go.Pie(
                labels=list(issue_types.keys()),
                values=list(issue_types.values()),
                name='Issues'
            ),
            row=1, col=2
        )

    # Bar chart for validation status
    if 'validation_results' in results:
        validation_results = results['validation_results']
        statuses = {}
        for result in validation_results:
            status = result.get('status', 'Unknown')
            statuses[status] = statuses.get(status, 0) + 1

        fig.add_trace(
            go.Bar(
                x=list(statuses.keys()),
                y=list(statuses.values()),
                name='Validation Status'
            ),
            row=2, col=1
        )

    # Scatter plot for trend (placeholder - would need historical data)
    fig.add_trace(
        go.Scatter(
            x=[1, 2, 3, 4, 5],
            y=[summary['quality_score']] * 5,  # Placeholder
            mode='lines+markers',
            name='Quality Trend'
        ),
        row=2, col=2
    )

    # Update layout
    fig.update_layout(height=800, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()