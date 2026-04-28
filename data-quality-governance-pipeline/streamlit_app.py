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
            # Check if data source exists
            if not os.path.exists(data_source):
                st.error(f"❌ Data source not found: {data_source}")
                st.info("Available datasets in data/kaggle/:")
                kaggle_dir = "data/kaggle"
                if os.path.exists(kaggle_dir):
                    files = os.listdir(kaggle_dir)
                    for f in files:
                        st.text(f"  - {f}")
                return
            
            # Check if config exists
            if not os.path.exists(config_path):
                st.warning(f"⚠️ Config file not found: {config_path}")
                st.info("Using default configuration")
            
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
            
            # Debug info
            with st.expander("🔍 Debug Info"):
                st.json({
                    'data_source': data_source,
                    'dataset_name': dataset_name,
                    'config_path': config_path,
                    'summary': summary
                })

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

    # Set default values for None values
    overall_status = summary.get('overall_status', 'UNKNOWN') or 'UNKNOWN'
    quality_score = summary.get('quality_score', 0.0) or 0.0
    overall_grade = summary.get('overall_grade', 'N/A') or 'N/A'
    total_issues = summary.get('total_issues', 0) or 0
    critical_issues = summary.get('critical_issues', 0) or 0

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        status_class = "status-good" if overall_status == 'PASS' else "status-warning" if overall_status == 'WARNING' else "status-critical"
        st.markdown(f"""
        <div class="metric-card">
            <h4>Overall Status</h4>
            <p class="{status_class}">{overall_status}</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h4>Quality Score</h4>
            <p style="font-size: 1.5rem; font-weight: bold;">{quality_score:.3f}</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        grade_color = "#28a745" if overall_grade in ['A', 'B'] else "#ffc107" if overall_grade == 'C' else "#dc3545"
        st.markdown(f"""
        <div class="metric-card">
            <h4>Grade</h4>
            <p style="font-size: 1.5rem; font-weight: bold; color: {grade_color};">{overall_grade}</p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h4>Total Issues</h4>
            <p style="font-size: 1.5rem; font-weight: bold;">{total_issues}</p>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        critical_color = "#dc3545" if critical_issues > 0 else "#28a745"
        st.markdown(f"""
        <div class="metric-card">
            <h4>Critical Issues</h4>
            <p style="font-size: 1.5rem; font-weight: bold; color: {critical_color};">{critical_issues}</p>
        </div>
        """, unsafe_allow_html=True)

def display_detailed_results(results):
    """Display detailed validation results."""
    st.markdown("### 🔍 Detailed Results")

    if not results:
        st.info("No results to display")
        return

    # Validation Results
    if 'validation_results' in results and results['validation_results']:
        st.markdown("#### Validation Results")
        try:
            # Handle if validation_results is a dict of dicts
            validation_data = results['validation_results']
            if isinstance(validation_data, dict):
                validation_df = pd.DataFrame([validation_data]).T
            else:
                validation_df = pd.DataFrame(validation_data)
            st.dataframe(validation_df, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not display validation results: {e}")
            st.json(validation_data)

    # Metrics
    if 'quality_metrics' in results and results['quality_metrics']:
        st.markdown("#### Quality Metrics")
        try:
            metrics_dict = results['quality_metrics']
            # Flatten nested structure if needed
            metrics_flat = {}
            for key, val in metrics_dict.items():
                if isinstance(val, dict):
                    for k, v in val.items():
                        metrics_flat[f"{key}_{k}"] = v
                else:
                    metrics_flat[key] = val
            
            metrics_df = pd.DataFrame([metrics_flat])
            st.dataframe(metrics_df, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not display metrics: {e}")
            st.json(results.get('quality_metrics'))

    # Overall Assessment
    if 'overall_assessment' in results and results['overall_assessment']:
        st.markdown("#### Overall Assessment")
        try:
            assessment = results['overall_assessment']
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Grade", assessment.get('overall_grade', 'N/A'))
            with col2:
                st.metric("Total Issues", assessment.get('total_issues', 0))
            with col3:
                st.metric("Critical Issues", assessment.get('critical_issues', 0))
            with col4:
                st.metric("Warnings", assessment.get('warnings', 0))
            
            if assessment.get('recommendations'):
                st.markdown("**Recommendations:**")
                for rec in assessment['recommendations']:
                    st.write(f"- {rec}")
        except Exception as e:
            st.warning(f"Could not display assessment: {e}")
            st.json(results.get('overall_assessment'))

    # Issues
    if 'issues' in results and results['issues']:
        st.markdown("#### Detected Issues")
        try:
            issues_df = pd.DataFrame(results['issues'])
            st.dataframe(issues_df, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not display issues: {e}")
            st.json(results['issues'])

def display_quality_visualizations(results, summary):
    """Display quality visualizations using Plotly."""
    st.markdown("### 📊 Quality Visualizations")

    try:
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Quality Dimensions', 'Issue Distribution', 'Validation Status', 'Quality Trend'),
            specs=[[{'type': 'bar'}, {'type': 'pie'}],
                   [{'type': 'bar'}, {'type': 'scatter'}]],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # 1. Bar chart for quality dimensions
        if 'metrics' in results and results['metrics']:
            metrics = results['metrics']
            # Ensure metrics is a dictionary
            if isinstance(metrics, dict):
                dimensions = ['Completeness', 'Accuracy', 'Consistency', 'Timeliness', 'Validity']
                values = [
                    (metrics.get('completeness_score') or 0) * 100,
                    (metrics.get('accuracy_score') or 0) * 100,
                    (metrics.get('consistency_score') or 0) * 100,
                    (metrics.get('timeliness_score') or 0) * 100,
                    (metrics.get('validity_score') or 0) * 100
                ]
                fig.add_trace(
                    go.Bar(
                        y=dimensions,
                        x=values,
                        orientation='h',
                        marker=dict(color=values, colorscale='Viridis'),
                        name='Quality Score'
                    ),
                    row=1, col=1
                )
            else:
                fig.add_trace(
                    go.Bar(y=['No Data'], x=[0], name='Quality Score'),
                    row=1, col=1
                )
        else:
            fig.add_trace(
                go.Bar(y=['No Data'], x=[0], name='Quality Score'),
                row=1, col=1
            )

        # 2. Pie chart for issue distribution
        if 'issues' in results and results['issues']:
            issue_types = {}
            for issue in results['issues']:
                # Handle both dict and string types
                if isinstance(issue, dict):
                    issue_type = issue.get('type', 'Unknown')
                else:
                    issue_type = str(issue)
                issue_types[issue_type] = issue_types.get(issue_type, 0) + 1

            if issue_types:
                fig.add_trace(
                    go.Pie(
                        labels=list(issue_types.keys()),
                        values=list(issue_types.values()),
                        name='Issues'
                    ),
                    row=1, col=2
                )
            else:
                fig.add_trace(
                    go.Pie(labels=['No Issues'], values=[1], name='Issues'),
                    row=1, col=2
                )
        else:
            fig.add_trace(
                go.Pie(labels=['No Issues'], values=[1], name='Issues'),
                row=1, col=2
            )

        # 3. Bar chart for validation status
        if 'validation_results' in results and results['validation_results']:
            validation_results = results['validation_results']
            statuses = {}
            for result in validation_results:
                # Handle both dict and string types
                if isinstance(result, dict):
                    status = result.get('status', 'Unknown')
                else:
                    status = str(result)
                statuses[status] = statuses.get(status, 0) + 1

            if statuses:
                fig.add_trace(
                    go.Bar(
                        x=list(statuses.keys()),
                        y=list(statuses.values()),
                        name='Validation Status',
                        marker=dict(color=['#28a745' if s == 'PASS' else '#ffc107' for s in statuses.keys()])
                    ),
                    row=2, col=1
                )
            else:
                fig.add_trace(
                    go.Bar(x=['No Data'], y=[0], name='Validation Status'),
                    row=2, col=1
                )
        else:
            fig.add_trace(
                go.Bar(x=['No Data'], y=[0], name='Validation Status'),
                row=2, col=1
            )

        # 4. Scatter plot for trend
        quality_score = summary.get('quality_score', 0.5) or 0.5
        fig.add_trace(
            go.Scatter(
                x=[1, 2, 3, 4, 5],
                y=[quality_score] * 5,
                mode='lines+markers',
                name='Quality Trend',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=8)
            ),
            row=2, col=2
        )

        # Update layout
        fig.update_layout(height=800, showlegend=True)
        fig.update_xaxes(title_text="Score", row=1, col=1)
        fig.update_xaxes(title_text="Status", row=2, col=1)
        fig.update_xaxes(title_text="Time Period", row=2, col=2)
        fig.update_yaxes(title_text="Count", row=2, col=1)
        fig.update_yaxes(title_text="Quality Score", row=2, col=2)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"⚠️ Could not generate visualizations: {e}")
        st.info("💡 Tip: Run the pipeline with data to see visualizations.")

if __name__ == "__main__":
    main()