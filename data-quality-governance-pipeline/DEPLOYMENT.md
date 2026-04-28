# Streamlit Deployment Guide

## Quick Start (Local Development)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the Streamlit app
streamlit run streamlit_app.py
```

The app will open at `http://localhost:8501`

---

## Deploying to Streamlit Cloud

### Prerequisites
- [ ] GitHub account with repository pushed
- [ ] Streamlit Cloud account (free tier available at https://streamlit.io/cloud)
- [ ] Repository must be public (for free tier)

### Steps

1. **Push your code to GitHub**
   ```bash
   git add .
   git commit -m "Add Streamlit app for Data Quality Dashboard"
   git push origin master
   ```

2. **Connect to Streamlit Cloud**
   - Go to https://share.streamlit.io
   - Click "New app"
   - Select your GitHub repository
   - Branch: `master`
   - Main file path: `streamlit_app.py`
   - Click "Deploy"

3. **Configure App Settings**
   - Wait for dependencies to install (usually 2-3 minutes)
   - The app will be available at `https://your-username-data-quality-app.streamlit.app`

### Troubleshooting

**Error: "Error installing requirements"**
- ✅ **Fixed**: Removed built-in modules (`logging`, `sqlite3`, `smtplib`, `multiprocessing`) from `requirements.txt`
- ✅ **Fixed**: Removed development dependencies (`pytest`, `black`, `flake8`, `mypy`)
- Check Streamlit Cloud UI for detailed error logs

**Error: "Module not found"**
- Ensure your `requirements.txt` has all dependencies
- Check for typos in package names
- Verify Python version compatibility

**App runs slow on Streamlit Cloud**
- The app uses `@st.cache_data` for caching (built-in)
- Consider limiting dataset size for demo
- Kaggle datasets are downloaded on first run (might take 1-2 min)

---

## Environment Variables (Optional)

For sensitive data like Kaggle API keys:

1. Go to your app settings in Streamlit Cloud
2. Add secrets in the "Secrets" section:
   ```toml
   [credentials]
   kaggle_username = "your_username"
   kaggle_key = "your_api_key"
   ```

3. Access in your code:
   ```python
   import streamlit as st
   kaggle_username = st.secrets["credentials"]["kaggle_username"]
   ```

---

## Alternative Deployment Options

### Docker (for Heroku, AWS, GCP, etc.)

Create `Dockerfile`:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Build and run:
```bash
docker build -t data-quality-app .
docker run -p 8501:8501 data-quality-app
```

### Heroku

1. Create `Procfile`:
   ```
   web: streamlit run streamlit_app.py --server.port=$PORT --server.address=0.0.0.0
   ```

2. Deploy:
   ```bash
   heroku login
   heroku create your-app-name
   git push heroku master
   ```

### AWS Elastic Beanstalk, Google Cloud Run, Azure App Service
- Similar Docker-based approaches
- Use containerization for maximum compatibility

---

## Performance Tips

- **Cache data downloads**: Use `@st.cache_data` decorator
- **Optimize dataset size**: Consider data sampling for demos
- **Use session state**: Prevent re-running expensive computations
- **Deploy during off-hours**: Build times are faster

---

## Support Resources

- [Streamlit Documentation](https://docs.streamlit.io)
- [Streamlit Community](https://discuss.streamlit.io)
- [GitHub Issues](https://github.com/Vaishnavidorlikar/data-quality-governance-pipeline/issues)
