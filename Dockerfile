# Hugging Face Spaces Dockerfile for Git Pattern Analysis
FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr and writing bytecode
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=7860

# Install system dependencies:
# - default-jre-headless: Java JRE required for PySpark MLlib clustering
# - git: Git tooling for repo operations
# - curl: Healthchecks and diagnostic tooling
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set up Hugging Face Spaces default non-root user (UID 1000)
RUN useradd -m -u 1000 user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy repository source code and assets with ownership assigned to user 1000
COPY --chown=user:user . $HOME/app

# Ensure runtime directories exist and user 1000 has full ownership
RUN mkdir -p $HOME/app/data/runs && chown -R user:user $HOME

# Switch to non-root user
USER user

# Hugging Face Spaces listens on port 7860
EXPOSE 7860

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:7860/api/runs || exit 1

# Start the dashboard server on 0.0.0.0:7860
CMD ["python3", "dashboard/server.py", "--host", "0.0.0.0", "--port", "7860"]
