FROM python:3.11-slim

# Work under /app in the container
WORKDIR /app

# Copy only what we actually need from the build context (/app in repo)
# The build context already *is* /app because of the Cloud Build trigger.
COPY backend /app/backend
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make sure /app is on Python path explicitly
ENV PYTHONPATH=/app
ENV UPLOAD_DIR=/tmp/recon_uploads

EXPOSE 8080

# Cloud Run will inject $PORT; default to 8080 for local runs.
CMD ["bash", "-c", "uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8080}"]