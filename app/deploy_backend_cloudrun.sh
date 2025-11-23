#!/bin/bash
PROJECT_ID=${PROJECT_ID:-your_project}
SERVICE_NAME=${SERVICE_NAME:-recon-backend}
REGION=${REGION:-us-central1}
gcloud run deploy $SERVICE_NAME --source ./backend --project $PROJECT_ID --region $REGION --platform managed --allow-unauthenticated
