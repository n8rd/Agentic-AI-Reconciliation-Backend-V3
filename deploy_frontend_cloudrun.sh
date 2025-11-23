#!/bin/bash
PROJECT_ID=${PROJECT_ID:-your_project}
SERVICE_NAME=${SERVICE_NAME:-recon-frontend}
REGION=${REGION:-us-central1}
gcloud run deploy $SERVICE_NAME --source ./ui --project $PROJECT_ID --region $REGION --platform managed --allow-unauthenticated
