#!/usr/bin/sh
gcloud run deploy prism-gui \
  --source . \
  --platform managed \
  --region europe-west4 \
  --allow-unauthenticated \
  --set-secrets="AUTH_USERNAME=AUTH_USERNAME:latest,AUTH_PASSWORD=AUTH_PASSWORD:latest,PROJECT_ID=PROJECT_ID:latest,AUTH_GOOGLE=AUTH_GOOGLE:latest"
