#!/usr/bin/sh
gcloud run deploy prism-gui \
  --source . \
  --platform managed \
  --region europe-west4 \
  --allow-unauthenticated \
  --set-secrets="ADMIN_USER=ADMIN_USER:latest,ADMIN_PASSWORD=ADMIN_PASSWORD:latest"
