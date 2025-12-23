#!/usr/bin/sh
gcloud run deploy prism-gui \
  --source . \
  --platform managed \
  --region europe-west4 \
  --allow-unauthenticated \
  --set-secrets="AUTH_USERNAME=prism-auth-username:latest,AUTH_PASSWORD=prism-auth-password:latest,PROJECT_ID=prism-auth-projectid:latest"
