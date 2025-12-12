# GOODMUSIC PRISM Toolkit

Tools for collecting YouTube music videos from Substack, seeding a Firestore catalog, creating YouTube playlists, and rating/filtering videos via a small Flask UI.

## Concept
- Scrape Substack posts, extract YouTube IDs, fetch metadata, and store them as documents in Firestore (`musicvideos` collection).
- Optionally let Vertex AI guess the genre.
- Build YouTube playlists automatically from Substack archives or local HTML.
- Rate and filter the catalog in a browser (play mode for discovery, rate mode for unrated items).

## Repository layout
- `prism-gui.py` — Flask app that renders the rating (`/rate`) and play (`/play`) pages using Firestore data.
- `scrape_to_firestore.py` — Scrapes Substack posts, pulls YouTube metadata, predicts genres (Vertex AI), and writes new videos to Firestore.
- `scrape_to_YT-playlists.py` — Creates YouTube playlists from Substack archives or local HTML; tracks progress in `progress.json`. This script is deprecated and only included for historical reasons.
- `templates/` — HTML templates for the Flask UI.
- `requirements.txt` — Python dependencies.

## Prerequisites
- Python 3.10+ and `pip`.
- A Google Cloud project with these APIs enabled: Cloud Firestore, Vertex AI (optional for genre prediction), YouTube Data API v3.
- Firestore in Native mode with a collection named `musicvideos` (created automatically when seeding).
- Google Cloud SDK (`gcloud`) installed for local Application Default Credentials.
- YouTube OAuth 2.0 Client ID JSON (desktop type) downloaded as `client_secret.json` in the project root.

## Setup
```bash
git clone <this-repo>
cd GOODMUSIC
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Authenticate
- Google Cloud (Firestore/Vertex): `gcloud auth application-default login`
- YouTube Data API: first run of the playlist or scrape scripts opens a browser OAuth flow; `token.pickle` will be written next to the scripts.
- Set your project ID for the UI (and for scraping if you want to override ADC):
  ```bash
  export GCP_PROJECT=<your-project-id>
  ```

### Required files
- `client_secret.json` — OAuth client for YouTube Data API v3 (Desktop app).
- `progress.json` — Created automatically to avoid duplicate playlist creation.

### Prepare Google Cloud Secrets
To secure the Flask UI in Cloud Run without exposing credentials in deployment commands:
1. Enable Secret Manager: `gcloud services enable secretmanager.googleapis.com`
2. Create secrets for the UI login:
   ```bash
   printf "your-username" | gcloud secrets create prism-auth-username --data-file=-
   printf "your-password" | gcloud secrets create prism-auth-password --data-file=-
   printf "your-project-id" | gcloud secrets create prism-auth-projectid --data-file=-
   ```
3. Grant the Compute Engine default service account access to the secrets (replace `<PROJECT_NUMBER>` with your project number):
   ```bash
   gcloud secrets add-iam-policy-binding prism-auth-username \
       --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
       --role="roles/secretmanager.secretAccessor"
   gcloud secrets add-iam-policy-binding prism-auth-password \
       --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
       --role="roles/secretmanager.secretAccessor"
   gcloud secrets add-iam-policy-binding prism-auth-projectid \
       --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
       --role="roles/secretmanager.secretAccessor"
   ```

## Data model (Firestore `musicvideos`)
Each document key is the YouTube `video_id` and stores fields like:
- `title`, `source` (Substack URL), `genre`, `musical_value`, `video_value`
- `favorite` (bool), `rejected` (bool)
- `date_prism`, `date_substack`, `date_youtube`, `date_rated`

## Scripts

### 0) Build YouTube playlists (deprecated)

`scrape_to_YT-playlists.py` creates playlists from Substack or a local HTML file; remembers processed posts in `progress.json`.

```bash
# From Substack archive
python scrape_to_YT-playlists.py --substack https://goodmusic.substack.com/archive \
  --privacy private --limit 10 --sleep 0.2

# From local HTML
python scrape_to_YT-playlists.py path/to/file.html --privacy unlisted
```

Flags:

- `--privacy private|unlisted|public`
- `--dry-run` to inspect without creating playlists
- `--limit` to cap posts/videos
- `--sleep` to throttle API calls
  Auth:
- Requires `client_secret.json`; first run writes `token.pickle`.
- Enable YouTube Data API v3 in your project.

### 1) Scrape Substack to Firestore
`scrape_to_firestore.py` fetches posts, extracts video IDs, fetches YouTube metadata, lets Vertex AI guess genre, and writes new docs.
```bash
python scrape_to_firestore.py --substack https://goodmusic.substack.com/archive \
  --project <your-project-id> \
  --limit 50     # optional
```
Notes:
- Uses ADC (`gcloud auth application-default login`) and `GCP_PROJECT`/`--project`.
- Needs `client_secret.json` for YouTube metadata; falls back gracefully if missing.
- Vertex AI genre prediction is optional; if unavailable, genre defaults to `Unknown`.

### 2a) Run the Flask UI locally
`prism-gui.py` serves:

- `/rate` — shows unrated videos (`musical_value == 0`) to rate.
- `/play` — lets you filter (genre, min ratings, favorites, unrated inclusion, rejected exclusion) and play/rate.
```bash
export GCP_PROJECT=<your-project-id>
python prism-gui.py
# open http://127.0.0.1:8080
```
For production you can run `gunicorn prism-gui:app`.

### 2b) Run the Flask UI in Google Cloud
```bash
gcloud run deploy prism-gui \
  --source . \
  --platform managed \
  --region europe-west4 \
  --allow-unauthenticated \
  --set-secrets="AUTH_USERNAME=prism-auth-username:latest,AUTH_PASSWORD=prism-auth-password:latest,PROJECT_ID=prism-auth-projectid:latest"
```

## Operational tips
- Quotas: YouTube inserts and playlist creation consume quota; the playlist script stops and cleans up on `quotaExceeded`.
- Progress: `progress.json` prevents duplicate playlists; delete it if you want to rebuild everything.
- Tokens: remove `token.pickle` to force a new YouTube OAuth flow.
- Firestore indexes: filtering in the UI may require composite indexes if you add more complex queries; current filters use simple field filters.

## Troubleshooting
- “Video unavailable” in the UI: check the console for YouTube player errors; embedding may be blocked or the video ID malformed.
- Firestore permission errors: ensure the Firestore API is enabled and ADC credentials belong to a project with database access.
- Vertex AI errors: the scraper will continue; genres become `Unknown`.
