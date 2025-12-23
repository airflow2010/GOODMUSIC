# GOODMUSIC prism

Tools for collecting YouTube music videos from Substack or other sources, seeding a Firestore catalog, and rating/filtering videos via a small Flask UI & export collection to YouTube-playlists.

Goal: Create your own MTV! 🎶

## Screenshots
<table>
  <tr>
    <th>rate</th>
    <th>play</th>
    <th>admin</th>
  </tr>
  <tr>
    <td valign="top"><img src="prism-ss-rate.png" alt="rate videos"></td>
    <td valign="top"><img src="prism-ss-play.png" alt="filter and play videos"></td>
    <td valign="top"><img src="prism-ss-admin.png" alt="admin section"></td>
  </tr>
</table>

## Concept
- Ingest musicvideo: extract YouTube IDs, fetch metadata, and store them as documents in Firestore (`musicvideos` collection). The source of the videos can be Substacks (this is how this project originated), YouTube playlists, or manual input of specific videos.
- Ingestion also includes categorization of videos into genres
  - This uses the publicly available AI-models of Google. This can be configured ([Configuration](#Configuration)).

- Rate and filter the catalog in a browser (play mode for discovery, playback and export, rate mode for unrated items)
- Import methods can be found in admin section
- Export the desired selection (by filter) to YouTube-Playlist

## Repository layout

- `prism-gui.py` — Flask app that renders the admin, rating (`/rate`), and play (`/play`) pages using Firestore data.
- `scrape_to_firestore.py` — Scrapes Substack posts, pulls YouTube metadata, predicts genres/artist/track, and writes new videos to Firestore. This script can also be called from the GUI.
- `templates/` — HTML templates for the Flask UI.
- `static/` — CSS, JS, and static image assets.
- `Dockerfile` — Container image build for deployment.
- `update-google-cloud-run.sh` — Deployment helper for Cloud Run.
- `requirements.txt` — Python dependencies.
- `prism-ss-*.png` — UI screenshots.

not needed except your are really curious:

* `ingestion.py` — Shared ingestion helpers (Firestore init, YouTube auth, Gemini predictions, audio handling).

- `update-genre.py` — Re-evaluates genre classification for existing Firestore docs.
- `update-db-fields.py` — Backfills missing artist/track/ai_model fields in Firestore.
- `test-ai-model.py` — CLI for comparing Gemini model outputs on sample videos.

## Prerequisites
- Python 3.10+ and `pip`.
- A Google Cloud project with these APIs enabled: Cloud Firestore & YouTube Data API v3.
- Firestore in Native mode with a collection named `musicvideos` (created automatically when seeding).
- Google Cloud SDK (`gcloud`) installed for local Application Default Credentials.
- YouTube OAuth 2.0 Client ID JSON (desktop type) downloaded as `client_secret.json` in the project root.
- An API-Key for integration of AI functions (genre classification)

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

* Set username/password for the UI

  ```bash
  export AUTH_USERNAME=<your-username>
  export AUTH_PASSWORD=<your-password>
  ```

* You can set all of the above env-variables in an .env file permanently

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

## Configuration

### ingestion.py

The variety of allowed genres for automated classification is restricted to the following genres per default.

```python
    allowed_genres = [
        "Avant-garde & experimental",
        "Blues",
        "Classical",
        "Country",
        "Easy listening",
        "Electronic",
        "Folk",
        "Hip hop",
        "Jazz",
        "Pop",
        "R&B & soul",
        "Rock",
        "Metal",
        "Punk",
    ]
```

The AI-model which is used for classification is also defined in this file.

```python
AI_MODEL_NAME = "gemini-3-flash-preview"
```

### prism-gui.py

The simple rating-system from 1 (worst) to 5 (best) can be labeled with descriptive texts:

```python
MUSIC_RATINGS = {
    5: "5️⃣ 🤩 Masterpiece",
    4: "4️⃣ 🙂 Strong",
    3: "3️⃣ 😐 Decent",
    2: "2️⃣ 🥱 Weak",
    1: "1️⃣ 😖 Awful",
}

VIDEO_RATINGS = {
    5: "5️⃣ 🤩 Visionary",
    4: "4️⃣ 🙂 Creative",
    3: "3️⃣ 😐 OK",
    2: "2️⃣ 🥱 Meh",
    1: "1️⃣ 😖 Unwatchable",
}
```



## Data model (Firestore `musicvideos`)

Each document key is the YouTube `video_id` and stores fields like:
- `title`, `source` (Substack URL), `genre`, `rating_music`, `rating_video`
- `artist`, `track`, `ai_model`, `genre_ai_fidelity`, `genre_ai_remarks`
- `favorite` (bool), `rejected` (bool)
- `date_prism`, `date_substack`, `date_youtube`, `date_rated`

## Scripts

### 1) Scrape Substack to Firestore
`scrape_to_firestore.py` fetches posts, extracts video IDs, fetches YouTube metadata, lets Vertex AI guess genre/artist/track, and writes new docs.
```bash
python scrape_to_firestore.py
  --substack: The URL of the Substack archive to scrape. Defaults to https://goodmusic.substack.com/archive.
  --project: The Google Cloud Project ID. If not provided, it attempts to infer it from the environment (ADC).
  --limit-substack-posts: Limits the number of Substack posts (articles) to process. Defaults to 0 (process all found posts). Useful for testing or incremental updates.
  --limit-new-db-entries: Limits the number of new videos added to Firestore in this run. Defaults to 0 (no limit). Useful to control costs or batch updates.
```
Notes:
- Uses ADC (`gcloud auth application-default login`) and `GCP_PROJECT`/`--project`.
- Needs `client_secret.json` for YouTube metadata; falls back gracefully if missing.
- Vertex AI genre prediction is optional; if unavailable, genre defaults to `Unknown`.

### 2) Flask UI

`prism-gui.py` serves:

- `/rate` — shows unrated videos (`date_rated` is null) to rate.
- `/play` — lets you filter (genre, min ratings, favorites, unrated inclusion, rejected exclusion) and play/rate.
- `/admin` — shows some statistics and allows importing videos

### 2a) Run the Flask UI locally

```bash
python prism-gui.py
# open http://127.0.0.1:8080
```
### 2b) Run the Flask UI in Google Cloud
```bash
gcloud run deploy prism-gui \
  --source . \
  --platform managed \
  --region europe-west4 \
  --allow-unauthenticated \
  --set-secrets="AUTH_USERNAME=prism-auth-username:latest,AUTH_PASSWORD=prism-auth-password:latest,PROJECT_ID=prism-auth-projectid:latest"
```

You will get a dynamic URL which you can then use to access the app. You can map a custom domain to the app (in GCC/Cloud Run/Domain Mappings).

## Operational tips

- Quotas: YouTube inserts and playlist creation consume quota; the playlist script stops and cleans up on `quotaExceeded`.
- Tokens: remove `token.pickle` to force a new YouTube OAuth flow.
- Firestore indexes: filtering in the UI may require composite indexes if you add more complex queries; current filters use simple field filters.

## Troubleshooting
- “Video unavailable” in the UI: check the console for YouTube player errors; embedding may be blocked or the video ID malformed.
- Firestore permission errors: ensure the Firestore API is enabled and ADC credentials belong to a project with database access.
