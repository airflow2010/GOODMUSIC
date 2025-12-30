import os
import pickle
import shutil
import time
import json
import base64
import random
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

import google.auth
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.cloud import firestore
from google.cloud import secretmanager
from googleapiclient.discovery import build
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import yt_dlp

load_dotenv()

COLLECTION_NAME = "musicvideos"
TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]

# Centralized AI Model Configuration
AI_MODEL_NAME = "gemini-3-flash-preview"

class MusicAnalysis(BaseModel):
    genre: str = Field(description="The music genre. Must be one of the allowed genres or 'Unknown'.")
    fidelity: int = Field(description="Confidence score between 0 and 100.")
    remarks: str = Field(description="Give reasoning (2 sentences) for the classification. More specific genres than the ones allowed for classification may be mentioned in the reasoning.")
    artist: str = Field(description="The name of the artist, or empty string if unknown.")
    track: str = Field(description="The name of the track, or empty string if unknown.")

def get_gcp_secret(secret_id: str, project_id: str, version_id: str = "latest") -> Optional[str]:
    """
    Fetches a secret from Google Cloud Secret Manager.
    Requires 'Secret Manager Secret Accessor' role.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"⚠️  Could not fetch secret '{secret_id}': {e}")
        return None

def update_gcp_secret(secret_id: str, project_id: str, content_str: str) -> bool:
    """Adds a new version to the specified secret in Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}/secrets/{secret_id}"
        payload = {"data": content_str.encode("UTF-8")}
        client.add_secret_version(request={"parent": parent, "payload": payload})
        return True
    except Exception as e:
        print(f"⚠️  Could not update secret '{secret_id}': {e}")
        return False

def init_ai_model(project_id: str, location: str = "europe-west4", credentials=None):
    """Initializes and returns the GenAI client using API Key (Env or Secret Manager)."""
    api_key = os.environ.get("GEMINI_API_KEY")

    if not api_key:
        print(f"   🔑 Env var not found. Fetching secret 'GEMINI_API_KEY' from project '{project_id}'...")
        api_key = get_gcp_secret("GEMINI_API_KEY", project_id)

    if not api_key:
        print("⚠️  Error: Could not find API Key in environment or Secret Manager.")
        return None

    try:
        return genai.Client(api_key=api_key)
    except Exception as e:
        print(f"⚠️ GenAI Client Init Error: {e}")
        return None

def init_firestore_db(project_id: Optional[str] = None) -> Optional[firestore.Client]:
    """Initializes and returns the Firestore Client using ADC."""
    try:
        # Get default credentials
        creds, calculated_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        
        # Determine final project ID: Argument > Env Var > ADC Default
        final_project_id = project_id or os.environ.get("PROJECT_ID") or calculated_project_id
        
        if not final_project_id:
            print("⚠️  Error: Could not determine Google Cloud Project ID.")
            return None

        return firestore.Client(project=final_project_id, credentials=creds)
    except Exception as e:
        print(f"⚠️  Firestore Init Error: {e}")
        return None

def parse_datetime(value: str | None) -> datetime | None:
    """Parse various ISO-ish date strings into timezone-aware datetimes."""
    if not value:
        return None
    try:
        clean = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def get_youtube_service(token_file: str = TOKEN_FILE, client_secrets_file: str = CLIENT_SECRETS_FILE):
    """Gets an authenticated YouTube service using the local token file."""
    creds = None
    project_id = os.environ.get("PROJECT_ID")
    secret_name = "YOUTUBE_TOKEN_PICKLE"
    
    # 1. Try to load from local file
    creds = None
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)

    # 2. If no local file, try to load from Secret Manager (Cloud Context)
    if not creds and project_id:
        secret_data = get_gcp_secret(secret_name, project_id)
        if secret_data:
            try:
                decoded = base64.b64decode(secret_data)
                creds = pickle.loads(decoded)
                # Save locally so we don't hit the API on every function call
                with open(token_file, "wb") as token:
                    pickle.dump(creds, token)
            except Exception as e:
                print(f"⚠️  Error loading credentials from secret: {e}")

    # 3. Refresh or Login if needed
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"⚠️  Token refresh failed: {e}. Initiating new login...")
                creds = None

        if not creds:
            if not os.path.exists(client_secrets_file):
                # Try fetching from Secret Manager
                print(f"ℹ️  '{client_secrets_file}' not found. Checking Secret Manager for 'CLIENT_SECRET_JSON'...")
                secret_content = get_gcp_secret("CLIENT_SECRET_JSON", project_id) if project_id else None
                
                if secret_content:
                    # Write to temp file because InstalledAppFlow expects a file
                    with open(client_secrets_file, "w") as f:
                        f.write(secret_content)
                else:
                    print(f"⚠️  Client secrets file '{client_secrets_file}' not found and secret 'CLIENT_SECRET_JSON' could not be retrieved.")
                    return None
            from google_auth_oauthlib.flow import InstalledAppFlow

            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, YOUTUBE_SCOPES)
                # For 'Web' client IDs, we must use a fixed port that matches the 
                # "Authorized redirect URIs" in the Google Cloud Console.
                # Ensure 'http://localhost:8080/' is added to your Console.
                print("ℹ️  Launching auth server. Ensure 'http://localhost:8080/' is in your Authorized Redirect URIs.")
                creds = flow.run_local_server(port=8080)
            except Exception as e:
                print(f"⚠️  Interactive login failed: {e}")
                return None
        
        # 4. Save valid credentials (Local File + Secret Manager)
        with open(token_file, "wb") as token:
            pickle.dump(creds, token)
        
        # If we have a project ID and just generated/refreshed a token, upload it.
        # We check for InstalledAppFlow usage implicitly or just upload on any save to be safe.
        if project_id:
            print(f"🔄 Syncing new token to Secret Manager ({secret_name})...")
            b64_creds = base64.b64encode(pickle.dumps(creds)).decode()
            update_gcp_secret(secret_name, project_id, b64_creds)

    return build("youtube", "v3", credentials=creds)


def fetch_playlist_video_ids(yt, playlist_id: str) -> List[str]:
    """Return all video IDs from a playlist, skipping private/unavailable items."""
    ids: List[str] = []
    seen = set()
    req = yt.playlistItems().list(playlistId=playlist_id, part="snippet,status", maxResults=50)
    while req:
        resp = req.execute()
        for item in resp.get("items", []):
            status = item.get("status", {})
            snippet = item.get("snippet", {})
            if status.get("privacyStatus") == "private":
                continue
            resource = snippet.get("resourceId", {})
            vid = resource.get("videoId")
            if vid and vid not in seen:
                seen.add(vid)
                ids.append(vid)
        req = yt.playlistItems().list_next(req, resp)
    return ids


def get_video_metadata(youtube, video_id: str) -> tuple[str, str, datetime | None] | None:
    """Fetches video title, description and upload date from YouTube API to help Gemini."""
    if not youtube:
        return "", "", None
    try:
        response = youtube.videos().list(part="snippet,status", id=video_id).execute()
        if "items" in response and len(response["items"]) > 0:
            item = response["items"][0]
            status = item.get("status", {})
            if status.get("privacyStatus") == "private":
                return None

            snippet = item.get("snippet", {})
            title = snippet.get("title", "")
            description = snippet.get("description", "")
            uploaded_at = parse_datetime(snippet.get("publishedAt"))
            return title, description, uploaded_at
        else:
            return None
    except Exception:
        return "", "", None


def select_best_audio_format(formats: list, max_size_mb: int = 25) -> Optional[str]:
    """Selects the best audio format under a given size, handling unknown sizes."""

    def get_filesize(f):
        # filesize_approx is often available when filesize is not
        return f.get("filesize") or f.get("filesize_approx")

    # --- Pass 1: Find audio-only formats ---
    # More robust check: vcodec is 'none' or the key doesn't exist, but acodec must exist.
    all_audio_formats = [
        f
        for f in formats
        if (f.get("vcodec") == "none" or not f.get("vcodec")) and f.get("acodec") != "none"
    ]

    # 1a. Prefer audio-only formats with a known, suitable size
    suitable_audio_known_size = [
        f for f in all_audio_formats if get_filesize(f) and get_filesize(f) < max_size_mb * 1024 * 1024
    ]
    if suitable_audio_known_size:
        suitable_audio_known_size.sort(key=lambda f: f.get("abr", 0) or 0, reverse=True)
        selected = suitable_audio_known_size[0]
        print(
            f"   ℹ️ Found suitable audio-only format with known size: {selected['format_id']} ({selected.get('abr')}k, {round(get_filesize(selected) / (1024*1024), 2)}MB)"
        )
        return selected["format_id"]

    # 1b. If none, take a chance on an audio-only format with unknown size (they are usually small)
    audio_unknown_size = [f for f in all_audio_formats if not get_filesize(f)]
    if audio_unknown_size:
        audio_unknown_size.sort(key=lambda f: f.get("abr", 0) or 0, reverse=True)
        selected = audio_unknown_size[0]
        print(
            f"   ℹ️ Found audio-only format with unknown size. Selecting best bitrate: {selected['format_id']} ({selected.get('abr')}k)"
        )
        return selected["format_id"]

    # --- Pass 2: Find video+audio formats as a fallback ---
    all_video_formats = [
        f
        for f in formats
        if f.get("vcodec") != "none" and f.get("acodec") != "none"
    ]

    # 2a. Prefer video+audio formats with a known, suitable size and low resolution
    suitable_video_known_size = [
        f for f in all_video_formats
        if get_filesize(f) and get_filesize(f) < max_size_mb * 1024 * 1024 and f.get("height", float("inf")) <= 480
    ]
    if suitable_video_known_size:
        suitable_video_known_size.sort(key=lambda f: (f.get("height", 0) or 0, f.get("abr", 0) or 0), reverse=True)
        selected = suitable_video_known_size[0]
        print(
            f"   ℹ️ No small audio-only format found. Falling back to video format with known size: {selected['format_id']} ({selected.get('height')}p, {round(get_filesize(selected) / (1024*1024), 2)}MB)"
        )
        return selected["format_id"]

    # 2b. If none, take a chance on a low-resolution video+audio format with unknown size
    video_unknown_size_low_res = [
        f for f in all_video_formats if not get_filesize(f) and f.get("height", float("inf")) <= 480
    ]
    if video_unknown_size_low_res:
        # Sort ascending by height to pick the SMALLEST resolution as a safer bet when size is unknown.
        video_unknown_size_low_res.sort(key=lambda f: (f.get("height", 0) or 0, f.get("abr", 0) or 0))
        selected = video_unknown_size_low_res[0]  # The first one is now the smallest
        print(
            f"   ℹ️ No small audio-only format found. Falling back to low-res video format with unknown size: {selected['format_id']} ({selected.get('height')}p)"
        )
        return selected["format_id"]

    # 3. If still nothing, return None to use the broad fallback.
    return None


def _attempt_download(video_id: str, use_cookies: bool) -> Optional[str]:
    """A single download attempt, with or without cookies."""
    # The final file will be .m4a after conversion
    target_m4a_path = f"/tmp/{video_id}.m4a"
    has_ffmpeg = shutil.which("ffmpeg") is not None

    # --- Step 1: Get video info without downloading ---
    info_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    if use_cookies and os.path.exists("cookies.txt"):
        info_opts["cookiefile"] = "cookies.txt"
        print(f"     - ℹ️  Using cookies from {os.path.abspath('cookies.txt')}")

    info = None
    try:
        with yt_dlp.YoutubeDL(info_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
    except Exception as e:
        if "Sign in to confirm your age" in str(e):
            print("     - ⚠️  YouTube requires sign-in (Age-gated). Cookies might be invalid or account not verified.")
        print(f"     - ⚠️ Could not fetch video formats: {e}")
        return None

    if not info or "formats" not in info:
        print("     - ⚠️ No format information found.")
        return None

    # --- Step 2: Select the best format ---
    selected_format_id = select_best_audio_format(info["formats"], max_size_mb=25)

    format_selector = selected_format_id
    if not format_selector:
        print(
            "     - ⚠️ No suitable small format found, falling back to 'bestaudio/best'. This might download a large file."
        )
        format_selector = "bestaudio/best"

    # --- Step 3: Download the selected format and extract audio ---
    ydl_opts = {
        "format": format_selector,
        "outtmpl": f"/tmp/{video_id}.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "max_filesize": 100 * 1024 * 1024,
    }

    if has_ffmpeg:
        ydl_opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "m4a",
                "preferredquality": "128",
            }
        ]
    else:
        print("     - ⚠️ FFmpeg not found. Skipping audio conversion. Downloading raw format.")

    if use_cookies and os.path.exists("cookies.txt"):
        ydl_opts["cookiefile"] = "cookies.txt"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # extract_info with download=True returns the info dict *after* download/processing
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
            
            # If ffmpeg was used, we expect the .m4a file
            if has_ffmpeg and os.path.exists(target_m4a_path):
                print(f"     - ✅ Successfully downloaded and converted audio to {target_m4a_path}")
                return target_m4a_path
            
            # If no ffmpeg, or conversion failed, find the actual downloaded file
            if "requested_downloads" in info:
                filepath = info["requested_downloads"][0]["filepath"]
            else:
                filepath = ydl.prepare_filename(info)

            if os.path.exists(filepath):
                print(f"     - ✅ Successfully downloaded audio (raw) to {filepath}")
                return filepath
            
            print(f"     - ⚠️ Download appeared to succeed, but file '{filepath}' was not found.")
            return None

    except Exception as e:
        print(f"     - ⚠️ Audio download failed: {e}")
        return None

    return None


def download_audio_for_analysis(video_id: str) -> str | None:
    """
    Downloads the audio of a YouTube video to a temporary file for AI analysis.
    It first tries un-authenticated, then falls back to using cookies if that fails.
    """
    # Clean up any previous attempts to ensure a fresh start
    for ext in ['.webm', '.mp4', '.m4a', '.m4a.part', '.part']:
        temp_file = f"/tmp/{video_id}{ext}"
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass

    # --- Attempt 1: Un-authenticated ---
    print("   - Attempting download without authentication...")
    download_path = _attempt_download(video_id, use_cookies=False)
    if download_path:
        return download_path

    # --- Attempt 2: Authenticated (if cookies exist) ---
    if os.path.exists("cookies.txt"):
        print("\n   - Un-authenticated download failed. Retrying with authentication (cookies)...")
        download_path = _attempt_download(video_id, use_cookies=True)
        if download_path:
            return download_path

    print("   - Both un-authenticated and authenticated download attempts failed.")
    return None


def predict_genre(
    model,
    video_id: str,
    video_title: str,
    video_description: str = "",
) -> tuple[Optional[tuple[str, int, str, str, str]], Optional[str]]:
    """Uses Gemini to predict genre, confidence, reasoning, artist, and track."""
    if not model:
        return None, "model_unavailable"

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

    audio_path = download_audio_for_analysis(video_id)
    
    # 1) Refrain from calling AI-API if audio download failed
    if not audio_path:
        return None, "audio_download_failed"

    parts = []

    prompt_parts = [f"Categorize the music genre of the song with YouTube Video ID '{video_id}'"]
    if video_title:
        prompt_parts.append(f" and Title '{video_title}'")
    if video_description:
        prompt_parts.append(f" and Description '{video_description}'")

    prompt_parts.append(
        ". I have provided the audio file. Please listen to the rhythm, instrumentation, and vocals to determine the genre."
    )

    instruction_text = (
        f'\n\nFor "genre", select ONE of {", ".join(allowed_genres)}. Use "Unknown" if unsure.\n'
        'The genre should be based PRIMARILY on the audio content. meta-data is just supplemental.\n'
        'For "fidelity", provide a confidence score between 0 (guessing) and 100 (absolutely certain).\n'
        'For "remarks", provide a brief reasoning (1-2 sentences) for your classification.\n'
        'For "artist" and "track", provide the most likely names, or leave empty if unknown.\n'
        'Do not hallucinate. If you don\'t know, return "Unknown".'
    )

    try:
        with open(audio_path, "rb") as f:
            audio_bytes = f.read()
        
        # Determine mime_type based on extension
        ext = os.path.splitext(audio_path)[1].lower().replace(".", "")
        mime_type = "audio/mp4" # default
        if ext == "webm":
            mime_type = "audio/webm"
        elif ext == "mp3":
            mime_type = "audio/mpeg"
        elif ext == "wav":
            mime_type = "audio/wav"
            
        parts.append(types.Part.from_bytes(data=audio_bytes, mime_type=mime_type))
    except Exception:
        pass

    parts.append(types.Part.from_text(text="".join(prompt_parts) + instruction_text))

    # 2) Configure Thinking & Schema
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=MusicAnalysis
    )

    if "gemini-3" in AI_MODEL_NAME:
        config.thinking_config = types.ThinkingConfig(
            include_thoughts=True,
            thinking_level="HIGH"
        )

    # 3) Retry Logic
    response = None
    max_retries = 3
    retry_delay = 10

    for attempt in range(max_retries + 1):
        try:
            response = model.models.generate_content(
                model=AI_MODEL_NAME,
                contents=parts,
                config=config
            )
            break
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                if attempt < max_retries:
                    print(f"      ⚠️ Quota exceeded. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
            return None, f"api_error: {str(e)}"

    try:
        text = response.text.strip()
        # Robust Markdown Cleaning
        if text.startswith("```json"):
            text = text.replace("```json", "").replace("```", "").strip()
        elif text.startswith("```"):
            text = text.replace("```", "").strip()
            
        parsed = json.loads(text)
        analysis = MusicAnalysis(**parsed)

        genre = analysis.genre
        genre = genre.strip()
        if genre not in allowed_genres and genre != "Unknown":
            genre = "Unknown"
        
        return (genre, analysis.fidelity, analysis.remarks, analysis.artist, analysis.track), None
    except Exception as e:
        return None, f"parse_error: {str(e)}"
    finally:
        if audio_path and os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except OSError:
                pass


def ingest_single_video(
    db,
    youtube,
    video_id: str,
    source: str,
    model=None,
    model_name: str = "unknown",
    extra_fields: Optional[Dict] = None,
):
    """Adds a single video to Firestore if missing; returns a status dict."""
    doc_ref = db.collection(COLLECTION_NAME).document(video_id)
    try:
        doc = doc_ref.get()
    except Exception as e:
        return {"status": "error", "message": f"Firestore error: {e}"}

    if doc.exists:
        return {"status": "exists", "message": "Already in database."}

    metadata = get_video_metadata(youtube, video_id)
    if metadata is None:
        return {"status": "unavailable", "message": "Video is private or unavailable."}

    title, description, date_youtube = metadata
    prediction, error = predict_genre(model, video_id, title, description)

    if prediction is None:
        if error == "audio_download_failed":
            return {
                "status": "error",
                "message": (
                    "Audio download failed. This often happens when YouTube blocks the IP address (e.g. in Cloud Run). "
                    "To fix this, either run the script from a residential IP or provide a 'cookies.txt' file "
                    "exported from a logged-in browser session in the working directory."
                ),
            }
        return {"status": "error", "message": f"AI analysis failed: {error}"}

    genre, fidelity, remarks, artist, track = prediction

    data = {
        "video_id": video_id,
        "source": source,
        "genre": genre,
        "genre_ai_fidelity": fidelity,
        "genre_ai_remarks": remarks,
        "ai_model": model_name,
        "artist": artist,
        "track": track,
        "rand": random.random(),
        "title": title,
        "date_prism": firestore.SERVER_TIMESTAMP,
        "date_youtube": date_youtube,
    }

    if extra_fields:
        data.update(extra_fields)

    try:
        doc_ref.set(data)
        return {"status": "added", "message": "Inserted", "title": title}
    except Exception as e:
        return {"status": "error", "message": f"Write failed: {e}"}


def ingest_video_batch(
    db,
    youtube,
    video_ids: Iterable[str],
    source: str,
    model=None,
    model_name: str = "unknown",
    extra_fields: Optional[Dict] = None,
    max_new_entries: int = 0,
    sleep_between: float = 0.0,
    progress_logger: Optional[Callable[[str], None]] = None,
):
    """Batch-ingest a list of video IDs; returns summary counts."""
    summary = {"added": 0, "exists": 0, "unavailable": 0, "errors": 0, "aborted": False}
    ids_list = list(video_ids)
    total = len(ids_list)
    consecutive_audio_failures = 0

    def log(msg: str):
        if progress_logger:
            progress_logger(msg)

    for idx, vid in enumerate(ids_list, start=1):
        if max_new_entries and summary["added"] >= max_new_entries:
            break

        log(f"[{idx}/{total}] Processing {vid}")
        result = ingest_single_video(
            db,
            youtube,
            vid,
            source=source,
            model=model,
            model_name=model_name,
            extra_fields=extra_fields,
        )

        status = result.get("status")
        if status == "added":
            summary["added"] += 1
            log(f"   ✅ Added {vid} ({result.get('title', '')})")
            consecutive_audio_failures = 0
        elif status == "exists":
            summary["exists"] += 1
            log(f"   ↩️  Skipped existing {vid}")
        elif status == "unavailable":
            summary["unavailable"] += 1
            log(f"   ⚠️  Unavailable/private {vid}")
        else:
            summary["errors"] += 1
            msg = result.get("message", "")
            log(f"   ❌ Error {vid}: {msg}")

            if "Audio download failed" in msg:
                consecutive_audio_failures += 1
                if consecutive_audio_failures >= 3:
                    log("   🛑 Aborting batch: 3 consecutive audio download failures (likely IP blocking or invalid cookies).")
                    summary["aborted"] = True
                    break

        if sleep_between:
            time.sleep(sleep_between)

    return summary
