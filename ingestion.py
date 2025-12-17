import os
import pickle
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.cloud import firestore
from googleapiclient.discovery import build
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import yt_dlp

COLLECTION_NAME = "musicvideos"
TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]

# Centralized AI Model Configuration
AI_MODEL_NAME = "gemini-2.5-flash"

def init_ai_model(project_id: str, location: str = "europe-west4", credentials=None):
    """Initializes and returns the Vertex AI model using the centralized model name."""
    try:
        vertexai.init(project=project_id, location=location, credentials=credentials)
        return GenerativeModel(AI_MODEL_NAME)
    except Exception as e:
        print(f"⚠️ Vertex AI Init Error: {e}")
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
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_file, "wb") as token:
                    pickle.dump(creds, token)
            except Exception:
                return None
        else:
            if not os.path.exists(client_secrets_file):
                return None
            from google_auth_oauthlib.flow import InstalledAppFlow

            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, YOUTUBE_SCOPES)
                creds = flow.run_local_server(port=0)
                with open(token_file, "wb") as token:
                    pickle.dump(creds, token)
            except Exception:
                return None

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


def get_video_metadata(youtube, video_id: str) -> tuple[str, datetime | None] | None:
    """Fetches video title and upload date from YouTube API to help Gemini."""
    if not youtube:
        return "", None
    try:
        response = youtube.videos().list(part="snippet,status", id=video_id).execute()
        if "items" in response and len(response["items"]) > 0:
            item = response["items"][0]
            status = item.get("status", {})
            if status.get("privacyStatus") == "private":
                return None

            snippet = item.get("snippet", {})
            title = snippet.get("title", "")
            uploaded_at = parse_datetime(snippet.get("publishedAt"))
            return title, uploaded_at
        else:
            return None
    except Exception:
        return "", None


def download_audio_for_analysis(video_id: str) -> str | None:
    """Downloads the audio of a YouTube video to a temporary file for AI analysis."""
    output_path = f"/tmp/{video_id}.m4a"

    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except OSError:
            pass

    ydl_opts = {
        "format": "bestaudio[ext=m4a]/bestaudio",
        "outtmpl": output_path,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "max_filesize": 25 * 1024 * 1024,
    }

    # Attempt to use cookies if available to bypass bot detection (especially on Cloud Run)
    if os.path.exists("cookies.txt"):
        ydl_opts["cookiefile"] = "cookies.txt"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

        if os.path.exists(output_path):
            return output_path
    except Exception:
        return None

    return None


def predict_genre(model, video_id: str, video_title: str) -> Optional[tuple[str, int, str, str, str]]:
    """Uses Gemini to predict genre, confidence, reasoning, artist, and track."""
    if not model:
        return "Unknown", 0, "AI model not available.", "", ""

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
    
    # 1) Refrain from calling Vertex API if audio download failed
    if not audio_path:
        return None

    parts = []

    prompt_parts = [f"Categorize the music genre of the song with YouTube Video ID '{video_id}'"]
    if video_title:
        prompt_parts.append(f" and Title '{video_title}'")

    prompt_parts.append(
        ". I have provided the audio file. Please listen to the rhythm, instrumentation, and vocals to determine the genre."
    )
    try:
        with open(audio_path, "rb") as f:
            audio_data = f.read()
        parts.append(Part.from_data(data=audio_data, mime_type="audio/mp4"))
    except Exception:
        pass

    # 2) Extra protection layer in prompt
    prompt_parts.append(
        "\n\nYour response must be a JSON object with the following keys:\n"
        f'1. "genre": A string. Choose ONE of the following allowed genres: {", ".join(allowed_genres)}. '
        'If the genre cannot be determined reliably, use "Unknown".\n'
        '2. "fidelity": Integer 0-100 for confidence.\n'
        '3. "remarks": Short reasoning.\n'
        '4. "artist": Artist or band name.\n'
        '5. "track": Song title.\n'
        'IMPORTANT: Do not hallucinate. If you cannot determine the genre/artist/track from the audio, return "Unknown" and empty strings.'
    )

    parts.append("".join(prompt_parts))

    try:
        response = model.generate_content(parts)
        text = response.text or ""
        import json
        import html as htmllib

        text = text.strip()
        if text.startswith("```json"):
            text = text.strip("` \n")
            text = text.replace("json", "", 1).strip()
        parsed = json.loads(htmllib.unescape(text))
        genre = parsed.get("genre", "Unknown") or "Unknown"
        if not isinstance(genre, str):
            genre = "Unknown"
        genre = genre.strip()
        if genre not in allowed_genres and genre != "Unknown":
            genre = "Unknown"

        raw_fidelity = parsed.get("fidelity", 0)
        fidelity = int(raw_fidelity) if isinstance(raw_fidelity, (int, float)) else 0
        fidelity = max(0, min(100, fidelity))

        remarks = parsed.get("remarks", "")
        artist = parsed.get("artist", "")
        track = parsed.get("track", "")
        if not isinstance(remarks, str):
            remarks = ""
        if not isinstance(artist, str):
            artist = ""
        if not isinstance(track, str):
            track = ""
        return genre, fidelity, remarks, artist, track
    except Exception as e:
        return "Unknown", 0, str(e), "", ""
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

    title, date_youtube = metadata
    prediction = predict_genre(model, video_id, title)

    if prediction is None:
        return {
            "status": "error",
            "message": (
                "Audio download failed. This often happens when YouTube blocks the IP address (e.g. in Cloud Run). "
                "To fix this, either run the script from a residential IP or provide a 'cookies.txt' file "
                "exported from a logged-in browser session in the working directory."
            ),
        }

    genre, fidelity, remarks, artist, track = prediction

    data = {
        "video_id": video_id,
        "source": source,
        "rating_music": 3,
        "rating_video": 3,
        "genre": genre,
        "genre_ai_fidelity": fidelity,
        "genre_ai_remarks": remarks,
        "ai_model": model_name,
        "artist": artist,
        "track": track,
        "favorite": False,
        "rejected": False,
        "title": title,
        "date_prism": firestore.SERVER_TIMESTAMP,
        "date_youtube": date_youtube,
        "date_rated": None,
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
                log("   🛑 Aborting batch: Audio download is failing (likely IP blocking). Retry if it was just a temporary problem.")
                summary["aborted"] = True
                break

        if sleep_between:
            time.sleep(sleep_between)

    return summary
