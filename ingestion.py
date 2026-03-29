import os
import pickle
import shutil
import time
import json
import base64
import random
import glob
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
APP_STATE_COLLECTION = "app_state"
APP_STATE_DOC = "db_version"
DEFAULT_GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
DOWNLOAD_MAX_FILESIZE_MB = 100
DOWNLOAD_LAST_RESORT_FILESIZE_MB = 400

# Centralized AI Model Configuration
AI_MODEL_NAME = "gemini-3-flash-preview"

class MusicAnalysis(BaseModel):
    genre: str = Field(description="The music genre. Must be one of the allowed genres or 'Unknown'.")
    fidelity: int = Field(description="Confidence score between 0 and 100.")
    remarks: str = Field(description="Give reasoning (2 sentences) for the classification. More specific genres than the ones allowed for classification may be mentioned in the reasoning.")
    artist: str = Field(description="The name of the artist, or empty string if unknown.")
    track: str = Field(description="The name of the track, or empty string if unknown.")

def resolve_project_id(explicit_project_id: Optional[str] = None, adc_project_id: Optional[str] = None) -> Optional[str]:
    if explicit_project_id:
        return explicit_project_id
    env_project_id = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
    )
    if env_project_id:
        return env_project_id
    if adc_project_id is not None:
        return adc_project_id
    try:
        _, detected_project_id = google.auth.default(scopes=DEFAULT_GCP_SCOPES)
    except Exception as e:
        print(f"Warning: Could not determine Google Cloud project ID: {e}")
        return None
    return detected_project_id

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

def _version_sort_key(version) -> tuple[int, int]:
    create_time = getattr(version, "create_time", None)
    if create_time and hasattr(create_time, "seconds"):
        return (int(create_time.seconds), int(getattr(create_time, "nanos", 0)))
    return (0, 0)

def prune_gcp_secret_versions(client, parent: str, keep_latest: int = 1) -> None:
    if keep_latest < 1:
        keep_latest = 1
    versions = list(client.list_secret_versions(request={"parent": parent}))
    candidates = [
        version
        for version in versions
        if version.state != secretmanager.SecretVersion.State.DESTROYED
    ]
    if len(candidates) <= keep_latest:
        return
    candidates.sort(key=_version_sort_key, reverse=True)
    for version in candidates[keep_latest:]:
        client.destroy_secret_version(request={"name": version.name})

def update_gcp_secret(
    secret_id: str,
    project_id: str,
    content_str: str,
    destroy_old_versions: bool = False,
    keep_latest: int = 1,
) -> bool:
    """Adds a new version to the specified secret in Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}/secrets/{secret_id}"
        payload = {"data": content_str.encode("UTF-8")}
        client.add_secret_version(request={"parent": parent, "payload": payload})
    except Exception as e:
        print(f"⚠️  Could not update secret '{secret_id}': {e}")
        return False
    if destroy_old_versions:
        try:
            prune_gcp_secret_versions(client, parent, keep_latest=keep_latest)
        except Exception as e:
            print(f"Warning: Could not prune secret versions for '{secret_id}': {e}")
    return True

def init_ai_model(project_id: Optional[str] = None, location: str = "europe-west4", credentials=None):
    """Initializes and returns the GenAI client using API Key (Env or Secret Manager)."""
    api_key = os.environ.get("GEMINI_API_KEY")

    if not api_key:
        resolved_project_id = resolve_project_id(project_id)
        if not resolved_project_id:
            print("Warning: GEMINI_API_KEY not found and project ID could not be resolved for Secret Manager.")
            return None
        print(f"   🔑 Env var not found. Fetching secret 'GEMINI_API_KEY' from project '{resolved_project_id}'...")
        api_key = get_gcp_secret("GEMINI_API_KEY", resolved_project_id)

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
        creds, adc_project_id = google.auth.default(scopes=DEFAULT_GCP_SCOPES)
        
        # Determine final project ID: Argument > Env Var > ADC Default
        final_project_id = resolve_project_id(project_id, adc_project_id)
        
        if not final_project_id:
            print("⚠️  Error: Could not determine Google Cloud Project ID.")
            return None

        return firestore.Client(project=final_project_id, credentials=creds)
    except Exception as e:
        print(f"⚠️  Firestore Init Error: {e}")
        return None

def _get_app_state_ref(db: firestore.Client):
    return db.collection(APP_STATE_COLLECTION).document(APP_STATE_DOC)

def read_db_version(db: firestore.Client, create_if_missing: bool = True) -> int:
    """Reads the global DB version used to invalidate caches."""
    doc_ref = _get_app_state_ref(db)
    doc = doc_ref.get()
    if doc.exists:
        data = doc.to_dict() or {}
        try:
            return int(data.get("version") or 0)
        except (TypeError, ValueError):
            return 0
    if create_if_missing:
        doc_ref.set({"version": 1, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        return 1
    return 0

def bump_db_version(db: firestore.Client) -> Optional[int]:
    """Atomically increments the global DB version and returns the new value."""
    if not db:
        return None
    doc_ref = _get_app_state_ref(db)

    @firestore.transactional
    def _bump(transaction):
        snapshot = doc_ref.get(transaction=transaction)
        current = 0
        if snapshot.exists:
            data = snapshot.to_dict() or {}
            try:
                current = int(data.get("version") or 0)
            except (TypeError, ValueError):
                current = 0
        new_version = current + 1
        transaction.set(
            doc_ref,
            {"version": new_version, "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        return new_version

    try:
        return _bump(db.transaction())
    except Exception as e:
        print(f"⚠️  Failed to bump DB version: {e}")
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
    secret_name = "YOUTUBE_TOKEN_PICKLE"
    project_id = None
    
    # 1. Try to load from local file
    creds = None
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)

    # 2. If no local file, try to load from Secret Manager (Cloud Context)
    if not creds:
        project_id = resolve_project_id()
        if project_id:
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
                project_id = project_id or resolve_project_id()
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
        project_id = project_id or resolve_project_id()
        if project_id:
            print(f"🔄 Syncing new token to Secret Manager ({secret_name})...")
            b64_creds = base64.b64encode(pickle.dumps(creds)).decode()
            update_gcp_secret(secret_name, project_id, b64_creds, destroy_old_versions=True)

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


def is_hls_format(fmt: dict) -> bool:
    return fmt.get("protocol") in {"m3u8", "m3u8_native"}


def is_hls_only_non_live_fallback(info: dict) -> bool:
    downloadable_formats = [
        f
        for f in info.get("formats", [])
        if f.get("format_id")
        and not str(f.get("format_id", "")).startswith("sb")
        and (f.get("vcodec") != "none" or f.get("acodec") != "none")
    ]
    if not downloadable_formats:
        return False
    if info.get("is_live") or info.get("live_status") == "is_live":
        return False
    return all(is_hls_format(f) for f in downloadable_formats)


def select_download_format_candidates(
    formats: list,
    max_size_mb: int = 25,
    max_download_size_mb: int = DOWNLOAD_MAX_FILESIZE_MB,
) -> List[str]:
    """
    Builds an ordered list of format selectors to try.
    We prefer audio-only formats up to the real download budget to avoid pulling
    oversized progressive video files for long uploads. Small progressive
    formats remain as fallbacks when audio-only URLs are blocked.
    """

    def get_filesize(f):
        # filesize_approx is often available when filesize is not
        return f.get("filesize") or f.get("filesize_approx")

    def add_candidates(source_formats: list, *, limit: int) -> None:
        for f in source_formats[:limit]:
            fmt = f.get("format_id")
            if fmt and fmt not in candidates:
                candidates.append(fmt)

    max_bytes = max_size_mb * 1024 * 1024
    max_download_bytes = max_download_size_mb * 1024 * 1024

    candidates: List[str] = []

    all_video_formats = [
        f
        for f in formats
        if f.get("vcodec") != "none" and f.get("acodec") != "none" and not is_hls_format(f)
    ]

    all_audio_formats = [
        f
        for f in formats
        if (
            (f.get("vcodec") == "none" or not f.get("vcodec"))
            and f.get("acodec") != "none"
            and not is_hls_format(f)
        )
    ]

    # 1) Audio-only formats within the actual download budget.
    suitable_audio_known_size = [
        f for f in all_audio_formats if get_filesize(f) and get_filesize(f) < max_download_bytes
    ]
    suitable_audio_known_size.sort(
        key=lambda f: (
            0 if get_filesize(f) and get_filesize(f) < max_bytes else 1,
            -(f.get("abr", 0) or 0),
            get_filesize(f) or float("inf"),
        )
    )
    add_candidates(suitable_audio_known_size, limit=4)

    # 2) Audio-only unknown size as a later audio fallback.
    audio_unknown_size = [f for f in all_audio_formats if not get_filesize(f)]
    audio_unknown_size.sort(key=lambda f: f.get("abr", 0) or 0, reverse=True)
    add_candidates(audio_unknown_size, limit=2)

    # 3) Progressive video+audio with known small size.
    progressive_known_size = [
        f for f in all_video_formats
        if get_filesize(f) and get_filesize(f) < max_bytes and f.get("height", float("inf")) <= 480
    ]
    progressive_known_size.sort(
        key=lambda f: (get_filesize(f) or float("inf"), f.get("height", 0) or 0, f.get("tbr", 0) or 0)
    )
    add_candidates(progressive_known_size, limit=4)

    # 4) Larger progressive fallbacks within the downloader budget.
    progressive_within_download_limit = [
        f for f in all_video_formats
        if get_filesize(f) and get_filesize(f) < max_download_bytes and f.get("height", float("inf")) <= 480
    ]
    progressive_within_download_limit.sort(
        key=lambda f: (get_filesize(f) or float("inf"), f.get("height", 0) or 0, f.get("tbr", 0) or 0)
    )
    add_candidates(progressive_within_download_limit, limit=3)

    # 5) Progressive low-res unknown size (last resort before broad selectors).
    video_unknown_size_low_res = [
        f for f in all_video_formats if not get_filesize(f) and f.get("height", float("inf")) <= 480
    ]
    video_unknown_size_low_res.sort(
        key=lambda f: (f.get("height", 0) or 0, f.get("tbr", 0) or 0)
    )
    add_candidates(video_unknown_size_low_res, limit=2)

    return candidates


def select_large_progressive_last_resort_candidates(
    formats: list,
    min_size_mb: int = DOWNLOAD_MAX_FILESIZE_MB,
    max_size_mb: int = DOWNLOAD_LAST_RESORT_FILESIZE_MB,
) -> List[str]:
    """Select low-res progressive formats above the normal budget as a last resort."""

    def get_filesize(f):
        return f.get("filesize") or f.get("filesize_approx")

    min_bytes = min_size_mb * 1024 * 1024
    max_bytes = max_size_mb * 1024 * 1024

    candidates: List[str] = []
    large_progressive_formats = [
        f
        for f in formats
        if (
            f.get("vcodec") != "none"
            and f.get("acodec") != "none"
            and not is_hls_format(f)
            and f.get("height", float("inf")) <= 480
            and get_filesize(f)
            and min_bytes <= get_filesize(f) <= max_bytes
        )
    ]
    large_progressive_formats.sort(
        key=lambda f: (get_filesize(f) or float("inf"), f.get("height", 0) or 0, f.get("tbr", 0) or 0)
    )
    for f in large_progressive_formats[:2]:
        fmt = f.get("format_id")
        if fmt and fmt not in candidates:
            candidates.append(fmt)

    return candidates


def has_oversized_progressive_fallback(
    formats: list,
    min_size_mb: int = DOWNLOAD_LAST_RESORT_FILESIZE_MB,
) -> bool:
    """Whether only even larger low-res progressive fallbacks remain beyond the last-resort budget."""

    def get_filesize(f):
        return f.get("filesize") or f.get("filesize_approx")

    min_bytes = min_size_mb * 1024 * 1024
    for f in formats:
        if (
            f.get("vcodec") != "none"
            and f.get("acodec") != "none"
            and not is_hls_format(f)
            and f.get("height", float("inf")) <= 480
            and get_filesize(f)
            and get_filesize(f) > min_bytes
        ):
            return True
    return False


def is_video_unavailable_error(error_text: str) -> bool:
    lowered = error_text.lower()
    availability_phrases = (
        "video unavailable",
        "this video is not available",
        "private video",
        "video is private",
        "not available in your country",
        "not available in your location",
        "blocked in your country",
        "blocked in your region",
        "blocked on copyright grounds",
        "blocked it on copyright grounds",
        "copyright grounds",
    )
    if any(phrase in lowered for phrase in availability_phrases):
        return True
    if "contains content from" in lowered and "blocked" in lowered:
        return True
    return False


def _resolve_downloaded_file_path(ydl, info: dict, video_id: str) -> Optional[str]:
    """Best-effort resolution of the final downloaded file path from yt-dlp metadata."""
    candidate_paths: List[str] = []

    requested_downloads = info.get("requested_downloads")
    if isinstance(requested_downloads, list):
        for download in requested_downloads:
            if not isinstance(download, dict):
                continue
            for key in ("filepath", "_filename"):
                path = download.get(key)
                if path and path not in candidate_paths:
                    candidate_paths.append(path)

    for key in ("filepath", "_filename"):
        path = info.get(key)
        if path and path not in candidate_paths:
            candidate_paths.append(path)

    try:
        prepared_path = ydl.prepare_filename(info)
    except Exception:
        prepared_path = None
    if prepared_path and prepared_path not in candidate_paths:
        candidate_paths.append(prepared_path)

    for path in candidate_paths:
        if path and os.path.exists(path):
            return path

    matching_files = [
        path
        for path in glob.glob(f"/tmp/{video_id}*")
        if os.path.isfile(path) and not path.endswith(".part")
    ]
    if matching_files:
        matching_files.sort(key=os.path.getmtime, reverse=True)
        return matching_files[0]

    return candidate_paths[0] if candidate_paths else None


def _attempt_download(video_id: str, use_cookies: bool) -> tuple[Optional[str], Optional[str]]:
    """A single download attempt, with or without cookies. Returns (path, error_code)."""
    # Enable JS challenge solving for YouTube (requires Node + EJS remote components).
    js_runtime_opts = {
        "js_runtimes": {"node": {}},
        "remote_components": ["ejs:github"],
    }

    # The final file will be .m4a after conversion
    target_m4a_path = f"/tmp/{video_id}.m4a"
    has_ffmpeg = shutil.which("ffmpeg") is not None

    # --- Step 1: Get video info without downloading ---
    info_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    info_opts.update(js_runtime_opts)
    if use_cookies and os.path.exists("cookies.txt"):
        info_opts["cookiefile"] = "cookies.txt"
        print(f"     - ℹ️  Using cookies from {os.path.abspath('cookies.txt')}")

    info = None
    try:
        with yt_dlp.YoutubeDL(info_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
    except Exception as e:
        error_text = str(e)
        if "Sign in to confirm your age" in error_text:
            print("     - ⚠️  YouTube requires sign-in (Age-gated). Cookies might be invalid or account not verified.")
        print(f"     - ⚠️ Could not fetch video formats: {e}")
        if is_video_unavailable_error(error_text):
            return None, "video_unavailable"
        return None, "audio_download_failed"

    if not info or "formats" not in info:
        print("     - ⚠️ No format information found.")
        return None, "audio_download_failed"

    if use_cookies and is_hls_only_non_live_fallback(info):
        print("     - ⚠️ Authenticated extraction exposed only HLS fallback formats for a non-live video.")
        print("     - ⚠️ Skipping HLS-only authenticated download path because it often produces empty files when YouTube challenge solving fails.")
        return None, "auth_hls_only_formats"

    # --- Step 2: Build ordered format candidates ---
    format_candidates = select_download_format_candidates(
        info["formats"],
        max_size_mb=25,
        max_download_size_mb=DOWNLOAD_MAX_FILESIZE_MB,
    )
    large_progressive_last_resort = select_large_progressive_last_resort_candidates(
        info["formats"],
        min_size_mb=DOWNLOAD_MAX_FILESIZE_MB,
        max_size_mb=DOWNLOAD_LAST_RESORT_FILESIZE_MB,
    )
    oversized_progressive_exists = has_oversized_progressive_fallback(
        info["formats"],
        min_size_mb=DOWNLOAD_LAST_RESORT_FILESIZE_MB,
    )

    attempt_plan: List[tuple[str, int]] = [
        (format_selector, DOWNLOAD_MAX_FILESIZE_MB) for format_selector in format_candidates
    ]

    if large_progressive_last_resort:
        for format_selector in large_progressive_last_resort:
            if format_selector not in format_candidates:
                attempt_plan.append((format_selector, DOWNLOAD_LAST_RESORT_FILESIZE_MB))

    if attempt_plan:
        print(
            "     - ℹ️ Trying "
            f"{len(attempt_plan)} format candidate(s): "
            f"{', '.join(format_selector for format_selector, _ in attempt_plan)}"
        )
        if large_progressive_last_resort:
            print(
                "     - ℹ️ Large progressive last-resort candidates will only be tried "
                "after audio-only/standard candidates fail."
            )
    else:
        print("     - ⚠️ No preferred format candidates found. Falling back to broad selectors.")
        attempt_plan = [
            (f"bestaudio[filesize<{DOWNLOAD_MAX_FILESIZE_MB}M]/bestaudio", DOWNLOAD_MAX_FILESIZE_MB),
            (
                (
                    f"best[acodec!=none][vcodec!=none][height<=480][filesize<{DOWNLOAD_MAX_FILESIZE_MB}M]"
                    "/best[acodec!=none][vcodec!=none][height<=480]"
                ),
                DOWNLOAD_MAX_FILESIZE_MB,
            ),
            (
                (
                    f"best[acodec!=none][vcodec!=none][height<=480][filesize<{DOWNLOAD_LAST_RESORT_FILESIZE_MB}M]"
                    "/best[acodec!=none][vcodec!=none][height<=480]"
                ),
                DOWNLOAD_LAST_RESORT_FILESIZE_MB,
            ),
        ]

    # --- Step 3: Download the selected format and extract audio ---
    last_error_text = ""
    for attempt_idx, (format_selector, max_filesize_mb) in enumerate(attempt_plan, start=1):
        ydl_opts = {
            "format": format_selector,
            "outtmpl": f"/tmp/{video_id}.%(ext)s",
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "max_filesize": max_filesize_mb * 1024 * 1024,
        }
        ydl_opts.update(js_runtime_opts)

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

        size_note = ""
        if max_filesize_mb != DOWNLOAD_MAX_FILESIZE_MB:
            size_note = f" (max {max_filesize_mb} MB, last resort)"

        print(
            f"     - ℹ️ Download attempt {attempt_idx}/{len(attempt_plan)} "
            f"with format '{format_selector}'{size_note}"
        )

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # extract_info with download=True returns the info dict after processing
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)

                # If ffmpeg was used, we expect the .m4a file
                if has_ffmpeg and os.path.exists(target_m4a_path):
                    print(f"     - ✅ Successfully downloaded and converted audio to {target_m4a_path}")
                    return target_m4a_path, None

                # If no ffmpeg, or conversion failed, find the actual downloaded file
                filepath = _resolve_downloaded_file_path(ydl, info, video_id)

                if filepath and os.path.exists(filepath):
                    print(f"     - ✅ Successfully downloaded audio (raw) to {filepath}")
                    return filepath, None

                if filepath:
                    print(f"     - ⚠️ Download appeared to succeed, but file '{filepath}' was not found.")
                else:
                    print("     - ⚠️ Download appeared to succeed, but yt-dlp did not expose a usable output path.")
                last_error_text = "file_not_found_after_download"

        except Exception as e:
            error_text = str(e)
            last_error_text = error_text
            if is_video_unavailable_error(error_text):
                print(f"     - ⚠️ Video unavailable: {e}")
                return None, "video_unavailable"
            print(f"     - ⚠️ Audio download failed for format '{format_selector}': {e}")

        # Cleanup partial files before trying the next format candidate.
        for path in glob.glob(f"/tmp/{video_id}*"):
            try:
                os.remove(path)
            except OSError:
                pass

    if last_error_text:
        print(f"     - ⚠️ Exhausted all format candidates. Last error: {last_error_text}")
    if oversized_progressive_exists:
        print(
            "     - ⚠️ Audio-only downloads failed, and the remaining progressive fallback formats "
            f"exceed the configured last-resort size limit of {DOWNLOAD_LAST_RESORT_FILESIZE_MB} MB."
        )
        return None, "progressive_fallback_too_large"
    return None, "audio_download_failed"


def download_audio_for_analysis(video_id: str) -> tuple[Optional[str], Optional[str]]:
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
    error_codes = []
    download_path, error_code = _attempt_download(video_id, use_cookies=False)
    if download_path:
        return download_path, None
    if error_code:
        error_codes.append(error_code)

    # --- Attempt 2: Authenticated (if cookies exist) ---
    if os.path.exists("cookies.txt"):
        print("\n   - Un-authenticated download failed. Retrying with authentication (cookies)...")
        download_path, error_code = _attempt_download(video_id, use_cookies=True)
        if download_path:
            return download_path, None
        if error_code:
            error_codes.append(error_code)

    print("   - Both un-authenticated and authenticated download attempts failed.")
    if "video_unavailable" in error_codes:
        return None, "video_unavailable"
    if "auth_hls_only_formats" in error_codes:
        return None, "auth_hls_only_formats"
    return None, "audio_download_failed"


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

    audio_path, audio_error = download_audio_for_analysis(video_id)
    
    # 1) Refrain from calling AI-API if audio download failed
    if not audio_path:
        return None, audio_error or "audio_download_failed"

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
        if error == "video_unavailable":
            return {
                "status": "unavailable",
                "message": "Video is not available for audio download or streaming.",
            }
        if error == "audio_download_failed":
            return {
                "status": "error",
                "message": (
                    "Audio download failed after trying multiple format fallbacks. "
                    "Likely causes: YouTube request restrictions (SABR/PO token), IP/network blocking, or stale cookies. "
                    "Try updating yt-dlp, refreshing cookies.txt, and running from a different network."
                ),
            }
        if error == "progressive_fallback_too_large":
            return {
                "status": "error",
                "message": (
                    "Audio-only formats could not be downloaded, and the remaining progressive fallback "
                    f"formats exceed the configured size limit of {DOWNLOAD_LAST_RESORT_FILESIZE_MB} MB. "
                    "Increase the last-resort limit if you want to allow very large low-resolution video downloads "
                    "for audio extraction."
                ),
            }
        if error == "auth_hls_only_formats":
            return {
                "status": "error",
                "message": (
                    "Authenticated extraction exposed only HLS fallback formats for this non-live video, "
                    "which currently leads to empty downloads in this environment. "
                    "Try refreshing cookies.txt and checking yt-dlp JS challenge solving/EJS support."
                ),
            }
        return {"status": "error", "message": f"AI analysis failed: {error}"}

    genre, fidelity, remarks, artist, track = prediction

    data = {
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
        new_version = bump_db_version(db)
        return {
            "status": "added",
            "message": "Inserted",
            "title": title,
            "data": data,
            "version": new_version,
        }
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
                    log("   🛑 Aborting batch: 3 consecutive audio download failures (likely network or YouTube format restrictions).")
                    summary["aborted"] = True
                    break

        if sleep_between:
            time.sleep(sleep_between)

    return summary
