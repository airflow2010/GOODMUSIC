import os
import json
import base64
import secrets
import sys
import subprocess
import random
import time
from functools import wraps
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs, urlparse
from typing import Optional

from flask import Flask, render_template, request, redirect, url_for, Response, stream_with_context, session, g
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash
from authlib.integrations.flask_client import OAuth
from google.cloud import firestore
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
import re
from ingestion import fetch_playlist_video_ids, get_youtube_service as ingestion_get_youtube_service, ingest_single_video, init_ai_model, init_firestore_db, AI_MODEL_NAME, get_gcp_secret

load_dotenv()

COLLECTION_NAME = "musicvideos"
USERS_COLLECTION = "users"
IMPORT_REQUESTS_COLLECTION = "import_requests"

CSRF_TOKEN_KEY = "_csrf_token"
LOGIN_RATE_LIMIT = 10
LOGIN_RATE_WINDOW_SECONDS = 600
IMPORT_REQUEST_RATE_LIMIT = 10
IMPORT_REQUEST_WINDOW_SECONDS = 3600
MAX_IMPORT_PAYLOAD_LEN = 2000
MAX_IMPORT_NOTES_LEN = 500
MAX_IMPORT_VIDEO_IDS = 50

RATE_LIMIT_BUCKETS = {}

# --- Google OAuth Configuration (will be set later by reading client_secret.json)---
GOOGLE_CLIENT_ID = None
GOOGLE_CLIENT_SECRET = None

# Determine absolute path to client_secret.json to ensure it's found regardless of CWD
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_SECRET_FILE = os.path.join(BASE_DIR, "client_secret.json")

# --- Firestore Client Initialization ---
# Initialize DB early to get Project ID for Secret Manager access
db = init_firestore_db()

if db:
    print(f"✅ Configuration loaded for Project ID: {db.project}")
else:
    # We don't exit here yet; we'll catch critical DB failures at the end of startup
    pass

# --- Authentication Configuration ---
def get_conf(key, project_id=None):
    val = os.environ.get(key)
    if not val and project_id:
        print(f"   🔑 Env var '{key}' not found. Fetching from Secret Manager...")
        val = get_gcp_secret(key, project_id)
    return val

AUTH_USERNAME = get_conf("AUTH_USERNAME", db.project if db else None)
AUTH_PASSWORD = get_conf("AUTH_PASSWORD", db.project if db else None)
AUTH_GOOGLE = get_conf("AUTH_GOOGLE", db.project if db else None)
FLASK_SECRET_KEY = get_conf("FLASK_SECRET_KEY", db.project if db else None)

# Load Google Credentials from client_secret.json or Secret Manager
if os.path.exists(CLIENT_SECRET_FILE):
    try:
        with open(CLIENT_SECRET_FILE, "r") as f:
            client_data = json.load(f)
            # Look for 'web' (preferred) or 'installed'
            creds = client_data.get("web") or client_data.get("installed")
            if creds:
                GOOGLE_CLIENT_ID = creds.get("client_id")
                GOOGLE_CLIENT_SECRET = creds.get("client_secret")
    except Exception as e:
        print(f"⚠️  Warning: Could not parse client_secret.json: {e}")
elif db and db.project:
    # Try fetching from Secret Manager if file is missing
    print(f"ℹ️  client_secret.json not found. Attempting to fetch secret 'CLIENT_SECRET_JSON'...")
    secret_content = get_gcp_secret("CLIENT_SECRET_JSON", db.project)
    if secret_content:
        try:
            client_data = json.loads(secret_content)
            creds = client_data.get("web") or client_data.get("installed")
            if creds:
                GOOGLE_CLIENT_ID = creds.get("client_id")
                GOOGLE_CLIENT_SECRET = creds.get("client_secret")
        except Exception as e:
            print(f"⚠️  Warning: Could not parse secret 'CLIENT_SECRET_JSON': {e}")

def check_prerequisites():
    """Checks for prerequisites for successful deployment and refuses to start if not met."""
    missing_vars = []
    if not AUTH_USERNAME:
        missing_vars.append("AUTH_USERNAME")
    if not AUTH_PASSWORD:
        missing_vars.append("AUTH_PASSWORD")
    if not FLASK_SECRET_KEY:
        missing_vars.append("FLASK_SECRET_KEY")

    if missing_vars:
        print("\n" + "!" * 60)
        print("❌ STARTUP ERROR: Missing configuration variables.")
        print("!" * 60)
        print("The following environment variables are missing:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nINSTRUCTIONS:")
        if os.environ.get("K_SERVICE"):
            print("   You appear to be running on Google Cloud Run.")
            print("   Ensure you have mounted the Google Cloud Secrets as environment variables.")
            print("   Verify your 'gcloud run deploy' command includes:")
            print('   --set-secrets="AUTH_USERNAME=AUTH_USERNAME:latest,AUTH_PASSWORD=AUTH_PASSWORD:latest,PROJECT_ID=PROJECT_ID:latest"')
        else:
            print("   You appear to be running locally.")
            print("   Ensure you have a .env file or exported environment variables.")
        sys.exit(1)

    if AUTH_GOOGLE and (not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET):
        print("\n⚠️  WARNING: AUTH_GOOGLE is set, but client_secret.json is missing or invalid.")
        print(f"   Expected file at: {CLIENT_SECRET_FILE}")
        print("   Google Authentication will be disabled, falling back to Basic Auth only.")
        print("   Ensure you have a valid client_secret.json file or 'CLIENT_SECRET_JSON' in Secret Manager.")
    elif AUTH_GOOGLE:
        print(f"✅ Google Authentication enabled (admin): {AUTH_GOOGLE}")
    else:
        print("ℹ️  Google Authentication disabled (AUTH_GOOGLE not set).")

    # Check for required templates
    required_templates = ['rate.html', 'play.html', 'admin.html']
    missing_templates = []
    for t in required_templates:
        if not os.path.exists(os.path.join('templates', t)):
            missing_templates.append(t)

    if missing_templates:
        print("\n" + "!" * 60)
        print("❌ STARTUP ERROR: Missing HTML templates.")
        print("!" * 60)
        print("The following templates are missing in the 'templates/' directory:")
        for t in missing_templates:
            print(f"   - {t}")
        sys.exit(1)


def check_firestore_access(db_client):
    """Verifies access to the specific collection and warns if empty."""
    print(f"🔍 Checking access to Firestore collection: {COLLECTION_NAME}...")
    try:
        # Attempt to fetch a single document to verify read permissions and data existence
        docs = list(db_client.collection(COLLECTION_NAME).limit(1).stream())
        if not docs:
            print(f"⚠️  WARNING: The Firestore collection '{COLLECTION_NAME}' appears to be empty.")
            print("   The application will start, but you may not see any videos.")
            print("   Run 'python scrape_to_firestore.py' to populate the database.")
        else:
            print(f"✅ Firestore collection '{COLLECTION_NAME}' is accessible and contains data.")
    except Exception as e:
        print("\n" + "!" * 60)
        print(f"❌ STARTUP ERROR: Could not read from collection '{COLLECTION_NAME}'.")
        print("!" * 60)
        print(f"Error details: {e}")
        print("\nINSTRUCTIONS:")
        print("   1. Ensure the Service Account has 'Cloud Datastore User' permissions.")
        print("   2. Verify the collection name is correct.")
        sys.exit(1)


def get_client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or "unknown"


def get_csrf_token() -> str:
    token = session.get(CSRF_TOKEN_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[CSRF_TOKEN_KEY] = token
    return token


def validate_csrf() -> bool:
    token = session.get(CSRF_TOKEN_KEY)
    if not token:
        return False
    submitted = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if not submitted:
        return False
    return secrets.compare_digest(token, submitted)


def rate_limit_exceeded(key: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    bucket = RATE_LIMIT_BUCKETS.get(key, [])
    bucket = [ts for ts in bucket if now - ts < window_seconds]
    RATE_LIMIT_BUCKETS[key] = bucket
    return len(bucket) >= limit


def rate_limit_hit(key: str) -> None:
    bucket = RATE_LIMIT_BUCKETS.setdefault(key, [])
    bucket.append(time.time())



def rating_key_for_user_id(user_id: str) -> str:
    token = base64.urlsafe_b64encode(user_id.encode("utf-8")).decode("ascii").rstrip("=")
    return token or "user"


def get_user_doc(user_id: str) -> Optional[dict]:
    if not db:
        return None
    doc = db.collection(USERS_COLLECTION).document(user_id).get()
    return doc.to_dict() if doc.exists else None


def ensure_user_record(
    user_id: str,
    role: str,
    auth_provider: str,
    email: Optional[str] = None,
    force_role: bool = False,
    force_auth_provider: bool = False,
) -> Optional[dict]:
    if not db:
        return None
    doc_ref = db.collection(USERS_COLLECTION).document(user_id)
    doc = doc_ref.get()
    data = doc.to_dict() if doc.exists else {}
    rating_key = data.get("rating_key") or rating_key_for_user_id(user_id)
    role_value = role if force_role else (data.get("role") or role)
    auth_value = auth_provider if force_auth_provider else (data.get("auth_provider") or auth_provider)
    update = {
        "rating_key": rating_key,
        "role": role_value,
        "status": data.get("status") or "active",
        "auth_provider": auth_value,
    }
    if email:
        update["email"] = email
    if not doc.exists:
        update["created_at"] = firestore.SERVER_TIMESTAMP
    doc_ref.set(update, merge=True)
    data.update(update)
    return data


def build_user_context(user_id: str, auth_provider: str, user_doc: Optional[dict], default_role: str) -> Optional[dict]:
    if user_doc and user_doc.get("status") == "disabled":
        return None
    rating_key = (user_doc or {}).get("rating_key") or rating_key_for_user_id(user_id)
    if user_doc is not None and not user_doc.get("rating_key") and db:
        db.collection(USERS_COLLECTION).document(user_id).set({"rating_key": rating_key}, merge=True)
    role = (user_doc or {}).get("role") or default_role
    return {"id": user_id, "role": role, "auth_provider": auth_provider, "rating_key": rating_key}


def resolve_basic_user(username: str, password: str) -> Optional[dict]:
    if AUTH_USERNAME and AUTH_PASSWORD and username == AUTH_USERNAME and password == AUTH_PASSWORD:
        user_doc = ensure_user_record(
            username,
            role="admin",
            auth_provider="basic",
            force_role=True,
            force_auth_provider=True,
        )
        return build_user_context(username, "basic", user_doc, default_role="admin")

    user_doc = get_user_doc(username)
    if not user_doc:
        return None
    if user_doc.get("auth_provider") not in ("basic", "any"):
        return None
    stored_hash = user_doc.get("password_hash")
    if not stored_hash or not check_password_hash(stored_hash, password):
        return None
    return build_user_context(username, "basic", user_doc, default_role="user")


def resolve_google_user(email: str) -> Optional[dict]:
    is_admin = AUTH_GOOGLE and email == AUTH_GOOGLE
    if is_admin:
        user_doc = ensure_user_record(
            email,
            role="admin",
            auth_provider="google",
            email=email,
            force_role=True,
            force_auth_provider=True,
        )
        return build_user_context(email, "google", user_doc, default_role="admin")

    user_doc = ensure_user_record(
        email,
        role="user",
        auth_provider="google",
        email=email,
        force_role=True,
        force_auth_provider=True,
    )
    return build_user_context(email, "google", user_doc, default_role="user")


def resolve_request_user() -> Optional[dict]:
    g.auth_rate_limited = False
    if AUTH_GOOGLE and session.get("user"):
        user = resolve_google_user(session.get("user"))
        if user:
            return user
    auth = request.authorization
    if auth:
        ip = get_client_ip()
        ip_key = f"login:ip:{ip}"
        user_key = f"login:user:{auth.username}"
        if rate_limit_exceeded(ip_key, LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW_SECONDS) or rate_limit_exceeded(
            user_key, LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW_SECONDS
        ):
            g.auth_rate_limited = True
            return None
        user = resolve_basic_user(auth.username, auth.password)
        if user:
            return user
        rate_limit_hit(ip_key)
        rate_limit_hit(user_key)
    return None


def current_user() -> dict:
    user = getattr(g, "current_user", None)
    if not user:
        raise RuntimeError("User context missing for authenticated route.")
    return user


LAST_SEEN_INTERVAL = timedelta(minutes=15)


def touch_user_activity(user: dict) -> None:
    if not db or not user:
        return
    now = datetime.now(timezone.utc)
    session_key = f"last_seen_at:{user.get('id')}"
    last_seen_str = session.get(session_key)
    if last_seen_str:
        try:
            last_seen = datetime.fromisoformat(last_seen_str)
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            if now - last_seen < LAST_SEEN_INTERVAL:
                return
        except ValueError:
            pass
    try:
        db.collection(USERS_COLLECTION).document(user["id"]).set(
            {"last_seen_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        session[session_key] = now.isoformat()
    except Exception as e:
        print(f"⚠️  Failed to update user activity: {e}")


# Perform checks before initializing the app
check_prerequisites()

# --- Rating Descriptions ---
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

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
if os.environ.get("K_SERVICE") or os.environ.get("FORCE_HTTPS") == "1":
    app.config["SESSION_COOKIE_SECURE"] = True

# --- OAuth Initialization ---
oauth = OAuth(app)
google = None
if AUTH_GOOGLE and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    google = oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email'}
    )


@app.context_processor
def inject_csrf_token():
    return {"csrf_token": get_csrf_token()}

# Verify collection access
if db:
    check_firestore_access(db)
else:
    print("\n" + "!" * 60)
    print("❌ STARTUP ERROR: Could not connect to Firestore.")
    print("!" * 60)
    sys.exit(1)

@app.after_request
def add_security_headers(response):
    if request.is_secure or request.headers.get("X-Forwarded-Proto", "") == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

# --- Auth Decorator ---
def check_auth(username, password):
    """Checks if the username and password are correct."""
    return resolve_basic_user(username, password) is not None

def authenticate():
    """Sends a 401 response that enables basic auth."""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = resolve_request_user()
        if user:
            g.current_user = user
            touch_user_activity(user)
            return f(*args, **kwargs)

        if getattr(g, "auth_rate_limited", False):
            return Response("Too many login attempts. Please try again later.", 429)

        if AUTH_GOOGLE and not session.get('prefer_legacy'):
            if not google:
                return Response("Configuration Error: AUTH_GOOGLE is set but Google Client is not initialized. Check server logs for missing client_secret.json.", 500)

            if session.get('google_attempted'):
                return Response(f"Access Denied: Your account is disabled.<br><a href='{url_for('login_google')}'>Try again</a><br>or <a href='{url_for('login_legacy')}'>Legacy Login</a>", 403)

            return redirect(url_for('login_google'))

        return authenticate()
    return decorated

def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = current_user()
        if user.get("role") != "admin":
            return Response("Access Denied: Admin privileges required.", 403)
        return f(*args, **kwargs)
    return decorated


def requires_csrf(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            if not validate_csrf():
                return Response("CSRF validation failed.", 403)
        return f(*args, **kwargs)
    return decorated

# --- Helper Functions ---
def extract_index_error_info(error: Exception):
    """Return dict with info when Firestore reports a missing composite index."""
    message = str(error)
    if "requires an index" not in message:
        return None
    link_match = re.search(r"https://console\.firebase\.google\.com/\S+", message)
    link = link_match.group(0) if link_match else None
    if "currently building" in message:
        summary = "Firestore index is still building; try again in a few minutes."
    else:
        summary = "Firestore query requires a composite index."
    return {"summary": summary, "link": link, "raw": message}


def get_last_activity_ts(user_data: dict) -> Optional[datetime]:
    value = user_data.get("last_seen_at") or user_data.get("created_at")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value
    return None


def format_ts(value: Optional[datetime]) -> str:
    if not value:
        return "never"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def is_protected_user(user_id: str, user_data: dict, current_user_id: str) -> bool:
    if user_id == current_user_id:
        return True
    if AUTH_GOOGLE and user_id == AUTH_GOOGLE:
        return True
    if AUTH_USERNAME and user_id == AUTH_USERNAME:
        return True
    if user_data.get("role") == "admin":
        return True
    return False


def delete_user_and_ratings(user_id: str) -> dict:
    if not db:
        return {"deleted": False, "ratings_removed": 0}
    user_doc = db.collection(USERS_COLLECTION).document(user_id).get()
    if not user_doc.exists:
        return {"deleted": False, "ratings_removed": 0}
    user_data = user_doc.to_dict() or {}
    rating_key = user_data.get("rating_key") or rating_key_for_user_id(user_id)

    batch = db.batch()
    pending = 0
    removed = 0
    docs = db.collection(COLLECTION_NAME).stream()
    for doc in docs:
        data = doc.to_dict() or {}
        ratings = data.get("ratings") or {}
        if rating_key not in ratings:
            continue
        batch.update(doc.reference, {f"ratings.{rating_key}": firestore.DELETE_FIELD})
        pending += 1
        removed += 1
        if pending >= 400:
            batch.commit()
            batch = db.batch()
            pending = 0
    if pending:
        batch.commit()

    db.collection(USERS_COLLECTION).document(user_id).delete()
    return {"deleted": True, "ratings_removed": removed}

def coerce_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def legacy_rating_from_video(video_data: dict) -> dict:
    if not video_data or not video_data.get("date_rated"):
        return {}
    rating = {
        "rating_music": video_data.get("rating_music", 3),
        "rating_video": video_data.get("rating_video", 3),
        "favorite": bool(video_data.get("favorite", False)),
        "rejected": bool(video_data.get("rejected", False)),
        "rated_at": video_data.get("date_rated"),
    }
    if video_data.get("genre"):
        rating["genre_override"] = video_data.get("genre")
    return rating


def extract_user_rating(video_data: dict, user: dict) -> dict:
    ratings = (video_data or {}).get("ratings") or {}
    rating = ratings.get(user.get("rating_key")) or {}
    if not rating and user.get("role") == "admin":
        rating = legacy_rating_from_video(video_data)
    return rating


def merge_user_rating(video_data: dict, user: dict) -> dict:
    rating = extract_user_rating(video_data, user)
    merged = dict(video_data or {})
    merged["rating_music"] = coerce_int(rating.get("rating_music", 3), 3)
    merged["rating_video"] = coerce_int(rating.get("rating_video", 3), 3)
    merged["favorite"] = bool(rating.get("favorite", False))
    merged["rejected"] = bool(rating.get("rejected", False))
    merged["date_rated"] = rating.get("rated_at")
    merged["genre_ai"] = video_data.get("genre") if video_data else None
    merged["genre_override"] = rating.get("genre_override")
    merged["genre"] = rating.get("genre_override") or (video_data or {}).get("genre")
    return merged


def build_rating_update(doc_ref, user: dict, form) -> dict:
    doc = doc_ref.get()
    if not doc.exists:
        raise ValueError("Video not found.")
    video_data = doc.to_dict() or {}
    existing_rating = extract_user_rating(video_data, user)

    base_genre = (video_data.get("genre") or "").strip()
    submitted_genre = (form.get("genre") or "Unknown").strip()
    genre_override = None
    if submitted_genre:
        if not base_genre or submitted_genre.lower() != base_genre.lower():
            genre_override = submitted_genre

    rated_at = existing_rating.get("rated_at") or firestore.SERVER_TIMESTAMP
    rating_data = {
        "rating_music": coerce_int(form.get("rating_music", 3), 3),
        "rating_video": coerce_int(form.get("rating_video", 3), 3),
        "favorite": "favorite" in form,
        "rejected": "rejected" in form,
        "rated_at": rated_at,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    if genre_override:
        rating_data["genre_override"] = genre_override
    return rating_data


def get_filtered_videos_list(db, filters, user):
    """Reusable function to filter videos based on user-specific criteria."""
    try:
        candidate_videos = []
        docs = db.collection(COLLECTION_NAME).stream()
        for doc in docs:
            video_data = doc.to_dict() or {}
            video_data["video_id"] = doc.id
            rating = extract_user_rating(video_data, user)
            rated_at = rating.get("rated_at")
            rating_music = coerce_int(rating.get("rating_music", 3), 3)
            rating_video = coerce_int(rating.get("rating_video", 3), 3)
            favorite = bool(rating.get("favorite", False))
            rejected = bool(rating.get("rejected", False))
            effective_genre = rating.get("genre_override") or video_data.get("genre")

            if filters["exclude_rejected"] and rejected:
                continue
            if filters["favorite_only"] and not favorite:
                continue
            if filters["genre_filter"] != "All" and effective_genre != filters["genre_filter"]:
                continue

            if rated_at:
                if rating_music < filters["min_rating_music"]:
                    continue
                if rating_video < filters["min_rating_video"]:
                    continue
                candidate_videos.append(merge_user_rating(video_data, user))
            elif filters["include_unrated"]:
                candidate_videos.append(merge_user_rating(video_data, user))

        candidate_videos.sort(key=lambda x: (str(x.get("artist") or "").lower(), str(x.get("track") or "").lower()))
        return candidate_videos, None
    except Exception as e:
        print(f"An error occurred while fetching/filtering videos: {e}")
        return [], extract_index_error_info(e)


def normalize_playlist_id(raw: str) -> str:
    """Extract playlist ID from plain ID or URL."""
    value = (raw or "").strip()
    if not value:
        return ""
    if "://" in value:
        try:
            parsed = urlparse(value)
            qs = parse_qs(parsed.query)
            value = qs.get("list", [value])[-1]
        except Exception:
            pass
    return value


def normalize_video_id(raw: str) -> str:
    """Extract video ID from plain ID or typical YouTube URL."""
    value = (raw or "").strip()
    if not value:
        return ""
    if "youtu" in value:
        try:
            parsed = urlparse(value)
            if parsed.hostname and "youtu.be" in parsed.hostname:
                candidate = parsed.path.lstrip("/")
                if candidate:
                    return candidate
            qs = parse_qs(parsed.query)
            candidate = qs.get("v", [value])[-1]
            if candidate:
                return candidate
        except Exception:
            pass
    return value

# --- Auth Routes ---
@app.route('/login/google')
def login_google():
    if not google:
        print("⚠️  Google Login requested but 'google' client is not initialized.")
        return redirect(url_for('index'))

    ip = get_client_ip()
    ip_key = f"login_google:ip:{ip}"
    if rate_limit_exceeded(ip_key, LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW_SECONDS):
        return Response("Too many login attempts. Please try again later.", 429)
    rate_limit_hit(ip_key)
    
    # Check if we need to force account selection (if user is already logged in but unauthorized)
    extra_params = {}
    if session.get('user'):
        extra_params = {'prompt': 'select_account'}

    # Clear any existing session flags to ensure a fresh attempt
    session.pop('google_attempted', None)
    session.pop('prefer_legacy', None)
    session.pop('user', None)
    
    # Mark that we have attempted Google Auth to prevent infinite redirect loops
    # if the user fails auth and falls back to Basic Auth.
    session['google_attempted'] = True
    redirect_uri = url_for('auth_google_callback', _external=True)
    return google.authorize_redirect(redirect_uri, **extra_params)

@app.route('/google/callback')
def auth_google_callback():
    if not google:
        return redirect(url_for('index'))
    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')
        
        # Store email regardless of authorization status
        if user_info and user_info.get('email'):
            session['user'] = user_info['email']
            if resolve_google_user(user_info['email']):
                session.pop('google_attempted', None)
    except Exception as e:
        print(f"Google Auth Error: {e}")
    
    # Redirect to index. If auth failed, requires_auth will trigger Basic Auth.
    return redirect(url_for('index'))

@app.route('/login/legacy')
def login_legacy():
    # Clear Google attempt flag to allow Basic Auth fallback on protected routes
    session.pop('google_attempted', None)
    session['prefer_legacy'] = True
    session.pop('user', None)
    auth = request.authorization
    if auth:
        ip = get_client_ip()
        ip_key = f"login:ip:{ip}"
        user_key = f"login:user:{auth.username}"
        if rate_limit_exceeded(ip_key, LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW_SECONDS) or rate_limit_exceeded(
            user_key, LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW_SECONDS
        ):
            return Response("Too many login attempts. Please try again later.", 429)
    if auth and check_auth(auth.username, auth.password):
        return redirect(url_for('rating_mode'))
    if auth:
        rate_limit_hit(ip_key)
        rate_limit_hit(user_key)
    return authenticate()

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/privacy')
def privacy():
    return render_template('privacy.html', current_date=datetime.now().strftime("%Y-%m-%d"))

@app.route('/tos')
def tos():
    return render_template('tos.html', current_date=datetime.now().strftime("%Y-%m-%d"))

@app.route("/")
def index():
    """
    Landing page. Redirects to app if logged in, else shows login page.
    """
    if resolve_request_user():
        return redirect(url_for('rating_mode'))

    unauthorized_email = None
    if AUTH_GOOGLE and session.get('user'):
        if not resolve_google_user(session.get('user')):
            unauthorized_email = session.get('user')
        
    return render_template('login.html', google_enabled=bool(AUTH_GOOGLE), unauthorized_email=unauthorized_email)


@app.route("/rate", methods=['GET'])
@requires_auth
def rating_mode():
    """
    Presents a random, unrated music video to the user.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    user = current_user()
    index_error = None
    unrated_videos = []
    videos_left = 0

    # Fetch genres and unrated videos in one pass to avoid relying on null field queries.
    try:
        docs = db.collection(COLLECTION_NAME).get()
        unique_genres = set()
        for doc in docs:
            if not doc.exists:
                continue
            data = doc.to_dict() or {}
            data["video_id"] = doc.id
            if genre := data.get("genre"):
                unique_genres.add(genre)
            rating = extract_user_rating(data, user)
            if rating.get("genre_override"):
                unique_genres.add(rating.get("genre_override"))
            if not rating.get("rated_at"):
                unrated_videos.append(merge_user_rating(data, user))

        unique_genres.discard("Unknown")
        sorted_genres = sorted(list(unique_genres))
        videos_left = len(unrated_videos)
    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")
        index_error = extract_index_error_info(e)
        sorted_genres = []
        unrated_videos = []
        videos_left = 0

    if not unrated_videos:
        return render_template('rate.html', video=None, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS, videos_left=0, index_error=index_error)

    # Select a random video from the fetched list
    video = random.choice(unrated_videos)

    return render_template('rate.html', video=video, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS, videos_left=videos_left, index_error=index_error)


@app.route('/play', methods=['GET', 'POST'])
@requires_auth
@requires_csrf
def playing_mode():
    """
    Presents a random, filtered music video to the user, with editing capabilities.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    user = current_user()

    # --- Get Filter Criteria ---
    # Filters can come from a POST (submitting the filter form) or GET (direct link, or redirect after save)
    source = request.form if request.method == 'POST' else request.args

    min_rating_music = source.get('min_rating_music', 3, type=int)
    min_rating_video = source.get('min_rating_video', 3, type=int)
    genre_filter = source.get('genre_filter', 'All')

    if request.method == 'POST':
        favorite_only = 'favorite_only' in source
        include_unrated = 'include_unrated' in source
        exclude_rejected = 'exclude_rejected' in source
    else:
        favorite_only = source.get('favorite_only', 'false') == 'true'
        include_unrated = source.get('include_unrated', 'false') == 'true'
        exclude_rejected = source.get('exclude_rejected', 'true') == 'true'

    current_filters = {
        'min_rating_music': min_rating_music,
        'min_rating_video': min_rating_video,
        'genre_filter': genre_filter,
        'favorite_only': favorite_only,
        'include_unrated': include_unrated,
        'exclude_rejected': exclude_rejected,
    }

    filtered_videos, index_error = get_filtered_videos_list(db, current_filters, user)

    videos_count = len(filtered_videos)

    # Select video
    video = None
    selected_video_id = source.get('selected_video_id')

    if filtered_videos:
        if selected_video_id:
            video = next((v for v in filtered_videos if v.get('video_id') == selected_video_id), None)
        
        if not video:
            video = random.choice(filtered_videos)

    # --- Fetch All Genres for Dropdowns ---
    try:
        docs = db.collection(COLLECTION_NAME).get()
        unique_genres = set()
        for doc in docs:
            if not doc.exists:
                continue
            data = doc.to_dict() or {}
            if data.get("genre"):
                unique_genres.add(data.get("genre"))
            rating = extract_user_rating(data, user)
            if rating.get("genre_override"):
                unique_genres.add(rating.get("genre_override"))
        unique_genres.discard("Unknown")
        sorted_genres = sorted(list(unique_genres))
    except Exception as e:
        print(f"An error occurred while fetching genres: {e}")
        sorted_genres = []

    export_message = request.args.get('export_message')

    return render_template(
        'play.html',
        video=video,
        genres=sorted_genres,
        filters=current_filters,
        music_ratings=MUSIC_RATINGS,
        video_ratings=VIDEO_RATINGS,
        videos_count=videos_count,
        playlist=filtered_videos,
        export_message=export_message,
        index_error=index_error,
        google_client_id=GOOGLE_CLIENT_ID,
    )

@app.route("/admin")
@requires_auth
def admin_mode():
    """
    Secret admin page with database statistics.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    user = current_user()
    is_admin = user.get("role") == "admin"

    try:
        # Fetch all documents to calculate stats
        # Note: For very large collections, this might be slow and expensive.
        docs = list(db.collection(COLLECTION_NAME).stream())
        total_entries = len(docs)

        rated_count = 0
        favorite_count = 0
        rejected_count = 0
        genre_counts = {}

        for doc in docs:
            data = doc.to_dict()
            if not data:
                continue

            rating = extract_user_rating(data, user)
            if rating.get("rated_at"):
                rated_count += 1
            if rating.get("favorite"):
                favorite_count += 1
            if rating.get("rejected"):
                rejected_count += 1

            genre = rating.get("genre_override") or data.get("genre") or "Unknown"
            genre_counts[genre] = genre_counts.get(genre, 0) + 1

        def calc_pct(count, total):
            return round((count / total) * 100, 1) if total > 0 else 0.0

        stats = {
            'total_entries': total_entries,
            'rated_count': rated_count,
            'rated_pct': calc_pct(rated_count, total_entries),
            'favorite_count': favorite_count,
            'favorite_pct': calc_pct(favorite_count, total_entries),
            'rejected_count': rejected_count,
            'rejected_pct': calc_pct(rejected_count, total_entries),
            'genre_stats': []
        }

        # Sort genres by count descending
        sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)
        for genre, count in sorted_genres:
            stats['genre_stats'].append({
                'name': genre,
                'count': count,
                'pct': calc_pct(count, total_entries)
            })

        # Safeguard: Detect if running in cloud without cookies
        is_cloud = os.environ.get("K_SERVICE") is not None
        has_cookies = os.path.exists("cookies.txt")
        import_restricted = is_cloud and not has_cookies

        # Determine Authentication Status
        auth_status = f"{user.get('id')} ({user.get('role')})"

        import_requests = []
        if is_admin:
            try:
                req_docs = db.collection(IMPORT_REQUESTS_COLLECTION).where("status", "==", "pending").stream()
                for req in req_docs:
                    req_data = req.to_dict() or {}
                    req_data["id"] = req.id
                    import_requests.append(req_data)
                import_requests.sort(
                    key=lambda r: r.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True,
                )
                import_requests = import_requests[:50]
            except Exception as e:
                print(f"An error occurred while fetching import requests: {e}")

        users = []
        inactive_days = request.args.get("inactive_days", type=int)
        if is_admin:
            try:
                now = datetime.now(timezone.utc)
                # Get all videos to count ratings per user
                video_docs = list(db.collection(COLLECTION_NAME).stream())

                for doc in db.collection(USERS_COLLECTION).stream():
                    data = doc.to_dict() or {}
                    user_id = doc.id
                    last_seen = get_last_activity_ts(data)
                    inactive = False
                    if inactive_days and last_seen:
                        inactive = (now - last_seen).days >= inactive_days

                    # Count ratings for this user using the same logic as General Statistics
                    ratings_count = 0
                    # Create user object with rating_key for extract_user_rating
                    user_rating_key = data.get("rating_key") or rating_key_for_user_id(user_id)
                    user_obj = {"id": user_id, "rating_key": user_rating_key, "role": data.get("role") or "user"}
                    for video_doc in video_docs:
                        video_data = video_doc.to_dict()
                        if not video_data:
                            continue
                        rating = extract_user_rating(video_data, user_obj)
                        if rating.get("rated_at"):
                            ratings_count += 1

                    # Format date only (without time)
                    last_seen_date = last_seen.strftime("%Y-%m-%d") if last_seen else "Never"

                    users.append({
                        "id": user_id,
                        "role": data.get("role") or "user",
                        "ratings_count": ratings_count,
                        "last_seen": last_seen,
                        "last_seen_display": format_ts(last_seen),
                        "last_seen_date": last_seen_date,
                        "inactive": inactive,
                        "protected": is_protected_user(user_id, data, user.get("id")),
                    })
                users.sort(key=lambda u: (u["protected"], u["id"]))
            except Exception as e:
                print(f"An error occurred while fetching users: {e}")

        return render_template(
            'admin.html',
            stats=stats,
            import_restricted=import_restricted,
            auth_status=auth_status,
            user_id=user.get("id"),
            is_admin=is_admin,
            import_requests=import_requests,
            request_submitted=bool(request.args.get("request_submitted")),
            request_message=request.args.get("request_message"),
            users=users,
            user_message=request.args.get("user_message"),
            inactive_days=inactive_days or "",
        )

    except Exception as e:
        return f"An error occurred: {e}", 500


@app.route("/request-import", methods=["POST"])
@requires_auth
@requires_csrf
def request_import():
    """Allow non-admin users to suggest new imports for review."""
    if not db:
        return "Error: Firestore client not initialized.", 500

    user = current_user()
    request_type = (request.form.get("request_type") or "").strip()
    payload = (request.form.get("payload") or "").strip()
    notes = (request.form.get("notes") or "").strip()

    if request_type not in {"substack", "playlist", "video_ids"} or not payload:
        return redirect(url_for("admin_mode", request_message="Invalid import request."))

    if len(payload) > MAX_IMPORT_PAYLOAD_LEN:
        return redirect(url_for("admin_mode", request_message="Payload too large."))

    if len(notes) > MAX_IMPORT_NOTES_LEN:
        return redirect(url_for("admin_mode", request_message="Notes too large."))

    ip = get_client_ip()
    user_key = f"import_request:user:{user.get('id')}"
    ip_key = f"import_request:ip:{ip}"
    if rate_limit_exceeded(user_key, IMPORT_REQUEST_RATE_LIMIT, IMPORT_REQUEST_WINDOW_SECONDS) or rate_limit_exceeded(
        ip_key, IMPORT_REQUEST_RATE_LIMIT, IMPORT_REQUEST_WINDOW_SECONDS
    ):
        return redirect(url_for("admin_mode", request_message="Too many import requests. Please try again later."))

    if request_type == "substack":
        parsed = urlparse(payload)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return redirect(url_for("admin_mode", request_message="Invalid Substack URL."))
        payload = payload
    elif request_type == "playlist":
        playlist_id = normalize_playlist_id(payload)
        if not playlist_id or len(playlist_id) > 120:
            return redirect(url_for("admin_mode", request_message="Invalid playlist ID or URL."))
        payload = playlist_id
    elif request_type == "video_ids":
        raw_ids = payload.replace("\n", ",")
        ids = []
        for vid in raw_ids.split(","):
            norm = normalize_video_id(vid)
            if norm:
                ids.append(norm)
        if not ids:
            return redirect(url_for("admin_mode", request_message="No valid video IDs found."))
        if len(ids) > MAX_IMPORT_VIDEO_IDS:
            return redirect(url_for("admin_mode", request_message="Too many video IDs in one request."))
        for vid in ids:
            if not re.fullmatch(r"[A-Za-z0-9_-]{5,20}", vid):
                return redirect(url_for("admin_mode", request_message="Invalid video ID format."))
        payload = ", ".join(ids)

    rate_limit_hit(user_key)
    rate_limit_hit(ip_key)

    data = {
        "requested_by": user.get("id"),
        "request_type": request_type,
        "payload": payload,
        "notes": notes,
        "status": "pending",
        "created_at": firestore.SERVER_TIMESTAMP,
    }
    try:
        db.collection(IMPORT_REQUESTS_COLLECTION).add(data)
    except Exception as e:
        return f"An error occurred: {e}", 500

    return redirect(url_for("admin_mode", request_submitted="1"))


@app.route("/admin/import-request/<string:request_id>", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def update_import_request(request_id):
    """Update status for an import request."""
    if not db:
        return "Error: Firestore client not initialized.", 500

    user = current_user()
    status = (request.form.get("status") or "").strip()
    if status not in {"approved", "rejected", "imported"}:
        return "Invalid status.", 400

    try:
        db.collection(IMPORT_REQUESTS_COLLECTION).document(request_id).update({
            "status": status,
            "processed_by": user.get("id"),
            "processed_at": firestore.SERVER_TIMESTAMP,
        })
    except Exception as e:
        return f"An error occurred: {e}", 500

    return redirect(url_for("admin_mode"))


@app.route("/admin/users/delete", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def delete_user():
    """Delete a user and remove their ratings."""
    if not db:
        return "Error: Firestore client not initialized.", 500
    user = current_user()
    user_id = (request.form.get("user_id") or "").strip()
    if not user_id:
        return redirect(url_for("admin_mode", user_message="Missing user id."))

    user_doc = get_user_doc(user_id)
    if not user_doc:
        return redirect(url_for("admin_mode", user_message="User not found."))

    if is_protected_user(user_id, user_doc, user.get("id")):
        return redirect(url_for("admin_mode", user_message="Cannot delete protected user."))

    result = delete_user_and_ratings(user_id)
    if result.get("deleted"):
        msg = f"Deleted {user_id} and removed {result.get('ratings_removed', 0)} rating entries."
    else:
        msg = f"Failed to delete user {user_id}."
    return redirect(url_for("admin_mode", user_message=msg))


@app.route("/admin/users/self-delete", methods=["POST"])
@requires_auth
@requires_csrf
def delete_own_account():
    """Allow non-admin users to delete their own account and ratings."""
    if not db:
        return "Error: Firestore client not initialized.", 500
    user = current_user()
    if user.get("role") == "admin":
        return redirect(url_for("admin_mode", user_message="Admin account cannot be deleted."))

    user_id = user.get("id")
    if not user_id:
        return redirect(url_for("admin_mode", user_message="Missing user id."))

    result = delete_user_and_ratings(user_id)
    if result.get("deleted"):
        session.clear()
        return redirect(url_for("index"))

    return redirect(url_for("admin_mode", user_message="Failed to delete your account."))


@app.route("/admin/users/purge", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def purge_inactive_users():
    """Delete users inactive for N days and remove their ratings."""
    if not db:
        return "Error: Firestore client not initialized.", 500
    user = current_user()
    raw_days = (request.form.get("inactive_days") or "").strip()
    try:
        days = int(raw_days)
    except ValueError:
        days = 0

    if days <= 0:
        return redirect(url_for("admin_mode", user_message="Inactive days must be >= 1."))

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    deleted = 0
    removed = 0

    for doc in db.collection(USERS_COLLECTION).stream():
        data = doc.to_dict() or {}
        if is_protected_user(doc.id, data, user.get("id")):
            continue
        last_seen = get_last_activity_ts(data)
        if not last_seen:
            continue
        if last_seen <= cutoff:
            result = delete_user_and_ratings(doc.id)
            if result.get("deleted"):
                deleted += 1
                removed += result.get("ratings_removed", 0)

    msg = f"Deleted {deleted} inactive users; removed {removed} rating entries."
    return redirect(url_for("admin_mode", user_message=msg, inactive_days=days))
@app.route("/skip_video", methods=['POST'])
@requires_auth
@requires_csrf
def skip_video():
    """
    Skips the current video but preserves the filter settings.
    """
    # Re-apply filters for the next video by redirecting with query parameters
    filters = {
        'min_rating_music': request.form.get('min_rating_music_hidden'),
        'min_rating_video': request.form.get('min_rating_video_hidden'),
        'genre_filter': request.form.get('genre_filter_hidden'),
        'favorite_only': request.form.get('favorite_only_hidden'),
        'include_unrated': request.form.get('include_unrated_hidden'),
        'exclude_rejected': request.form.get('exclude_rejected_hidden')
    }
    return redirect(url_for('playing_mode', **{k: v for k, v in filters.items() if v is not None}))

@app.route("/save_rating/<string:video_id>", methods=['POST'])
@requires_auth
@requires_csrf
def save_rating(video_id):
    """
    Saves the user's rating and other attributes to the database.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    try:
        user = current_user()
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)
        rating_data = build_rating_update(doc_ref, user, request.form)
        doc_ref.update({f"ratings.{user.get('rating_key')}": rating_data})

    except Exception as e:
        return f"An error occurred: {e}", 500

    # Redirect to the next video to rate
    return redirect(url_for('rating_mode'))

@app.route("/save_play_rating/<string:video_id>", methods=['POST'])
@requires_auth
@requires_csrf
def save_play_rating(video_id):
    """
    Saves ratings from the play mode and redirects back to play mode
    with the same filters.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    try:
        user = current_user()
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)
        rating_data = build_rating_update(doc_ref, user, request.form)
        doc_ref.update({f"ratings.{user.get('rating_key')}": rating_data})
    except Exception as e:
        return f"An error occurred: {e}", 500

    # Re-apply filters for the next video by redirecting with query parameters
    filters = {
        'min_rating_music': request.form.get('min_rating_music_hidden'),
        'min_rating_video': request.form.get('min_rating_video_hidden'),
        'genre_filter': request.form.get('genre_filter_hidden'),
        'favorite_only': request.form.get('favorite_only_hidden'),
        'include_unrated': request.form.get('include_unrated_hidden'),
        'exclude_rejected': request.form.get('exclude_rejected_hidden')
    }
    return redirect(url_for('playing_mode', **{k: v for k, v in filters.items() if v is not None}))

@app.route("/api/youtube_info")
@requires_auth
@requires_admin
def api_youtube_info():
    """Returns the connected YouTube channel and list of playlists."""
    yt = ingestion_get_youtube_service()
    if not yt:
        return {"error": "Not authenticated. Please run scrape scripts locally once to generate token.pickle."}
    
    try:
        # Get Channel Info
        channels_response = yt.channels().list(mine=True, part="snippet").execute()
        channel_title = channels_response["items"][0]["snippet"]["title"] if channels_response["items"] else "Unknown"

        # Get Playlists
        playlists = []
        request_pl = yt.playlists().list(mine=True, part="snippet,id", maxResults=50)
        while request_pl:
            response = request_pl.execute()
            for item in response.get("items", []):
                playlists.append({"id": item["id"], "title": item["snippet"]["title"]})
            request_pl = yt.playlists().list_next(request_pl, response)
            
        return {"channel": channel_title, "playlists": playlists}
    except Exception as e:
        return {"error": str(e)}

@app.route("/export_playlist", methods=['POST'])
@requires_auth
@requires_admin
@requires_csrf
def export_playlist():
    """Exports filtered videos to a YouTube playlist."""
    user = current_user()
    filters = {
        'min_rating_music': request.form.get('min_rating_music', 3, type=int),
        'min_rating_video': request.form.get('min_rating_video', 3, type=int),
        'genre_filter': request.form.get('genre_filter', 'All'),
        'favorite_only': 'favorite_only' in request.form,
        'include_unrated': 'include_unrated' in request.form,
        'exclude_rejected': 'exclude_rejected' in request.form,
    }
    
    videos, index_error = get_filtered_videos_list(db, filters, user)
    if index_error:
        return redirect(url_for('playing_mode', export_message="Error: Firestore index missing or building. Please open the page to see details.", **filters))
    yt = ingestion_get_youtube_service()
    if not yt:
        return redirect(url_for('playing_mode', export_message="Error: YouTube API not connected.", **filters))
        
    target_playlist_id = request.form.get('playlist_id')
    new_playlist_name = request.form.get('new_playlist_name')
    
    added_count = 0
    skipped_count = 0
    error_msg = None

    try:
        if new_playlist_name:
            res = yt.playlists().insert(part="snippet,status", body={
                "snippet": {"title": new_playlist_name},
                "status": {"privacyStatus": "private"}
            }).execute()
            target_playlist_id = res["id"]
            
        if not target_playlist_id:
            return redirect(url_for('playing_mode', export_message="Error: No playlist selected.", **filters))

        # Fetch existing videos to avoid duplicates
        existing_ids = set()
        if not new_playlist_name:
            req = yt.playlistItems().list(playlistId=target_playlist_id, part="snippet", maxResults=50)
            while req:
                resp = req.execute()
                for item in resp.get("items", []):
                    existing_ids.add(item["snippet"]["resourceId"]["videoId"])
                req = yt.playlistItems().list_next(req, resp)

        for v in videos:
            vid = v['video_id']
            if vid in existing_ids:
                skipped_count += 1
                continue
            try:
                yt.playlistItems().insert(part="snippet", body={
                    "snippet": {"playlistId": target_playlist_id, "resourceId": {"kind": "youtube#video", "videoId": vid}}
                }).execute()
                added_count += 1
                existing_ids.add(vid)
            except HttpError as e:
                if "quotaExceeded" in str(e):
                    error_msg = "YouTube API quota exceeded."
                    break
    except HttpError as e:
        if e.resp.status == 403 and "insufficientPermissions" in str(e):
            error_msg = "Error: Insufficient permissions. Please delete token.pickle and re-run scrape_to_firestore.py locally."
        else:
            error_msg = f"YouTube API Error: {str(e)}"
    except Exception as e:
        error_msg = f"Error: {str(e)}"

    msg = f"Exported {added_count} videos. {skipped_count} skipped (duplicates)."
    if error_msg:
        msg += f" Stopped: {error_msg}"
        
    return redirect(url_for('playing_mode', export_message=msg, **filters))

@app.route("/admin/import-playlist", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def import_playlist():
    """Import videos from a YouTube playlist into Firestore."""
    raw_playlist = (request.form.get("playlist_id") or "").strip()
    playlist_id = normalize_playlist_id(raw_playlist)
    limit = coerce_int(request.form.get("limit"), 0)

    def generate():
        if not playlist_id:
            yield "❌ Missing playlist_id query parameter.\n"
            return

        yt = ingestion_get_youtube_service()
        if not yt:
            yield "❌ YouTube API not connected. Run the scrape script locally once to generate token.pickle.\n"
            return

        yield f"📥 Fetching playlist {playlist_id}...\n"
        try:
            ids = fetch_playlist_video_ids(yt, playlist_id)
        except Exception as e:
            yield f"❌ Error fetching playlist: {e}\n"
            return

        if not ids:
            yield "⚠️ No videos found in playlist or playlist is private.\n"
            return

        # Initialize AI model for consistency
        model = init_ai_model(db.project)

        if limit and limit > 0:
            ids = ids[:limit]
            yield f"ℹ️ Limiting to first {limit} entries.\n"

        total = len(ids)
        yield f"✅ Found {total} unique videos. Starting import...\n"

        added = exists = unavailable = errors = 0
        source = f"https://www.youtube.com/playlist?list={playlist_id}"
        for idx, vid in enumerate(ids, start=1):
            result = ingest_single_video(
                db,
                yt,
                vid,
                source=source,
                model=model,
                model_name=AI_MODEL_NAME,
            )
            status = result.get("status")
            title = result.get("title") or ""
            if status == "added":
                added += 1
                yield f"[{idx}/{total}] ✅ Added {vid} {title}\n"
            elif status == "exists":
                exists += 1
                yield f"[{idx}/{total}] ↩️  Already in DB {vid}\n"
            elif status == "unavailable":
                unavailable += 1
                yield f"[{idx}/{total}] ⚠️  Unavailable/private {vid}\n"
            else:
                errors += 1
                yield f"[{idx}/{total}] ❌ Error {vid}: {result.get('message')}\n"

        yield f"\nSummary: {added} added, {exists} existing, {unavailable} unavailable, {errors} errors.\n"

    return Response(stream_with_context(generate()), mimetype="text/plain")

@app.route("/admin/import-video", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def import_video():
    """Import one or more comma-separated YouTube video IDs into Firestore."""
    raw_ids = (request.form.get("video_ids") or "").replace("\n", ",")
    ids = []
    for vid in raw_ids.split(","):
        norm = normalize_video_id(vid)
        if norm:
            ids.append(norm)

    def generate():
        if not ids:
            yield "❌ Provide video_ids (comma-separated) in the query parameters.\n"
            return

        yt = ingestion_get_youtube_service()
        if not yt:
            yield "❌ YouTube API not connected. Run the scrape script locally once to generate token.pickle.\n"
            return

        total = len(ids)
        yield f"✅ Received {total} video ID(s). Starting import...\n"
        source = "manual-import"
        added = exists = unavailable = errors = 0
        
        # Initialize AI model for consistency
        model = init_ai_model(db.project)

        for idx, vid in enumerate(ids, start=1):
            result = ingest_single_video(
                db,
                yt,
                vid,
                source=source,
                model=model,
                model_name=AI_MODEL_NAME,
            )
            status = result.get("status")
            title = result.get("title") or ""
            if status == "added":
                added += 1
                yield f"[{idx}/{total}] ✅ Added {vid} {title}\n"
            elif status == "exists":
                exists += 1
                yield f"[{idx}/{total}] ↩️  Already in DB {vid}\n"
            elif status == "unavailable":
                unavailable += 1
                yield f"[{idx}/{total}] ⚠️  Unavailable/private {vid}\n"
            else:
                errors += 1
                yield f"[{idx}/{total}] ❌ Error {vid}: {result.get('message')}\n"

        yield f"\nSummary: {added} added, {exists} existing, {unavailable} unavailable, {errors} errors.\n"

    return Response(stream_with_context(generate()), mimetype="text/plain")

@app.route("/admin/run-scraper", methods=["POST"])
@requires_auth
@requires_admin
@requires_csrf
def run_scraper():
    """
    Executes the scrape_to_firestore.py script as a subprocess and streams the output.
    """
    limit_posts = request.form.get('limit_posts', '0')
    limit_entries = request.form.get('limit_entries', '10')
    substack_url = request.form.get('substack_url', 'https://goodmusic.substack.com/archive')
    
    def generate():
        # Construct the command. 
        # sys.executable ensures we use the same Python interpreter (virtualenv) as the web app.
        # -u forces unbuffered binary stdout/stderr, allowing real-time streaming.
        cmd = [
            sys.executable, '-u', 'scrape_to_firestore.py',
            '--project', db.project,
            '--limit-substack-posts', limit_posts,
            '--limit-new-db-entries', limit_entries,
            '--substack', substack_url
        ]
        
        yield f"🚀 Executing command: {' '.join(cmd)}\n\n"
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, # Merge stderr into stdout
                text=True,
                bufsize=1 # Line buffered
            )
            
            # Read output line by line and yield to the browser
            for line in iter(process.stdout.readline, ''):
                yield line
                
            process.stdout.close()
            return_code = process.wait()
            
            if return_code != 0:
                yield f"\n❌ Script exited with error code {return_code}"
            else:
                yield "\n✅ Script finished successfully."
                
        except Exception as e:
            yield f"\n❌ Error executing script: {str(e)}"

    return Response(stream_with_context(generate()), mimetype='text/plain')

if __name__ == "__main__":
    # Use PORT environment variable for Cloud Run
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
