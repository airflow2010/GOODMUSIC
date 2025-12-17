import os
import sys
import subprocess
import random
from functools import wraps
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from flask import Flask, render_template, request, redirect, url_for, Response, stream_with_context
from google.cloud import firestore
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from ingestion import fetch_playlist_video_ids, get_youtube_service as ingestion_get_youtube_service, ingest_single_video

load_dotenv()

# --- Configuration ---
# Get the project ID from the environment variable.
# This is set automatically when running on Google Cloud.
PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
COLLECTION_NAME = "musicvideos"

# --- Authentication Configuration ---
AUTH_USERNAME = os.environ.get("AUTH_USERNAME")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD")

def check_prerequisites():
    """Checks for prerequisites for successful deployment and refuses to start if not met."""
    missing_vars = []
    if not PROJECT_ID:
        missing_vars.append("PROJECT_ID (or GCP_PROJECT)")
    if not AUTH_USERNAME:
        missing_vars.append("AUTH_USERNAME")
    if not AUTH_PASSWORD:
        missing_vars.append("AUTH_PASSWORD")

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
            print('   --set-secrets="AUTH_USERNAME=prism-auth-username:latest,AUTH_PASSWORD=prism-auth-password:latest,PROJECT_ID=prism-auth-projectid:latest"')
        else:
            print("   You appear to be running locally.")
            print("   Ensure you have a .env file or exported environment variables.")
        sys.exit(1)

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

    print(f"✅ Configuration loaded for Project ID: {PROJECT_ID}")

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

# --- Firestore Client Initialization ---
try:
    db = firestore.Client(project=PROJECT_ID)
except Exception as e:
    print("\n" + "!" * 60)
    print("❌ STARTUP ERROR: Could not connect to Firestore.")
    print("!" * 60)
    print(f"Error details: {e}")
    print("\nINSTRUCTIONS:")
    print("   1. Check if the Google Cloud Project ID is correct.")
    print("   2. Ensure the Service Account has 'Cloud Datastore User' or 'Firestore User' role.")
    print("   3. If running locally, check your 'gcloud auth application-default login' credentials.")
    sys.exit(1)

# Verify collection access
check_firestore_access(db)

# --- Auth Decorator ---
def check_auth(username, password):
    """Checks if the username and password are correct."""
    return username == AUTH_USERNAME and password == AUTH_PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth."""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

# --- Helper Functions ---
def get_filtered_videos_list(db, filters):
    """Reusable function to query and filter videos based on criteria."""
    try:
        base_query = db.collection(COLLECTION_NAME)
        if filters['exclude_rejected']:
            base_query = base_query.where(filter=firestore.FieldFilter("rejected", "==", False))
        if filters['genre_filter'] != 'All':
            base_query = base_query.where(filter=firestore.FieldFilter("genre", "==", filters['genre_filter']))
        if filters['favorite_only']:
            base_query = base_query.where(filter=firestore.FieldFilter("favorite", "==", True))

        candidate_videos = []

        # Query 1: Rated
        rated_query = base_query.where(
            filter=firestore.FieldFilter("rating_music", ">=", filters['min_rating_music'])
        )
        rated_docs = rated_query.stream()
        for doc in rated_docs:
            video_data = doc.to_dict()
            if video_data.get("date_rated") and video_data.get("rating_video", 0) >= filters['min_rating_video']:
                candidate_videos.append(video_data)

        # Query 2: Unrated
        if filters['include_unrated']:
            unrated_query = base_query.where(filter=firestore.FieldFilter("date_rated", "==", None))
            unrated_docs = unrated_query.stream()
            candidate_videos.extend([doc.to_dict() for doc in unrated_docs])

        candidate_videos.sort(key=lambda x: (str(x.get('artist') or '').lower(), str(x.get('track') or '').lower()))
        return candidate_videos
    except Exception as e:
        print(f"An error occurred while fetching/filtering videos: {e}")
        return []


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

@app.route("/")
@requires_auth
def index():
    """
    Redirects the root URL to the rating page.
    """
    return redirect(url_for('rating_mode'))


@app.route("/rate", methods=['GET'])
@requires_auth
def rating_mode():
    """
    Presents a random, unrated music video to the user.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    # Fetch all unique genres for the dropdown
    try:
        # Use .get() to fetch all documents at once. This can be more reliable
        # for smaller collections than using a stream.
        docs = db.collection(COLLECTION_NAME).get()
        unique_genres = set()
        for doc in docs:
            if doc.exists:
                data = doc.to_dict()
                if data and (genre := data.get('genre')):
                    unique_genres.add(genre)

        unique_genres.discard("Unknown")
        sorted_genres = sorted(list(unique_genres))
    except Exception as e:
        print(f"An error occurred while fetching genres: {e}")
        sorted_genres = []

    # Query for unrated videos (where date_rated is None)
    query = db.collection(COLLECTION_NAME).where(filter=firestore.FieldFilter("date_rated", "==", None)).limit(20).stream()
    
    unrated_videos = [doc.to_dict() for doc in query]

    # --- Get total count of unrated videos ---
    videos_left = 0
    try:
        # Use a projection query to count unrated videos.
        # We select no fields (keys only) to minimize cost and latency.
        # This avoids the limitations of count() with null filters.
        docs = db.collection(COLLECTION_NAME).where("date_rated", "==", None).select([]).stream()
        videos_left = sum(1 for _ in docs)
    except Exception as e:
        print(f"Error counting videos for 'rate' page: {e}")
        videos_left = "N/A" # Display an error indicator

    if not unrated_videos:
        return render_template('rate.html', video=None, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS, videos_left=0)

    # Select a random video from the fetched list
    video = random.choice(unrated_videos)

    return render_template('rate.html', video=video, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS, videos_left=videos_left)


@app.route('/play', methods=['GET', 'POST'])
@requires_auth
def playing_mode():
    """
    Presents a random, filtered music video to the user, with editing capabilities.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

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

    filtered_videos = get_filtered_videos_list(db, current_filters)

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
        unique_genres = {doc.to_dict().get('genre') for doc in docs if doc.exists and doc.to_dict().get('genre')}
        unique_genres.discard("Unknown")
        sorted_genres = sorted(list(unique_genres))
    except Exception as e:
        print(f"An error occurred while fetching genres: {e}")
        sorted_genres = []

    export_message = request.args.get('export_message')

    return render_template('play.html', video=video, genres=sorted_genres, filters=current_filters, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS, videos_count=videos_count, playlist=filtered_videos, export_message=export_message)

@app.route("/admin")
@requires_auth
def admin_mode():
    """
    Secret admin page with database statistics.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

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
            
            if data.get('date_rated'):
                rated_count += 1
            if data.get('favorite'):
                favorite_count += 1
            if data.get('rejected'):
                rejected_count += 1
            
            genre = data.get('genre') or 'Unknown'
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

        return render_template('admin.html', stats=stats, import_restricted=import_restricted)

    except Exception as e:
        return f"An error occurred: {e}", 500

@app.route("/skip_video", methods=['POST'])
@requires_auth
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
def save_rating(video_id):
    """
    Saves the user's rating and other attributes to the database.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    try:
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)

        # Prepare the data to update
        update_data = {
            'rating_music': int(request.form.get('rating_music', 3)),
            'rating_video': int(request.form.get('rating_video', 3)),
            'genre': request.form.get('genre', 'Unknown'),
            'favorite': 'favorite' in request.form,
            'rejected': 'rejected' in request.form,
            'date_rated': firestore.SERVER_TIMESTAMP
        }

        doc_ref.update(update_data)

    except Exception as e:
        return f"An error occurred: {e}", 500

    # Redirect to the next video to rate
    return redirect(url_for('rating_mode'))

@app.route("/save_play_rating/<string:video_id>", methods=['POST'])
@requires_auth
def save_play_rating(video_id):
    """
    Saves ratings from the play mode and redirects back to play mode
    with the same filters.
    """
    if not db:
        return "Error: Firestore client not initialized.", 500

    try:
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)
        update_data = {
            'rating_music': int(request.form.get('rating_music', 3)),
            'rating_video': int(request.form.get('rating_video', 3)),
            'genre': request.form.get('genre', 'Unknown'),
            'favorite': 'favorite' in request.form,
            'rejected': 'rejected' in request.form,
            'date_rated': firestore.SERVER_TIMESTAMP
        }
        doc_ref.update(update_data)
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
def export_playlist():
    """Exports filtered videos to a YouTube playlist."""
    filters = {
        'min_rating_music': request.form.get('min_rating_music', 3, type=int),
        'min_rating_video': request.form.get('min_rating_video', 3, type=int),
        'genre_filter': request.form.get('genre_filter', 'All'),
        'favorite_only': 'favorite_only' in request.form,
        'include_unrated': 'include_unrated' in request.form,
        'exclude_rejected': 'exclude_rejected' in request.form,
    }
    
    videos = get_filtered_videos_list(db, filters)
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

@app.route("/admin/import-playlist")
@requires_auth
def import_playlist():
    """Import videos from a YouTube playlist into Firestore."""
    raw_playlist = (request.args.get("playlist_id") or "").strip()
    playlist_id = normalize_playlist_id(raw_playlist)
    limit = request.args.get("limit", default=0, type=int)

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
                model=None,  # UI import skips AI to keep UI lightweight
                model_name="ui",
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

@app.route("/admin/import-video")
@requires_auth
def import_video():
    """Import one or more comma-separated YouTube video IDs into Firestore."""
    raw_ids = (request.args.get("video_ids") or "").replace("\n", ",")
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
        for idx, vid in enumerate(ids, start=1):
            result = ingest_single_video(
                db,
                yt,
                vid,
                source=source,
                model=None,
                model_name="ui",
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

@app.route("/admin/run-scraper")
@requires_auth
def run_scraper():
    """
    Executes the scrape_to_firestore.py script as a subprocess and streams the output.
    """
    limit_posts = request.args.get('limit_posts', '0')
    limit_entries = request.args.get('limit_entries', '10')
    substack_url = request.args.get('substack_url', 'https://goodmusic.substack.com/archive')
    
    def generate():
        # Construct the command. 
        # sys.executable ensures we use the same Python interpreter (virtualenv) as the web app.
        # -u forces unbuffered binary stdout/stderr, allowing real-time streaming.
        cmd = [
            sys.executable, '-u', 'scrape_to_firestore.py',
            '--project', PROJECT_ID,
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
