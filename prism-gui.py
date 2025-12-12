import os
import sys
import random
from functools import wraps

from flask import Flask, render_template, request, redirect, url_for, Response
from google.cloud import firestore
from dotenv import load_dotenv

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

    # Query for unrated videos (where musical_value is 0)
    query = db.collection(COLLECTION_NAME).where(filter=firestore.FieldFilter("date_rated", "==", None)).limit(20).stream()
    
    unrated_videos = [doc.to_dict() for doc in query]

    if not unrated_videos:
        return render_template('rate.html', video=None, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS)

    # Select a random video from the fetched list
    video = random.choice(unrated_videos)

    return render_template('rate.html', video=video, genres=sorted_genres, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS)


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

    min_rating_music = source.get('min_rating_music', 1, type=int)
    min_rating_video = source.get('min_rating_video', 1, type=int)
    genre_filter = source.get('genre_filter', 'All')

    if request.method == 'POST':
        favorite_only = 'favorite_only' in source
        include_unrated = 'include_unrated' in source
        exclude_rejected = 'exclude_rejected' in source
    else:
        favorite_only = source.get('favorite_only', 'false') == 'true'
        include_unrated = source.get('include_unrated', 'false') == 'true'
        exclude_rejected = source.get('exclude_rejected', 'true') == 'true'

    # --- Fetch and Filter Videos ---
    try:
        # Build a base query with filters that apply to both rated and unrated videos
        base_query = db.collection(COLLECTION_NAME)
        if exclude_rejected:
            base_query = base_query.where(filter=firestore.FieldFilter("rejected", "==", False))
        if genre_filter != 'All':
            base_query = base_query.where(filter=firestore.FieldFilter("genre", "==", genre_filter))
        if favorite_only:
            base_query = base_query.where(filter=firestore.FieldFilter("favorite", "==", True))

        candidate_videos = []

        # --- Query 1: Get RATED videos that might meet the criteria ---
        # Firestore only allows one range filter per query. We filter on music
        # rating here and will filter on video rating in Python.
        # We also can't combine a '!=' with a range filter, so we check for
        # rated status in Python as well.
        rated_query = base_query.where(
            filter=firestore.FieldFilter("rating_music", ">=", min_rating_music)
        )
        rated_docs = rated_query.stream()

        # Client-side filtering for the remaining conditions
        for doc in rated_docs:
            video_data = doc.to_dict()
            if video_data.get("date_rated") and video_data.get("rating_video", 0) >= min_rating_video:
                candidate_videos.append(video_data)

        # --- Query 2: Get UNRATED videos, if requested ---
        if include_unrated:
            # This query only has equality filters from base_query and one for date_rated
            unrated_query = base_query.where(filter=firestore.FieldFilter("date_rated", "==", None))
            unrated_docs = unrated_query.stream()
            candidate_videos.extend([doc.to_dict() for doc in unrated_docs])

        filtered_videos = candidate_videos

    except Exception as e:
        print(f"An error occurred while fetching/filtering videos: {e}")
        filtered_videos = []

    # Select a random video from the filtered list
    video = None
    if filtered_videos:
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

    current_filters = {
        'min_rating_music': min_rating_music,
        'min_rating_video': min_rating_video,
        'genre_filter': genre_filter,
        'favorite_only': favorite_only,
        'include_unrated': include_unrated,
        'exclude_rejected': exclude_rejected,
    }

    return render_template('play.html', video=video, genres=sorted_genres, filters=current_filters, music_ratings=MUSIC_RATINGS, video_ratings=VIDEO_RATINGS)

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

        return render_template('admin.html', stats=stats)

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

if __name__ == "__main__":
    # Use PORT environment variable for Cloud Run
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
