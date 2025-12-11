import os
import random
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, Response
from google.cloud import firestore

# --- Configuration ---
# Get the project ID from the environment variable.
# This is set automatically when running on Google Cloud.
PROJECT_ID = os.environ.get("GCP_PROJECT")
if not PROJECT_ID:
    # For local development, you can set this manually.
    # Ensure you have run `gcloud auth application-default login`
    PROJECT_ID = "goodmusic-470520"

COLLECTION_NAME = "musicvideos"

# --- Authentication Configuration ---
AUTH_USERNAME = os.environ.get("AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "changeme")

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Firestore Client Initialization ---
try:
    db = firestore.Client(project=PROJECT_ID)
except Exception as e:
    print(f"Error initializing Firestore client: {e}")
    db = None


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
    query = db.collection(COLLECTION_NAME).where(filter=firestore.FieldFilter("musical_value", "==", 0)).limit(20).stream()
    
    unrated_videos = [doc.to_dict() for doc in query]

    if not unrated_videos:
        return "No unrated videos found!"

    # Select a random video from the fetched list
    video = random.choice(unrated_videos)

    return render_template('rating.html', video=video, genres=sorted_genres)


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

    min_musical_value = source.get('min_musical_value', 1, type=int)
    min_video_value = source.get('min_video_value', 1, type=int)
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

        # --- Query 1: Get RATED videos that meet the criteria ---
        # This query uses multiple range filters (>=), which requires a composite index.
        rated_query = base_query.where(
            filter=firestore.FieldFilter("musical_value", ">=", min_musical_value)
        ).where(
            filter=firestore.FieldFilter("video_value", ">=", min_video_value)
        )
        rated_docs = rated_query.stream()
        candidate_videos.extend([doc.to_dict() for doc in rated_docs])

        # --- Query 2: Get UNRATED videos, if requested ---
        if include_unrated:
            unrated_query = base_query.where(filter=firestore.FieldFilter("musical_value", "==", 0))
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
        'min_musical_value': min_musical_value,
        'min_video_value': min_video_value,
        'genre_filter': genre_filter,
        'favorite_only': favorite_only,
        'include_unrated': include_unrated,
        'exclude_rejected': exclude_rejected,
    }

    return render_template('play.html', video=video, genres=sorted_genres, filters=current_filters)

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
            'musical_value': int(request.form.get('musical_value', 0)),
            'video_value': int(request.form.get('video_value', 0)),
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
            'musical_value': int(request.form.get('musical_value', 0)),
            'video_value': int(request.form.get('video_value', 0)),
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
        'min_musical_value': request.form.get('min_musical_value_hidden'),
        'min_video_value': request.form.get('min_video_value_hidden'),
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
