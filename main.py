import os
import random
from flask import Flask, render_template, request, redirect, url_for
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

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Firestore Client Initialization ---
try:
    db = firestore.Client(project=PROJECT_ID)
except Exception as e:
    print(f"Error initializing Firestore client: {e}")
    db = None


@app.route("/")
def index():
    """
    Redirects the root URL to the rating page.
    """
    return redirect(url_for('rating_mode'))


@app.route("/rate", methods=['GET'])
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
        include_unrated = source.get('include_unrated', 'true') == 'true'
        exclude_rejected = source.get('exclude_rejected', 'true') == 'true'

    # --- Fetch and Filter Videos ---
    try:
        # Base query
        query = db.collection(COLLECTION_NAME)
        
        if exclude_rejected:
            query = query.where(filter=firestore.FieldFilter("rejected", "==", False))
            
        docs = query.stream()
        all_videos = [doc.to_dict() for doc in docs]

        # Apply filters in Python
        filtered_videos = []
        for v in all_videos:
            if genre_filter != 'All' and v.get('genre') != genre_filter:
                continue
            if favorite_only and not v.get('favorite'):
                continue
            
            musical_val = v.get('musical_value', 0)
            video_val = v.get('video_value', 0)
            is_unrated = (musical_val == 0)
            
            if is_unrated:
                if not include_unrated:
                    continue
            elif musical_val < min_musical_value or video_val < min_video_value:
                continue
                
            filtered_videos.append(v)
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
    # For local development:
    # To run this, use: `python main.py`
    # And open your browser to http://127.0.0.1:8080
    app.run(host="127.0.0.1", port=8080, debug=True)
