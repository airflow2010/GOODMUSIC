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


if __name__ == "__main__":
    # For local development:
    # To run this, use: `python main.py`
    # And open your browser to http://127.0.0.1:8080
    app.run(host="127.0.0.1", port=8080, debug=True)
