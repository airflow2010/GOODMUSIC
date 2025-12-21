import sys
import time

# Google Cloud imports
from google.cloud import firestore

# Local application imports
from ingestion import (
    AI_MODEL_NAME,
    COLLECTION_NAME,
    get_video_metadata,
    get_youtube_service,
    init_ai_model,
    init_firestore_db,
    predict_genre,
)

# --- Configuration ---
# Configure the preferred AI model for genre classification.
# The script will re-evaluate any document that was last evaluated with a different model.
PREFERRED_AI_MODEL = AI_MODEL_NAME


def ask_for_confirmation(prompt: str) -> bool:
    """Asks the user for a yes/no confirmation."""
    while True:
        response = input(f"{prompt} (y/n): ").lower().strip()
        if response in ["y", "yes"]:
            return True
        if response in ["n", "no"]:
            return False
        print("Invalid input. Please enter 'y' or 'n'.")


def main():
    """
    Iterates through music videos in Firestore and re-evaluates the genre
    if the AI model used previously differs from the configured PREFERRED_AI_MODEL.
    """
    print("🚀 Starting Genre Update Script...")
    print(f"   - Preferred AI Model: {PREFERRED_AI_MODEL}")
    print(f"   - Target Collection: {COLLECTION_NAME}")
    print("-" * 50)

    # 1. Initialize services
    db = init_firestore_db()
    if not db:
        print("❌ Could not connect to Firestore. Exiting.", file=sys.stderr)
        sys.exit(1)
    print(f"✅ Connected to Firestore project: {db.project}")

    model = init_ai_model(db.project)
    if not model:
        print("❌ Could not initialize AI Model. Exiting.", file=sys.stderr)
        sys.exit(1)
    print("✅ Initialized AI Model client.")

    youtube = get_youtube_service()
    if not youtube:
        print("⚠️ Could not authenticate with YouTube API. Will proceed without fetching fresh metadata.")
    else:
        print("✅ Connected to YouTube API.")

    print("-" * 50)

    # 2. Fetch all documents from the collection
    try:
        docs = db.collection(COLLECTION_NAME).stream()
        all_docs = list(docs)
        total_docs = len(all_docs)
        print(f"🔍 Found {total_docs} documents to check.")
    except Exception as e:
        print(f"❌ Failed to fetch documents from Firestore: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Iterate and process each document
    for i, doc in enumerate(all_docs):
        video_id = doc.id
        doc_data = doc.to_dict()

        old_ai_model = doc_data.get("ai_model", "unknown")
        old_genre = doc_data.get("genre", "N/A")
        old_remark = doc_data.get("genre_ai_remarks", "N/A")

        print(f"\n[{i+1}/{total_docs}] Processing Video: {video_id} ({doc_data.get('title', 'No Title')})")

        # 4. Check if re-evaluation is needed
        if old_ai_model == PREFERRED_AI_MODEL:
            print(f"   ✅ Video {video_id} already evaluated with {PREFERRED_AI_MODEL}. Skipping.")
            continue

        print(f"   🔄 Model mismatch ('{old_ai_model}' vs '{PREFERRED_AI_MODEL}'). Re-evaluating genre...")

        # 5. Re-evaluate using the "ID + Metadata + Audio" scenario
        metadata = get_video_metadata(youtube, video_id)
        if metadata is None:
            print(f"   ⚠️ Could not fetch metadata for {video_id}. It might be private or deleted. Skipping.")
            continue

        title, description, _ = metadata
        print(f"   - Fetching audio and calling AI for '{title}'...")

        # This function handles audio download, AI call, and cleanup
        prediction = predict_genre(model, video_id, title, description)

        if prediction is None:
            print(f"   ❌ AI prediction failed for {video_id} (e.g., audio download failed). Skipping.")
            continue

        new_genre, new_fidelity, new_remark, new_artist, new_track = prediction

        print("   🤖 AI evaluation complete.")

        # Always show the link for easy verification
        print(f"   - Link: https://www.youtube.com/watch?v={video_id}")

        # 6. Evaluate the result and ask for confirmation
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)
        updates = {}

        if new_genre == old_genre:
            print(f"   - ✅ {PREFERRED_AI_MODEL} resulted in the same genre-classification as {old_ai_model}: {new_genre}.")
            print(f"   - old remark: {old_remark}")
            print(f"   - new remark: {new_remark}")

            if ask_for_confirmation("   - Should the remark and AI model info be updated?"):
                updates = {
                    "genre_ai_remarks": new_remark,
                    "genre_ai_fidelity": new_fidelity,
                    "ai_model": PREFERRED_AI_MODEL,
                    "artist": new_artist,
                    "track": new_track,
                }
        else:  # Genre is different
            print("   - ❗ Genre classified differently!")
            print(f"   - From: '{old_genre}'")
            print(f"   - To:   '{new_genre}'")
            print("-" * 20)
            print(f"   - old remark: {old_remark}")
            print(f"   - new remark: {new_remark}")
            print("-" * 20)

            if ask_for_confirmation(f"   - Update genre to '{new_genre}' and save new details?"):
                updates = {
                    "genre": new_genre,
                    "genre_ai_remarks": new_remark,
                    "genre_ai_fidelity": new_fidelity,
                    "ai_model": PREFERRED_AI_MODEL,
                    "artist": new_artist,
                    "track": new_track,
                }

        if updates:
            try:
                doc_ref.update(updates)
                print(f"   💾 Document {video_id} updated successfully.")
            except Exception as e:
                print(f"   ❌ Failed to update document {video_id}: {e}", file=sys.stderr)
        else:
            print("   - No changes made.")

        # Small delay to avoid hitting API rate limits too hard
        time.sleep(1)

    print("\n🎉 Script finished.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Script interrupted by user. Exiting.")
        sys.exit(0)