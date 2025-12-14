#!/usr/bin/env python3
import argparse
import json
import time
import sys
import google.auth
from google.cloud import firestore
import vertexai
from vertexai.generative_models import GenerativeModel

# ====== Configuration ======
COLLECTION_NAME = "musicvideos"
ADC_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
MODEL_NAME = "gemini-2.5-flash"

def predict_artist_track(model, video_id: str, video_title: str) -> tuple[str, str]:
    """Uses Gemini to predict artist and track based on metadata."""
    if not model:
        return "", ""
    
    prompt_parts = [
        f"Identify the artist and track name for the YouTube video with ID '{video_id}'"
    ]
    if video_title:
        prompt_parts.append(f" and Title '{video_title}'")
    
    prompt_parts.append("\n\nYour response must be a JSON object with the following keys:")
    prompt_parts.append('1. "artist": A string containing the name of the artist or band.')
    prompt_parts.append('2. "track": A string containing the name of the song or track.')
    prompt_parts.append('\nExample response:\n{\n  "artist": "The Beatles",\n  "track": "Hey Jude"\n}')
    
    prompt_text = "".join(prompt_parts)

    try:
        response = model.generate_content(prompt_text)
        cleaned_text = response.text.strip()
        # Remove markdown code blocks if present
        if cleaned_text.startswith("```json"):
            cleaned_text = cleaned_text[7:]
        elif cleaned_text.startswith("```"):
            cleaned_text = cleaned_text[3:]
        if cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[:-3]
        
        data = json.loads(cleaned_text)
        artist = data.get("artist", "")
        track = data.get("track", "")
        
        return artist, track

    except Exception as e:
        print(f"      ⚠️ AI Error: {e}")
        return "", ""

def main():
    parser = argparse.ArgumentParser(description="Backfill missing fields in Firestore.")
    parser.add_argument("--project", help="Google Cloud Project ID")
    args = parser.parse_args()

    # 1. Auth & Clients
    try:
        creds, project_id = google.auth.default(scopes=ADC_SCOPES)
        if args.project:
            project_id = args.project
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        sys.exit(1)
    
    print(f"🚀 Initializing for Project: {project_id}")
    
    # Firestore
    try:
        db = firestore.Client(project=project_id, credentials=creds)
    except Exception as e:
        print(f"❌ Firestore Init Error: {e}")
        sys.exit(1)
    
    # Vertex AI
    try:
        vertexai.init(project=project_id, location="europe-west4", credentials=creds)
        model = GenerativeModel(MODEL_NAME)
    except Exception as e:
        print(f"⚠️ Vertex AI Init Error: {e}")
        model = None
        sys.exit(1)

    print(f"🔍 Scanning collection '{COLLECTION_NAME}' for documents with missing fields...")
    
    docs = db.collection(COLLECTION_NAME).stream()
    
    count_processed = 0
    count_updated = 0
    
    for doc in docs:
        count_processed += 1
        data = doc.to_dict()
        video_id = doc.id
        title = data.get("title", "")
        
        # Check for missing fields
        missing_artist = "artist" not in data
        missing_track = "track" not in data
        missing_ai_model = "ai_model" not in data
        
        if not (missing_artist or missing_track or missing_ai_model):
            continue

        print(f"📝 Processing Document ID: {video_id}")
        print(f"   Current Title: {title}")

        updates = {}
        
        # 1. Backfill ai_model
        if missing_ai_model:
             updates["ai_model"] = MODEL_NAME
        
        # 2. Backfill artist/track using AI
        if missing_artist or missing_track:
            print("   🤖 Calling Vertex AI for artist/track...")
            artist, track = predict_artist_track(model, video_id, title)
            print(f"      AI Determined Artist: {artist}")
            print(f"      AI Determined Track: {track}")
            
            if missing_artist and artist:
                updates["artist"] = artist
            if missing_track and track:
                updates["track"] = track
        
        # 3. Apply updates
        if updates:
            try:
                db.collection(COLLECTION_NAME).document(video_id).update(updates)
                print("   ✅ Document updated successfully.")
                count_updated += 1
            except Exception as e:
                print(f"   ❌ Error updating document: {e}")
        else:
            print("   ⚠️ No updates applied (AI might have returned empty strings).")
        
        print("-" * 40)
        # Rate limit to be nice to APIs
        time.sleep(0.2)

    print(f"\n🎉 Finished. Processed {count_processed} documents. Updated {count_updated} documents.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user. Exiting gracefully.")
        sys.exit(0)