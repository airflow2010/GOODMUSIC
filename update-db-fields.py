#!/usr/bin/env python3
import argparse
import json
import time
import sys
import google.auth
from google.cloud import firestore
from google.genai import types
from pydantic import BaseModel, Field
from ingestion import init_ai_model, init_firestore_db, AI_MODEL_NAME

# ====== Configuration ======
COLLECTION_NAME = "musicvideos"
MODEL_NAME = AI_MODEL_NAME

class ArtistTrack(BaseModel):
    artist: str = Field(description="The name of the artist or band.")
    track: str = Field(description="The name of the song or track.")

def predict_artist_track(client, video_id: str, video_title: str) -> tuple[str, str]:
    """Uses Gemini to predict artist and track using Structured Outputs."""
    if not client:
        return "", ""
    
    prompt_parts = [
        f"Identify the artist and track name for the YouTube video with ID '{video_id}'"
    ]
    if video_title:
        prompt_parts.append(f" and Title '{video_title}'")
    
    prompt_parts.append("\nIMPORTANT: Do not hallucinate. If unknown, return empty strings.")
    
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=ArtistTrack
    )

    # Retry logic for quota issues
    response = None
    max_retries = 3
    retry_delay = 10

    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents="".join(prompt_parts),
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
            print(f"      ⚠️ AI Error: {e}")
            return "", ""

    try:
        cleaned_text = response.text.strip()
        # Remove markdown code blocks if present
        if cleaned_text.startswith("```json"):
            cleaned_text = cleaned_text[7:]
        elif cleaned_text.startswith("```"):
            cleaned_text = cleaned_text[3:]
        if cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[:-3]
        
        data = json.loads(cleaned_text)
        result = ArtistTrack(**data)
        
        return result.artist, result.track

    except Exception as e:
        print(f"      ⚠️ AI Error: {e}")
        return "", ""

def main():
    parser = argparse.ArgumentParser(description="Backfill missing fields in Firestore.")
    parser.add_argument("--project", help="Google Cloud Project ID")
    args = parser.parse_args()

    print(f"🚀 Initializing for Project: {args.project or 'Default'}")
    
    # 1. Initialize Firestore
    db = init_firestore_db(args.project)
    if not db:
        print("❌ Failed to initialize Firestore.")
        sys.exit(1)
    
    # 2. Initialize AI
    # Note: We pass db.project to ensure AI uses the same project context if needed for secrets
    model = init_ai_model(db.project)
    if not model:
        print("❌ Failed to initialize AI model.")
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