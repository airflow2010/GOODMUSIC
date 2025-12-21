import argparse
import json
import os
import pickle
import sys
import time
from datetime import datetime, timezone
from typing import Optional, List

# --- GOOGLE GENAI SDK ---
from google import genai
from google.genai import types

# --- PYDANTIC (For Structured Outputs) ---
from pydantic import BaseModel, Field

# --- YOUTUBE & AUTH ---
import google.auth
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# --- ADDED: SECRET MANAGER & DOTENV ---
from google.cloud import secretmanager
import google.auth.exceptions
from dotenv import load_dotenv

# Load environment variables immediately
load_dotenv()

from ingestion import download_audio_for_analysis

TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]

AI_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
    "gemini-3-pro-preview",
]

# ---------------------------------------------------------------------------
#  DATA MODELS (Structured Output Schema)
# ---------------------------------------------------------------------------

class MusicAnalysis(BaseModel):
    genre: str = Field(description="The music genre. Must be one of the allowed genres or 'Unknown'.")
    fidelity: int = Field(description="Confidence score between 0 and 100.")
    remarks: str = Field(description="Short reasoning (1-2 sentences) for the classification.")
    artist: str = Field(description="The name of the artist, or empty string if unknown.")
    track: str = Field(description="The name of the track, or empty string if unknown.")

# ---------------------------------------------------------------------------
#  HELPER FUNCTIONS (YouTube, Audio, Time)
# ---------------------------------------------------------------------------

# --- ADDED: SECRET MANAGER HELPER ---
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
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_file, "wb") as token:
                    pickle.dump(creds, token)
            except Exception:
                return None
        else:
            if not os.path.exists(client_secrets_file):
                return None
            from google_auth_oauthlib.flow import InstalledAppFlow

            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, YOUTUBE_SCOPES)
                creds = flow.run_local_server(port=0)
                with open(token_file, "wb") as token:
                    pickle.dump(creds, token)
            except Exception:
                return None

    return build("youtube", "v3", credentials=creds)

def get_video_metadata(youtube, video_id: str) -> tuple[str, str, datetime | None] | None:
    """Fetches video title, description and upload date from YouTube API."""
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

# ---------------------------------------------------------------------------
#  AI LOGIC (Updated for Structured Outputs)
# ---------------------------------------------------------------------------

def predict_genre(client: genai.Client, model_name: str, video_id: Optional[str], video_title: str = None, video_description: str = None, audio_path: str = None) -> Optional[tuple[str, int, str, str, str]]:
    """Uses Gemini to predict genre using strict Structured Output enforcement."""
    
    allowed_genres = [
        "Avant-garde & experimental", "Blues", "Classical", "Country",
        "Easy listening", "Electronic", "Folk", "Hip hop",
        "Jazz", "Pop", "R&B & soul", "Rock", "Metal", "Punk",
    ]

    # 1. Construct the Prompt
    prompt_text = "Categorize the music genre of the song"
    if video_id:
        prompt_text += f" with YouTube Video ID '{video_id}'"
    if video_title: prompt_text += f", Title '{video_title}'"
    if video_description: prompt_text += f", Description '{video_description}'"
    
    # Specific instructions
    instruction_text = (
        f'\n\nFor "genre", select ONE of {", ".join(allowed_genres)}. Use "Unknown" if unsure.\n'
        'IMPORTANT: Do not hallucinate. If you don\'t know, return "Unknown".'
    )

    contents = []
    
    # Add Audio if available
    if audio_path:
        try:
            with open(audio_path, "rb") as f:
                audio_bytes = f.read()
            contents.append(types.Part.from_bytes(data=audio_bytes, mime_type="audio/mp4"))
            prompt_text += ". Analyze the audio rhythm, instrumentation, and vocals."
        except Exception as e:
            return "Unknown", 0, f"Audio read error: {e}", "", ""
    else:
        prompt_text += ". Analyze based on ID and metadata only."

    contents.append(types.Part.from_text(text=prompt_text + instruction_text))

    # 2. Configure Thinking & Schema
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=MusicAnalysis  # <--- Strictly enforces the Pydantic model
    )
    
    if "gemini-3" in model_name:
        config.thinking_config = types.ThinkingConfig(
            include_thoughts=True,
            thinking_level="HIGH"
        )

    response = None
    max_retries = 3
    retry_delay = 10

    for attempt in range(max_retries + 1):
        try:
            # 3. Generate Content
            response = client.models.generate_content(
                model=model_name,
                contents=contents,
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
            return "Unknown", 0, f"API Error: {str(e)}", "", ""

    try:
        # 4. Extract "Thoughts" (Gemini 3 specific)
        thoughts_text = ""
        if response.candidates and response.candidates[0].content.parts:
            for part in response.candidates[0].content.parts:
                if getattr(part, 'thought', False):
                     thoughts_text += f"[Internal Thought]: {part.text}\n"

        # 5. Parse JSON (Now strictly guaranteed by Schema)
        # Using built-in SDK parsing to Pydantic object if available, or standard JSON load
        # response.parsed is available in newer SDKs when schema is used, but manual load is safer across versions
        text = response.text.strip()
        
        # Clean markdown code blocks just in case (though schema usually prevents them)
        if text.startswith("```json"):
            text = text.replace("```json", "").replace("```", "").strip()
        elif text.startswith("```"):
            text = text.replace("```", "").strip()
            
        parsed_dict = json.loads(text)
        
        # Verify it matches our Pydantic model (extra validation layer)
        analysis = MusicAnalysis(**parsed_dict)

        genre = analysis.genre
        # Post-process genre to ensure it's in our allowed list
        if genre not in allowed_genres and genre != "Unknown":
            genre = "Unknown"

        fidelity = analysis.fidelity
        
        remarks = analysis.remarks

        return genre, fidelity, remarks, analysis.artist, analysis.track

    except Exception as e:
        return "Unknown", 0, f"Processing Error: {str(e)}", "", ""

# ---------------------------------------------------------------------------
#  MAIN LOOP
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Test AI models for music genre categorization.")
    parser.add_argument("video_id", help="YouTube Video ID")
    parser.add_argument("--project", default="goodmusic-470520", help="Google Cloud Project ID")
    # --- ADDED: Arg for secret name ---
    parser.add_argument("--secret_name", default="GOOGLE_API_KEY", help="Name of the secret in Secret Manager")
    parser.add_argument("--location", default="global", help="Vertex AI Location") # Kept to avoid breaking existing args, though unused for AI Studio
    args = parser.parse_args()

    print(f"🚀 Testing AI Models for Video ID: {args.video_id}")
    print(f"   Project: {args.project}")
    print(f"   Location: {args.location}")

    # --- CHANGED: API Key Logic (Env -> Secret -> Fail) ---
    api_key = os.environ.get("GOOGLE_API_KEY")

    if not api_key:
        print(f"   🔑 Env var not found. Fetching secret '{args.secret_name}' from project '{args.project}'...")
        api_key = get_gcp_secret(args.secret_name, args.project)

    if not api_key:
        print("❌ Error: Could not find API Key in environment or Secret Manager.")
        sys.exit(1)

    # Init GenAI Client with API Key
    try:
        client = genai.Client(api_key=api_key)
        print("✅ GenAI Client initialized successfully (AI Studio Mode).")
    except Exception as e:
        print(f"❌ Client Init Error: {e}")
        sys.exit(1)

    # YouTube Metadata
    youtube = get_youtube_service()
    if not youtube:
        print("❌ YouTube API not available. Aborting test.")
        print("   Please run `rm token.pickle` and re-run the script to re-authenticate.")
        sys.exit(1)

    title = ""
    description = ""
    metadata = get_video_metadata(youtube, args.video_id)
    if metadata:
        title, description, _ = metadata
        print(f"   Video Title: {title}")
    else:
        print("   ⚠️ Could not fetch video metadata.")

    # Download Audio
    print("   Downloading audio for analysis...")
    audio_path = download_audio_for_analysis(args.video_id)
    if audio_path:
        print(f"   ✅ Audio downloaded to {audio_path}")
    else:
        print("   ⚠️ Audio download failed.")

    print("-" * 60)
    
    results = []
    
    scenarios = [
        # ("ID Only", False, False),
        ("ID + Metadata", True, False),
        ("ID + Metadata + Audio", True, True),
        ("Audio Only", False, True),
    ]

    for model_name in AI_MODELS:
        print(f"\n🤖 Testing Model: {model_name}...")
        
        for scenario_name, use_meta, use_audio in scenarios:
            print(f"   👉 Scenario: {scenario_name}")
            
            if use_audio and not audio_path:
                results.append({
                    "model": model_name,
                    "scenario": scenario_name,
                    "error": "Audio not available"
                })
                continue

            start_time = time.time()
            
            t = title if use_meta else None
            d = description if use_meta else None
            a = audio_path if use_audio else None
            
            vid_arg = args.video_id
            if scenario_name == "Audio Only":
                vid_arg = None

            genre, fidelity, remarks, artist, track = predict_genre(client, model_name, vid_arg, t, d, a)
            duration = time.time() - start_time
            
            results.append({
                "model": model_name,
                "scenario": scenario_name,
                "genre": genre,
                "fidelity": fidelity,
                "remarks": remarks,
                "artist": artist,
                "track": track,
                "duration": f"{duration:.2f}s"
            })
            print(f"      ✅ Done in {duration:.2f}s (Artist: {artist}, Genre: {genre})")
            print(f"      📝 Remarks: {remarks}")

    # Cleanup
    if audio_path and os.path.exists(audio_path):
        os.remove(audio_path)

    # Summary Table
    print("\n" + "=" * 100)
    print(f"{'SUMMARY TABLE (Genre)':^100}")
    print("-" * 100)
    
    header = f"{'Model':<25}"
    for s in [x[0] for x in scenarios]: header += f" | {s:<22}"
    print(header)
    print("-" * len(header))

    for model_name in AI_MODELS:
        row_str = f"{model_name:<25}"
        for s in [x[0] for x in scenarios]:
            res = next((r for r in results if r["model"] == model_name and r.get("scenario") == s), None)
            val = res.get("genre", "Unknown") if res and "error" not in res else "-"
            row_str += f" | {val:<22}"
        print(row_str)

if __name__ == "__main__":
    main()