import argparse
import json
import os
import pickle
import sys
import time
from datetime import datetime, timezone
from typing import Optional, List

# --- GOOGLE GENAI SDK (The new standard) ---
from google import genai
from google.genai import types

# --- YOUTUBE & AUTH ---
import google.auth
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import yt_dlp

TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]

# Updated Model List
AI_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
    "gemini-3-pro-preview",
]

# ---------------------------------------------------------------------------
#  HELPER FUNCTIONS (YouTube, Audio, Time)
# ---------------------------------------------------------------------------

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

def download_audio_for_analysis(video_id: str) -> str | None:
    """Downloads the audio of a YouTube video to a temporary file."""
    output_path = f"/tmp/{video_id}.m4a"

    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except OSError:
            pass

    ydl_opts = {
        "format": "bestaudio[ext=m4a]/bestaudio",
        "outtmpl": output_path,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "max_filesize": 25 * 1024 * 1024,
    }

    if os.path.exists("cookies.txt"):
        ydl_opts["cookiefile"] = "cookies.txt"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

        if os.path.exists(output_path):
            return output_path
    except Exception:
        return None

    return None

# ---------------------------------------------------------------------------
#  AI LOGIC (Updated for google-genai SDK)
# ---------------------------------------------------------------------------

def predict_genre(client: genai.Client, model_name: str, video_id: str, video_title: str = None, video_description: str = None, audio_path: str = None) -> Optional[tuple[str, int, str, str, str]]:
    """Uses Gemini (2.5 or 3.0) to predict genre, confidence, reasoning, artist, and track."""
    
    allowed_genres = [
        "Avant-garde & experimental", "Blues", "Classical", "Country",
        "Easy listening", "Electronic", "Folk", "Hip hop",
        "Jazz", "Pop", "R&B & soul", "Rock", "Metal", "Punk",
    ]

    # 1. Construct the Prompt (Multimodal)
    prompt_text = f"Categorize the music genre of the song with YouTube Video ID '{video_id}'"
    if video_title: prompt_text += f", Title '{video_title}'"
    if video_description: prompt_text += f", Description '{video_description}'"
    
    # Add instructions
    instruction_text = (
        "\n\nReturn a JSON object with the following keys:\n"
        f'1. "genre": ONE of {", ".join(allowed_genres)}. Use "Unknown" if unsure.\n'
        '2. "fidelity": Integer 0-100.\n'
        '3. "remarks": Short reasoning.\n'
        '4. "artist": Artist name.\n'
        '5. "track": Song title.\n'
        'IMPORTANT: Do not hallucinate. If you analyze audio, describe the instruments in the remarks.'
    )

    contents = []
    
    # If audio exists, add it first (standard best practice for Gemini)
    if audio_path:
        try:
            with open(audio_path, "rb") as f:
                audio_bytes = f.read()
            # New SDK syntax for bytes
            contents.append(types.Part.from_bytes(data=audio_bytes, mime_type="audio/mp4"))
            prompt_text += ". Analyze the audio rhythm, instrumentation, and vocals."
        except Exception as e:
            return "Unknown", 0, f"Audio read error: {e}", "", ""
    else:
        prompt_text += ". Analyze based on ID and metadata only."

    contents.append(types.Part.from_text(text=prompt_text + instruction_text))

    # 2. Configure Thinking (Only for Gemini 3)
    # Using 'types.GenerateContentConfig' is the safe way in the new SDK
    config = types.GenerateContentConfig(
        response_mime_type="application/json" # Enforce JSON output mode
    )
    
    if "gemini-3" in model_name:
        config.thinking_config = types.ThinkingConfig(
            include_thoughts=True,
            thinking_level="HIGH"
        )

    try:
        # 3. Generate Content
        response = client.models.generate_content(
            model=model_name,
            contents=contents,
            config=config
        )
        
        # 4. Extract "Thoughts" (Gemini 3 specific)
        thoughts_text = ""
        # The new SDK parses candidates differently. We look for thought parts.
        if response.candidates and response.candidates[0].content.parts:
            for part in response.candidates[0].content.parts:
                # Check for thought attribute or text that looks like a thought
                if getattr(part, 'thought', False):
                     thoughts_text += f"[Internal Thought]: {part.text}\n"

        # 5. Parse JSON
        text = response.text.strip()
        # Clean markdown code blocks if present
        if text.startswith("```json"):
            text = text.replace("```json", "").replace("```", "").strip()
        elif text.startswith("```"):
            text = text.replace("```", "").strip()
            
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # Fallback if strict JSON mode failed
            return "Unknown", 0, f"JSON Parse Error. Raw: {text[:50]}...", "", ""

        genre = parsed.get("genre", "Unknown")
        if genre not in allowed_genres and genre != "Unknown":
            genre = "Unknown"

        fidelity = int(parsed.get("fidelity", 0))
        
        # Combine internal thoughts with the model's final remarks for better debugging
        remarks = parsed.get("remarks", "")
        if thoughts_text:
            # Prepend thoughts to remarks for visibility
            remarks = f"{thoughts_text.strip()} || Final: {remarks}"

        artist = parsed.get("artist", "")
        track = parsed.get("track", "")

        return genre, fidelity, remarks, artist, track

    except Exception as e:
        return "Unknown", 0, f"API Error: {str(e)}", "", ""

# ---------------------------------------------------------------------------
#  MAIN LOOP
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Test AI models for music genre categorization.")
    parser.add_argument("video_id", help="YouTube Video ID")
    parser.add_argument("--project", default="goodmusic-470520", help="Google Cloud Project ID")
    # Defaulting to 'global' is safer for Gemini 3
    parser.add_argument("--location", default="global", help="Vertex AI Location (default: global)")
    args = parser.parse_args()

    # Init GenAI Client (Single client for all calls)
    try:
        client = genai.Client(vertexai=True, project=args.project, location=args.location)
    except Exception as e:
        print(f"❌ Client Init Error: {e}")
        sys.exit(1)

    print(f"🚀 Testing AI Models for Video ID: {args.video_id}")
    print(f"   Project: {args.project}")
    print(f"   Location: {args.location}")

    # YouTube Metadata
    youtube = get_youtube_service()
    title = ""
    description = ""
    if youtube:
        metadata = get_video_metadata(youtube, args.video_id)
        if metadata:
            title, description, _ = metadata
            print(f"   Video Title: {title}")
        else:
            print("   ⚠️ Could not fetch video metadata.")
    else:
        print("   ⚠️ YouTube API not available.")

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
        ("ID Only", False, False),
        ("ID + Metadata", True, False),
        ("ID + Metadata + Audio", True, True)
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
            
            genre, fidelity, remarks, artist, track = predict_genre(client, model_name, args.video_id, t, d, a)
            duration = time.time() - start_time
            
            # Shorten remarks for display if they contain long thoughts
            display_remarks = remarks
            if len(display_remarks) > 100:
                display_remarks = display_remarks[:97] + "..."

            results.append({
                "model": model_name,
                "scenario": scenario_name,
                "genre": genre,
                "fidelity": fidelity,
                "remarks": remarks, # Store full remarks
                "display_remarks": display_remarks, # Store short remarks
                "artist": artist,
                "track": track,
                "duration": f"{duration:.2f}s"
            })
            print(f"      ✅ Done in {duration:.2f}s")

    # Cleanup
    if audio_path and os.path.exists(audio_path):
        os.remove(audio_path)

    # Summary Table
    print("\n" + "=" * 120)
    print(f"{'Model':<25} | {'Scenario':<22} | {'Genre':<20} | {'Fid.':<4} | {'Reasoning'}")
    print("-" * 120)
    for res in results:
        if "error" in res:
             print(f"{res['model']:<25} | {res['scenario']:<22} | ERROR: {res['error']}")
        else:
            print(f"{res['model']:<25} | {res['scenario']:<22} | {res['genre']:<20} | {res['fidelity']:<4} | {res['display_remarks']}")
    
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