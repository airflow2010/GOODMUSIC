import argparse
import json
import os
import pickle
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

import google.auth
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import yt_dlp

TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]

AI_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
]

def init_ai_model(project_id: str, location: str, model_name: str, credentials=None):
    """Initializes and returns the Vertex AI model using the centralized model name."""
    try:
        vertexai.init(project=project_id, location=location, credentials=credentials)
        return GenerativeModel(model_name)
    except Exception as e:
        print(f"⚠️ Vertex AI Init Error for {model_name}: {e}")
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
    """Fetches video title, description and upload date from YouTube API to help Gemini."""
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
    """Downloads the audio of a YouTube video to a temporary file for AI analysis."""
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

    # Attempt to use cookies if available to bypass bot detection (especially on Cloud Run)
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


def predict_genre(model, video_id: str, video_title: str = None, video_description: str = None, audio_path: str = None) -> Optional[tuple[str, int, str, str, str]]:
    """Uses Gemini to predict genre, confidence, reasoning, artist, and track."""
    if not model:
        return "Unknown", 0, "AI model not available.", "", ""

    allowed_genres = [
        "Avant-garde & experimental",
        "Blues",
        "Classical",
        "Country",
        "Easy listening",
        "Electronic",
        "Folk",
        "Hip hop",
        "Jazz",
        "Pop",
        "R&B & soul",
        "Rock",
        "Metal",
        "Punk",
    ]

    parts = []

    prompt_text = f"Categorize the music genre of the song with YouTube Video ID '{video_id}'"
    if video_title:
        prompt_text += f", Title '{video_title}'"
    if video_description:
        prompt_text += f", Description '{video_description}'"

    prompt_parts = [prompt_text]

    if audio_path:
        prompt_parts.append(". Please analyze the audio of the YouTube video (listen to the rhythm, instrumentation, and vocals) to determine the genre.")
        try:
            with open(audio_path, "rb") as f:
                audio_data = f.read()
            parts.append(Part.from_data(data=audio_data, mime_type="audio/mp4"))
        except Exception as e:
            return "Unknown", 0, f"Error reading audio file: {e}", "", ""
    else:
        prompt_parts.append(". Please analyze the video based on the provided information (ID and/or Metadata). If the ID is invalid or you don't recognize it, return Unknown.")

    # 2) Extra protection layer in prompt
    prompt_parts.append(
        "\n\nYour response must be a JSON object with the following keys:\n"
        f'1. "genre": A string. Choose ONE of the following allowed genres: {", ".join(allowed_genres)}. '
        'If the genre cannot be determined reliably, or if the video ID is invalid/unknown, use "Unknown".\n'
        '2. "fidelity": Integer 0-100 for confidence.\n'
        '3. "remarks": Short reasoning.\n'
        '4. "artist": Artist or band name.\n'
        '5. "track": Song title.\n'
        'IMPORTANT: Do not hallucinate. If you cannot identify the video or determine the genre/artist/track from the provided information (ID, metadata, or audio), return "Unknown" and empty strings. Do not make up a genre if you are unsure.'
    )

    parts.append("".join(prompt_parts))

    try:
        response = model.generate_content(parts)
        text = response.text or ""
        import html as htmllib

        text = text.strip()
        if text.startswith("```json"):
            text = text.strip("` \n")
            text = text.replace("json", "", 1).strip()
        parsed = json.loads(htmllib.unescape(text))
        genre = parsed.get("genre", "Unknown") or "Unknown"
        if not isinstance(genre, str):
            genre = "Unknown"
        genre = genre.strip()
        if genre not in allowed_genres and genre != "Unknown":
            genre = "Unknown"

        raw_fidelity = parsed.get("fidelity", 0)
        fidelity = int(raw_fidelity) if isinstance(raw_fidelity, (int, float)) else 0
        fidelity = max(0, min(100, fidelity))

        remarks = parsed.get("remarks", "")
        artist = parsed.get("artist", "")
        track = parsed.get("track", "")
        if not isinstance(remarks, str):
            remarks = ""
        if not isinstance(artist, str):
            artist = ""
        if not isinstance(track, str):
            track = ""
        return genre, fidelity, remarks, artist, track
    except Exception as e:
        return "Unknown", 0, str(e), "", ""


def main():
    parser = argparse.ArgumentParser(description="Test AI models for music genre categorization.")
    parser.add_argument("video_id", help="YouTube Video ID")
    parser.add_argument("--project", help="Google Cloud Project ID")
    parser.add_argument("--location", default="europe-west4", help="Vertex AI Location")
    args = parser.parse_args()

    # Auth
    try:
        creds, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if args.project:
            project_id = args.project
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        sys.exit(1)

    print(f"🚀 Testing AI Models for Video ID: {args.video_id}")
    print(f"   Project: {project_id}")
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
            print("   ⚠️ Could not fetch video metadata (video might be private/deleted).")
    else:
        print("   ⚠️ YouTube API not available. Proceeding without title.")

    # Download Audio
    print("   Downloading audio for analysis...")
    audio_path = download_audio_for_analysis(args.video_id)
    if audio_path:
        print(f"   ✅ Audio downloaded to {audio_path}")
    else:
        print("   ⚠️ Audio download failed. 'ID + Metadata + Audio' scenario will be skipped or limited.")

    print("-" * 60)
    
    results = []
    
    scenarios = [
        ("ID Only", False, False),
        ("ID + Metadata", True, False),
        ("ID + Metadata + Audio", True, True)
    ]

    for model_name in AI_MODELS:
        print(f"\n🤖 Testing Model: {model_name}...")
        
        model = init_ai_model(project_id, args.location, model_name, creds)
        if not model:
            results.append({"model": model_name, "scenario": "All", "error": "Init failed"})
            continue

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
            
            # Prepare args
            t = title if use_meta else None
            d = description if use_meta else None
            a = audio_path if use_audio else None
            
            genre, fidelity, remarks, artist, track = predict_genre(model, args.video_id, t, d, a)
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
            print(f"      ✅ Done in {duration:.2f}s")

    # Cleanup audio
    if audio_path and os.path.exists(audio_path):
        try:
            os.remove(audio_path)
        except OSError:
            pass

    # Output Comparison
    print("\n" + "=" * 120)
    print(f"{'Model':<25} | {'Scenario':<22} | {'Genre':<20} | {'Fid.':<4} | {'Artist - Track'}")
    print("-" * 120)
    for res in results:
        if "error" in res:
             print(f"{res['model']:<25} | {res['scenario']:<22} | ERROR: {res['error']}")
        else:
            artist_track = f"{res['artist']} - {res['track']}"
            if len(artist_track) > 30:
                artist_track = artist_track[:27] + "..."
            print(f"{res['model']:<25} | {res['scenario']:<22} | {res['genre']:<20} | {res['fidelity']:<4} | {artist_track}")
            print(f"   Reasoning: {res['remarks']}")
            print(f"   Time: {res['duration']}")
            print("-" * 120)

    # Summary Table
    print("\n" + "=" * 100)
    print(f"{'SUMMARY TABLE (Genre)':^100}")
    print("-" * 100)

    scenarios_list = ["ID Only", "ID + Metadata", "ID + Metadata + Audio"]
    header = f"{'Model':<25}"
    for s in scenarios_list:
        header += f" | {s:<22}"
    print(header)
    print("-" * len(header))

    for model_name in AI_MODELS:
        row_str = f"{model_name:<25}"
        global_error = next((r for r in results if r["model"] == model_name and r.get("scenario") == "All"), None)
        for s in scenarios_list:
            if global_error:
                val = "Init Failed"
            else:
                res = next((r for r in results if r["model"] == model_name and r.get("scenario") == s), None)
                if res:
                    if "error" in res:
                        val = "ERROR"
                    else:
                        val = res.get("genre", "Unknown")
                else:
                    val = "-"
            row_str += f" | {val:<22}"
        print(row_str)

if __name__ == "__main__":
    main()
