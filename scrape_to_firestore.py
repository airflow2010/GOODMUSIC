#!/usr/bin/env python3
import argparse
import html as htmllib
import os
import re
import time
import pickle
import sys
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup

# Google Cloud imports
import google.auth
from google.cloud import firestore
from google.api_core import exceptions
import vertexai
from vertexai.generative_models import GenerativeModel

# YouTube API
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

# ====== Configuration ======
COLLECTION_NAME = "musicvideos"
YOUTUBE_ID_RE = r"[A-Za-z0-9_-]{11}"
# ADC Scopes (for Firestore & Vertex AI)
ADC_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform"
]

# YouTube OAuth Scopes
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"

# ====== HTML Parsing (Reused from your script) ======
def extract_title_from_html_text(html_text: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.find("h1", class_=re.compile("post-title"))
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    m = re.search(r"<title>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    return htmllib.unescape(m.group(1)).strip() if m else None

def extract_video_ids_from_html_text(html_text: str) -> list[str]:
    ids_in_order: List[str] = []
    
    # Pattern 1: id="youtube2-..."
    ids_in_order += re.findall(fr'id\s*=\s*"youtube2-({YOUTUBE_ID_RE})"', html_text)

    # Pattern 2: data-attrs with videoId
    for m in re.finditer(r'data-attrs\s*=\s*"(.*?)"', html_text, flags=re.IGNORECASE | re.DOTALL):
        unescaped = htmllib.unescape(m.group(1))
        mid = re.search(fr'"videoId"\s*:\s*"({YOUTUBE_ID_RE})"', unescaped)
        if mid:
            ids_in_order.append(mid.group(1))

    # Pattern 3: iframe src
    ids_in_order += re.findall(
        fr'<iframe[^>]+src\s*=\s*"[^"]*youtube(?:-nocookie)?\.com/embed/({YOUTUBE_ID_RE})',
        html_text, flags=re.IGNORECASE
    )
    # Pattern 4: image source
    ids_in_order += re.findall(
        fr'content\s*=\s*"[^"]*/image/youtube/({YOUTUBE_ID_RE})"',
        html_text, flags=re.IGNORECASE
    )

    seen, ordered = set(), []
    for vid in ids_in_order:
        if vid not in seen:
            seen.add(vid)
            ordered.append(vid)
    return ordered

def extract_from_html_text(html_text: str) -> Tuple[str, List[str]]:
    title = extract_title_from_html_text(html_text) or "Neue Playlist"
    video_ids = extract_video_ids_from_html_text(html_text)
    return title, video_ids

# ====== Substack Fetching (Reused) ======
def fetch_substack_posts_json(archive_url: str, limit_per_page: int = 50, max_pages: int = 1000) -> list[dict]:
    root = archive_url.split("/archive")[0]
    posts = []
    seen_urls = set()
    offset = 0
    session = requests.Session()
    pages = 0

    while True:
        if pages >= max_pages:
            break

        params = {"sort": "new", "search": "", "offset": offset, "limit": limit_per_page}
        try:
            r = session.get(f"{root}/api/v1/archive", params=params, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"❌ HTTP Error fetching archive: {e}")
            break

        data = r.json()
        if isinstance(data, dict):
            items = data.get("posts", []) or data.get("items", [])
        elif isinstance(data, list):
            items = data
        else:
            items = []

        if not items:
            break

        new_count = 0
        for it in items:
            url = it.get("canonical_url") or (f"{root}/p/{it['slug']}" if it.get("slug") else it.get("url"))
            if not url:
                continue
            if url.endswith("/comments"):
                url = url[:-9]
            if url in seen_urls:
                continue
            title = htmllib.unescape((it.get("title") or it.get("headline") or "Neue Playlist").strip())
            posts.append({"url": url, "title": title})
            seen_urls.add(url)
            new_count += 1

        pages += 1
        print(f"📥 Offset {offset}: {len(items)} Items (new: {new_count})")
        
        if new_count == 0:
            break

        offset += len(items)
        time.sleep(0.2)

    return posts

def fetch_post_html(url: str) -> str:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"⚠️ Error loading {url}: {e}")
        return ""

# ====== YouTube Auth ======
def get_youtube_service():
    """
    Authenticates with the YouTube API using an OAuth 2.0 flow based on a
    `client_secret.json` file. Caches credentials in `token.pickle`.
    """
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                print("⚠️ YouTube token has been revoked, starting new login...")
                creds = None
        
        if not creds:
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
            if not os.path.exists(CLIENT_SECRETS_FILE):
                print(f"❌ Missing credentials file: {CLIENT_SECRETS_FILE}", file=sys.stderr)
                print("   Please download your OAuth 2.0 Client ID from the Google Cloud Console and place it in the project directory.", file=sys.stderr)
                return None
            
            print("🔐 Please complete the browser-based authentication for the YouTube API...")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, YOUTUBE_SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(creds, token)

    return build("youtube", "v3", credentials=creds)

# ====== AI & Database Logic ======

def get_video_title(youtube, video_id: str) -> str:
    """Fetches video title from YouTube API to help Gemini."""
    if not youtube:
        return ""
    try:
        response = youtube.videos().list(
            part="snippet",
            id=video_id
        ).execute()
        if "items" in response and len(response["items"]) > 0:
            return response["items"][0]["snippet"]["title"]
    except Exception as e:
        print(f"⚠️ YouTube API Error for {video_id}: {e}")
    return ""

def predict_genre(model, video_id: str, video_title: str) -> str:
    """Uses Gemini to predict genre based on ID and Title."""
    if not model:
        return "Unknown"
    
    prompt = f"Categorize the music genre of the song with YouTube Video ID '{video_id}'"
    if video_title:
        prompt += f" and Title '{video_title}'"
    prompt += ". Return ONLY the genre name (e.g. 'Rock', 'Pop', 'Jazz', 'Electronic'). If unknown, return 'Unknown'."

    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Gemini Error: {e}")
        return "Unknown"

def process_post_to_firestore(db, model, youtube, post_url: str, html_text: str):
    _, video_ids = extract_from_html_text(html_text)
    if not video_ids:
        return

    print(f"   Found {len(video_ids)} videos in post.")
    
    for video_id in video_ids:
        doc_ref = db.collection(COLLECTION_NAME).document(video_id)
        
        # Check if exists to avoid re-processing and AI costs
        try:
            doc = doc_ref.get()
        except exceptions.PermissionDenied:
            print(f"\n❌ Error: Cloud Firestore API is likely disabled or database is missing.")
            print(f"   Please enable it here: https://console.developers.google.com/apis/api/firestore.googleapis.com/overview?project={db.project}")
            sys.exit(1)

        if doc.exists:
            continue
            
        print(f"   Processing new video: {video_id}")
        
        # 1. Get Title (optional but helpful for AI)
        title = get_video_title(youtube, video_id)
        
        # 2. Predict Genre
        genre = predict_genre(model, video_id, title)
        
        # 3. Prepare Data
        data = {
            "video_id": video_id,
            "source": post_url,
            "musical_value": 0,
            "video_value": 0,
            "genre": genre,
            "favorite": False,
            "rejected": False,
            "title": title,
            "created_at": firestore.SERVER_TIMESTAMP
        }
        
        # 4. Save
        doc_ref.set(data)
        
        # Rate limit to be nice to APIs
        time.sleep(0.5)

def main():
    parser = argparse.ArgumentParser(description="Scrape music videos to Firestore.")
    parser.add_argument("--substack", default="https://goodmusic.substack.com/archive", help="Substack Archive URL")
    parser.add_argument("--project", help="Google Cloud Project ID")
    parser.add_argument("--limit", type=int, default=5, help="Limit posts to process (0 for all)")
    args = parser.parse_args()

    # 1. Auth & Clients
    # Use Application Default Credentials (ADC) - works locally with `gcloud auth application-default login` and on Cloud Run automatically
    try:
        creds, project_id = google.auth.default(scopes=ADC_SCOPES)
        if args.project:
            project_id = args.project
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        sys.exit(1)
    
    print(f"🚀 Initializing for Project: {project_id}")
    
    # Firestore
    db = firestore.Client(project=project_id, credentials=creds)
    
    # Vertex AI
    try:
        vertexai.init(project=project_id, location="us-central1", credentials=creds)
        model = GenerativeModel("gemini-2.5-flash-lite")
    except Exception as e:
        print(f"⚠️ Vertex AI Init Error: {e}")
        model = None
    
    # YouTube API (using OAuth 2.0 flow)
    youtube = get_youtube_service()
    if not youtube:
        print("⚠️ Could not authenticate with YouTube API. Video titles will not be fetched.")

    # 2. Fetch Posts
    print(f"📥 Fetching posts from {args.substack}...")
    posts = fetch_substack_posts_json(args.substack, limit_per_page=20)
    
    if args.limit > 0:
        posts = posts[:args.limit]

    # 3. Process
    print(f"🔄 Processing {len(posts)} posts...")
    for i, post in enumerate(posts):
        print(f"[{i+1}/{len(posts)}] Processing {post['title']}...")
        html_text = fetch_post_html(post["url"])
        process_post_to_firestore(db, model, youtube, post["url"], html_text)

if __name__ == "__main__":
    main()
