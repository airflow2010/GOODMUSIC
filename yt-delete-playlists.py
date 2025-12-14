#!/usr/bin/env python3
import os
import pickle
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

# Configuration
SCOPES = ["https://www.googleapis.com/auth/youtube"]
TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
TARGET_STRING = "Automatisch erstellt aus HTML-Datei"

def get_youtube_service():
    """
    Authenticates with the YouTube API using the local token.pickle or starts a new flow.
    """
    creds = None
    # 1) Load token if exists
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            creds = pickle.load(token)

    # 2) If invalid or missing, authenticate
    if not creds or not creds.valid:
        try:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                raise RefreshError("Token invalid or expired", None)
        except RefreshError:
            print("⚠️ Token expired or invalid, starting new login...")
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
            if not os.path.exists(CLIENT_SECRETS_FILE):
                print(f"❌ Missing credentials file: {CLIENT_SECRETS_FILE}", file=sys.stderr)
                sys.exit(1)
            
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=8080)

        # 3) Save token
        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(creds, token)

    return build("youtube", "v3", credentials=creds)

def main():
    print("🚀 Starting playlist cleanup script...")
    youtube = get_youtube_service()
    if not youtube:
        print("❌ Failed to authenticate with YouTube API.")
        return

    print("🔍 Fetching playlists...")
    
    playlists_to_delete = []
    next_page_token = None
    
    try:
        while True:
            request = youtube.playlists().list(
                part="snippet",
                mine=True,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get("items", []):
                description = item["snippet"].get("description", "")
                title = item["snippet"]["title"]
                pid = item["id"]
                
                if TARGET_STRING in description:
                    playlists_to_delete.append((pid, title))
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
    except Exception as e:
        print(f"❌ Error fetching playlists: {e}")
        return

    if not playlists_to_delete:
        print(f"✅ No playlists found with description containing '{TARGET_STRING}'.")
        return

    print(f"⚠️ Found {len(playlists_to_delete)} playlists to delete.")
    
    for pid, title in playlists_to_delete:
        try:
            print(f"🗑️ Deleting playlist: '{title}' (ID: {pid})...", end=" ")
            youtube.playlists().delete(id=pid).execute()
            print("✅ Deleted.")
        except Exception as e:
            print(f"❌ Failed: {e}")

    print("🎉 Cleanup complete.")

if __name__ == "__main__":
    main()