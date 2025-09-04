#!/usr/bin/env python3
import argparse
import html as htmllib
import os
import pickle
import re
import sys
import time
import random
import json
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ====== Konfiguration ======
SCOPES = ["https://www.googleapis.com/auth/youtube"]
DEFAULT_PRIVACY = "private"
TOKEN_FILE = "token.pickle"
CLIENT_SECRETS_FILE = "client_secret.json"
PROGRESS_FILE = "progress.json"

YOUTUBE_ID_RE = r"[A-Za-z0-9_-]{11}"

# ====== Fortschritt speichern ======
def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_playlists": {}}

def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)

# ====== HTML-Parsing ======
def extract_title_from_html_text(html_text: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.find("h1", class_=re.compile("post-title"))
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    m = re.search(r"<title>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    return htmllib.unescape(m.group(1)).strip() if m else None

def extract_video_ids_from_html_text(html_text: str) -> list[str]:
    ids_in_order: List[str] = []
    ids_in_order += re.findall(fr'id\s*=\s*"youtube2-({YOUTUBE_ID_RE})"', html_text)

    for m in re.finditer(r'data-attrs\s*=\s*"(.*?)"', html_text, flags=re.IGNORECASE | re.DOTALL):
        unescaped = htmllib.unescape(m.group(1))
        mid = re.search(fr'"videoId"\s*:\s*"({YOUTUBE_ID_RE})"', unescaped)
        if mid:
            ids_in_order.append(mid.group(1))

    ids_in_order += re.findall(
        fr'<iframe[^>]+src\s*=\s*"[^"]*youtube(?:-nocookie)?\.com/embed/({YOUTUBE_ID_RE})',
        html_text, flags=re.IGNORECASE
    )
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

# ====== Substack Support (robuste JSON-API) ======
def fetch_substack_posts_json(archive_url: str, limit_per_page: int = 50, max_pages: int = 1000) -> list[dict]:
    """
    Robust: holt alle Posts via /api/v1/archive mit Offset/Limit.
    - offset wird um len(items) erhöht (nicht stur um limit_per_page).
    - Duplikate werden entfernt.
    """
    root = archive_url.split("/archive")[0]
    posts = []
    seen_urls = set()
    offset = 0
    session = requests.Session()
    pages = 0
    cumulative = 0

    while True:
        if pages >= max_pages:
            print(f"⚠️ Abbruch: max_pages ({max_pages}) erreicht.")
            break

        params = {"sort": "new", "search": "", "offset": offset, "limit": limit_per_page}
        r = session.get(f"{root}/api/v1/archive", params=params, timeout=20)
        try:
            r.raise_for_status()
        except Exception as e:
            print("❌ HTTP-Fehler beim Abruf:", e)
            break

        data = r.json()
        if isinstance(data, dict):
            items = data.get("posts", []) or data.get("items", [])
        elif isinstance(data, list):
            items = data
        else:
            items = []

        n = len(items)
        if n == 0:
            print(f"⚠️ Keine Items bei offset={offset} → Ende.")
            break

        new_count = 0
        for it in items:
            url = it.get("canonical_url") or (f"{root}/p/{it['slug']}" if it.get("slug") else it.get("url"))
            if not url:
                continue
            # remove trailing /comments if any
            if url.endswith("/comments"):
                url = url[:-9]
            if url in seen_urls:
                continue
            title = htmllib.unescape((it.get("title") or it.get("headline") or "Neue Playlist").strip())
            posts.append({"url": url, "title": title})
            seen_urls.add(url)
            new_count += 1

        cumulative += new_count
        pages += 1
        print(f"📥 Offset {offset}: {n} Items (davon neu {new_count}) — kumulativ: {cumulative}")

        # Schutz: wenn keine neuen Items in dieser Seite gefunden wurden, beenden
        if new_count == 0:
            print("⚠️ Keine neuen Einträge in dieser Seite (alle Duplikate) → Stop.")
            break

        # WICHTIG: offset um die tatsächliche Anzahl zurückgelieferter Items erhöhen
        offset += n

        # Kleiner Sleep, um nicht aggressiv zu fragen
        time.sleep(0.2)

    print(f"\n✅ Gesamt eindeutige Posts: {len(posts)}")
    return posts

def fetch_post_html(url: str) -> str:
    r = requests.get(url)
    r.raise_for_status()
    return r.text

# ====== YouTube Auth ======
def get_youtube_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                print(f"Fehlend: {CLIENT_SECRETS_FILE}", file=sys.stderr)
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=8080)
        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(creds, token)
    return build("youtube", "v3", credentials=creds)

# ====== YouTube API Helpers ======
def create_playlist(youtube, title: str, privacy: str = DEFAULT_PRIVACY) -> str:
    request = youtube.playlists().insert(
        part="snippet,status",
        body={
            "snippet": {"title": title, "description": "Automatisch erstellt aus Substack"},
            "status": {"privacyStatus": privacy},
        },
    )
    response = request.execute()
    return response["id"]

def delete_playlist(youtube, playlist_id: str):
    try:
        youtube.playlists().delete(id=playlist_id).execute()
        print(f"🗑️ Alte unfertige Playlist gelöscht: {playlist_id}")
    except HttpError as e:
        print(f"⚠️ Konnte Playlist {playlist_id} nicht löschen: {e}")

def safe_add_video_to_playlist(youtube, playlist_id: str, video_id: str, max_retries: int = 5):
    backoff = 0.5
    for attempt in range(max_retries):
        try:
            return youtube.playlistItems().insert(
                part="snippet",
                body={"snippet": {"playlistId": playlist_id, "resourceId": {"kind": "youtube#video", "videoId": video_id}}},
            ).execute()
        except HttpError as e:
            msg = str(e)
            if e.resp.status == 403 and "quotaExceeded" in msg:
                raise RuntimeError("❌ Quota exhausted (quotaExceeded). Bitte morgen erneut starten.")
            if e.resp.status == 400 and "failedPrecondition" in msg:
                print(f"⚠️ Video {video_id} übersprungen (failedPrecondition).")
                return None
            if "duplicate" in msg or "conflict" in msg:
                print(f"⚠️ Video {video_id} übersprungen (bereits vorhanden).")
                return None
            if e.resp.status in (409, 500, 502, 503, 504) or "SERVICE_UNAVAILABLE" in msg:
                wait = backoff * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"⚠️ Fehler bei {video_id}, Retry {attempt+1}/{max_retries} in {wait:.1f}s …")
                time.sleep(wait)
                continue
            raise
    print(f"❌ Video {video_id} nach {max_retries} Versuchen übersprungen.")
    return None

# ====== Workflow ======
def process_post(youtube, url: str, html_text: str, privacy: str, sleep: float, progress: dict,
                 index: int, total: int, forced_title: str | None = None):
    title, video_ids = extract_from_html_text(html_text)
    if forced_title:
        title = forced_title
    if not video_ids:
        print(f"⚠️ Keine Videos in '{title}' gefunden.")
        return

    if url in progress["processed_playlists"]:
        pl_id = progress["processed_playlists"][url]
        print(f"🔁 Beitrag {index}/{total} bereits verarbeitet (Playlist {pl_id}), überspringe.")
        return

    print(f"\n==> [{index}/{total}] Erstelle Playlist: {title} ({len(video_ids)} Videos)")
    playlist_id = create_playlist(youtube, title, privacy=privacy)
    progress["processed_playlists"][url] = playlist_id
    save_progress(progress)

    try:
        for i, vid in enumerate(video_ids, 1):
            res = safe_add_video_to_playlist(youtube, playlist_id, vid)
            if res is not None:
                print(f"✅ [{i}/{len(video_ids)}] hinzugefügt: {vid}")
            time.sleep(sleep)
        print("Fertig:", f"https://www.youtube.com/playlist?list={playlist_id}")
    except RuntimeError as e:
        if "quotaExceeded" in str(e):
            delete_playlist(youtube, playlist_id)
            del progress["processed_playlists"][url]
            save_progress(progress)
            print(str(e))
            sys.exit(4)
        else:
            raise

def main():
    parser = argparse.ArgumentParser(description="Erzeuge YouTube-Playlists aus HTML oder Substack.")
    parser.add_argument("html_file", nargs="?", help="Pfad zu einer lokalen HTML-Datei")
    parser.add_argument("--substack", help="Substack-Archiv-URL (z. B. https://goodmusic.substack.com/archive)")
    parser.add_argument("--privacy", choices=("private", "unlisted", "public"), default=DEFAULT_PRIVACY)
    parser.add_argument("--dry-run", action="store_true", help="Nur analysieren, nichts in YouTube anlegen")
    parser.add_argument("--limit", type=int, default=None, help="Max. Anzahl Videos oder Posts")
    parser.add_argument("--sleep", type=float, default=0.2, help="Pause zwischen API-Calls (Sekunden)")
    args = parser.parse_args()

    progress = load_progress()
    yt = None if args.dry_run else get_youtube_service()

    if args.substack:
        print(f"📥 Hole Posts von: {args.substack}")
        posts = fetch_substack_posts_json(args.substack)
        if args.limit:
            posts = posts[:args.limit]

        total = len(posts)
        done = sum(1 for p in posts if p["url"] in progress["processed_playlists"])
        open_ = total - done
        print(f"📊 Fortschritt: {done}/{total} verarbeitet ({open_} offen)")

        for idx, post in enumerate(posts, 1):
            print(f"\nLade Beitrag: {post['url']}")
            html_text = fetch_post_html(post["url"])
            if args.dry_run:
                title, vids = extract_from_html_text(html_text)
                print(f"[Dry-run] {post['title']} → {len(vids)} Videos")
                for v in vids:
                    print("   ", v)
            else:
                process_post(yt, post["url"], html_text, args.privacy, args.sleep,
                             progress, idx, total, forced_title=post["title"])

    elif args.html_file:
        if not os.path.exists(args.html_file):
            print(f"Datei nicht gefunden: {args.html_file}", file=sys.stderr)
            sys.exit(1)

        with open(args.html_file, "r", encoding="utf-8", errors="ignore") as f:
            html_text = f.read()

        if args.dry_run:
            title, vids = extract_from_html_text(html_text)
            print(f"[Dry-run] {title} → {len(vids)} Videos")
            for v in vids:
                print("   ", v)
        else:
            process_post(yt, args.html_file, html_text, args.privacy, args.sleep,
                         progress, 1, 1)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
