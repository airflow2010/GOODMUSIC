#!/usr/bin/env python3
import argparse
import html as htmllib
import re
import time
import sys
from typing import List, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Google Cloud imports
import google.auth
from google.cloud import firestore

from ingestion import get_youtube_service, ingest_video_batch, parse_datetime, init_ai_model, init_firestore_db, AI_MODEL_NAME

# ====== Configuration ======
YOUTUBE_ID_RE = r"[A-Za-z0-9_-]{11}"

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
    cumulative = 0

    while True:
        if pages >= max_pages:
            print(f"⚠️ Stopping: max_pages ({max_pages}) reached.")
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

        n = len(items)
        if n == 0:
            print(f"⚠️ No items at offset={offset} → End.")
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
            published_at = (
                it.get("post_date")
                or it.get("published_at")
                or it.get("created_at")
                or it.get("date")
            )
            posts.append({"url": url, "title": title, "published_at": published_at})
            seen_urls.add(url)
            new_count += 1

        cumulative += new_count
        pages += 1
        print(f"📥 Offset {offset}: {n} Items (new: {new_count}) — cumulative: {cumulative}")
        
        if new_count == 0:
            print("⚠️ No new items in this page (all duplicates) → Stop.")
            break

        offset += n
        time.sleep(0.2)

    print(f"\n✅ Total unique posts: {len(posts)}")
    return posts

def fetch_post_html(url: str) -> str:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"⚠️ Error loading {url}: {e}")
        return ""

def extract_substack_date_from_html(html_text: str) -> datetime | None:
    """Extract publish date from Substack HTML if present."""
    soup = BeautifulSoup(html_text, "html.parser")
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        dt = parse_datetime(meta["content"])
        if dt:
            return dt
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        dt = parse_datetime(time_tag["datetime"])
        if dt:
            return dt
    return None

def process_post_to_firestore(db, model, youtube, post: dict, html_text: str, max_new_entries: int = 0, model_name: str = "unknown") -> Tuple[int, bool]:
    _, video_ids = extract_from_html_text(html_text)
    if not video_ids:
        return 0, False
    
    post_url = post["url"]
    # Prefer archive JSON date, fall back to HTML meta
    date_substack = parse_datetime(post.get("published_at")) or extract_substack_date_from_html(html_text)

    print(f"   Found {len(video_ids)} videos in post.")
    summary = ingest_video_batch(
        db=db,
        youtube=youtube,
        video_ids=video_ids,
        source=post_url,
        model=model,
        model_name=model_name,
        extra_fields={"date_substack": date_substack},
        max_new_entries=max_new_entries,
        sleep_between=0.5,
        progress_logger=lambda msg: print(msg),
    )

    print()
    return summary["added"], summary.get("aborted", False)

def main():
    parser = argparse.ArgumentParser(description="Scrape music videos to Firestore.")
    parser.add_argument("--substack", default="https://goodmusic.substack.com/archive", help="Substack Archive URL")
    parser.add_argument("--project", help="Google Cloud Project ID")
    parser.add_argument("--limit-substack-posts", type=int, default=0, help="Limit posts to process (0 for all)")
    parser.add_argument("--limit-new-db-entries", type=int, default=0, help="Limit new DB entries to add (0 for all)")
    args = parser.parse_args()

    print(f"🚀 Initializing for Project: {args.project or 'Default'}")
    
    # 1. Firestore
    db = init_firestore_db(args.project)
    if not db:
        sys.exit(1)
    
    # 2. AI Model
    model = init_ai_model(db.project)
    model_name = AI_MODEL_NAME
    
    # YouTube API (using OAuth 2.0 flow)
    youtube = get_youtube_service()
    if not youtube:
        print("⚠️ Could not authenticate with YouTube API. Video titles will not be fetched.")

    # 2. Fetch Posts
    print(f"📥 Fetching posts from {args.substack}...")
    posts = fetch_substack_posts_json(args.substack, limit_per_page=20)
    
    if args.limit_substack_posts > 0:
        posts = posts[:args.limit_substack_posts]

    # 3. Process
    print(f"🔄 Processing {len(posts)} posts...")
    total_new_entries = 0
    for i, post in enumerate(posts):
        if args.limit_new_db_entries > 0 and total_new_entries >= args.limit_new_db_entries:
            print(f"🛑 Limit of {args.limit_new_db_entries} new DB entries reached.")
            break

        print(f"[{i+1}/{len(posts)}] Processing {post['title']}...")
        html_text = fetch_post_html(post["url"])
        
        remaining_limit = 0
        if args.limit_new_db_entries > 0:
            remaining_limit = args.limit_new_db_entries - total_new_entries

        added, aborted = process_post_to_firestore(db, model, youtube, post, html_text, max_new_entries=remaining_limit, model_name=model_name)
        
        if aborted:
            print("🛑 Aborting scraper due to critical error (IP blocking).")
            break
            
        total_new_entries += added

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user. Exiting gracefully.")
        sys.exit(0)
