#!/usr/bin/env python3
import requests
import time
import html

def fetch_all_posts_json(archive_url: str, limit_per_page: int = 50):
    root = archive_url.split("/archive")[0]
    posts = []
    offset = 0

    while True:
        params = {"sort": "new", "search": "", "offset": offset, "limit": limit_per_page}
        r = requests.get(f"{root}/api/v1/archive", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        # Fall A: {"posts": [...], "count": N, ...}
        if isinstance(data, dict):
            items = data.get("posts", [])
        # Fall B: [ {...}, {...} ]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        if not items:
            break

        for it in items:
            url = it.get("canonical_url") or (f"{root}/p/{it['slug']}" if it.get("slug") else None)
            if not url:
                continue
            title = html.unescape(it.get("title") or it.get("headline") or "Neue Playlist")
            posts.append({"url": url, "title": title.strip()})

        print(f"📥 Offset {offset}: {len(items)} Posts")
        offset += limit_per_page
        time.sleep(0.2)

    return posts

if __name__ == "__main__":
    archive_url = "https://goodmusic.substack.com/archive"
    posts = fetch_all_posts_json(archive_url, limit_per_page=50)
    print(f"\n✅ Gesamt: {len(posts)} Posts gefunden")
    for p in posts[:10]:
        print("-", p["title"], "→", p["url"])
    if len(posts) > 10:
        print("...")
