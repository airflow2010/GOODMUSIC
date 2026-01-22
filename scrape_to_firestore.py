#!/usr/bin/env python3
import argparse
import html as htmllib
import json
import re
import time
import sys
from typing import List, Tuple
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Google Cloud imports
import google.auth
from google.cloud import firestore
from google.genai import types
from pydantic import BaseModel, Field

from ingestion import get_youtube_service, ingest_video_batch, parse_datetime, init_ai_model, init_firestore_db, AI_MODEL_NAME

# ====== Configuration ======
YOUTUBE_ID_RE = r"[A-Za-z0-9_-]{11}"
DEFAULT_SCROLL_PAUSE_SECONDS = 1.0
DEFAULT_MATCH_THRESHOLD = 0.70
DEFAULT_MAX_SEARCH_RESULTS = 5
MAX_TEXT_CHUNK_CHARS = 6000
SCROLL_STABLE_CYCLES = 3
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

MATCH_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "of",
    "to",
    "in",
    "on",
    "for",
    "with",
    "official",
    "video",
    "music",
    "mv",
    "audio",
    "lyrics",
    "lyric",
    "visualizer",
    "live",
    "hd",
    "4k",
    "remastered",
    "remaster",
    "edit",
    "version",
    "feat",
    "ft",
    "featuring",
    "performance",
    "clip",
}

class MusicMention(BaseModel):
    artist: str = Field(description="Artist name (required).")
    track: str = Field(description="Track or song title (required).")
    confidence: float = Field(description="Confidence score between 0 and 1.")
    evidence: str = Field(description="Short quote from the text supporting the mention.")

class MusicMentionList(BaseModel):
    mentions: List[MusicMention] = Field(description="List of extracted music mentions.")

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
    # Pattern 5: youtube watch URLs
    ids_in_order += re.findall(
        fr'youtube\.com/watch\?[^"\s>]*v=({YOUTUBE_ID_RE})',
        html_text, flags=re.IGNORECASE
    )
    # Pattern 6: youtu.be short URLs
    ids_in_order += re.findall(
        fr'youtu\.be/({YOUTUBE_ID_RE})',
        html_text, flags=re.IGNORECASE
    )
    # Pattern 7: youtube shorts
    ids_in_order += re.findall(
        fr'youtube\.com/shorts/({YOUTUBE_ID_RE})',
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
        r = requests.get(url, timeout=20, headers={"User-Agent": DEFAULT_USER_AGENT})
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

def _normalize_text(text: str) -> str:
    clean = text.lower().replace("&", " and ")
    clean = re.sub(r"[^a-z0-9]+", " ", clean)
    return " ".join(clean.split())

def _tokenize(text: str, drop_stopwords: bool = True) -> List[str]:
    tokens = _normalize_text(text).split()
    if drop_stopwords:
        tokens = [t for t in tokens if t not in MATCH_STOPWORDS]
    return tokens

def _coverage(tokens: List[str], haystack: set[str]) -> float:
    if not tokens:
        return 0.0
    matches = sum(1 for t in tokens if t in haystack)
    return matches / max(len(tokens), 1)

def _phrase_match(artist: str, track: str, title: str) -> bool:
    phrase = _normalize_text(f"{artist} {track}")
    if not phrase:
        return False
    return phrase in _normalize_text(title)

def _score_candidate(artist: str, track: str, title: str, channel: str) -> tuple[float, float, float, bool, int]:
    title_tokens = set(_tokenize(title))
    channel_tokens = set(_tokenize(channel))
    combined_tokens = title_tokens | channel_tokens

    artist_tokens = _tokenize(artist)
    track_tokens = _tokenize(track)
    if not artist_tokens:
        artist_tokens = _tokenize(artist, drop_stopwords=False)
    if not track_tokens:
        track_tokens = _tokenize(track, drop_stopwords=False)

    artist_coverage = max(_coverage(artist_tokens, combined_tokens), _coverage(artist_tokens, title_tokens))
    track_coverage = _coverage(track_tokens, title_tokens)
    phrase_hit = _phrase_match(artist, track, title)

    score = (0.6 * track_coverage) + (0.4 * artist_coverage)
    if phrase_hit:
        score = min(score + 0.15, 1.0)

    return score, track_coverage, artist_coverage, phrase_hit, len(track_tokens)

def _is_acceptable_match(
    score: float,
    track_coverage: float,
    artist_coverage: float,
    phrase_hit: bool,
    track_token_count: int,
    threshold: float,
) -> bool:
    if score < threshold:
        return False
    if track_coverage < 0.60 or artist_coverage < 0.40:
        return False
    if track_token_count <= 1 and not phrase_hit and artist_coverage < 0.60:
        return False
    return True

def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    ordered = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered

def _extract_visible_text_blocks(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "meta", "link"]):
        tag.decompose()
    for tag in soup.select("[aria-hidden='true'], [hidden]"):
        tag.decompose()
    for tag in soup.find_all(style=re.compile(r"display\s*:\s*none|visibility\s*:\s*hidden", re.I)):
        tag.decompose()

    blocks: List[str] = []
    seen = set()
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote", "td"]):
        text = " ".join(tag.stripped_strings)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) < 3:
            continue
        if text in seen:
            continue
        seen.add(text)
        blocks.append(text)
    return blocks

def _chunk_text_blocks(blocks: List[str], max_chars: int = MAX_TEXT_CHUNK_CHARS) -> List[str]:
    chunks: List[str] = []
    current: List[str] = []
    current_size = 0
    for block in blocks:
        block_len = len(block)
        if current and current_size + block_len + 1 > max_chars:
            chunks.append("\n".join(current))
            current = [block]
            current_size = block_len
        else:
            current.append(block)
            current_size += block_len + 1
    if current:
        chunks.append("\n".join(current))
    return chunks

def _clean_json_response(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned.replace("```json", "").replace("```", "").strip()
    elif cleaned.startswith("```"):
        cleaned = cleaned.replace("```", "").strip()
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    return cleaned

def _extract_mentions_with_gemini(
    model,
    text_chunks: List[str],
    max_mentions: int = 0,
    debug: bool = False,
) -> List[MusicMention]:
    if not model:
        print("❌ AI model not available; cannot extract mentions.")
        return []

    mentions: List[MusicMention] = []
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=MusicMentionList
    )

    if "gemini-3" in AI_MODEL_NAME:
        config.thinking_config = types.ThinkingConfig(
            include_thoughts=True,
            thinking_level="LOW"
        )

    for idx, chunk in enumerate(text_chunks, start=1):
        if max_mentions and len(mentions) >= max_mentions:
            break

        remaining = max_mentions - len(mentions) if max_mentions else 0
        limit_note = f"Return at most {remaining} mentions." if remaining else ""
        prompt = (
            "Extract mentions of specific music videos or songs from the text below. "
            "Each mention must include BOTH the artist and the track name. "
            "Ignore albums, genres, or artists without a track. "
            "Do not guess. "
            f"{limit_note}\n\nTEXT:\n{chunk}"
        )

        response = None
        max_retries = 3
        retry_delay = 10
        for attempt in range(max_retries + 1):
            try:
                response = model.models.generate_content(
                    model=AI_MODEL_NAME,
                    contents=prompt,
                    config=config
                )
                break
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < max_retries:
                        if debug:
                            print(f"   ⚠️ Gemini quota exceeded. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                print(f"   ⚠️ Gemini error on chunk {idx}: {e}")
                response = None
                break

        if not response:
            continue

        try:
            cleaned = _clean_json_response(response.text or "")
            data = json.loads(cleaned) if cleaned else {}
            if isinstance(data, list):
                parsed = [MusicMention(**item) for item in data]
            else:
                parsed = MusicMentionList(**data).mentions
        except Exception as e:
            if debug:
                print(f"   ⚠️ Gemini parse error on chunk {idx}: {e}")
            continue

        for mention in parsed:
            if mention.artist.strip() and mention.track.strip():
                mentions.append(mention)
                if max_mentions and len(mentions) >= max_mentions:
                    break

    return mentions

def _dedupe_mentions(mentions: List[MusicMention]) -> List[MusicMention]:
    deduped: dict[str, MusicMention] = {}
    for mention in mentions:
        key = f"{_normalize_text(mention.artist)}::{_normalize_text(mention.track)}"
        if not key.strip(":"):
            continue
        current = deduped.get(key)
        if not current or mention.confidence > current.confidence:
            deduped[key] = mention
    return list(deduped.values())

def _search_youtube(youtube, query: str, max_results: int) -> List[dict]:
    response = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results,
        videoCategoryId="10"
    ).execute()
    items = []
    for item in response.get("items", []):
        vid = (item.get("id") or {}).get("videoId")
        snippet = item.get("snippet") or {}
        if not vid:
            continue
        items.append({
            "video_id": vid,
            "title": snippet.get("title") or "",
            "channel": snippet.get("channelTitle") or "",
        })
    return items

def _pick_best_match(
    artist: str,
    track: str,
    candidates: List[dict],
    threshold: float,
) -> tuple[dict | None, dict | None, bool]:
    best = None
    for candidate in candidates:
        score, track_cov, artist_cov, phrase_hit, track_tokens = _score_candidate(
            artist,
            track,
            candidate.get("title", ""),
            candidate.get("channel", "")
        )
        candidate_eval = dict(candidate)
        candidate_eval.update({
            "score": score,
            "track_coverage": track_cov,
            "artist_coverage": artist_cov,
            "phrase_hit": phrase_hit,
            "track_tokens": track_tokens,
        })
        if not best or score > best.get("score", 0):
            best = candidate_eval

    if not best:
        return None, None, False

    accepted = _is_acceptable_match(
        best["score"],
        best["track_coverage"],
        best["artist_coverage"],
        best["phrase_hit"],
        best["track_tokens"],
        threshold,
    )
    return (best if accepted else None), best, accepted

def _fetch_rendered_html(url: str, max_scrolls: int, scroll_pause: float, debug: bool) -> tuple[str, str, str, int]:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except Exception as e:
        raise RuntimeError(
            "Playwright is required for URL scanning. Install with `pip install playwright` "
            "and run `python -m playwright install chromium`."
        ) from e

    def _scroll_height(page) -> int:
        return page.evaluate(
            "() => Math.max(document.body ? document.body.scrollHeight : 0, "
            "document.documentElement ? document.documentElement.scrollHeight : 0)"
        ) or 0

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(user_agent=DEFAULT_USER_AGENT)
        page = context.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except PlaywrightTimeoutError:
            if debug:
                print("   ⚠️ Page load timed out; continuing with partial content.")
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PlaywrightTimeoutError:
            pass

        scrolls = 0
        stable_cycles = 0
        last_height = _scroll_height(page)

        while True:
            if max_scrolls and scrolls >= max_scrolls:
                break
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            scrolls += 1
            time.sleep(scroll_pause)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                pass
            new_height = _scroll_height(page)
            if new_height <= last_height:
                stable_cycles += 1
            else:
                stable_cycles = 0
                last_height = new_height
            if stable_cycles >= SCROLL_STABLE_CYCLES:
                break

        html = page.content()
        final_url = page.url
        try:
            title = page.title()
        except Exception:
            title = ""

        context.close()
        browser.close()

    return html, final_url, title, scrolls

def run_url_scan(args, db, model, youtube) -> None:
    url = (args.url or "").strip()
    if not url:
        print("❌ Missing URL.")
        return

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        print("❌ Invalid URL. Provide a full http(s) URL.")
        return

    print(f"🔎 Scanning URL: {url}")
    try:
        html_text, final_url, page_title, scrolls = _fetch_rendered_html(
            url,
            max_scrolls=args.url_max_scrolls,
            scroll_pause=args.url_scroll_pause,
            debug=args.url_debug,
        )
    except RuntimeError as e:
        print(f"❌ {e}")
        return

    if not html_text:
        print("❌ No HTML content retrieved.")
        return

    if final_url and final_url != url:
        print(f"↪️  Final URL: {final_url}")
    if page_title:
        print(f"📄 Title: {page_title}")
    if args.url_debug:
        print(f"🧵 Scrolls performed: {scrolls}")

    direct_ids = extract_video_ids_from_html_text(html_text)
    direct_ids = _dedupe_preserve_order(direct_ids)
    print(f"🔗 Found {len(direct_ids)} direct YouTube IDs.")
    if args.url_debug:
        for vid in direct_ids:
            print(f"   - {vid} (direct)")

    text_blocks = _extract_visible_text_blocks(html_text)
    text_chunks = _chunk_text_blocks(text_blocks)
    if args.url_debug:
        print(f"🧾 Extracted {len(text_blocks)} text blocks, {len(text_chunks)} chunks for Gemini.")

    mentions = _extract_mentions_with_gemini(
        model,
        text_chunks,
        max_mentions=args.url_max_mentions,
        debug=args.url_debug,
    )
    mentions = _dedupe_mentions(mentions)
    mentions.sort(key=lambda m: m.confidence, reverse=True)
    print(f"🧠 Gemini extracted {len(mentions)} candidate mentions.")
    if args.url_debug:
        for mention in mentions:
            print(f"   - {mention.artist} — {mention.track} (conf {mention.confidence:.2f})")
            if mention.evidence:
                print(f"     ↳ {mention.evidence}")

    resolved_ids: List[str] = []
    for mention in mentions:
        query = f"{mention.artist} {mention.track} official music video".strip()
        if args.url_debug:
            print(f"🔍 Searching YouTube: {query}")
        try:
            results = _search_youtube(
                youtube,
                query=query,
                max_results=args.url_max_search_results,
            )
        except Exception as e:
            print(f"   ⚠️ YouTube search failed for '{query}': {e}")
            continue

        best, best_for_debug, accepted = _pick_best_match(
            mention.artist,
            mention.track,
            results,
            threshold=args.url_match_threshold,
        )

        if accepted and best:
            resolved_ids.append(best["video_id"])
            if args.url_debug:
                print(f"   ✅ {best['video_id']} | {best['title']} (score {best['score']:.2f})")
        else:
            if args.url_debug:
                if best_for_debug:
                    print(
                        f"   ❌ No strong match. Best was {best_for_debug['video_id']} "
                        f"({best_for_debug['score']:.2f}) | {best_for_debug['title']}"
                    )
                else:
                    print("   ❌ No results returned.")

    all_ids = _dedupe_preserve_order(direct_ids + resolved_ids)
    print(f"✅ Total unique videos: {len(all_ids)}")
    if args.url_debug:
        for vid in all_ids:
            print(f"   - {vid}")

    if not all_ids:
        print("⚠️ No videos to ingest.")
        return

    if args.url_dry_run:
        print("🧪 Dry run enabled; skipping ingestion.")
        return

    summary = ingest_video_batch(
        db=db,
        youtube=youtube,
        video_ids=all_ids,
        source=url,
        model=model,
        model_name=AI_MODEL_NAME,
        sleep_between=0.5,
        progress_logger=lambda msg: print(msg),
    )
    print(
        f"\nSummary: {summary['added']} added, {summary['exists']} existing, "
        f"{summary['unavailable']} unavailable, {summary['errors']} errors."
    )

def main():
    parser = argparse.ArgumentParser(description="Scrape music videos to Firestore.")
    parser.add_argument("--substack", default="https://goodmusic.substack.com/archive", help="Substack Archive URL")
    parser.add_argument("--project", help="Google Cloud Project ID")
    parser.add_argument("--limit-substack-posts", type=int, default=0, help="Limit posts to process (0 for all)")
    parser.add_argument("--limit-new-db-entries", type=int, default=0, help="Limit new DB entries to add (0 for all)")
    parser.add_argument("--url", help="Scan a URL for embedded YouTube videos and mentions.")
    parser.add_argument("--url-dry-run", action="store_true", help="List found videos without ingesting.")
    parser.add_argument("--url-debug", action="store_true", help="Verbose debug output for URL scan.")
    parser.add_argument("--url-max-mentions", type=int, default=0, help="Limit Gemini mentions (0 for all).")
    parser.add_argument("--url-max-search-results", type=int, default=DEFAULT_MAX_SEARCH_RESULTS, help="Max YouTube results per mention.")
    parser.add_argument("--url-max-scrolls", type=int, default=0, help="Max scrolls for infinite pages (0 for no limit).")
    parser.add_argument("--url-scroll-pause", type=float, default=DEFAULT_SCROLL_PAUSE_SECONDS, help="Pause between scrolls (seconds).")
    parser.add_argument("--url-match-threshold", type=float, default=DEFAULT_MATCH_THRESHOLD, help="Match threshold for YouTube search results.")
    args = parser.parse_args()

    print(f"🚀 Initializing for Project: {args.project or 'Default'}")
    
    # 1. Firestore
    db = init_firestore_db(args.project)
    if not db:
        sys.exit(1)
    
    # 2. AI Model
    model = init_ai_model(db.project)
    model_name = AI_MODEL_NAME
    
    # 3. YouTube API (using OAuth 2.0 flow)
    youtube = get_youtube_service()
    if not youtube:
        print("❌ Could not authenticate with YouTube API. Aborting scraper.")
        print("   Please run `rm token.pickle` and re-run the script to re-authenticate.")
        sys.exit(1)

    if args.url:
        run_url_scan(args, db, model, youtube)
        return

    # 4. Fetch Posts
    print(f"📥 Fetching posts from {args.substack}...")
    posts = fetch_substack_posts_json(args.substack, limit_per_page=20)
    
    if args.limit_substack_posts > 0:
        posts = posts[:args.limit_substack_posts]

    # 5. Process
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
