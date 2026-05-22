"""
app.py — The Village Hall Signal Audit Tool
Runs the full pipeline: scrape → Notion write → tag (per platform)

Platforms:
  YouTube    — official YouTube Data API v3
  TikTok     — Apify clockworks/tiktok-comments-scraper
  Reddit     — Apify trudax/reddit-scraper-lite
               Posts → Reddit Thread database (thread-level signal)
               Comments → Comment Dataset (same as all platforms)
  App Store  — Apify canadesk/app-store-scraper
  Play Store — Apify canadesk/google-play-scraper
  Trustpilot — Apify automation-lab/trustpilot
  Substack   — Apify epctex/substack-scraper
  Forum      — Apify apify/website-content-crawler + Claude Haiku extraction

Additional:
  Google search volume — Apify google-search-scraper (Standard/Deep only)
  Author tracking      — Authors database built at end of pipeline
"""

import os
import uuid
import json
import time
import threading
from datetime import datetime, date

import requests
from flask import Flask, request, jsonify, send_from_directory
from dotenv import load_dotenv
import anthropic

load_dotenv()

app = Flask(__name__, template_folder="templates")

APIFY_API_KEY     = os.getenv("APIFY_API_KEY", "")
NOTION_API_KEY    = os.getenv("NOTION_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY", "")

NOTION_VERSION   = "2022-06-28"
BATCH_SIZE       = 40
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

MOTIVATION_TAGS = [
    "Praise", "Criticism", "Question", "Suggestion",
    "Feedback", "Comparison", "Self-expression",
]

MOTIVATION_DEFINITIONS = """
- Praise: positive judgment of the brand, content, or product ("love this", "incredible", "this changed everything for me")
- Criticism: negative judgment, not necessarily from personal experience ("this is wrong", "bad idea", "disagree with this approach")
- Question: seeking information or clarification ("how do I", "what temperature", "does this work for")
- Suggestion: directive or prescriptive ("you should do X", "please add Y", "it would be better if", "I think you should")
- Feedback: reporting a personal experience, positive or negative ("this didn't work for me", "I tried this and it came out perfectly", "mine was too dry", "followed this exactly and loved it")
- Comparison: placing the brand alongside another ("reminds me of", "better than", "similar to", "the X version of Y")
- Self-expression: using the brand to say something about identity or personal situation ("this is so me", "exactly how I want to live", "this describes my life")
""".strip()

SENTIMENT_TAGS = ["Positive", "Negative", "Neutral", "Mixed"]

PLATFORM_LABELS = {
    "youtube":    "YouTube",
    "tiktok":     "TikTok",
    "reddit":     "Reddit",
    "appstore":   "App Store",
    "playstore":  "Play Store",
    "trustpilot": "Trustpilot",
    "substack":   "Substack",
    "forum":      "Forum",
}

runs = {}  # In-memory run state


# ──────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def update_run(run_id, **kwargs):
    runs[run_id].update(kwargs)

def log(run_id, message):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {message}"
    runs[run_id].setdefault("log", []).append(entry)
    print(f"[{run_id[:8]}] {message}")

def safe_url(value):
    v = str(value).strip() if value else ""
    return v if v.startswith("http") else None

def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def parse_youtube_handle(channel_input):
    s = channel_input.strip()
    if "/@" in s:
        return s.split("/@")[1].split("/")[0].split("?")[0]
    if "/channel/" in s:
        return s.split("/channel/")[1].split("/")[0].split("?")[0]
    if "/c/" in s:
        return s.split("/c/")[1].split("/")[0].split("?")[0]
    if "/user/" in s:
        return s.split("/user/")[1].split("/")[0].split("?")[0]
    return s.lstrip("@")

def parse_trustpilot_domain(url_or_domain):
    s = url_or_domain.strip()
    if "trustpilot.com/review/" in s:
        return s.split("trustpilot.com/review/")[-1].strip("/")
    return s

def parse_forum_urls(raw):
    if isinstance(raw, list):
        return [u.strip() for u in raw if u.strip()]
    return [u.strip() for u in raw.split("\n") if u.strip()]

def is_reddit_post(item):
    """Distinguish Reddit posts from comments by presence of a title field."""
    return bool(item.get("title"))

def is_top_level(item):
    if item.get("parentId") or item.get("replyTo") or item.get("isReply"):
        return False
    if str(item.get("type", "")).lower() == "reply":
        return False
    return True


# ──────────────────────────────────────────────────────────────
# YouTube Data API v3
# ──────────────────────────────────────────────────────────────

def fetch_youtube_comments(channel_input, max_items, run_id):
    """
    Collect top-level comments via YouTube Data API v3.
    Flow: resolve handle → uploads playlist → video IDs → commentThreads.
    Quota: ~50–80 units for Standard, ~200–300 for Deep. Limit is 10,000/day.
    Never calls search.list (100 units/call) — uses playlistItems throughout.
    """
    if not YOUTUBE_API_KEY:
        log(run_id, "YouTube: YOUTUBE_API_KEY not configured — skipping")
        return []

    handle = parse_youtube_handle(channel_input)

    # 1. Resolve handle → channel ID → uploads playlist
    try:
        if handle.startswith("UC") and len(handle) == 24:
            params = {"key": YOUTUBE_API_KEY, "id": handle, "part": "contentDetails"}
        else:
            params = {"key": YOUTUBE_API_KEY, "forHandle": handle, "part": "contentDetails"}

        r = requests.get(f"{YOUTUBE_API_BASE}/channels", params=params, timeout=15)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            log(run_id, f"YouTube: channel not found for '{handle}'")
            return []
        uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        log(run_id, f"YouTube: resolved → uploads playlist {uploads_playlist_id}")
    except Exception as e:
        log(run_id, f"YouTube: channel resolution failed — {e}")
        return []

    # 2. Get recent video IDs from uploads playlist
    video_ids = []
    next_page  = None
    target_videos = 20

    while len(video_ids) < target_videos:
        params = {
            "key": YOUTUBE_API_KEY,
            "playlistId": uploads_playlist_id,
            "part": "contentDetails",
            "maxResults": 50,
        }
        if next_page:
            params["pageToken"] = next_page
        try:
            r = requests.get(f"{YOUTUBE_API_BASE}/playlistItems", params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log(run_id, f"YouTube: playlist fetch failed — {e}")
            break

        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        next_page = data.get("nextPageToken")
        if not next_page:
            break

    video_ids = video_ids[:target_videos]
    log(run_id, f"YouTube: {len(video_ids)} videos found")

    # 3. Fetch comment threads per video
    all_comments      = []
    per_video_target  = max(max_items // max(len(video_ids), 1), 25)

    for video_id in video_ids:
        if len(all_comments) >= max_items:
            break

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        next_page = None
        vid_count = 0

        while vid_count < per_video_target:
            params = {
                "key": YOUTUBE_API_KEY,
                "videoId": video_id,
                "part": "snippet",
                "maxResults": 100,
                "order": "relevance",
            }
            if next_page:
                params["pageToken"] = next_page

            try:
                r = requests.get(f"{YOUTUBE_API_BASE}/commentThreads", params=params, timeout=15)
                if r.status_code == 403:
                    break  # Comments disabled — skip silently
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log(run_id, f"YouTube: comment fetch failed for {video_id} — {e}")
                break

            for item in data.get("items", []):
                tl         = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                reply_count = item["snippet"].get("totalReplyCount", 0)
                all_comments.append({
                    "comment":     tl.get("textOriginal") or tl.get("textDisplay", ""),
                    "author":      tl.get("authorDisplayName", ""),
                    "publishedAt": tl.get("publishedAt", ""),
                    "likeCount":   tl.get("likeCount", 0),
                    "replyCount":  reply_count,
                    "videoUrl":    video_url,
                    "commentUrl":  f"{video_url}&lc={comment_id}",
                    "videoTitle":  "",
                })
                vid_count += 1

            next_page = data.get("nextPageToken")
            if not next_page:
                break

        time.sleep(0.1)

    log(run_id, f"YouTube API: {len(all_comments)} comments collected")
    return all_comments[:max_items]


# ──────────────────────────────────────────────────────────────
# Apify
# ──────────────────────────────────────────────────────────────

def create_apify_run(actor_id, actor_input, run_id):
    actor_id = actor_id.replace("/", "~")  # Apify REST API uses ~ not / as separator
    r = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        params={"token": APIFY_API_KEY},
        json={"input": actor_input},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()["data"]
    log(run_id, f"Apify run started — actor: {actor_id} | run: {data['id']}")
    return data["id"], data["defaultDatasetId"]


def wait_for_apify_run(apify_run_id, run_id, timeout_minutes=45):
    url      = f"https://api.apify.com/v2/actor-runs/{apify_run_id}"
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        r      = requests.get(url, params={"token": APIFY_API_KEY}, timeout=30)
        r.raise_for_status()
        status = r.json()["data"]["status"]
        if status == "SUCCEEDED":
            log(run_id, f"Apify run {apify_run_id} succeeded")
            return True
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            log(run_id, f"Apify run {apify_run_id} ended with status: {status}")
            return False
        log(run_id, f"Apify status: {status} — checking again in 30s")
        time.sleep(30)
    log(run_id, "Apify run timed out")
    return False


def fetch_apify_dataset(dataset_id, run_id):
    all_items, offset, limit = [], 0, 500
    while True:
        r = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            params={"token": APIFY_API_KEY, "offset": offset, "limit": limit},
            timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_items.extend(batch)
        log(run_id, f"Apify dataset: {len(all_items)} items retrieved")
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return all_items


def build_actor_config(platform, form_data, tier):
    """
    Returns (actor_id, actor_input) for Apify-based platforms.
    YouTube and Forum are handled separately in run_pipeline.
    """
    max_items = 2000 if tier == "deep" else 500

    if platform == "tiktok":
        handle = form_data.get("tiktok_handle", "").lstrip("@")
        posts  = 20
        return "clockworks/tiktok-comments-scraper", {
            "profiles":            [handle],
            "profileSorting":      "popular",
            "maxRepliesPerComment": 0,
            "commentsPerPost":     max_items // posts,
            "postsPerProfile":     posts,
        }

    elif platform == "reddit":
        return "trudax/reddit-scraper-lite", {
            "searches":       [form_data.get("reddit_term", "")],
            "maxPostCount":   20,
            "maxCommentCount": max_items,
        }

    elif platform == "appstore":
        return "canadesk/app-store-scraper", {
            "appIds":     [form_data.get("appstore_id", "")],
            "maxReviews": max_items,
        }

    elif platform == "playstore":
        return "canadesk/google-play-scraper", {
            "appIds":     [form_data.get("playstore_id", "")],
            "maxReviews": max_items,
        }

    elif platform == "trustpilot":
        domain = parse_trustpilot_domain(form_data.get("trustpilot_url", ""))
        return "automation-lab/trustpilot", {
            "domain":     domain,
            "maxReviews": max_items,
        }

    elif platform == "substack":
        return "epctex/substack-scraper", {
            "startUrls":      [{"url": form_data.get("substack_url", "")}],
            "maxItems":       max_items,
            "includeComments": True,
        }

    return None, None


# ──────────────────────────────────────────────────────────────
# Forum extraction (Claude Haiku)
# ──────────────────────────────────────────────────────────────

def extract_forum_comments(raw_text, source_url, run_id):
    """
    Extract individual comments from raw forum page text via Claude Haiku.
    Returns items in write_comment_row-compatible format.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""Extract individual user comments or posts from this forum page.

Return ONLY a valid JSON array. Each object must have:
- "text": the comment text (preserve original wording exactly)
- "author": username or display name (empty string if not visible)
- "position": integer position in thread (1 = original post, incrementing)

Rules:
- Include only substantive user-written content — exclude navigation, ads, boilerplate
- Maximum 100 comments per page
- If no user comments found, return []
- No preamble, no markdown fences

Forum page text:
{raw_text[:8000]}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        comments = json.loads(raw.strip())

        items = []
        for c in comments:
            text = str(c.get("text", "")).strip()
            if not text:
                continue
            items.append({
                "comment": text,
                "author":  c.get("author", ""),
                "pageUrl": source_url,
            })

        log(run_id, f"Forum: extracted {len(items)} comments from {source_url}")
        return items

    except Exception as e:
        log(run_id, f"Forum extraction failed for {source_url}: {e}")
        return []


# ──────────────────────────────────────────────────────────────
# Google search volume (Apify)
# ──────────────────────────────────────────────────────────────

def fetch_search_volume(brand_name, run_id):
    """
    Fetch Google result counts for community-intent terms via Apify google-search-scraper.
    resultsTotal is the 'About X results' figure — proxy for inbound search intent.
    """
    terms = [
        f"{brand_name} community",
        f"{brand_name} discord",
        f"{brand_name} forum",
        f"{brand_name} group",
    ]

    actor_input = {
        "queries":          "\n".join(terms),
        "maxPagesPerQuery": 1,
        "resultsPerPage":   10,
        "countryCode":      "gb",
        "languageCode":     "en",
        "saveHtml":         False,
        "saveMarkdown":     False,
    }

    try:
        apify_run_id, dataset_id = create_apify_run(
            "apify/google-search-scraper", actor_input, run_id
        )
        success = wait_for_apify_run(apify_run_id, run_id, timeout_minutes=10)
        if not success:
            log(run_id, "Search volume: Apify run failed")
            return None

        items   = fetch_apify_dataset(dataset_id, run_id)
        results = {}
        for item in items:
            term  = item.get("searchQuery", {}).get("term", "")
            total = item.get("resultsTotal")
            if term:
                results[term] = total

        log(run_id, f"Search volume results: {results}")
        return results

    except Exception as e:
        log(run_id, f"Search volume fetch failed: {e}")
        return None


def write_search_volume_to_notion(brand_page_id, volume_data, run_id):
    """Append search result counts as a callout block on the brand page."""
    if not volume_data:
        return

    lines   = []
    for term, total in volume_data.items():
        lines.append(f"{term}: ~{total:,} results" if total else f"{term}: no results")
    content = "  ·  ".join(lines)

    payload = {
        "children": [{
            "object": "block",
            "type":   "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": f"Search volume  —  {content}"}}],
                "icon":  {"type": "emoji", "emoji": "🔍"},
                "color": "gray_background",
            },
        }]
    }

    r = requests.patch(
        f"https://api.notion.com/v1/blocks/{brand_page_id}/children",
        headers=notion_headers(),
        json=payload,
        timeout=30,
    )
    if r.status_code == 200:
        log(run_id, "Search volume written to brand page")
    else:
        log(run_id, f"Search volume Notion write failed: {r.status_code}")


# ──────────────────────────────────────────────────────────────
# Notion — Comment Dataset
# ──────────────────────────────────────────────────────────────

def create_comment_database(brand_page_id, brand_name, subject_tags, run_id):
    today          = date.today().strftime("%Y-%m-%d")
    subject_options = [{"name": t} for t in subject_tags if t]

    payload = {
        "parent": {"page_id": brand_page_id},
        "title":  [{"text": {"content": f"Comment Dataset — {brand_name} — {today}"}}],
        "properties": {
            "Comment":  {"title": {}},
            "Platform": {
                "select": {
                    "options": [
                        {"name": "YouTube"}, {"name": "TikTok"},    {"name": "Reddit"},
                        {"name": "App Store"}, {"name": "Play Store"},
                        {"name": "Trustpilot"}, {"name": "Substack"}, {"name": "Forum"},
                    ]
                }
            },
            "Source URL":     {"url": {}},
            "Item URL":       {"url": {}},
            "Published Date": {"rich_text": {}},
            "Author":         {"rich_text": {}},
            "Reply Count":    {"number": {}},
            "Like Count":     {"number": {}},
            "Has Replies":    {"checkbox": {}},
            "Item Title":     {"rich_text": {}},
            "Motivation Tag": {
                "select": {"options": [{"name": t} for t in MOTIVATION_TAGS]}
            },
            "Sentiment Tag": {
                "select": {"options": [{"name": t} for t in SENTIMENT_TAGS]}
            },
            "Subject Tag": {
                "multi_select": {"options": subject_options}
            },
            "Untaggable": {"checkbox": {}},
            "Note":       {"rich_text": {}},
        },
    }

    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(),
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    db_id  = r.json()["id"]
    db_url = r.json()["url"]
    log(run_id, f"Comment Dataset created: {db_url}")
    return db_id, db_url


def write_comment_row(database_id, item, platform_label):
    text = (
        item.get("comment") or item.get("text") or item.get("body") or
        item.get("content") or item.get("review") or ""
    )
    text = str(text)[:2000]

    reply_count = safe_int(item.get("replyCount") or item.get("repliesCount")) or 0
    has_replies = reply_count > 0

    props = {
        "Comment":     {"title": [{"text": {"content": text}}]},
        "Platform":    {"select": {"name": platform_label}},
        "Has Replies": {"checkbox": has_replies},
    }

    source_url = safe_url(
        item.get("pageUrl") or item.get("url") or
        item.get("videoUrl") or item.get("sourceUrl")
    )
    if source_url:
        props["Source URL"] = {"url": source_url}

    item_url = safe_url(item.get("commentUrl") or item.get("itemUrl") or item.get("replyUrl"))
    if item_url:
        props["Item URL"] = {"url": item_url}

    pub = str(
        item.get("publishedTimeText") or item.get("date") or
        item.get("publishedAt") or item.get("at") or ""
    )
    if pub:
        props["Published Date"] = {"rich_text": [{"text": {"content": pub[:100]}}]}

    author = str(
        item.get("authorText") or item.get("author") or
        item.get("userName") or item.get("user") or ""
    )
    if author:
        props["Author"] = {"rich_text": [{"text": {"content": author[:200]}}]}

    if reply_count:
        props["Reply Count"] = {"number": reply_count}

    lc = safe_int(
        item.get("likeCount") or item.get("voteCount") or
        item.get("likes") or item.get("thumbsUpCount") or item.get("helpfulCount")
    )
    if lc is not None:
        props["Like Count"] = {"number": lc}

    title = str(item.get("videoTitle") or item.get("title") or "")
    if title:
        props["Item Title"] = {"rich_text": [{"text": {"content": title[:200]}}]}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(),
        json={"parent": {"database_id": database_id}, "properties": props},
        timeout=30,
    )
    return r.status_code == 200, (r.json().get("id") if r.status_code == 200 else None)


# ──────────────────────────────────────────────────────────────
# Notion — Reddit Thread database
# ──────────────────────────────────────────────────────────────

def create_reddit_thread_database(brand_page_id, brand_name, run_id):
    """
    Separate database for Reddit posts/threads.
    Thread-level data is the primary signal for Non-owned Channel Presence —
    distinct from comment-level data in the Comment Dataset.
    """
    today = date.today().strftime("%Y-%m-%d")

    payload = {
        "parent": {"page_id": brand_page_id},
        "title":  [{"text": {"content": f"Reddit Threads — {brand_name} — {today}"}}],
        "properties": {
            "Title":         {"title": {}},
            "Subreddit":     {"rich_text": {}},
            "URL":           {"url": {}},
            "Upvote Count":  {"number": {}},
            "Comment Count": {"number": {}},
            "Date":          {"rich_text": {}},
            "Body":          {"rich_text": {}},
            "Search Term":   {"rich_text": {}},
        },
    }

    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(),
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    db_id  = r.json()["id"]
    db_url = r.json()["url"]
    log(run_id, f"Reddit Thread database created: {db_url}")
    return db_id, db_url


def write_reddit_thread_row(database_id, item, search_term):
    title = str(item.get("title", ""))[:2000]
    if not title:
        return False

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
    }

    subreddit = str(item.get("subreddit") or item.get("community") or "")
    if subreddit:
        props["Subreddit"] = {"rich_text": [{"text": {"content": subreddit[:200]}}]}

    url = safe_url(item.get("url") or item.get("postUrl") or item.get("link"))
    if url:
        props["URL"] = {"url": url}

    score = safe_int(item.get("score") or item.get("upvotes") or item.get("ups"))
    if score is not None:
        props["Upvote Count"] = {"number": score}

    num_comments = safe_int(item.get("numComments") or item.get("num_comments") or item.get("commentsCount"))
    if num_comments is not None:
        props["Comment Count"] = {"number": num_comments}

    date_val = str(item.get("createdAt") or item.get("date") or item.get("created") or "")
    if date_val:
        props["Date"] = {"rich_text": [{"text": {"content": date_val[:100]}}]}

    body = str(item.get("body") or item.get("selftext") or item.get("text") or "")
    if body:
        props["Body"] = {"rich_text": [{"text": {"content": body[:2000]}}]}

    if search_term:
        props["Search Term"] = {"rich_text": [{"text": {"content": search_term[:200]}}]}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(),
        json={"parent": {"database_id": database_id}, "properties": props},
        timeout=30,
    )
    return r.status_code == 200


# ──────────────────────────────────────────────────────────────
# Notion — Author tracking
# ──────────────────────────────────────────────────────────────

def create_author_database(brand_page_id, brand_name, run_id):
    """
    Authors database for community leader identification.
    Built at end of pipeline from the full comment dataset.
    """
    today = date.today().strftime("%Y-%m-%d")

    payload = {
        "parent": {"page_id": brand_page_id},
        "title":  [{"text": {"content": f"Authors — {brand_name} — {today}"}}],
        "properties": {
            "Author":         {"title": {}},
            "Platform":       {"rich_text": {}},
            "Comment Count":  {"number": {}},
            "Like Count Total": {"number": {}},
        },
    }

    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(),
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    db_id  = r.json()["id"]
    db_url = r.json()["url"]
    log(run_id, f"Authors database created: {db_url}")
    return db_id, db_url


def build_author_database(comment_db_id, author_db_id, run_id):
    """
    Query the full comment dataset, aggregate by author+platform,
    and write one row per unique author to the Authors database.
    """
    log(run_id, "Building Authors database from comment dataset")

    # Fetch all comment rows
    rows, cursor = [], None
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{comment_db_id}/query",
            headers=notion_headers(),
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    # Aggregate by author + platform
    author_map = {}  # (author, platform) → {comment_count, like_total}
    for row in rows:
        props    = row.get("properties", {})
        author   = ""
        platform = ""
        likes    = 0

        author_prop = props.get("Author", {}).get("rich_text", [])
        if author_prop:
            author = author_prop[0].get("plain_text", "")

        platform_prop = props.get("Platform", {}).get("select")
        if platform_prop:
            platform = platform_prop.get("name", "")

        like_prop = props.get("Like Count", {}).get("number")
        if like_prop:
            likes = like_prop

        if not author:
            continue

        key = (author, platform)
        if key not in author_map:
            author_map[key] = {"comment_count": 0, "like_total": 0}
        author_map[key]["comment_count"] += 1
        author_map[key]["like_total"]    += likes

    log(run_id, f"Authors: {len(author_map)} unique author/platform combinations found")

    # Write to Authors database — sorted by comment count descending
    sorted_authors = sorted(author_map.items(), key=lambda x: x[1]["comment_count"], reverse=True)
    written = 0
    for (author, platform), stats in sorted_authors:
        props = {
            "Author":           {"title": [{"text": {"content": author[:200]}}]},
            "Platform":         {"rich_text": [{"text": {"content": platform}}]},
            "Comment Count":    {"number": stats["comment_count"]},
            "Like Count Total": {"number": stats["like_total"]},
        }
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(),
            json={"parent": {"database_id": author_db_id}, "properties": props},
            timeout=30,
        )
        if r.status_code == 200:
            written += 1
        time.sleep(0.34)

    log(run_id, f"Authors: {written} rows written")


# ──────────────────────────────────────────────────────────────
# Tagging (per platform, runs immediately after each scrape)
# ──────────────────────────────────────────────────────────────

def fetch_untagged_rows(database_id):
    rows, cursor = [], None
    while True:
        payload = {
            "filter":    {"property": "Motivation Tag", "select": {"is_empty": True}},
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{database_id}/query",
            headers=notion_headers(),
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return rows


def extract_text(row):
    title = row.get("properties", {}).get("Comment", {}).get("title", [])
    return title[0].get("plain_text", "") if title else ""


def tag_batch(comments, subject_tags):
    """
    Tag a batch of comments with motivation tag, sentiment tag, and up to 2 subject tags.
    Returns list of {id, motivation_tag, sentiment_tag, subject_tags}.
    """
    client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    numbered = "\n".join(
        f"{i+1}. [ID:{c['id']}] {c['text'][:400]}"
        for i, c in enumerate(comments)
    )

    prompt = f"""Tag every comment with a MOTIVATION tag, a SENTIMENT tag, and 1–2 SUBJECT tags.

MOTIVATION tags (pick exactly one — the primary motivation):
{', '.join(MOTIVATION_TAGS)}

Definitions:
{MOTIVATION_DEFINITIONS}

SENTIMENT tags (pick exactly one):
Positive, Negative, Neutral, Mixed

SUBJECT tags (pick 1–2 that apply, from this list only):
{', '.join(subject_tags)}

Rules:
- motivation_tag: single tag, dominant motivation only
- sentiment_tag: single tag
- subject_tags: array of 1–2 tags; use the nearest match if no exact fit
- If a comment cannot be meaningfully tagged, set motivation_tag and sentiment_tag to "Untaggable" and subject_tags to []

Comments:
{numbered}

Respond ONLY with a valid JSON array. No preamble, no markdown fences.
Format: [{{"id": "page-id", "motivation_tag": "Praise", "sentiment_tag": "Positive", "subject_tags": ["Recipes", "Mental health"]}}]"""

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def update_row_tags(page_id, motivation_tag, sentiment_tag, subject_tags, untaggable=False):
    props = {"Untaggable": {"checkbox": untaggable}}
    if not untaggable:
        props["Motivation Tag"] = {"select": {"name": motivation_tag}}
        props["Sentiment Tag"]  = {"select": {"name": sentiment_tag}}
        props["Subject Tag"]    = {"multi_select": [{"name": t} for t in subject_tags[:2] if t]}
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers(),
        json={"properties": props},
        timeout=30,
    )
    return r.status_code == 200


def tag_untagged_rows(db_id, subject_tags, run_id):
    """
    Fetch all currently untagged rows and tag them.
    Called after each platform completes — isolates failures per platform.
    """
    rows = fetch_untagged_rows(db_id)
    if not rows:
        return 0

    log(run_id, f"Tagging {len(rows)} rows")
    tagged = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch    = rows[i: i + BATCH_SIZE]
        comments = [
            {"id": r["id"], "text": extract_text(r)}
            for r in batch if extract_text(r)
        ]
        if not comments:
            continue

        try:
            results = tag_batch(comments, subject_tags)
            tag_map = {t["id"]: t for t in results}

            for comment in comments:
                tag = tag_map.get(comment["id"])
                if not tag:
                    continue
                is_untaggable = tag.get("motivation_tag") == "Untaggable"
                ok = update_row_tags(
                    comment["id"],
                    tag.get("motivation_tag", ""),
                    tag.get("sentiment_tag", ""),
                    tag.get("subject_tags", []),
                    untaggable=is_untaggable,
                )
                if ok:
                    tagged += 1
                time.sleep(0.34)

            log(run_id, f"Tagged {tagged} / {len(rows)} rows")
            time.sleep(1)

        except Exception as e:
            log(run_id, f"Tagging batch {i // BATCH_SIZE + 1} failed: {e}")

    return tagged


# ──────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────

def run_pipeline(run_id, form_data):
    try:
        brand_name   = form_data["brand_name"]
        page_id      = form_data["notion_page_id"].replace("-", "")
        tier         = form_data.get("tier", "standard")
        platforms    = form_data.get("platforms", [])
        subject_tags = [t.strip() for t in form_data.get("subject_tags", "").split(",") if t.strip()]
        reddit_term  = form_data.get("reddit_term", "")

        log(run_id, f"Pipeline started — {brand_name} | {tier} | platforms: {platforms}")

        # ── Create Notion databases ──
        update_run(run_id, phase="Creating Notion databases")
        db_id, db_url = create_comment_database(page_id, brand_name, subject_tags, run_id)
        update_run(run_id, database_url=db_url)

        reddit_thread_db_id = None
        if "reddit" in platforms:
            reddit_thread_db_id, _ = create_reddit_thread_database(page_id, brand_name, run_id)

        total_written = 0
        total_tagged  = 0

        # ── YouTube ──
        if "youtube" in platforms:
            update_run(run_id, phase="Scraping YouTube")
            max_yt   = 2000 if tier == "deep" else 500
            yt_items = fetch_youtube_comments(form_data.get("youtube_url", ""), max_yt, run_id)
            written  = 0
            for item in yt_items:
                ok, _ = write_comment_row(db_id, item, "YouTube")
                if ok:
                    written += 1
                update_run(run_id, items_written=total_written + written)
                time.sleep(0.34)
            total_written += written
            log(run_id, f"YouTube: {written} rows written")

            update_run(run_id, phase="Tagging YouTube comments")
            tagged = tag_untagged_rows(db_id, subject_tags, run_id)
            total_tagged += tagged
            update_run(run_id, items_tagged=total_tagged)

        # ── Reddit ──
        if "reddit" in platforms:
            update_run(run_id, phase="Scraping Reddit")
            actor_id, actor_input = build_actor_config("reddit", form_data, tier)
            try:
                apify_run_id, dataset_id = create_apify_run(actor_id, actor_input, run_id)
                success = wait_for_apify_run(apify_run_id, run_id)
                if success:
                    items = fetch_apify_dataset(dataset_id, run_id)

                    # Split posts → Reddit Thread DB, comments → Comment Dataset
                    posts    = [i for i in items if is_reddit_post(i)]
                    comments = [i for i in items if not is_reddit_post(i) and is_top_level(i)]

                    log(run_id, f"Reddit: {len(posts)} threads, {len(comments)} comments")

                    # Write threads
                    if reddit_thread_db_id:
                        thread_written = 0
                        for post in posts:
                            ok = write_reddit_thread_row(reddit_thread_db_id, post, reddit_term)
                            if ok:
                                thread_written += 1
                            time.sleep(0.34)
                        log(run_id, f"Reddit: {thread_written} threads written")

                    # Write comments. For Reddit posts with no body, use title as fallback text.
                    comment_written = 0
                    for item in comments:
                        # Title+body concatenation for posts that appear as comments
                        if not item.get("comment") and not item.get("text") and not item.get("body"):
                            title = str(item.get("title", ""))
                            if title:
                                item["comment"] = title
                        ok, _ = write_comment_row(db_id, item, "Reddit")
                        if ok:
                            comment_written += 1
                        update_run(run_id, items_written=total_written + comment_written)
                        time.sleep(0.34)
                    total_written += comment_written
                    log(run_id, f"Reddit: {comment_written} comments written")

                    update_run(run_id, phase="Tagging Reddit comments")
                    tagged = tag_untagged_rows(db_id, subject_tags, run_id)
                    total_tagged += tagged
                    update_run(run_id, items_tagged=total_tagged)

            except Exception as e:
                log(run_id, f"Reddit scraping failed: {e}")

        # ── All other Apify platforms ──
        apify_platforms = [p for p in platforms if p not in ("youtube", "reddit", "forum")]

        for platform in apify_platforms:
            actor_id, actor_input = build_actor_config(platform, form_data, tier)
            if not actor_id:
                log(run_id, f"No actor configured for: {platform}")
                continue

            label = PLATFORM_LABELS.get(platform, platform)
            update_run(run_id, phase=f"Scraping {label}")

            try:
                apify_run_id, dataset_id = create_apify_run(actor_id, actor_input, run_id)
            except Exception as e:
                log(run_id, f"Failed to start Apify run for {label}: {e}")
                continue

            success = wait_for_apify_run(apify_run_id, run_id)
            if not success:
                log(run_id, f"Apify run failed for {label} — skipping")
                continue

            update_run(run_id, phase=f"Retrieving {label} data")
            items     = fetch_apify_dataset(dataset_id, run_id)
            top_level = [i for i in items if is_top_level(i)]
            log(run_id, f"{label}: {len(items)} total, {len(top_level)} top-level")

            update_run(run_id, phase=f"Writing {label} to Notion")
            written = 0
            for item in top_level:
                ok, _ = write_comment_row(db_id, item, label)
                if ok:
                    written += 1
                update_run(run_id, items_written=total_written + written)
                time.sleep(0.34)
            total_written += written
            log(run_id, f"{label}: {written} rows written")

            update_run(run_id, phase=f"Tagging {label} comments")
            tagged = tag_untagged_rows(db_id, subject_tags, run_id)
            total_tagged += tagged
            update_run(run_id, items_tagged=total_tagged)

        # ── Forum ──
        if "forum" in platforms:
            update_run(run_id, phase="Scraping forums")
            forum_urls = parse_forum_urls(form_data.get("forum_urls", ""))

            if not forum_urls:
                log(run_id, "Forum: no URLs provided — skipping")
            else:
                log(run_id, f"Forum: crawling {len(forum_urls)} URL(s)")
                try:
                    apify_run_id, dataset_id = create_apify_run(
                        "apify/website-content-crawler",
                        {
                            "startUrls":     [{"url": u} for u in forum_urls],
                            "maxCrawlPages": len(forum_urls) * 5,
                            "crawlerType":   "cheerio",
                            "maxCrawlDepth": 1,
                        },
                        run_id,
                    )
                    success = wait_for_apify_run(apify_run_id, run_id)
                    if success:
                        crawled_pages  = fetch_apify_dataset(dataset_id, run_id)
                        log(run_id, f"Forum: {len(crawled_pages)} pages crawled")
                        written = 0
                        for page in crawled_pages:
                            raw_text   = page.get("text") or page.get("markdown") or ""
                            source_url = page.get("url", "")
                            if not raw_text or not source_url:
                                continue
                            update_run(run_id, phase=f"Extracting comments from {source_url[:60]}…")
                            comments = extract_forum_comments(raw_text, source_url, run_id)
                            for comment in comments:
                                ok, _ = write_comment_row(db_id, comment, "Forum")
                                if ok:
                                    written += 1
                                update_run(run_id, items_written=total_written + written)
                                time.sleep(0.34)
                        total_written += written
                        log(run_id, f"Forum: {written} rows written")

                        update_run(run_id, phase="Tagging forum comments")
                        tagged = tag_untagged_rows(db_id, subject_tags, run_id)
                        total_tagged += tagged
                        update_run(run_id, items_tagged=total_tagged)

                except Exception as e:
                    log(run_id, f"Forum scraping failed: {e}")

        log(run_id, f"Collection complete — {total_written} rows, {total_tagged} tagged")

        # ── Google search volume (Standard and Deep only) ──
        if tier in ("standard", "deep"):
            update_run(run_id, phase="Fetching search volume")
            volume_data = fetch_search_volume(brand_name, run_id)
            if volume_data:
                write_search_volume_to_notion(page_id, volume_data, run_id)

        # ── Author database (end of pipeline) ──
        if total_written > 0:
            update_run(run_id, phase="Building Authors database")
            author_db_id, _ = create_author_database(page_id, brand_name, run_id)
            build_author_database(db_id, author_db_id, run_id)

        update_run(run_id,
            status="complete",
            phase="Done",
            completed_at=datetime.now().isoformat(),
        )
        log(run_id, "Pipeline complete. Open Claude Chat for analysis.")

    except Exception as e:
        log(run_id, f"Pipeline error: {e}")
        update_run(run_id, status="error", error=str(e), phase="Error")


# ──────────────────────────────────────────────────────────────
# Flask routes
# ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/run", methods=["POST"])
def start_run():
    data   = request.json
    run_id = str(uuid.uuid4())
    runs[run_id] = {
        "status":        "running",
        "phase":         "Starting",
        "items_written":  0,
        "items_tagged":   0,
        "database_url":   None,
        "error":          None,
        "log":            [],
        "started_at":     datetime.now().isoformat(),
        "completed_at":   None,
    }
    threading.Thread(target=run_pipeline, args=(run_id, data), daemon=True).start()
    return jsonify({"run_id": run_id})


@app.route("/status/<run_id>")
def get_status(run_id):
    run = runs.get(run_id)
    if not run:
        return jsonify({"error": "Run not found"}), 404
    return jsonify(run)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"\nVillage Hall Signal Audit Tool")
    print(f"Open http://localhost:{port} in your browser\n")
    app.run(debug=False, host="0.0.0.0", port=port)
