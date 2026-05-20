"""
app.py — The Village Hall Signal Audit Tool
Runs the full pipeline: scrape → Notion write → Claude tagging

Platforms:
  YouTube    — official YouTube Data API v3
  TikTok     — Apify clockworks/tiktok-comments-scraper
  Reddit     — Apify trudax/reddit-scraper-lite
  App Store  — Apify canadesk/app-store-scraper
  Play Store — Apify canadesk/google-play-scraper
  Trustpilot — Apify automation-lab/trustpilot
  Substack   — Apify epctex/substack-scraper
  Forum      — Apify apify/website-content-crawler + Claude Haiku extraction

Additional:
  Google search volume — DataForSEO API (Standard/Deep only)
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

APIFY_API_KEY       = os.getenv("APIFY_API_KEY", "")
NOTION_API_KEY      = os.getenv("NOTION_API_KEY", "")
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
YOUTUBE_API_KEY     = os.getenv("YOUTUBE_API_KEY", "")


NOTION_VERSION   = "2022-06-28"
BATCH_SIZE       = 40
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"   # 40 comments per tagging batch; 8000 max_tokens avoids truncation

MOTIVATION_TAGS = [
    "Praise", "Criticism", "Question", "Suggestion", "Comparison",
    "Sharing", "Aspiration", "Disillusionment", "Affection", "Self-expression",
]

MOTIVATION_DEFINITIONS = """
- Praise: this is good
- Criticism: this is bad (includes problem reports)
- Question: I want to know something
- Suggestion: here's what you should do
- Comparison: this sits alongside X in my world
- Sharing: active peer referral — tagging others to see this
- Aspiration: I want this / I want to live like this
- Disillusionment: this isn't real / easy for you to say
- Affection: personal attachment to the creator
- Self-expression: I use this to say something about myself or my situation
""".strip()

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


def parse_trustpilot_domain(url_or_domain):
    """Extract bare domain from a Trustpilot review URL, or return as-is."""
    s = url_or_domain.strip()
    if "trustpilot.com/review/" in s:
        return s.split("trustpilot.com/review/")[-1].strip("/")
    return s

def parse_forum_urls(raw):
    """Parse newline-separated forum URLs from form input."""
    if isinstance(raw, list):
        return [u.strip() for u in raw if u.strip()]
    return [u.strip() for u in raw.split("\n") if u.strip()]



def parse_youtube_handle(channel_input):
    """Extract handle or channel ID from a YouTube URL or raw @handle/ID."""
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


# ──────────────────────────────────────────────────────────────
# YouTube Data API v3
# ──────────────────────────────────────────────────────────────

def fetch_youtube_comments(channel_input, max_items, run_id):
    """
    Collect top-level comments from a YouTube channel via the official Data API v3.
    Flow: resolve handle → uploads playlist → video IDs → commentThreads
    Returns items in write_comment_row-compatible format.

    Quota cost: ~1–2 units for channel resolution, ~1 unit per 50 playlist items,
    ~1 unit per 100 comments. A Standard run (~500 comments) costs roughly 50–80 units
    against a 10,000/day free quota.
    """
    if not YOUTUBE_API_KEY:
        log(run_id, "YouTube: YOUTUBE_API_KEY not configured — skipping")
        return []

    handle = parse_youtube_handle(channel_input)

    # ── 1. Resolve to channel ID + uploads playlist ID ──
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
        log(run_id, f"YouTube: resolved channel → uploads playlist {uploads_playlist_id}")
    except Exception as e:
        log(run_id, f"YouTube: channel resolution failed — {e}")
        return []

    # ── 2. Get recent video IDs from uploads playlist ──
    # Uses playlistItems.list (1 unit/page) — never search.list (100 units/call)
    video_ids = []
    next_page = None
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
    log(run_id, f"YouTube: {len(video_ids)} videos to collect comments from")

    # ── 3. Fetch comment threads per video ──
    all_comments = []
    per_video_target = max(max_items // max(len(video_ids), 1), 25)

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
                    break  # Comments disabled on this video — skip silently
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log(run_id, f"YouTube: comment fetch failed for {video_id} — {e}")
                break

            for item in data.get("items", []):
                tl = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                all_comments.append({
                    "comment":     tl.get("textOriginal") or tl.get("textDisplay", ""),
                    "author":      tl.get("authorDisplayName", ""),
                    "publishedAt": tl.get("publishedAt", ""),
                    "likeCount":   tl.get("likeCount", 0),
                    "replyCount":  item["snippet"].get("totalReplyCount", 0),
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
    url = f"https://api.apify.com/v2/actor-runs/{apify_run_id}"
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        r = requests.get(url, params={"token": APIFY_API_KEY}, timeout=30)
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


def is_top_level(item):
    if item.get("parentId") or item.get("replyTo") or item.get("isReply"):
        return False
    if str(item.get("type", "")).lower() == "reply":
        return False
    return True


def build_actor_config(platform, form_data, tier):
    """
    Returns (actor_id, actor_input) for Apify-based platforms.
    YouTube and Forum are handled separately in run_pipeline — not routed here.
    """
    max_items = 2000 if tier == "deep" else 500

    if platform == "tiktok":
        handle = form_data.get("tiktok_handle", "").lstrip("@")
        posts = 20
        return "clockworks/tiktok-comments-scraper", {
            "profiles": [handle],
            "profileSorting": "popular",
            "maxRepliesPerComment": 0,
            "commentsPerPost": max_items // posts,
            "postsPerProfile": posts,
        }

    elif platform == "reddit":
        return "trudax/reddit-scraper-lite", {
            "searches": [form_data.get("reddit_term", "")],
            "maxPostCount": 20,
            "maxCommentCount": max_items,
        }

    elif platform == "appstore":
        return "canadesk/app-store-scraper", {
            "appIds": [form_data.get("appstore_id", "")],
            "maxReviews": max_items,
        }

    elif platform == "playstore":
        return "canadesk/google-play-scraper", {
            "appIds": [form_data.get("playstore_id", "")],
            "maxReviews": max_items,
        }

    elif platform == "trustpilot":
        # Uses automation-lab/trustpilot — takes domain, not full URL
        domain = parse_trustpilot_domain(form_data.get("trustpilot_url", ""))
        return "automation-lab/trustpilot", {
            "domain": domain,
            "maxReviews": max_items,
        }

    elif platform == "substack":
        # Note: input schema may need adjustment on first live test
        return "epctex/substack-scraper", {
            "startUrls": [{"url": form_data.get("substack_url", "")}],
            "maxItems": max_items,
            "includeComments": True,
        }

    return None, None


# ──────────────────────────────────────────────────────────────
# Forum comment extraction (Claude Haiku preprocessing)
# ──────────────────────────────────────────────────────────────

def extract_forum_comments(raw_text, source_url, run_id):
    """
    Use Claude Haiku to extract individual comments from raw forum page text.
    Called once per crawled page after website-content-crawler retrieves it.
    Returns items in write_comment_row-compatible format.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""Extract individual user comments or posts from this forum page.

Return ONLY a valid JSON array. Each object must have:
- "text": the comment text (string, preserve original wording exactly)
- "author": username or display name if visible (string, empty string if not identifiable)
- "position": position in thread as integer (1 = first post or OP, incrementing)

Rules:
- Include only substantive user-written comments — exclude navigation, ads, boilerplate, and repeated headers
- Preserve original wording; do not summarise or paraphrase
- Maximum 100 comments per page
- If you cannot identify any user comments, return an empty array: []
- No preamble, no markdown fences — the JSON array only

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
# Google search volume (DataForSEO)
# ──────────────────────────────────────────────────────────────

def fetch_search_volume(brand_name, run_id):
    """
    Fetch Google result counts for community-intent terms via Apify google-search-scraper.
    Terms: [brand] community / discord / forum / group
    Returns dict {term: results_total} or None on failure.

    resultsTotal is the 'About X results' figure Google shows — a proxy for inbound
    search intent rather than exact monthly volume, but sufficient for scoring purposes.
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

        items = fetch_apify_dataset(dataset_id, run_id)

        results = {}
        for item in items:
            term  = item.get("searchQuery", {}).get("term", "")
            total = item.get("resultsTotal")
            if term:
                results[term] = total

        log(run_id, f"Search volume (result counts): {results}")
        return results

    except Exception as e:
        log(run_id, f"Search volume fetch failed: {e}")
        return None


def write_search_volume_to_notion(brand_page_id, volume_data, run_id):
    """Append a search result count callout block to the brand page in Notion."""
    if not volume_data:
        return

    lines = []
    for term, total in volume_data.items():
        lines.append(f"{term}: ~{total:,} results" if total else f"{term}: no results")
    content = "  ·  ".join(lines)

    payload = {
        "children": [{
            "object": "block",
            "type": "callout",
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
        log(run_id, f"Search volume Notion write failed: {r.status_code} — {r.text[:200]}")


# ──────────────────────────────────────────────────────────────
# Notion
# ──────────────────────────────────────────────────────────────

def create_comment_database(brand_page_id, brand_name, subject_tags, run_id):
    today = date.today().strftime("%Y-%m-%d")
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
            "Item Title":     {"rich_text": {}},
            "Motivation Tag": {
                "select": {"options": [{"name": t} for t in MOTIVATION_TAGS]}
            },
            "Subject Tag": {
                "select": {"options": subject_options}
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
    log(run_id, f"Notion database created: {db_url}")
    return db_id, db_url


def write_comment_row(database_id, item, platform_label):
    text = (
        item.get("comment") or item.get("text") or item.get("body") or
        item.get("content") or item.get("review") or ""
    )
    text = str(text)[:2000]

    props = {
        "Comment":  {"title": [{"text": {"content": text}}]},
        "Platform": {"select": {"name": platform_label}},
    }

    source_url = safe_url(
        item.get("pageUrl") or item.get("url") or
        item.get("videoUrl") or item.get("sourceUrl")
    )
    if source_url:
        props["Source URL"] = {"url": source_url}

    item_url = safe_url(
        item.get("commentUrl") or item.get("itemUrl") or item.get("replyUrl")
    )
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

    rc = safe_int(item.get("replyCount") or item.get("repliesCount"))
    if rc is not None:
        props["Reply Count"] = {"number": rc}

    # likeCount added for YouTube API compatibility
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
# Tagging
# ──────────────────────────────────────────────────────────────

def fetch_untagged_rows(database_id):
    rows, cursor = [], None
    while True:
        payload = {
            "filter": {"property": "Motivation Tag", "select": {"is_empty": True}},
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
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    numbered = "\n".join(
        f"{i+1}. [ID:{c['id']}] {c['text'][:400]}"
        for i, c in enumerate(comments)
    )
    prompt = f"""Tag every comment with exactly one MOTIVATION tag and one SUBJECT tag.

MOTIVATION tags (use exactly as written): {', '.join(MOTIVATION_TAGS)}
Definitions:
{MOTIVATION_DEFINITIONS}

SUBJECT tags (use exactly as written): {', '.join(subject_tags)}

If a comment fits no subject tag closely, use the nearest match.
If a comment cannot be tagged at all, set motivation_tag to "Untaggable" and subject_tag to "Untaggable".

Comments to tag:
{numbered}

Respond ONLY with a valid JSON array. No preamble, no markdown fences.
Required format: [{{"id": "page-id-here", "motivation_tag": "Praise", "subject_tag": "Workouts"}}]"""

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


def update_row_tags(page_id, motivation_tag, subject_tag, untaggable=False):
    props = {"Untaggable": {"checkbox": untaggable}}
    if not untaggable:
        props["Motivation Tag"] = {"select": {"name": motivation_tag}}
        props["Subject Tag"]    = {"select": {"name": subject_tag}}
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers(),
        json={"properties": props},
        timeout=30,
    )
    return r.status_code == 200


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

        log(run_id, f"Pipeline started — {brand_name} | {tier} | platforms: {platforms}")

        # ── Create Notion Comment Dataset database ──
        update_run(run_id, phase="Creating Notion database")
        db_id, db_url = create_comment_database(page_id, brand_name, subject_tags, run_id)
        update_run(run_id, database_url=db_url)

        total_written = 0

        # ── YouTube (official Data API v3, not Apify) ──
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
            log(run_id, f"YouTube: {written} rows written to Notion")

        # ── Apify platforms (TikTok, Reddit, App Store, Play Store, Trustpilot, Substack) ──
        apify_platforms = [p for p in platforms if p not in ("youtube", "forum")]
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
                log(run_id, f"Apify run failed for {label} — skipping platform")
                continue

            update_run(run_id, phase=f"Retrieving {label} data")
            items = fetch_apify_dataset(dataset_id, run_id)
            top_level = [i for i in items if is_top_level(i)]
            log(run_id, f"{label}: {len(items)} total items, {len(top_level)} top-level")

            update_run(run_id, phase=f"Writing {label} data to Notion")
            written = 0
            for item in top_level:
                ok, _ = write_comment_row(db_id, item, label)
                if ok:
                    written += 1
                update_run(run_id, items_written=total_written + written)
                time.sleep(0.34)

            total_written += written
            log(run_id, f"{label}: {written} rows written to Notion")

        # ── Forum (website-content-crawler + Claude Haiku extraction) ──
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
                        crawled_pages = fetch_apify_dataset(dataset_id, run_id)
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
                        log(run_id, f"Forum: {written} rows written to Notion")
                except Exception as e:
                    log(run_id, f"Forum scraping failed: {e}")

        log(run_id, f"Collection complete — {total_written} total rows")

        # ── Google search volume (Standard and Deep only) ──
        if tier in ("standard", "deep"):
            update_run(run_id, phase="Fetching search volume")
            volume_data = fetch_search_volume(brand_name, run_id)
            if volume_data:
                write_search_volume_to_notion(page_id, volume_data, run_id)

        # ── Tagging ──
        if total_written > 0:
            update_run(run_id, phase="Tagging comments via Claude API")
            rows = fetch_untagged_rows(db_id)
            log(run_id, f"Tagging {len(rows)} rows in batches of {BATCH_SIZE}")

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
                            tag.get("subject_tag", ""),
                            untaggable=is_untaggable,
                        )
                        if ok:
                            tagged += 1
                        time.sleep(0.34)

                    update_run(run_id, items_tagged=tagged)
                    log(run_id, f"Tagged {tagged} / {len(rows)} rows")
                    time.sleep(1)

                except Exception as e:
                    log(run_id, f"Tagging batch {i // BATCH_SIZE + 1} failed: {e}")

            log(run_id, f"Tagging complete — {tagged} rows tagged")

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
