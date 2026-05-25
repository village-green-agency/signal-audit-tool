"""
app.py — The Village Hall Signal Audit Tool
Platforms: YouTube (Data API v3) · TikTok (Apify clockworks/tiktok-comments-scraper)
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

# Six-tag motivation system (canonical — see Signal Scoring Framework doc)
MOTIVATION_TAGS = [
    "Praise", "Criticism", "Question", "Suggestion", "Comparison", "Sharing",
]

MOTIVATION_DEFINITIONS = """
- Praise: positive judgment of the brand, content, or product ("love this", "incredible", "this changed everything for me")
- Criticism: negative judgment, not necessarily from personal experience ("this is wrong", "bad idea", "disagree with this approach")
- Question: seeking information or clarification ("how do I", "what temperature", "does this work for")
- Suggestion: directive or prescriptive ("you should do X", "please add Y", "it would be better if")
- Comparison: placing the brand alongside another ("reminds me of", "better than", "similar to", "the X version of Y")
- Sharing: active peer referral — tagging others to see this ("@name you need to see this", "sending this to everyone")
""".strip()

SENTIMENT_TAGS = ["Positive", "Negative", "Neutral", "Mixed"]

runs = {}


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
    runs[run_id].setdefault("log", []).append(f"[{ts}] {message}")
    print(f"[{run_id[:8]}] {message}")

def safe_url(value):
    v = str(value).strip() if value else ""
    return v if v.startswith("http") else None

def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def parse_youtube_handle(raw):
    s = raw.strip()
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
    Collect top-level comments via YouTube Data API v3.
    Flow: resolve handle → uploads playlist → video IDs → commentThreads (order=time).
    Fetches from the most recent videos, most recent comments first within each video.
    Quota: ~2–3 units per video. Limit is 10,000 units/day.
    """
    if not YOUTUBE_API_KEY:
        log(run_id, "YouTube: YOUTUBE_API_KEY not set — skipping")
        return []

    handle = parse_youtube_handle(channel_input)

    # 1. Resolve handle → uploads playlist ID
    try:
        params = {"key": YOUTUBE_API_KEY, "part": "contentDetails"}
        if handle.startswith("UC") and len(handle) == 24:
            params["id"] = handle
        else:
            params["forHandle"] = handle

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

    # 2. Get recent video IDs from uploads playlist (playlist is already newest-first)
    video_ids = []
    next_page = None
    target_videos = max(20, max_items // 100)

    while len(video_ids) < target_videos:
        params = {
            "key":        YOUTUBE_API_KEY,
            "playlistId": uploads_playlist_id,
            "part":       "contentDetails",
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
    log(run_id, f"YouTube: {len(video_ids)} videos to scrape")

    # 3. Fetch comment threads per video, ordered by time (most recent first)
    all_comments     = []
    per_video_target = max(max_items // max(len(video_ids), 1), 25)

    for video_id in video_ids:
        if len(all_comments) >= max_items:
            break

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        next_page = None
        vid_count = 0

        while vid_count < per_video_target:
            params = {
                "key":       YOUTUBE_API_KEY,
                "videoId":   video_id,
                "part":      "snippet",
                "maxResults": 100,
                "order":     "time",   # most recent comments first
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
                tl          = item["snippet"]["topLevelComment"]["snippet"]
                comment_id  = item["id"]
                reply_count = item["snippet"].get("totalReplyCount", 0)
                all_comments.append({
                    "comment":     tl.get("textOriginal") or tl.get("textDisplay", ""),
                    "author":      tl.get("authorDisplayName", ""),
                    "publishedAt": tl.get("publishedAt", ""),
                    "likeCount":   tl.get("likeCount", 0),
                    "replyCount":  reply_count,
                    "videoUrl":    video_url,
                    "commentUrl":  f"{video_url}&lc={comment_id}",
                })
                vid_count += 1

            next_page = data.get("nextPageToken")
            if not next_page:
                break

        time.sleep(0.1)

    result = all_comments[:max_items]
    log(run_id, f"YouTube: {len(result)} comments collected")
    return result


# ──────────────────────────────────────────────────────────────
# Apify — TikTok
# ──────────────────────────────────────────────────────────────

def create_apify_run(actor_id, actor_input, run_id):
    slug = actor_id.replace("/", "~")
    r = requests.post(
        f"https://api.apify.com/v2/acts/{slug}/runs",
        params={"token": APIFY_API_KEY},
        json=actor_input,   # Raw body — no {"input": ...} wrapper
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()["data"]
    log(run_id, f"Apify run started — {actor_id} | {data['id']}")
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
            log(run_id, f"Apify run {apify_run_id} ended: {status}")
            return False
        log(run_id, f"Apify: {status} — checking in 30s")
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


def fetch_tiktok_comments(handle, max_items, run_id):
    """
    clockworks/tiktok-comments-scraper.
    profileSorting=latest → pulls from most recent posts first.
    """
    posts = 20
    actor_input = {
        "profiles":            [handle.lstrip("@")],
        "profileSorting":      "latest",   # most recent posts first
        "maxRepliesPerComment": 0,
        "commentsPerPost":     max(1, max_items // posts),
        "postsPerProfile":     posts,
    }

    try:
        apify_run_id, dataset_id = create_apify_run(
            "clockworks/tiktok-comments-scraper", actor_input, run_id
        )
    except Exception as e:
        log(run_id, f"TikTok: failed to start Apify run — {e}")
        return []

    success = wait_for_apify_run(apify_run_id, run_id)
    if not success:
        log(run_id, "TikTok: Apify run failed")
        return []

    items     = fetch_apify_dataset(dataset_id, run_id)
    top_level = [i for i in items if is_top_level(i)]
    log(run_id, f"TikTok: {len(items)} total, {len(top_level)} top-level")
    return top_level[:max_items]


# ──────────────────────────────────────────────────────────────
# Notion — Comment Dataset
# ──────────────────────────────────────────────────────────────

def create_comment_database(brand_page_id, brand_name, subject_tags, run_id):
    today           = date.today().strftime("%Y-%m-%d")
    subject_options = [{"name": t} for t in subject_tags if t]

    payload = {
        "parent": {"page_id": brand_page_id},
        "title":  [{"text": {"content": f"Comment Dataset — {brand_name} — {today}"}}],
        "properties": {
            "Comment":        {"title": {}},
            "Platform":       {"select": {"options": [{"name": "YouTube"}, {"name": "TikTok"}]}},
            "Source URL":     {"url": {}},
            "Item URL":       {"url": {}},
            "Published Date": {"rich_text": {}},
            "Author":         {"rich_text": {}},
            "Reply Count":    {"number": {}},
            "Like Count":     {"number": {}},
            "Has Replies":    {"checkbox": {}},
            "Item Title":     {"rich_text": {}},
            "Motivation Tag": {"select": {"options": [{"name": t} for t in MOTIVATION_TAGS]}},
            "Sentiment Tag":  {"select": {"options": [{"name": t} for t in SENTIMENT_TAGS]}},
            "Subject Tag":    {"multi_select": {"options": subject_options}},
            "Untaggable":     {"checkbox": {}},
            "Note":           {"rich_text": {}},
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

    props = {
        "Comment":     {"title": [{"text": {"content": text}}]},
        "Platform":    {"select": {"name": platform_label}},
        "Has Replies": {"checkbox": reply_count > 0},
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
        item.get("author") or item.get("authorText") or
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
# Notion — Authors database
# ──────────────────────────────────────────────────────────────

def create_author_database(brand_page_id, brand_name, run_id):
    today = date.today().strftime("%Y-%m-%d")
    payload = {
        "parent": {"page_id": brand_page_id},
        "title":  [{"text": {"content": f"Authors — {brand_name} — {today}"}}],
        "properties": {
            "Author":           {"title": {}},
            "Platform":         {"rich_text": {}},
            "Comment Count":    {"number": {}},
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
    log(run_id, "Building Authors database")

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

    author_map = {}
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

    log(run_id, f"Authors: {len(author_map)} unique author/platform pairs")

    sorted_authors = sorted(
        author_map.items(), key=lambda x: x[1]["comment_count"], reverse=True
    )
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
# Tagging — runs immediately after each platform completes
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
    client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    numbered = "\n".join(
        f"{i+1}. [ID:{c['id']}] {c['text'][:400]}"
        for i, c in enumerate(comments)
    )
    prompt = f"""Tag every comment with one MOTIVATION tag, one SENTIMENT tag, and 1–2 SUBJECT tags.

MOTIVATION tags (pick exactly one):
{', '.join(MOTIVATION_TAGS)}

Definitions:
{MOTIVATION_DEFINITIONS}

SENTIMENT tags (pick exactly one): Positive, Negative, Neutral, Mixed

SUBJECT tags (pick 1–2 from this list only):
{', '.join(subject_tags)}

Rules:
- motivation_tag: single tag, dominant motivation only
- sentiment_tag: single tag
- subject_tags: array of 1–2; use nearest match if no exact fit
- If a comment cannot be meaningfully tagged, set motivation_tag and sentiment_tag to "Untaggable" and subject_tags to []

Comments:
{numbered}

Respond ONLY with a valid JSON array. No preamble, no markdown fences.
Format: [{{"id": "page-id", "motivation_tag": "Praise", "sentiment_tag": "Positive", "subject_tags": ["Recipes"]}}]"""

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


def tag_platform_rows(db_id, subject_tags, run_id):
    """Fetch all untagged rows and tag them. Called after each platform writes."""
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
        platforms    = form_data.get("platforms", [])
        subject_tags = [t.strip() for t in form_data.get("subject_tags", "").split(",") if t.strip()]

        youtube_max = int(form_data.get("youtube_max_comments", 5000))
        tiktok_max  = int(form_data.get("tiktok_max_comments", 1000))

        log(run_id, f"Pipeline started — {brand_name} | platforms: {platforms}")

        update_run(run_id, phase="Creating Notion database")
        db_id, db_url = create_comment_database(page_id, brand_name, subject_tags, run_id)
        update_run(run_id, database_url=db_url)

        total_written = 0
        total_tagged  = 0

        # ── YouTube ──
        if "youtube" in platforms:
            handle = form_data.get("youtube_handle", "").strip()
            if not handle:
                log(run_id, "YouTube: no handle provided — skipping")
            else:
                update_run(run_id, phase="Scraping YouTube")
                items   = fetch_youtube_comments(handle, youtube_max, run_id)
                written = 0
                for item in items:
                    ok, _ = write_comment_row(db_id, item, "YouTube")
                    if ok:
                        written += 1
                    update_run(run_id, items_written=total_written + written)
                    time.sleep(0.34)
                total_written += written
                log(run_id, f"YouTube: {written} rows written")

                update_run(run_id, phase="Tagging YouTube comments")
                tagged        = tag_platform_rows(db_id, subject_tags, run_id)
                total_tagged += tagged
                update_run(run_id, items_tagged=total_tagged)

        # ── TikTok ──
        if "tiktok" in platforms:
            handle = form_data.get("tiktok_handle", "").strip()
            if not handle:
                log(run_id, "TikTok: no handle provided — skipping")
            else:
                update_run(run_id, phase="Scraping TikTok")
                items   = fetch_tiktok_comments(handle, tiktok_max, run_id)
                written = 0
                for item in items:
                    ok, _ = write_comment_row(db_id, item, "TikTok")
                    if ok:
                        written += 1
                    update_run(run_id, items_written=total_written + written)
                    time.sleep(0.34)
                total_written += written
                log(run_id, f"TikTok: {written} rows written")

                update_run(run_id, phase="Tagging TikTok comments")
                tagged        = tag_platform_rows(db_id, subject_tags, run_id)
                total_tagged += tagged
                update_run(run_id, items_tagged=total_tagged)

        log(run_id, f"Collection complete — {total_written} rows, {total_tagged} tagged")

        # ── Authors database ──
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
