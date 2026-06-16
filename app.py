"""
app.py — The Village Hall Signal Audit Tool
Platforms: YouTube (Data API v3) · TikTok (Apify clockworks/tiktok-comments-scraper)
Output: CSV download (Notion dependency removed)
"""

import csv
import io
import json
import os
import threading
import time
import uuid
from datetime import date, datetime

import anthropic
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, make_response, request, send_from_directory

load_dotenv()

app = Flask(__name__, template_folder="templates")

APIFY_API_KEY     = os.getenv("APIFY_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY", "")

BATCH_SIZE       = 40
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Six-tag motivation system (canonical — see Signal Scoring Framework doc)
MOTIVATION_TAGS = ["Praise", "Criticism", "Question", "Suggestion", "Comparison", "Sharing"]

MOTIVATION_DEFINITIONS = """
- Praise: positive judgment of the brand, content, or product ("love this", "incredible", "this changed everything for me")
- Criticism: negative judgment, not necessarily from personal experience ("this is wrong", "bad idea", "disagree with this approach")
- Question: seeking information or clarification ("how do I", "what temperature", "does this work for")
- Suggestion: directive or prescriptive ("you should do X", "please add Y", "it would be better if")
- Comparison: placing the brand alongside another ("reminds me of", "better than", "similar to", "the X version of Y")
- Sharing: active peer referral — tagging others to see this ("@name you need to see this", "sending this to everyone")
""".strip()

SENTIMENT_TAGS = ["Positive", "Negative", "Neutral", "Mixed"]

CSV_COLUMNS = [
    "comment", "platform", "author", "published_date",
    "like_count", "reply_count", "has_replies",
    "source_url", "comment_url", "item_title",
    "motivation_tag", "sentiment_tag", "subject_tags", "untaggable",
]

runs = {}


# ──────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────

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


def normalize_comment(item, platform):
    """Normalise a raw Apify / YouTube comment dict into a consistent flat structure."""
    text = (
        item.get("comment") or item.get("text") or item.get("body") or
        item.get("content") or ""
    )
    text = str(text)[:2000]

    author = str(
        item.get("author") or item.get("authorText") or
        item.get("userName") or item.get("user") or ""
    )[:200]

    pub = str(
        item.get("publishedAt") or item.get("publishedTimeText") or
        item.get("date") or item.get("at") or ""
    )[:100]

    like_count = safe_int(
        item.get("likeCount") or item.get("voteCount") or
        item.get("likes") or item.get("thumbsUpCount") or 0
    ) or 0

    reply_count = safe_int(
        item.get("replyCount") or item.get("repliesCount") or 0
    ) or 0

    source_url = safe_url(
        item.get("pageUrl") or item.get("url") or
        item.get("videoUrl") or item.get("sourceUrl") or ""
    ) or ""

    comment_url = safe_url(
        item.get("commentUrl") or item.get("itemUrl") or ""
    ) or ""

    item_title = str(item.get("videoTitle") or item.get("title") or "")[:200]

    return {
        "comment":        text,
        "platform":       platform,
        "author":         author,
        "published_date": pub,
        "like_count":     like_count,
        "reply_count":    reply_count,
        "has_replies":    reply_count > 0,
        "source_url":     source_url,
        "comment_url":    comment_url,
        "item_title":     item_title,
        "motivation_tag": "",
        "sentiment_tag":  "",
        "subject_tags":   "",
        "untaggable":     False,
    }


# ──────────────────────────────────────────────────────────────
# YouTube Data API v3
# ──────────────────────────────────────────────────────────────

def fetch_youtube_comments(channel_input, max_items, run_id):
    """
    Collect top-level comments via YouTube Data API v3.
    Flow: resolve handle → uploads playlist → video IDs → commentThreads (order=time).
    Fetches most-recent videos, most-recent comments first within each video.
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
        log(run_id, f"YouTube: resolved → {uploads_playlist_id}")
    except Exception as e:
        log(run_id, f"YouTube: channel resolution failed — {e}")
        return []

    # 2. Get recent video IDs from uploads playlist (already newest-first)
    video_ids     = []
    next_page     = None
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
                "key":        YOUTUBE_API_KEY,
                "videoId":    video_id,
                "part":       "snippet",
                "maxResults": 100,
                "order":      "time",
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
                tl         = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                all_comments.append({
                    "comment":     tl.get("textOriginal") or tl.get("textDisplay", ""),
                    "author":      tl.get("authorDisplayName", ""),
                    "publishedAt": tl.get("publishedAt", ""),
                    "likeCount":   tl.get("likeCount", 0),
                    "replyCount":  item["snippet"].get("totalReplyCount", 0),
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
    if item.get("repliesToId") or item.get("parentId") or item.get("replyTo") or item.get("isReply"):
        return False
    if str(item.get("type", "")).lower() == "reply":
        return False
    return True


def fetch_tiktok_comments(handle, max_items, run_id):
    """
    clockworks/tiktok-comments-scraper.
    Input: profile handle → actor discovers recent videos automatically.
    """
    handle = handle.lstrip("@")
    actor_input = {
        "profiles":             [handle],
        "profileSorting":       "latest",
        "commentsPerPost":      max_items,
        "postsPerProfile":      30,
        "maxRepliesPerComment": 20,
    }

    try:
        apify_run_id, dataset_id = create_apify_run(
            "clockworks/tiktok-comments-scraper", actor_input, run_id
        )
    except Exception as e:
        log(run_id, f"TikTok: failed to start Apify run — {e}")
        return []

    if not wait_for_apify_run(apify_run_id, run_id):
        log(run_id, "TikTok: Apify run failed")
        return []

    items     = fetch_apify_dataset(dataset_id, run_id)
    top_level = [i for i in items if is_top_level(i)]
    log(run_id, f"TikTok: {len(items)} total, {len(top_level)} top-level")
    return top_level[:max_items]


# ──────────────────────────────────────────────────────────────
# Tagging — in memory
# ──────────────────────────────────────────────────────────────

def tag_batch(comments, subject_tags):
    """Send a batch to Claude Haiku for tagging. Returns list of tag dicts."""
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


def tag_comments_in_memory(all_comments, subject_tags, run_id):
    """Tag all comments in memory. Mutates each dict in all_comments in place."""
    tagged = 0
    for i in range(0, len(all_comments), BATCH_SIZE):
        batch       = all_comments[i:i + BATCH_SIZE]
        batch_input = [
            {"id": str(i + j), "text": c["comment"][:400]}
            for j, c in enumerate(batch)
            if c.get("comment")
        ]
        if not batch_input:
            continue

        try:
            results = tag_batch(batch_input, subject_tags)
            tag_map = {r["id"]: r for r in results}

            for j in range(len(batch)):
                tag = tag_map.get(str(i + j))
                if not tag:
                    continue
                is_untaggable          = tag.get("motivation_tag") == "Untaggable"
                batch[j]["untaggable"]     = is_untaggable
                batch[j]["motivation_tag"] = "" if is_untaggable else tag.get("motivation_tag", "")
                batch[j]["sentiment_tag"]  = "" if is_untaggable else tag.get("sentiment_tag", "")
                # Pipe-separated for CSV compatibility
                batch[j]["subject_tags"]   = "" if is_untaggable else "|".join(tag.get("subject_tags", []))
                tagged += 1

            update_run(run_id, items_tagged=tagged)
            log(run_id, f"Tagged {tagged} / {len(all_comments)}")
            time.sleep(1)

        except Exception as e:
            log(run_id, f"Tagging batch {i // BATCH_SIZE + 1} failed: {e}")

    return tagged


# ──────────────────────────────────────────────────────────────
# CSV generation
# ──────────────────────────────────────────────────────────────

def generate_csv(comments):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for c in comments:
        writer.writerow(c)
    return output.getvalue()


# ──────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────

def run_pipeline(run_id, form_data):
    try:
        brand_name   = form_data["brand_name"]
        platforms    = form_data.get("platforms", [])
        subject_tags = [t.strip() for t in form_data.get("subject_tags", "").split(",") if t.strip()]
        youtube_max  = int(form_data.get("youtube_max_comments", 5000))
        tiktok_max   = int(form_data.get("tiktok_max_comments", 1000))

        log(run_id, f"Pipeline started — {brand_name} | platforms: {platforms}")

        all_comments = []

        # ── YouTube ──
        if "youtube" in platforms:
            handle = form_data.get("youtube_handle", "").strip()
            if not handle:
                log(run_id, "YouTube: no handle provided — skipping")
            else:
                update_run(run_id, phase="Scraping YouTube")
                items = fetch_youtube_comments(handle, youtube_max, run_id)
                for item in items:
                    all_comments.append(normalize_comment(item, "YouTube"))
                update_run(run_id, items_written=len(all_comments))
                log(run_id, f"YouTube: {len(items)} comments normalised")

        # ── TikTok ──
        if "tiktok" in platforms:
            handle = form_data.get("tiktok_handle", "").strip()
            if not handle:
                log(run_id, "TikTok: no handle provided — skipping")
            else:
                update_run(run_id, phase="Scraping TikTok")
                items = fetch_tiktok_comments(handle, tiktok_max, run_id)
                for item in items:
                    all_comments.append(normalize_comment(item, "TikTok"))
                update_run(run_id, items_written=len(all_comments))
                log(run_id, f"TikTok: {len(items)} comments normalised")

        log(run_id, f"Collection complete — {len(all_comments)} comments")

        # ── Tag all in memory ──
        update_run(run_id, phase="Tagging")
        tag_comments_in_memory(all_comments, subject_tags, run_id)

        # ── Generate CSV ──
        update_run(run_id, phase="Generating CSV")
        slug     = "".join(c if c.isalnum() or c == "-" else "-" for c in brand_name.lower().replace(" ", "-"))
        filename = f"{slug}-signal-audit-{date.today()}.csv"
        runs[run_id]["csv_data"]     = generate_csv(all_comments)
        runs[run_id]["csv_filename"] = filename

        update_run(run_id,
            status="complete",
            phase="Done",
            completed_at=datetime.now().isoformat(),
        )
        log(run_id, f"Complete — {len(all_comments)} comments. CSV ready.")

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
        "items_written": 0,
        "items_tagged":  0,
        "csv_data":      None,
        "csv_filename":  None,
        "error":         None,
        "log":           [],
        "started_at":    datetime.now().isoformat(),
        "completed_at":  None,
    }
    threading.Thread(target=run_pipeline, args=(run_id, data), daemon=True).start()
    return jsonify({"run_id": run_id})


@app.route("/status/<run_id>")
def get_status(run_id):
    run = runs.get(run_id)
    if not run:
        return jsonify({"error": "Run not found"}), 404
    # Strip csv_data — can be large, not needed for status polling
    return jsonify({k: v for k, v in run.items() if k != "csv_data"})


@app.route("/download/<run_id>")
def download_csv(run_id):
    run = runs.get(run_id)
    if not run or not run.get("csv_data"):
        return jsonify({"error": "CSV not ready or run not found"}), 404
    response = make_response(run["csv_data"])
    response.headers["Content-Type"]        = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f'attachment; filename="{run["csv_filename"]}"'
    return response


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"\nVillage Hall Signal Audit Tool — CSV mode")
    print(f"Open http://localhost:{port} in your browser\n")
    app.run(debug=False, host="0.0.0.0", port=port)
