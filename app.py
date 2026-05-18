"""
app.py — The Village Hall Signal Audit Tool
Runs the full pipeline: Apify scrape → Notion write → Claude tagging
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

APIFY_API_KEY   = os.getenv("APIFY_API_KEY", "")
NOTION_API_KEY  = os.getenv("NOTION_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
NOTION_VERSION  = "2022-06-28"
BATCH_SIZE      = 80

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


# ──────────────────────────────────────────────────────────────
# Apify
# ──────────────────────────────────────────────────────────────

def create_apify_run(actor_id, actor_input, run_id):
    actor_id = actor_id.replace("/", "~")  # Apify REST API requires ~ not /
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
            params={"token": APIFY_API_KEY, "offset": offset, "limit": limit, "clean": True},
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
    max_items = 2000 if tier == "deep" else 500

    if platform == "youtube":
        return "streamers~youtube-comments-scraper", {
            "startUrls": form_data.get("youtube_video_urls", []),
            "maxComments": max_items // 20,  # split across videos
            "sortCommentsBy": "NEWEST_FIRST",
        }

    elif platform == "youtube_discover":
        # Phase 1: discover video URLs from channel
        posts = 20 if tier == "deep" else 10
        return "streamers~youtube-scraper", {
            "startUrls": [{"url": form_data.get("youtube_url", "")}],
            "maxResults": posts,
            "sortVideosBy": "NEWEST",
        }

    elif platform == "tiktok":
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
        return "automation-lab~trustpilot", {
            "companyUrls": [form_data.get("trustpilot_url", "")],
            "maxReviewsPerCompany": max_items,
            "sort": "recency",
        }

    return None, None


# ──────────────────────────────────────────────────────────────
# Notion
# ──────────────────────────────────────────────────────────────

def create_comment_database(brand_page_id, brand_name, subject_tags, run_id):
    today = date.today().strftime("%Y-%m-%d")
    subject_options = [{"name": t} for t in subject_tags if t]

    payload = {
        "parent": {"page_id": brand_page_id},
        "title": [{"text": {"content": f"Comment Dataset — {brand_name} — {today}"}}],
        "properties": {
            "Comment": {"title": {}},
            "Platform": {
                "select": {
                    "options": [
                        {"name": "YouTube"}, {"name": "TikTok"}, {"name": "Reddit"},
                        {"name": "App Store"}, {"name": "Play Store"},
                        {"name": "Trustpilot"}, {"name": "Forum"},
                    ]
                }
            },
            "Source URL":    {"url": {}},
            "Item URL":      {"url": {}},
            "Published Date":{"rich_text": {}},
            "Author":        {"rich_text": {}},
            "Reply Count":   {"number": {}},
            "Like Count":    {"number": {}},
            "Item Title":    {"rich_text": {}},
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

    source_url = safe_url(item.get("pageUrl") or item.get("url") or item.get("videoUrl"))
    if source_url:
        props["Source URL"] = {"url": source_url}

    item_url = safe_url(item.get("commentUrl") or item.get("replyUrl"))
    if item_url:
        props["Item URL"] = {"url": item_url}

    pub = str(item.get("publishedTimeText") or item.get("date") or item.get("publishedAt") or item.get("at") or "")
    if pub:
        props["Published Date"] = {"rich_text": [{"text": {"content": pub[:100]}}]}

    author = str(item.get("authorText") or item.get("author") or item.get("userName") or item.get("user") or "")
    if author:
        props["Author"] = {"rich_text": [{"text": {"content": author[:200]}}]}

    rc = safe_int(item.get("replyCount") or item.get("repliesCount"))
    if rc is not None:
        props["Reply Count"] = {"number": rc}

    lc = safe_int(item.get("voteCount") or item.get("likes") or item.get("thumbsUpCount") or item.get("helpfulCount"))
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
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def update_row_tags(page_id, motivation_tag, subject_tag, untaggable=False):
    props = {
        "Untaggable": {"checkbox": untaggable},
    }
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
        brand_name    = form_data["brand_name"]
        page_id       = form_data["notion_page_id"].replace("-", "")
        tier          = form_data.get("tier", "standard")
        platforms     = form_data.get("platforms", [])
        subject_tags  = [t.strip() for t in form_data.get("subject_tags", "").split(",") if t.strip()]

        log(run_id, f"Pipeline started — {brand_name} | {tier} | platforms: {platforms}")

        # Create Notion database
        update_run(run_id, phase="Creating Notion database")
        db_id, db_url = create_comment_database(page_id, brand_name, subject_tags, run_id)
        update_run(run_id, database_url=db_url)

        total_written = 0

        # ── Per-platform scrape ──
        for platform in platforms:

        # ── YouTube: use video URLs directly ──
            if platform == "youtube":
                label = "YouTube"
                raw_urls = form_data.get("youtube_video_urls", "")
                video_urls = [
                    {"url": u.strip()}
                    for u in raw_urls.replace("\n", ",").split(",")
                    if u.strip() and "watch?v=" in u
                ]

                if not video_urls:
                    log(run_id, "No valid YouTube video URLs provided — skipping")
                    continue

                log(run_id, f"Scraping comments from {len(video_urls)} YouTube videos")
                update_run(run_id, phase="Scraping YouTube comments")

                max_items = 2000 if tier == "deep" else 500
                comment_input = {
                    "startUrls": video_urls,
                    "maxComments": max(10, max_items // len(video_urls)),
                    "sortCommentsBy": "NEWEST_FIRST",
                }

                try:
                    apify_run_id, dataset_id = create_apify_run(
                        "streamers~youtube-comments-scraper", comment_input, run_id
                    )
                except Exception as e:
                    log(run_id, f"Failed to start YouTube comments run: {e}")
                    continue

                success = wait_for_apify_run(apify_run_id, run_id)
                if not success:
                    log(run_id, "YouTube comments run failed — skipping")
                    continue

                update_run(run_id, phase="Retrieving YouTube comments")
                items = fetch_apify_dataset(dataset_id, run_id)
                top_level = [i for i in items if is_top_level(i)]
                log(run_id, f"YouTube: {len(items)} items, {len(top_level)} top-level")

                update_run(run_id, phase="Writing YouTube comments to Notion")
                written = 0
                for item in top_level:
                    ok, _ = write_comment_row(db_id, item, label)
                    if ok:
                        written += 1
                    update_run(run_id, items_written=total_written + written)
                    time.sleep(0.34)

                total_written += written
                log(run_id, f"YouTube: {written} rows written")
                continue
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

        log(run_id, f"Collection complete — {total_written} total rows")

        # ── Tagging ──
        if total_written > 0:
            update_run(run_id, phase="Tagging comments via Claude API")
            rows = fetch_untagged_rows(db_id)
            log(run_id, f"Tagging {len(rows)} rows in batches of {BATCH_SIZE}")

            tagged = 0
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i: i + BATCH_SIZE]
                comments = [
                    {"id": r["id"], "text": extract_text(r)}
                    for r in batch if extract_text(r)
                ]
                if not comments:
                    continue

                try:
                    results  = tag_batch(comments, subject_tags)
                    tag_map  = {t["id"]: t for t in results}

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
        "status":       "running",
        "phase":        "Starting",
        "items_written": 0,
        "items_tagged":  0,
        "database_url":  None,
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
    return jsonify(run)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"\nVillage Hall Signal Audit Tool")
    print(f"Open http://localhost:{port} in your browser\n")
    app.run(debug=False, host="0.0.0.0", port=port)
