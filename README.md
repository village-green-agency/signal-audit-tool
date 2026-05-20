# Village Hall — Signal Audit Tool

Runs the full data pipeline for a signal audit: scrape → Notion write → tag (per platform) → author aggregation. Leaves Claude Chat free for analysis only.

---

## Environment variables

Set these in Railway (Settings → Variables) or in a local `.env` file:

| Variable | Where to find it |
|---|---|
| `APIFY_API_KEY` | apify.com → Settings → Integrations → API token |
| `NOTION_API_KEY` | notion.so/my-integrations → Village Hall Tool → Internal Integration Secret |
| `ANTHROPIC_API_KEY` | console.anthropic.com → API Keys |
| `YOUTUBE_API_KEY` | Google Cloud Console → APIs & Services → YouTube Data API v3 |

---

## Option A — Run locally

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your four API keys
python app.py
```

Open `http://localhost:5000`. Keep the terminal open during a run.

---

## Option B — Deploy to Railway

1. Push this folder to a GitHub repository
2. Railway: New Project → Deploy from GitHub → select repo
3. Set the four environment variables in Railway → Settings → Variables
4. Railway gives you a public URL — bookmark it

Deployment is automatic on every GitHub push.

---

## What the tool creates in Notion

Three databases are created as children of the brand page on every run:

**Comment Dataset** — one row per top-level comment across all platforms. Fields:
- Comment, Platform, Source URL, Item URL, Published Date, Author
- Reply Count, Like Count, Has Replies (boolean)
- Motivation Tag, Sentiment Tag, Subject Tag (multi-select, up to 2)
- Untaggable (checkbox), Note

**Reddit Threads** — one row per Reddit post/thread (created only if Reddit is selected). Fields:
- Title, Subreddit, URL, Upvote Count, Comment Count, Date, Body, Search Term

**Authors** — one row per unique author/platform combination, sorted by comment count. Fields:
- Author, Platform, Comment Count, Like Count Total

A search volume callout block is also appended to the brand page (Standard and Deep tiers).

---

## Tag system

**Motivation tag** (single, primary motivation):
- Praise — positive judgment
- Criticism — negative judgment
- Question — seeking information
- Suggestion — directive or prescriptive
- Feedback — reporting personal experience
- Comparison — placing brand alongside another
- Self-expression — using brand to say something about identity

**Sentiment tag** (single):
- Positive, Negative, Neutral, Mixed

**Subject tags** (multi-select, 1–2 per comment):
- Confirmed in Claude Chat during Phase 0, before running

---

## Pipeline flow

Tagging runs immediately after each platform completes — not at the end. A failure in one platform's tagging does not affect others.

```
For each platform:
  → Scrape (Apify or YouTube API)
  → Write rows to Comment Dataset
  → Tag untagged rows via Claude API (Haiku)

Reddit additionally:
  → Posts → Reddit Threads database
  → Comments → Comment Dataset (same as other platforms)

After all platforms:
  → Google search volume (Standard/Deep only)
  → Build Authors database from full comment dataset
```

---

## How to use

### Before opening the tool (in Claude Chat)
1. Brand verification
2. Subject tag confirmation — Phase 0
3. Platform routing — Claude produces a Platform Routing Output block

### In the tool
1. Enter brand name and Notion brand page ID
2. Select run tier
3. Tick confirmed platforms and fill in their URLs/IDs
4. Paste confirmed subject tags
5. Click Run — status panel shows live progress

### After the run
Open a fresh Claude Chat session. Ask it to read the Comment Dataset and Reddit Threads databases from Notion and run the signal audit analysis (Phases 4–7).

---

## Platform notes

| Platform | Input | Notes |
|---|---|---|
| YouTube | Channel URL or @handle | Resolved to uploads playlist automatically. Up to 20 recent videos. |
| TikTok | @handle | Most popular posts |
| Reddit | Search term | Posts → Reddit Threads DB. Comments → Comment Dataset. Only run if Reddit presence confirmed in Claude Chat. |
| Substack | Publication URL | Input schema may need adjustment on first live test |
| App Store | App ID or bundle ID | |
| Play Store | Package name | |
| Trustpilot | Full review page URL | Domain parsed automatically |
| Forum | Thread URLs, one per line | Claude Haiku extracts individual comments from each crawled page |

Instagram: not supported — platform blocks scraping.

---

## Timing

- Standard tier: 15–30 minutes
- Deep tier: 45–90 minutes

If a platform scrape fails, the tool logs the error and continues. Tagging is resumable — resubmitting the same run only tags untagged rows.
