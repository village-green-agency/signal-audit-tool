# Village Hall — Signal Audit Tool

Runs the full data pipeline for a signal audit: Apify scrape → Notion write → Claude API tagging. Leaves Claude Chat free for analysis only.

---

## Option A — Run locally (Python required)

### 1. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up your API keys
```bash
cp .env.example .env
```
Open `.env` and fill in:
- `APIFY_API_KEY` — Apify dashboard → Settings → Integrations → API token
- `NOTION_API_KEY` — notion.so/my-integrations → your integration → Internal Integration Secret
- `ANTHROPIC_API_KEY` — console.anthropic.com → API Keys

### 3. Start the tool
```bash
python app.py
```

### 4. Open in your browser
```
http://localhost:5000
```

That's it. Keep the terminal window open while a run is in progress.

---

## Option B — Deploy to Railway (no terminal needed after setup)

Railway hosts the app in the cloud. The form works from any browser, on any device.

### 1. Create a Railway account
Go to railway.app and sign up (free tier available).

### 2. Deploy from GitHub
- Push this folder to a GitHub repository
- In Railway: New Project → Deploy from GitHub repo → select your repo
- Railway detects the Procfile and deploys automatically

### 3. Set environment variables in Railway
In your Railway project: Settings → Variables → add:
- `APIFY_API_KEY`
- `NOTION_API_KEY`
- `ANTHROPIC_API_KEY`

### 4. Open your Railway URL
Railway gives you a public URL (e.g. `your-app.railway.app`). Bookmark it.

---

## How to use

### Before opening the tool
Run this in Claude Chat first (inside the Village Hall project):
1. Brand verification — confirm the right brand
2. Subject tag confirmation — locked before any scraping begins
3. Platform Routing — Claude produces a Platform Routing Output block

Copy the Platform Routing Output block. You'll paste parts of it into the form.

### In the tool
1. Enter the brand name and Notion brand page ID
2. Select the run tier (Standard for most pitches)
3. Tick the platforms confirmed by Claude Chat and fill in their URLs/IDs
4. Paste the confirmed subject tags
5. Click Run — the form submits and the status panel appears
6. Walk away. You'll see live progress in the status panel.

### After the run
The status panel shows a link to the Comment Dataset database in Notion when complete. Open a fresh Claude Chat session and ask it to run the thematic analysis and produce the Signal Audit report.

---

## What the tool does not do

- It does not run the analysis — that stays in Claude Chat intentionally
- It does not create the brand page in Notion — do this quickly in Claude Chat for new brands
- It does not confirm subject tags — these must be locked in Claude Chat before running
- It does not scrape specialist forums — forum URLs flagged in Platform Routing are noted for Claude Chat manual research (automation coming later)

---

## Notes

- Notion rate limit: 3 requests/second — built into the tool, do not modify
- Standard tier: typically 15–25 minutes end to end
- Deep tier: typically 45–90 minutes end to end
- If a platform scrape fails, the tool logs the error and continues with remaining platforms
- Tagging is resumable — if interrupted, restart the tool and re-submit the same run; the tagger only touches untagged rows
- Run history is in-memory only — it resets when the tool restarts
