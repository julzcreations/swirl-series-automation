# Swirl Series Automation

Daily GitHub Actions job that keeps the Swirlie Notion content calendar in sync with Instagram, then drafts the next reel script.

## What it does (once per day at 10am America/Chicago)

1. **Fetch** the last ~10 IG reels and their insights (reach, likes, comments, saves, shares, views).
2. **Reconcile** against the Notion DB: match by caption similarity or ±2-day date proximity, PATCH matched rows to `Status = Posted` with permalink/insights/caption stored in Notes. Create new rows for unmatched reels. Never deletes. Never overwrites a `Scripted` row with content unless its Notes contains the literal token `REGENERATE`.
3. **Generate** the next Scripted row via a two-model Claude pipeline:
   - Hook Type cycles Curiosity / Visual Movement / Unpredictability (least-recently-used)
   - Content Split stays ~50/50 between App/Lifestyle Blend and Lifestyle (whichever's behind)
   - **Analyst pass** (Opus 4.6 by default): reasons over past performance data, picks the theme/angle, cites which past reels are shaping the decision, warns off duplicates. Returns a short creative brief.
   - **Writer pass** (Sonnet 4.6 by default): takes the brief + constraints and produces the final reel script JSON.
4. **Fill** Title, Clip Order, On-Screen Text, Caption (8–12 hashtags including the four required ones), Cover Scene, Suno Prompt, Transitions, Reel Total Time, Notes rationale (which begins with the analyst's citations).

Idempotent — running twice in the same day is a no-op on step 2 and will refresh/replace the generated row on step 3.

## Setup

### 1. Secrets

Five repo secrets are required. Set them via `gh secret set` or the repo Settings → Secrets and variables → Actions page:

| Secret | What it is |
| --- | --- |
| `IG_TOKEN` | Instagram Graph API long-lived user access token |
| `IG_USER_ID` | IG business/creator account ID |
| `NOTION_TOKEN` | Notion integration token (must have the Swirl Series DB shared with it) |
| `NOTION_DB_ID` | Swirl Series Content Calendar database ID |
| `ANTHROPIC_API_KEY` | Anthropic API key used for script generation |

```bash
gh secret set IG_TOKEN          # paste when prompted
gh secret set IG_USER_ID
gh secret set NOTION_TOKEN
gh secret set NOTION_DB_ID
gh secret set ANTHROPIC_API_KEY
```

### 2. Verify

Trigger a manual run:

```bash
gh workflow run "Swirl Series Sync"
gh run watch
```

### 3. Schedule

The cron is `0 15 * * *` (15:00 UTC daily), which is 10am CST / 11am CDT. Edit `.github/workflows/swirl-sync.yml` if you want a different time.

## Model configuration

Two models are used per run (configurable via env in the workflow):

| Env var | Default | Role |
| --- | --- | --- |
| `ANALYST_MODEL` | `claude-opus-4-6` | Reasons over past performance, picks theme/angle, cites informative reels |
| `WRITER_MODEL` | `claude-sonnet-4-6` | Writes the final reel script JSON from the analyst's brief |

To run cheaper (~$0.015/run vs ~$0.065), set both to `claude-sonnet-4-6`. To run everything on Opus for max quality, set both to `claude-opus-4-6`.

## Notion schema expectations

The script reads the DB schema at runtime and adapts, but it looks for these property names (optional ones are ignored if missing):

- **Status** — select/status with values including `Scripted` and `Posted`
- **Title** — title property (any name works; script picks the title column automatically)
- **Reel #** — number
- **Hook Type** — select: Curiosity / Visual Movement / Unpredictability
- **Content Split** — select: App/Lifestyle Blend / Lifestyle
- **Clip Order**, **On-Screen Text**, **Caption**, **Cover Scene**, **Suno Prompt**, **Transitions**, **Reel Total Time**, **Notes** — rich text
- **Permalink** (or **Link**) — url
- **Posted Date** — date (optional)

If your DB uses different names, either rename them in Notion or adjust the `updates` dict in `sync.py`.

## Local testing

```bash
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

export IG_TOKEN=...
export IG_USER_ID=...
export NOTION_TOKEN=...
export NOTION_DB_ID=...
export ANTHROPIC_API_KEY=...

python sync.py
```

## Notes on the REGENERATE escape hatch

If you want the automation to rewrite a specific Scripted row on the next run, put the literal string `REGENERATE` anywhere in that row's Notes. The script will treat it as the "next empty Scripted row" and overwrite it. Remove the token once you're happy with the result.
