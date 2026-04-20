# Swirl Series Automation

GitHub Actions job that keeps the Swirl Series content calendar in sync with Instagram, then drafts the next reel script.

**Major rewrite 2026-04-19** — see `~/.claude/plans/can-we-reexamine-the-snoopy-swing.md` for the full plan and `~/.claude/projects/.../memory/project_swirl_series_automation.md` for the canonical project memory.

## Schedule

Runs at **16:00 UTC on Mon/Wed/Fri** = 11am CDT (summer) / 10am CST (winter), aligned with Julie's posting cadence.

## What it does on each run

1. **Fetches** all IG reels via cursor pagination, with per-reel insights (reach/likes/comments/saves/shares/views/avg watch time).
2. **Reconciles** against Notion using the 4-tier matcher:
   - **Path 1 — exact `IG Media ID`** → refresh metrics on existing row.
   - **Path 2 — caption similarity ≥ 0.85 to a Scripted Swirl Series row** → flip Scripted to Posted (you posted exactly the plan), tag `Category = Swirl Series`, assign next Reel #.
   - **Path 3 — slot-position match** (post date within ±2 days of an unfilled Scripted `Slot Date`) → create **Posted twin** row from observed reality, set `Original Plan` relation back to the Scripted row, populate `Off-Script Delta` via Claude. The Scripted row is left untouched as historical record of the suggestion.
   - **Path 4 — no Swirl Series match** → create a Posted row tagged `Category = Other`. Captures every IG post for cross-category analytics.
3. **Ages out** stale Scripted rows: if a `Slot Date` is more than 7 days in the past with no match, status flips to `Skipped`.
4. **Frame-extracts + vision-analyzes** any Posted row missing analysis (15 frames covering the full reel arc, weighted toward the retention-critical first 2.5s).
5. **On script-gen days** (Mon/Wed/Fri), drafts ONE new Scripted row for the next available Slot Date via the two-model pipeline.

Idempotent — running twice in the same day is a no-op on existing matches and won't duplicate Posted twins.

## Hard rule changes (was vs now)

| Aspect | Before | After |
| --- | --- | --- |
| Sync creating rows | Never (only the writer creates the next Scripted row) | Always for unmatched IG posts (tagged `Other`) + for off-script Posted twins |
| Off-script reels | Frankenstein — Scripted row got Posted status + observed metrics but kept planned content | Posted twin row created from observed reality, original Scripted row left untouched as historical record |
| Reel # on Scripted rows | Assigned at creation time | Empty until the row becomes Posted (Reel # is IG-truth, monotonic in actual posting order) |
| Identifier on Scripted rows | Reel # | `Slot Date` (planned posting date) |
| Caption voice rule | Lowercase sentence starts (overgeneralized from AI drafts) | Proper sentence case (matches Julie's actual posted captions) |
| Title case rule | Implicit/inconsistent | Sentence case enforced by writer prompt |
| Audio field | `Suno Prompt` (always Suno-generated) | `Soundtrack` (writer picks `IG TRENDING:` or `SUNO:` per reel energy) |
| Aging | Stale Scripted rows sat forever | Flipped to `Skipped` after 7 days past Slot Date |

## Workflow_dispatch inputs

```bash
# Force script generation today (bypass M/W/F gate)
gh workflow run "Swirl Series Sync" -f force_regen=true

# Reprocess a single off-script reel by IG media ID
gh workflow run "Swirl Series Sync" -f target_media_id=18118333795657737

# Both at once
gh workflow run "Swirl Series Sync" -f force_regen=true -f target_media_id=18118333795657737
```

## Setup

### Secrets

Required:

| Secret | What it is |
| --- | --- |
| `IG_TOKEN` | Instagram Graph API long-lived user access token (auto-refreshes monthly) |
| `IG_USER_ID` | IG business/creator account ID |
| `NOTION_TOKEN` | Notion integration token (must have the IG Content Calendar DB shared with it) |
| `NOTION_DB_ID` | Notion database ID — `33c52fb3-31e4-80d6-8c74-d73a22d3e4f8` |
| `ANTHROPIC_API_KEY` | Anthropic API key |

Optional (for JulzOps ops dashboard ingest):

| Secret | What it is |
| --- | --- |
| `JULZOPS_INGEST_URL` | JulzOps event-ingest endpoint URL |
| `JULZOPS_INGEST_SECRET` | Bearer token for the ingest endpoint |

```bash
gh secret set IG_TOKEN          # paste when prompted
gh secret set IG_USER_ID
gh secret set NOTION_TOKEN
gh secret set NOTION_DB_ID
gh secret set ANTHROPIC_API_KEY
```

### Verify

```bash
gh workflow run "Swirl Series Sync"
gh run watch
```

## Model configuration

| Env var | Default | Role |
| --- | --- | --- |
| `ANALYST_MODEL` | `claude-sonnet-4-6` | Reasons over past performance + Off-Script Delta history, picks theme/angle, surfaces divergence patterns |
| `WRITER_MODEL` | `claude-sonnet-4-6` | Writes the final reel script JSON + computes Off-Script Delta on Posted twins |
| `VISION_MODEL` | `claude-haiku-4-5-20251001` | Analyzes 15-frame reel arc for retention learning |

### Pricing (per 1M tokens, input / output)

| Model | Input | Output |
| --- | --- | --- |
| Opus 4.6 (prior analyst) | $15 | $75 |
| Sonnet 4.6 (analyst + writer) | $3 | $15 |
| Haiku 4.5 (vision) | $0.80 | $4 |

### Per-run cost breakdown (post-optimization, 2026-04-20)

Each Mon/Wed/Fri run is a script-gen day, so all four call types fire:

| Call | Model | ~Input tokens | ~Output tokens | Cost / call | Calls / run | Subtotal |
| --- | --- | --- | --- | --- | --- | --- |
| Analyst | Sonnet 4.6 | 10,000 | 900 | $0.044 | 1 | $0.044 |
| Writer | Sonnet 4.6 | 1,500 | 1,500 | $0.027 | 3 | $0.081 |
| Vision (per unanalyzed reel) | Haiku 4.5 | 23,000 | 500 | $0.020 | 0–1 | ~$0.010 |
| Off-script delta (per new twin) | Sonnet 4.6 | 1,000 | 300 | $0.008 | 0–1 | ~$0.004 |
| **Per run** |  |  |  |  |  | **~$0.14** |

### Monthly estimate

- 13 runs/month (3x/week) × ~$0.14 = **~$1.85/mo**
- Annual: ~$22/year
- Prior setup (Opus analyst + Sonnet vision): ~$5.00/mo → ~$60/year
- **Phase 1 savings: ~63% / ~$38/year**

### Cost levers if spend becomes a concern

- `ANALYST_MODEL=claude-haiku-4-5-20251001` — further 75% cut on analyst calls (~$0.40/mo). Tradeoff: Haiku may miss subtler divergence patterns in the Off-Script Delta history. Try and roll back if briefs feel shallow.
- `MAX_COST_PER_RUN_USD` (default $2.00) is the hard abort budget — lowering it to `0.50` would fail fast on any pricing/prompt blow-up.
- All usage is tracked per-call via `track_usage()` in `sync.py` and POSTed to JulzOps for long-term cost visibility.

## Notion schema

Required columns (read at runtime, script adapts to whatever's present):

- **Title** (title), **Status** (select: `Posted` / `Scripted` / `Skipped`)
- **Slot Date** (date) — planned post date for Scripted rows
- **Reel #** (number) — assigned only to Posted Swirl Series rows
- **Category** (multi-select) — `Swirl Series` / `Coffee (other)` / `Personal` / `Other`
- **Original Plan** (relation, self) — Posted twin → Scripted plan it diverged from
- **Off-Script Delta** (rich text) — Claude-generated divergence note
- **Soundtrack** (rich text) — `IG TRENDING:` or `SUNO:` formatted
- **Hook Type** (select: Curiosity / Visual Movement / Unpredictability)
- **Content Split** (select: App/Lifestyle Blend / Lifestyle)
- **Caption**, **Hashtags**, **Clip Order**, **On-Screen Text**, **Cover Scene**, **Transitions**, **Reel Total Time**, **Notes**, **Reel Vision Analysis**, **Alt Scripts** (rich text)
- **Permalink** (url), **IG Media ID**, **Avg Watch Time** (rich text), **Post Date**, **Metrics Updated** (date)
- **Views / Likes / Comments / Saves / Shares / Reach** (numbers)

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

## On-demand re-examine via Claude chat

The local Claude memory file `feedback_swirl_offscript_trigger.md` recognizes phrases like "I went off-script" / "re-examine the latest reel" / "rethink the next reel based on what I actually posted" and invokes:

```bash
gh workflow run swirl-sync.yml --repo jw-yue/swirl-series-automation -f force_regen=true
```

If you name a specific reel, it adds `-f target_media_id=<id>`. The chat session polls the run, fetches the resulting Notion page URL, and surfaces it back.
