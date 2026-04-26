#!/usr/bin/env python3
"""Swirl Series content automation.

Runs Mon/Wed/Fri at 11am America/Chicago (16:00 UTC during CDT, 17:00 UTC
during CST — cron set to 16:00 UTC which is 11am CDT in summer / 10am CST
in winter). Same days as Julie's posting cadence so each run lands the
morning of the day she's filming.

Each run does:
1. Refreshes IG metrics on all existing Posted Notion rows.
2. New 4-tier matcher (see HARD RULE below) creates Posted rows from
   observed reality where appropriate, including Posted twins for
   off-script reels (linked back to their original Scripted plan via
   Original Plan relation).
3. Ages out stale Scripted rows: if a Slot Date is more than 7 days in
   the past with no match, status flips to Skipped.
4. Frame-extracts + vision-analyzes any Posted row missing analysis.
5. On script-gen days, drafts ONE new Scripted row for the next posting
   slot (with Slot Date instead of Reel #).

HARD RULE (2026-04-19 rewrite): sync DOES create a row for every new IG
post. Category multi-select differentiates Swirl Series from Other content.
Match priority:
  1. Exact IG Media ID  -> refresh metrics on existing row.
  2. Caption similarity >= 0.85 to a Scripted Swirl-Series row -> flip
     Scripted to Posted (you posted exactly the plan). Tag Swirl Series,
     assign next Reel #.
  3. Slot-position match: IG post date within +/-2 days of an unfilled
     Scripted Slot Date -> create Posted twin, set Original Plan
     relation, populate Off-Script Delta. Scripted row left alone as
     historical record.
  4. No Swirl Series match -> still create a Posted row, tag Category =
     Other, no Original Plan, no Reel #.

All credentials come from environment variables. See README.md.
"""
import difflib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import requests
from anthropic import Anthropic

# ---- Config from env ----
IG_TOKEN = os.environ["IG_TOKEN"]
IG_USER_ID = os.environ["IG_USER_ID"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
# Optional: POST run summary to JulzOps ops dashboard at end of each run.
# If either env var is missing, the POST is a no-op.
JULZOPS_INGEST_URL = os.environ.get("JULZOPS_INGEST_URL", "")
JULZOPS_INGEST_SECRET = os.environ.get("JULZOPS_INGEST_SECRET", "")
# Model routing by task fit (2026-04-26 revert pass):
#   Analyst = Opus 4.7   — judgment work: theme picks, dupe warnings, deciding
#                          which past reels are shaping the next call. Reasoning
#                          quality is the lever here, not cost.
#   Writer  = Sonnet 4.6 — voice + variation across 3 script drafts. Brand voice
#                          (warm, intimate, casual) is Sonnet's mid-range
#                          strength; Haiku flattens it.
#   Vision  = Haiku 4.5  — frame description, well within Haiku's range;
#                          ~3.75x cheaper than Sonnet for this workload.
# Earlier 2026-04-20 pass downshifted analyst Opus -> Sonnet under the assumption
# that the task was pattern-matching over structured history. Reverted: the
# analyst makes real judgment calls (theme/angle/dupe warnings), and the
# +$2/month cost delta is trivial vs the reasoning lift.
ANALYST_MODEL = os.environ.get("ANALYST_MODEL", "claude-opus-4-7")
WRITER_MODEL = os.environ.get("WRITER_MODEL", "claude-sonnet-4-6")
VISION_MODEL = os.environ.get("VISION_MODEL", "claude-haiku-4-5-20251001")
# Script generation runs on Mon=0, Wed=2, Fri=4 (Python weekday() numbering).
# Aligned to posting cadence so each run drafts that-day's reel.
SCRIPT_GEN_WEEKDAYS = {0, 2, 4}
# Allow overriding via env for manual runs / testing. FORCE_REGEN is the
# canonical name; FORCE_SCRIPT_GEN is kept as an alias for backwards-compat
# with existing workflow_dispatch invocations.
FORCE_REGEN = (
    os.environ.get("FORCE_REGEN", "").lower() in ("1", "true", "yes")
    or os.environ.get("FORCE_SCRIPT_GEN", "").lower() in ("1", "true", "yes")
)
# When set via workflow_dispatch, force-reconcile this specific IG media ID
# even if it is already matched and Posted. Useful for reprocessing a single
# off-script reel.
TARGET_MEDIA_ID = os.environ.get("TARGET_MEDIA_ID", "").strip() or None

# ---- New behavior thresholds (2026-04-19 rewrite) ----
# Caption similarity >= this means "you posted what we planned" (path 2 match).
CAPTION_MATCH_THRESHOLD = 0.85
# Slot-position match window — IG post date within +/- this many days of an
# unfilled Scripted Slot Date matches that slot.
SLOT_MATCH_DAYS = 2
# Scripted rows whose Slot Date is more than this many days in the past with
# no match get flipped to Skipped (keeps the calendar honest).
SKIPPED_AGE_DAYS = 7
# Category options used by the multi-select Category column.
CATEGORY_SWIRL = "Swirl Series"
CATEGORY_OTHER = "Other"

# Cost tracking was removed when JulzOps gained access to the Anthropic
# Cost API (via Organization-tier admin keys). See
# julzops/src/app/api/jobs/reconcile-usage/route.ts — that route pulls
# authoritative per-workspace dollar amounts hourly. Local rate-card
# multiplication is no longer needed.


def post_run_to_julzops(summary: dict, started_at: datetime, ended_at: datetime) -> None:
    """POST run summary to JulzOps ops dashboard. Never raises — failure is
    logged to stderr and the sync run continues as normal.

    The env vars JULZOPS_INGEST_URL + JULZOPS_INGEST_SECRET must both be set
    (they are in GH Actions secrets). If either is missing this is a no-op.

    Only non-cost run metadata is sent (reconciled counts, next-script info,
    timings, status). Cost is tracked separately via the Anthropic Cost API
    pull in JulzOps.
    """
    if not JULZOPS_INGEST_URL or not JULZOPS_INGEST_SECRET:
        return
    try:
        status = "failure" if summary.get("errors") else "success"
        run_url = None
        repo = os.environ.get("GITHUB_REPOSITORY")
        run_id = os.environ.get("GITHUB_RUN_ID")
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        if repo and run_id:
            run_url = f"{server}/{repo}/actions/runs/{run_id}"
        new_script = summary.get("new_script_row")
        payload = {
            "projectSlug": "swirl-series-automation",
            "source": "github_actions",
            "workflowTitle": f"Swirl Series sync {started_at.strftime('%Y-%m-%d')}",
            "startedAt": started_at.isoformat(),
            "endedAt": ended_at.isoformat(),
            "durationMs": int((ended_at - started_at).total_seconds() * 1000),
            "status": status,
            "runUrl": run_url,
            "metadata": {
                "igFetched": summary.get("ig_fetched", 0),
                "metricsRefreshed": len(summary.get("metrics_refreshed", [])),
                "promotedToPosted": len(summary.get("promoted_to_posted", [])),
                "twinsCreated": len(summary.get("twins_created", [])),
                "othersCreated": len(summary.get("others_created", [])),
                "agedOutToSkipped": len(summary.get("aged_out_to_skipped", [])),
                "framesAnalyzed": len(summary.get("frames_analyzed", [])),
                "unmatchedWarnings": len(summary.get("unmatched_warnings", [])),
                "scriptGenSkipped": summary.get("script_gen_skipped", False),
                "newScriptRow": {
                    "reelNum": new_script["reel_num"],
                    "slotDate": new_script.get("slot_date"),
                    "title": new_script["title"],
                } if new_script else None,
                "errors": summary.get("errors", [])[:10],
            },
        }
        r = requests.post(
            JULZOPS_INGEST_URL,
            json=payload,
            headers={"Authorization": f"Bearer {JULZOPS_INGEST_SECRET}"},
            timeout=5,
        )
        if r.status_code >= 400:
            print(
                f"  [julzops] ingest returned {r.status_code}: {r.text[:200]}",
                file=sys.stderr,
            )
        else:
            print(f"  [julzops] ingest ok: {r.json().get('eventId', '?')}", file=sys.stderr)
    except Exception as e:
        print(f"  [julzops] ingest error (non-fatal): {e}", file=sys.stderr)

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

HOOK_TYPES = ["Curiosity", "Visual Movement", "Unpredictability"]
CONTENT_SPLITS = ["App/Lifestyle Blend", "Lifestyle"]
# Max hashtags per caption (updated 2026-04-11 — was 8-12, now 5 hard cap).
MAX_HASHTAGS = 5

# IG insight key → Notion column name. Avg watch time gets formatted separately.
METRIC_MAP = {
    "views": "Views",
    "likes": "Likes",
    "comments": "Comments",
    "saved": "Saves",
    "shares": "Shares",
    "reach": "Reach",
}


# ---- String helpers ----
def strip_emoji(s: str) -> str:
    return re.sub(r"[^\w\s#@.,!?'\"-]", "", s or "", flags=re.UNICODE)


def caption_key(s: str) -> str:
    return strip_emoji((s or "").lower())[:60].strip()


def parse_ts(s):
    """Parse an ISO timestamp or date string, always returning UTC-aware."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def format_watch_time_ms(ms) -> str:
    """Convert IG's ms-duration insight into a human-readable 'X.Ys' string."""
    if ms is None:
        return ""
    try:
        return f"{float(ms) / 1000:.1f}s"
    except (TypeError, ValueError):
        return ""


# ---- IG token refresh (monthly, via GH Actions secret) ----
def maybe_refresh_ig_token():
    """Refresh the IG token on the 1st of each month only.

    IG long-lived tokens last 60 days. Refreshing on the 1st of each month
    resets the window with ~30 days of margin. The refreshed token is written
    back to the GitHub Actions secret via gh CLI so the next run picks it up.

    This runs at most once per month. Every other day, it's a no-op.
    """
    if datetime.now(timezone.utc).day != 1:
        return False
    print("  [token] 1st of the month — refreshing IG token", file=sys.stderr)
    url = "https://graph.instagram.com/refresh_access_token"
    params = {"grant_type": "ig_refresh_token", "access_token": IG_TOKEN}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code >= 400:
            print(f"  [warn] IG token refresh failed: {r.status_code} {r.text[:200]}", file=sys.stderr)
            return False
        r.raise_for_status()
        new_token = r.json().get("access_token")
        expires_in = r.json().get("expires_in", "?")
        if not new_token:
            print("  [warn] IG token refresh: no new token in response", file=sys.stderr)
            return False
        # Update the GH Actions secret
        repo = os.environ.get("GITHUB_REPOSITORY", "jw-yue/swirl-series-automation")
        result = subprocess.run(
            ["gh", "secret", "set", "IG_TOKEN", "--body", new_token, "--repo", repo],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            days = int(int(expires_in) / 86400) if str(expires_in).isdigit() else "?"
            print(f"  [token] IG token refreshed (~{days} days). GH secret updated.", file=sys.stderr)
            return True
        else:
            print(f"  [warn] gh secret update failed: {result.stderr[:200]}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"  [warn] IG token refresh error: {e}", file=sys.stderr)
        return False


# ---- IG Graph API ----
def fetch_ig_reels():
    """Fetch ALL reels by following Graph API cursor pagination.

    Includes media_url (the signed MP4 URL) for frame extraction.
    """
    url = f"https://graph.instagram.com/v21.0/{IG_USER_ID}/media"
    params = {
        "fields": "id,caption,media_type,media_product_type,permalink,timestamp,thumbnail_url,media_url",
        "limit": 100,
        "access_token": IG_TOKEN,
    }
    all_reels = []
    next_url = url
    next_params = params
    while next_url:
        r = requests.get(next_url, params=next_params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        for m in payload.get("data", []):
            if m.get("media_product_type") == "REELS":
                all_reels.append(m)
        next_url = payload.get("paging", {}).get("next")
        next_params = None  # cursor URL already has everything
        if not next_url:
            break
    return all_reels


def fetch_insights(media_id: str) -> dict:
    """Pull per-reel metrics including ig_reels_avg_watch_time."""
    url = f"https://graph.instagram.com/v21.0/{media_id}/insights"
    metrics_full = "reach,likes,comments,saved,shares,views,ig_reels_avg_watch_time"
    metrics_fallback = "reach,likes,comments,saved,shares,views"
    try:
        r = requests.get(
            url,
            params={"metric": metrics_full, "access_token": IG_TOKEN},
            timeout=30,
        )
        if r.status_code >= 400:
            r = requests.get(
                url,
                params={"metric": metrics_fallback, "access_token": IG_TOKEN},
                timeout=30,
            )
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        out = {}
        for d in r.json().get("data", []):
            vals = d.get("values", [])
            if vals:
                out[d["name"]] = vals[0].get("value")
        return out
    except requests.RequestException as e:
        print(f"  [warn] insights failed for {media_id}: {e}", file=sys.stderr)
        return {}


# ---- Notion read ----
def notion_get_db():
    r = requests.get(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        headers=NOTION_HEADERS, timeout=30,
    )
    r.raise_for_status()
    return r.json()


def notion_query_all():
    rows = []
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            headers=NOTION_HEADERS, json=body, timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return rows


# ---- Property read/write (schema-aware) ----
def prop_type(schema_props: dict, name: str):
    p = schema_props.get(name)
    return p.get("type") if p else None


def read_prop(page: dict, name: str):
    p = page["properties"].get(name)
    if not p:
        return None
    t = p["type"]
    v = p.get(t)
    if t in ("title", "rich_text"):
        return "".join(x.get("plain_text", "") for x in v or [])
    if t in ("select", "status"):
        return v["name"] if v else None
    if t == "multi_select":
        return [x["name"] for x in v or []]
    if t == "number":
        return v
    if t == "url":
        return v
    if t == "date":
        return (v or {}).get("start") if v else None
    return v


def write_prop(schema_props: dict, name: str, value):
    t = prop_type(schema_props, name)
    if not t:
        return None
    if t == "title":
        return {"title": [{"text": {"content": str(value)[:2000]}}]}
    if t == "rich_text":
        return {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
    if t == "select":
        return {"select": {"name": str(value)}}
    if t == "status":
        return {"status": {"name": str(value)}}
    if t == "multi_select":
        names = value if isinstance(value, list) else [value]
        return {"multi_select": [{"name": str(n)} for n in names]}
    if t == "number":
        return {"number": float(value) if value is not None else None}
    if t == "url":
        return {"url": str(value) if value else None}
    if t == "date":
        return {"date": {"start": value} if value else None}
    return None


def build_props(schema_props: dict, updates: dict) -> dict:
    out = {}
    for name, val in updates.items():
        wrapped = write_prop(schema_props, name, val)
        if wrapped is not None:
            out[name] = wrapped
    return out


# ---- Category helpers (multi-select Category column added 2026-04-19) ----
def read_category(row) -> list:
    """Return the row's Category multi-select values as a list of strings."""
    val = read_prop(row, "Category")
    if not val:
        return []
    return val if isinstance(val, list) else [val]


def is_swirl_series(row) -> bool:
    return CATEGORY_SWIRL in read_category(row)


def row_slot_date(page) -> "datetime | None":
    v = read_prop(page, "Slot Date")
    return parse_ts(v) if v else None


# ---- Matching: 4-tier algorithm (2026-04-19 rewrite) ----
def match_by_media_id(reel_id, rows):
    """Path 1: exact match on the IG Media ID column. Rock solid, no false positives."""
    for row in rows:
        row_id = read_prop(row, "IG Media ID")
        if row_id and str(row_id).strip() == str(reel_id).strip():
            return row
    return None


def row_posted_date(page, schema_props):
    """Read the row's Post Date column (handles legacy aliases)."""
    for field in ("Posted Date", "Post Date", "Date", "Posted", "Scheduled Date"):
        if field in schema_props:
            v = read_prop(page, field)
            if v:
                return parse_ts(v)
    return None


def caption_similarity(reel, row) -> float:
    """SequenceMatcher ratio between IG caption and row caption/title (0-1)."""
    reel_cap = caption_key(reel.get("caption", ""))
    if not reel_cap:
        return 0.0
    row_cap = ""
    for field in ("Caption", "Title"):
        v = read_prop(row, field)
        if v:
            row_cap = caption_key(v)
            if row_cap:
                break
    if not row_cap:
        return 0.0
    return difflib.SequenceMatcher(None, reel_cap, row_cap).ratio()


def match_by_caption_similarity(reel, candidates):
    """Path 2: high-confidence caption match (>= CAPTION_MATCH_THRESHOLD)
    against an unmatched Scripted row. Returns (row, score) or (None, 0.0).
    Considers only rows without an IG Media ID (Path 1 already handled those)."""
    best = None
    best_score = 0.0
    for row in candidates:
        if read_prop(row, "IG Media ID"):
            continue
        if read_prop(row, "Status") != "Scripted":
            continue
        if not is_swirl_series(row):
            continue
        score = caption_similarity(reel, row)
        if score >= CAPTION_MATCH_THRESHOLD and score > best_score:
            best_score = score
            best = row
    return best, best_score


def match_by_slot_position(reel, candidates, claimed_ids: set):
    """Path 3: slot-position match. IG post date within +/- SLOT_MATCH_DAYS
    of an unfilled Scripted Slot Date and that slot has not already been
    claimed in this run. Caption is presumed to NOT match (handled by Path 2
    upstream) — anything reaching Path 3 has diverged enough to be off-script.

    Returns the matched Scripted row or None.
    """
    reel_ts = parse_ts(reel.get("timestamp"))
    if not reel_ts:
        return None
    best = None
    best_distance = None
    for row in candidates:
        if row["id"] in claimed_ids:
            continue
        if read_prop(row, "IG Media ID"):
            continue
        if read_prop(row, "Status") != "Scripted":
            continue
        if not is_swirl_series(row):
            continue
        slot = row_slot_date(row)
        if not slot:
            continue
        distance = abs((slot - reel_ts).days)
        if distance <= SLOT_MATCH_DAYS and (best_distance is None or distance < best_distance):
            best = row
            best_distance = distance
    return best


def is_reel_row(row) -> bool:
    """Filter out clearly-not-reel pages (e.g. 'Asset library') from matching."""
    if read_prop(row, "Reel #") is not None:
        return True
    if read_prop(row, "Status") in ("Posted", "Scripted", "Skipped"):
        return True
    if read_category(row):
        return True
    title = (read_prop(row, "Title") or "").lower()
    return title.startswith("reel ")


def reconcile(reels, rows, schema_props):
    """4-tier match algorithm. Returns four buckets:

    - refreshes:  list of (page_id, reel) — Path 1, refresh metrics on existing row
    - promotes:   list of (page_id, reel) — Path 2, flip Scripted to Posted
    - twins:      list of (scripted_row, reel) — Path 3, create Posted twin
                  with Original Plan relation back to the Scripted row
    - others:     list of reel — Path 4, create a Posted row tagged Other
                  (non-Swirl-Series content captured for analytics)

    Differs from the pre-2026-04-19 version:
    - Off-script reels no longer corrupt the Scripted row's structured fields.
    - Sync now creates rows for non-Swirl posts (tagged Other) instead of
      logging-and-skipping.
    """
    refreshes = []
    promotes = []
    twins = []
    others = []
    candidates = [r for r in rows if is_reel_row(r)]
    # Track Scripted row ids that have been claimed within THIS run so two
    # IG reels can't both match the same Scripted slot.
    claimed_scripted_ids: set = set()

    for reel in reels:
        # Path 1: exact IG Media ID match -> refresh
        match = match_by_media_id(reel["id"], candidates)
        if match is not None:
            refreshes.append((match["id"], reel))
            continue

        # Path 2: high-confidence caption similarity to a Scripted row -> promote
        cap_match, cap_score = match_by_caption_similarity(reel, candidates)
        if cap_match is not None:
            promotes.append((cap_match["id"], reel))
            claimed_scripted_ids.add(cap_match["id"])
            continue

        # Path 3: slot-position match -> create Posted twin
        slot_match = match_by_slot_position(reel, candidates, claimed_scripted_ids)
        if slot_match is not None:
            twins.append((slot_match, reel))
            claimed_scripted_ids.add(slot_match["id"])
            continue

        # Path 4: no Swirl Series match -> Other
        others.append(reel)

    return refreshes, promotes, twins, others


# ---- Update builders ----
def _metric_updates(reel, schema_props):
    """Build metric column updates from a reel's insights, always including
    Metrics Updated date and IG Media ID (so future runs match by ID)."""
    updates = {}
    ins = reel.get("_insights", {})
    for key, col in METRIC_MAP.items():
        if col in schema_props and key in ins and ins[key] is not None:
            updates[col] = ins[key]
    if "Avg Watch Time" in schema_props and "ig_reels_avg_watch_time" in ins:
        updates["Avg Watch Time"] = format_watch_time_ms(ins["ig_reels_avg_watch_time"])
    if "IG Media ID" in schema_props and reel.get("id"):
        updates["IG Media ID"] = str(reel["id"])
    if "Metrics Updated" in schema_props:
        updates["Metrics Updated"] = datetime.now(timezone.utc).date().isoformat()
    return updates


def _permalink_updates(reel, schema_props):
    if "Permalink" in schema_props and reel.get("permalink"):
        return {"Permalink": reel["permalink"]}
    if "Link" in schema_props and reel.get("permalink"):
        return {"Link": reel["permalink"]}
    return {}


def _post_date_updates(reel, schema_props):
    if "Post Date" in schema_props and reel.get("timestamp"):
        return {"Post Date": reel["timestamp"][:10]}
    if "Posted Date" in schema_props and reel.get("timestamp"):
        return {"Posted Date": reel["timestamp"][:10]}
    return {}


def _patch_row(page_id: str, body: dict):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS, json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json()


def refresh_row_metrics(page_id, reel, schema_props):
    """Refresh metrics/permalink on a row already marked Posted. Never touches
    Status, Title, Caption, or any creative content fields."""
    updates = {}
    updates.update(_metric_updates(reel, schema_props))
    updates.update(_permalink_updates(reel, schema_props))
    updates.update(_post_date_updates(reel, schema_props))
    if not updates:
        return None
    return _patch_row(page_id, {"properties": build_props(schema_props, updates)})


def promote_row_to_posted(page_id, reel, schema_props, reel_num: int):
    """Flip a Scripted row to Posted (Path 2 match — caption matched the plan).
    Tags Category = Swirl Series and assigns the next sequential Reel #.
    Does NOT touch creative fields — the human authored those and the post
    matches the plan, so they stay as-is."""
    updates = {"Status": "Posted"}
    if "Category" in schema_props:
        updates["Category"] = [CATEGORY_SWIRL]
    if "Reel #" in schema_props:
        updates["Reel #"] = reel_num
    updates.update(_metric_updates(reel, schema_props))
    updates.update(_permalink_updates(reel, schema_props))
    updates.update(_post_date_updates(reel, schema_props))
    return _patch_row(page_id, {"properties": build_props(schema_props, updates)})


# ---- New row creation: Posted twins (Path 3) and Other rows (Path 4) ----
def _short_caption_title(caption: str, n_words: int) -> str:
    """First N words of a caption, sentence-cased for a clean title."""
    words = (caption or "").strip().split()[:n_words]
    if not words:
        return "(untitled IG post)"
    s = " ".join(words)
    return s[:1].upper() + s[1:] if s else s


def create_posted_row(
    reel,
    schema_props,
    category: str,
    *,
    original_plan_id: "str | None" = None,
    off_script_delta: "str | None" = None,
    reel_num: "int | None" = None,
):
    """Create a brand-new Posted row from observed IG data only.

    Used by Path 3 (Posted twin for off-script reel) and Path 4 (Other
    category for non-Swirl-Series posts). Never copies content from a
    Scripted row — observed reality only. Vision Analysis is filled in
    later by the frame-extraction pass.
    """
    title_prop = next((n for n, p in schema_props.items() if p["type"] == "title"), None)
    if not title_prop:
        raise RuntimeError("No title property found in schema")

    caption = reel.get("caption", "") or ""
    if category == CATEGORY_SWIRL:
        title = "[Off-Script] " + _short_caption_title(caption, 4)
    else:
        title = _short_caption_title(caption, 6)

    updates = {
        title_prop: title,
        "Status": "Posted",
    }
    if "Category" in schema_props:
        updates["Category"] = [category]
    if reel_num is not None and "Reel #" in schema_props:
        updates["Reel #"] = reel_num
    if caption:
        updates["Caption"] = caption
        # Parse hashtags out of the caption into the Hashtags column for
        # consistency with Scripted rows.
        tags = re.findall(r"#\w+", caption)
        if tags and "Hashtags" in schema_props:
            updates["Hashtags"] = " ".join(tags)
    if "Soundtrack" in schema_props:
        updates["Soundtrack"] = "(audio not derivable from frames \u2014 fill in manually if known)"
    if off_script_delta and "Off-Script Delta" in schema_props:
        updates["Off-Script Delta"] = off_script_delta
    if "Notes" in schema_props:
        if category == CATEGORY_SWIRL:
            note = (
                f"Reconciled from IG observation {datetime.now(timezone.utc).date().isoformat()}. "
                f"Posted twin for an off-script reel. Original Plan relation points to the Scripted "
                f"row that was the original plan for this slot."
            )
        else:
            note = (
                f"Auto-created from IG on {datetime.now(timezone.utc).date().isoformat()} "
                f"because no Swirl Series Scripted row matched. Tagged Other for analytics. "
                f"Reclassify Category if this should be Swirl Series."
            )
        updates["Notes"] = note

    updates.update(_metric_updates(reel, schema_props))
    updates.update(_permalink_updates(reel, schema_props))
    updates.update(_post_date_updates(reel, schema_props))

    body = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": build_props(schema_props, updates),
    }
    # Original Plan is a relation type — write_prop doesn't handle relation,
    # so we add it directly to the properties dict.
    if original_plan_id and "Original Plan" in schema_props:
        body["properties"]["Original Plan"] = {"relation": [{"id": original_plan_id}]}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS, json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ---- Off-Script Delta computation ----
OFF_SCRIPT_DELTA_PROMPT = """You are reconciling a Swirl Series Instagram reel that was filmed off-script. The original plan and the actually-posted content are below. Write a single short paragraph (2-4 sentences) describing how the post diverged from the plan. Be concrete and specific.

Cover any of these axes that changed:
- Subject / topic (what the reel is "about")
- Voice / tone (cozy, playful, narrative, etc.)
- Hook type
- Wardrobe / setting
- Length / pacing
- Content split (App/Lifestyle Blend vs Lifestyle)
- On-screen text presence

If something matches between plan and post, you can briefly note that too.

PLANNED (Scripted row):
- Title: {plan_title}
- Caption: {plan_caption}
- Clip Order: {plan_clip_order}
- On-Screen Text: {plan_ost}
- Hook Type: {plan_hook}
- Content Split: {plan_split}
- Notes: {plan_notes}

POSTED (actual IG reel):
- Caption: {posted_caption}
- Post Date: {posted_date}

Return ONLY the paragraph, no headings, no JSON, no quotes around it."""


def compute_off_script_delta(client, scripted_row, reel) -> str:
    """One Claude call per off-script reel. Compares the planned Scripted row
    against the observed IG reel and returns a short structured description."""
    plan_caption = (read_prop(scripted_row, "Caption") or "")[:500]
    plan_clip_order = (read_prop(scripted_row, "Clip Order") or "")[:500]
    plan_ost = (read_prop(scripted_row, "On-Screen Text") or "")[:300]
    plan_hook = read_prop(scripted_row, "Hook Type") or ""
    plan_split = read_prop(scripted_row, "Content Split") or ""
    plan_title = read_prop(scripted_row, "Title") or "(untitled)"
    plan_notes = (read_prop(scripted_row, "Notes") or "")[:400]
    posted_caption = (reel.get("caption", "") or "")[:500]
    posted_date = (reel.get("timestamp", "") or "")[:10]
    prompt = OFF_SCRIPT_DELTA_PROMPT.format(
        plan_title=plan_title,
        plan_caption=plan_caption or "(none)",
        plan_clip_order=plan_clip_order or "(none)",
        plan_ost=plan_ost or "(none)",
        plan_hook=plan_hook or "(none)",
        plan_split=plan_split or "(none)",
        plan_notes=plan_notes or "(none)",
        posted_caption=posted_caption or "(none)",
        posted_date=posted_date or "(unknown)",
    )
    try:
        msg = client.messages.create(
            model=WRITER_MODEL,  # Sonnet is plenty for this — short, structured
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in msg.content if b.type == "text").strip()
        return text[:1500]  # cap to a reasonable length
    except Exception as e:
        print(f"  [warn] off-script delta computation failed: {e}", file=sys.stderr)
        return f"(automatic delta unavailable: {e})"


# ---- Reel # accounting (Swirl Series only) ----
def next_swirl_reel_number(rows) -> int:
    """Highest Reel # among Posted Swirl Series rows, plus 1.
    Reel # is no longer assigned to Scripted rows or Other-tagged rows."""
    n = 0
    for row in rows:
        if read_prop(row, "Status") != "Posted":
            continue
        if not is_swirl_series(row):
            continue
        v = read_prop(row, "Reel #")
        if isinstance(v, (int, float)) and v > n:
            n = int(v)
    return n + 1


# ---- Aging: Slot Date > SKIPPED_AGE_DAYS in past with no match -> Skipped ----
def age_out_scripted_rows(rows, schema_props) -> list:
    """Flip Scripted rows whose Slot Date is more than SKIPPED_AGE_DAYS in
    the past to Status=Skipped. Keeps the calendar honest without losing
    the suggestion. Returns list of page ids that were aged out."""
    if "Slot Date" not in schema_props:
        return []
    today = datetime.now(timezone.utc).date()
    aged = []
    for row in rows:
        if read_prop(row, "Status") != "Scripted":
            continue
        slot = read_prop(row, "Slot Date")
        if not slot:
            continue
        try:
            slot_date = datetime.fromisoformat(slot[:10]).date()
        except Exception:
            continue
        days_past = (today - slot_date).days
        if days_past <= SKIPPED_AGE_DAYS:
            continue
        try:
            _patch_row(
                row["id"],
                {"properties": build_props(schema_props, {"Status": "Skipped"})},
            )
            aged.append(row["id"])
        except Exception as e:
            print(f"  [warn] failed to age out {row['id']}: {e}", file=sys.stderr)
    return aged


# ---- Slot Date computation for new Scripted rows ----
def next_open_slot_date(rows) -> str:
    """Find the next Mon/Wed/Fri slot date that doesn't already have a
    Scripted row claiming it. Returns ISO date string (YYYY-MM-DD)."""
    claimed: set = set()
    for row in rows:
        if read_prop(row, "Status") != "Scripted":
            continue
        slot = read_prop(row, "Slot Date")
        if slot:
            claimed.add(slot[:10])
    today = datetime.now(timezone.utc).date()
    candidate = today
    # Walk forward day by day, find next M/W/F not in `claimed`.
    for _ in range(60):  # 60-day search horizon is plenty
        if candidate.weekday() in {0, 2, 4} and candidate.isoformat() not in claimed:
            return candidate.isoformat()
        candidate = candidate + timedelta(days=1)
    # Fallback: today + 1 (shouldn't reach this in practice)
    return (today + timedelta(days=1)).isoformat()


# ---- Frame extraction + vision analysis ----
def download_mp4(media_url: str, out_path: str) -> bool:
    """Download the reel MP4 to out_path. Returns True on success."""
    try:
        with requests.get(media_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"  [warn] MP4 download failed: {e}", file=sys.stderr)
        return False


# Timestamps (seconds) to extract frames at. 15 frames covering the full reel
# arc, weighted toward the first 3 seconds (where retention dies) and the
# end (where the loop/payoff lives).
# ffmpeg silently drops timestamps past the reel's actual duration, so shorter
# reels just yield fewer usable frames — no need to fetch duration upfront.
FRAME_TIMESTAMPS = [
    0.0, 0.3, 0.7, 1.2, 1.8, 2.5,   # 6 dense in first 2.5s — the retention critical zone
    3.5, 5.0,                        # 2 early-mid
    7.0, 9.5,                        # 2 mid
    12.0, 15.0,                      # 2 late-mid
    18.0, 22.0, 27.0,                # 3 toward end (longer reels only)
]


def extract_frames(mp4_path: str, out_dir: str) -> list:
    """Extract up to 15 frames at FRAME_TIMESTAMPS using ffmpeg. Returns list
    of paths. Timestamps past the reel's actual duration are silently dropped
    by ffmpeg, so shorter reels yield fewer frames."""
    frame_paths = []
    for i, ts in enumerate(FRAME_TIMESTAMPS):
        out = os.path.join(out_dir, f"frame_{i:02d}_{int(ts*10):03d}.jpg")
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-ss", str(ts), "-i", mp4_path,
            "-frames:v", "1", "-q:v", "4",
            out,
        ]
        try:
            subprocess.run(cmd, check=True, timeout=30, capture_output=True)
            if os.path.exists(out) and os.path.getsize(out) > 0:
                frame_paths.append(out)
        except subprocess.CalledProcessError as e:
            print(f"  [warn] ffmpeg failed at ts={ts}: {e.stderr[:200] if e.stderr else e}", file=sys.stderr)
        except Exception as e:
            print(f"  [warn] frame extract ts={ts}: {e}", file=sys.stderr)
    return frame_paths


VISION_PROMPT = """You are analyzing the FULL ARC of an Instagram reel for a latte art practice app called Swirlie. You're receiving up to 15 frames in chronological order:

  0.0s, 0.3s, 0.7s, 1.2s, 1.8s, 2.5s  — dense sampling of the first 2.5s (retention-critical zone)
  3.5s, 5.0s, 7.0s, 9.5s               — early-to-mid section
  12.0s, 15.0s                         — late-mid section
  18.0s, 22.0s, 27.0s                  — toward end (may be absent for shorter reels)

Short reels just yield fewer frames, which is fine. The creator (Julie) is trying to maximize avg watch time AND understand how the full reel arc drives retention (not just the opener).

Track across the WHOLE reel:
- First frame — what is it? Human / app-product / wide-scene / close-up-object / text-card?
- When does a human first appear? (hand, face, shadow, full body) — timestamp matters
- If a human is visible: wardrobe, styling, color palette, anything K-drama-cohesive or distinctive
- Where/when does any app or product screen appear? Is it texture or subject?
- On-screen text across the reel — all overlays, in order
- Transition density: how fast are cuts throughout? Front-loaded, evenly-paced, back-loaded?
- Mid-reel beats: what happens around 5-10s (where Reel 5 dropped retention)?
- End / payoff: what's the last frame? Does it loop naturally?
- Overall aesthetic tags: 3-5 descriptors

Return ONLY a JSON object with these keys:
{
  "first_frame_type": "one of: human-in-frame / app-or-product / wide-scene / close-up-object / text-card",
  "first_frame_description": "one sentence",
  "human_present_in_first_3s": true or false,
  "human_first_appearance": "timestamp like '0s', '1.2s', '4s', or 'never'",
  "wardrobe_notes": "brief description across all frames showing a human, or 'N/A'",
  "app_appearance_timing": "one of: first_frame / within_3s / middle / end / not_visible",
  "on_screen_text_sequence": "overlay text in order separated by ' | ', or 'none'",
  "transition_density_by_section": "describe pacing in first_3s / mid / end (e.g. 'first_3s: fast; mid: slow; end: hard_cut')",
  "mid_reel_beat": "1 sentence on what's happening around 5-10s",
  "end_beat": "1 sentence on the final frame / how the reel closes",
  "aesthetic_tags": ["tag1", "tag2", "tag3"],
  "retention_concerns": "1 sentence: what in this arc might cause viewers to scroll or drop off mid-reel",
  "retention_strengths": "1 sentence: what in this arc is working and should be repeated",
  "arc_hypothesis": "1 sentence: if this reel had high/low avg watch time, your best guess for why based on what you see"
}

Return only the JSON, no prose."""


def analyze_first_10s(client, frame_paths, caption):
    """Send extracted frames to Claude vision for structured first-10s analysis."""
    import base64
    content = [{"type": "text", "text": VISION_PROMPT}]
    if caption:
        content.append({"type": "text", "text": f"\nCaption for context: {caption[:500]}"})
    for fp in frame_paths:
        try:
            with open(fp, "rb") as f:
                b64 = base64.standard_b64encode(f.read()).decode("ascii")
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": b64},
            })
        except Exception as e:
            print(f"  [warn] couldn't encode {fp}: {e}", file=sys.stderr)
    try:
        msg = client.messages.create(
            model=VISION_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": content}],
        )
        text = "".join(b.text for b in msg.content if b.type == "text").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL)
        return json.loads(text)
    except Exception as e:
        print(f"  [warn] vision analysis failed: {e}", file=sys.stderr)
        return None


def analyze_reel_frames_if_needed(client, row, reel, schema_props):
    """If the row doesn't yet have Reel Vision Analysis, download MP4, extract
    15 frames across the full reel arc, run vision, and write the result.
    Returns True if analysis was written."""
    if "Reel Vision Analysis" not in schema_props:
        return False
    existing = read_prop(row, "Reel Vision Analysis")
    if existing and existing.strip():
        return False
    media_url = reel.get("media_url")
    if not media_url:
        print(f"  [warn] no media_url for reel {reel.get('id')}", file=sys.stderr)
        return False
    with tempfile.TemporaryDirectory() as tmp:
        mp4_path = os.path.join(tmp, "reel.mp4")
        if not download_mp4(media_url, mp4_path):
            return False
        frames = extract_frames(mp4_path, tmp)
        if not frames:
            print(f"  [warn] no frames extracted for reel {reel.get('id')}", file=sys.stderr)
            return False
        print(f"  [vision] analyzing {len(frames)} frames for reel {reel.get('id')}", file=sys.stderr)
        analysis = analyze_first_10s(client, frames, reel.get("caption", ""))
    if not analysis:
        return False
    # Format as a compact text blob the analyst can read
    summary = (
        f"First frame: {analysis.get('first_frame_type','?')} — {analysis.get('first_frame_description','')}\n"
        f"Human first appears: {analysis.get('human_first_appearance','?')}\n"
        f"Human in first 3s: {analysis.get('human_present_in_first_3s','?')}\n"
        f"Wardrobe: {analysis.get('wardrobe_notes','')}\n"
        f"App appears: {analysis.get('app_appearance_timing','?')}\n"
        f"On-screen text sequence: {analysis.get('on_screen_text_sequence','none')}\n"
        f"Transition density: {analysis.get('transition_density_by_section','?')}\n"
        f"Mid-reel beat: {analysis.get('mid_reel_beat','')}\n"
        f"End beat: {analysis.get('end_beat','')}\n"
        f"Aesthetic: {', '.join(analysis.get('aesthetic_tags', []))}\n"
        f"Retention concerns: {analysis.get('retention_concerns','')}\n"
        f"Retention strengths: {analysis.get('retention_strengths','')}\n"
        f"Arc hypothesis: {analysis.get('arc_hypothesis','')}"
    )
    _patch_row(
        row["id"],
        {"properties": build_props(schema_props, {"Reel Vision Analysis": summary})},
    )
    return True


# ---- Rotation helpers (Swirl Series only) ----
def pick_next_hook(rows):
    """Pick a hook type that hasn't been used in the last 6 Swirl Series rows."""
    swirl_rows = [r for r in rows if is_swirl_series(r)]
    recent = []
    for row in swirl_rows[:6]:
        h = read_prop(row, "Hook Type")
        if h:
            recent.append(h)
    for h in HOOK_TYPES:
        if h not in recent:
            return h
    for h in reversed(recent):
        if h in HOOK_TYPES:
            return h
    return HOOK_TYPES[0]


def pick_next_split(rows):
    """Pick the Content Split that's behind in the Swirl Series catalog."""
    counts = {s: 0 for s in CONTENT_SPLITS}
    for row in rows:
        if not is_swirl_series(row):
            continue
        s = read_prop(row, "Content Split")
        if s in counts:
            counts[s] += 1
    return min(counts, key=counts.get)


# ---- Prompts ----
ANALYST_PROMPT = """You are the creative strategist for Swirlie, an indie latte art practice app. Brand voice: hand-drawn chalk-coffee-shop aesthetic, cozy, intentional, warm — quiet morning ritual meets indie coffee shop chalkboard.

You're planning the NEXT Swirl Series Instagram reel (will be assigned Reel #{next_reel_num} once posted). A separate writer will turn your brief into the actual script, so your job is JUDGMENT, not prose. Reason over real performance data and hand the writer a specific direction.

## Fixed constraints for the next reel (chosen by rotation logic)
- Hook Type: {hook_type}
- Content Split: {content_split}
- Slot Date (planned post date): {slot_date}

## Audience strategy (important)
- TODAY: primarily coffee community, with Swirlie app/dev as a SIDE narrative
- GOAL: eventually capture the DEV + COFFEE dual community. Around Reel 15 we start weighting dev content more heavily.
- RIGHT NOW (Reels 7-14): mostly coffee/lifestyle, with occasional dev moments as texture. Don't exclude dev entirely — keep it as a minority share of content — but lead with coffee/lifestyle hooks.
- EXPERIMENTATION MODE: sample size is small. Each reel is a probe. Prefer variation over consistency. Propose specific hypotheses each reel tests.

## Retention levers (ordered by confidence, from 2026-04-11 real-data analysis)
1. **Human in frame** + visually compelling wardrobe/styling is the primary retention driver. Julie has strong personal styling (K-drama-inspired diverse wardrobe — each look cohesive and aesthetic). Recommend specific wardrobe/styling variety when suggesting human-forward reels.
2. **Casual/personality captions** (voice, humor, light conversation) beat hashtag-spam captions on retention.
3. **NEVER open with an app/product screen.** Hard rule — Reel 5→6 A/B test proved this dropped retention 71%. App content only as middle-beat texture.
4. **Avoid passive openers** (wide shots from behind, black text transition cards). Reel 5's notes explicitly flagged these.
5. **12-18s target reel length.** Shorter reels held retention better in the data.

## Off-script divergence patterns (NEW signal as of 2026-04-19)
Some Posted rows below have an `Off-Script Delta` line — this means Julie filmed something different from what was originally planned for that slot. Treat divergence as STRONG signal: when she diverges from a script, what she actually films usually outperforms the plan (her improvisation reflects what she actually wants to make). Look for patterns across multiple Off-Script Deltas — if she keeps dropping app content, shortening reels, or shifting subject in a particular direction, future scripts should bake those tendencies in upfront.

## Reel 4 parallel concept (worth iterating)
Reel 4 paired Julie pouring latte with Julie doing dev work — a "two-process-in-parallel" format. 11.7s avg watch (~3x everything else), but only 13 reach because she forgot hashtags. Concept is compelling as a recurring series format: brewing ↔ building, morning ritual ↔ code commit, steam wand ↔ terminal typing. Consider proposing variations.

## Hashtag rule
Exactly 5 hashtags per caption. No more. Writer will enforce but you should suggest the 5 best ones for this reel's angle in your brief (don't always use the same ones — match to theme). ALWAYS include #swirlie as one of the 5.

## Content rules
- ALWAYS thread Swirlie into the caption naturally — every reel must mention the app, even pure lifestyle ones. A single sentence is enough ("tracking my progress in Swirlie", "the app is learning too"). Never generate a reel with zero app connection.
- #swirlie and #buildinginpublic must always be in the 5 hashtags. The other 3 should vary by theme.
- Content Split should default to App/Lifestyle Blend. Only pick Lifestyle if the concept genuinely has no app angle or Julie has no app footage available. A caption mention of Swirlie is enough to qualify as Blend.
- Optimize for SAVES and SHARES — currently zero across all reels. Every brief should include at least one save trigger (practical tip, aesthetic reference, educational moment, or a "save for later" worthy shot).
- Caption ending style: vary between questions, statements, and poetic endings. Questions on roughly 1 in 3 reels, never back-to-back. Check past scripted reels to see what the last one used and pick something different.
- Audio: recommend trending IG audio over custom Suno beats. Trending lo-fi/acoustic/cozy sounds get algorithmic push that custom beats don't. Include vibe keywords for searching trending sounds.

## Recent POSTED Swirl Series reels (sorted by avg watch time desc — emulate the top performers)
{past_posted}

## Recent SCRIPTED Swirl Series reels (avoid duplicating these ideas)
{past_scripted}

## Recent OTHER posts (non-Swirl IG content for cross-category awareness)
{other_posts}

## Recent first-10s frame analyses (for retention pattern learning)
{frame_analyses}

Return a creative brief as a JSON object:
{{
  "theme": "2-4 word theme label",
  "angle": "1-2 sentences on the specific angle, why it fits the fixed hook type + content split, and what hypothesis it tests",
  "informed_by": "cite 1-3 past reels by number with WHAT specifically from each is shaping this decision (e.g. 'Reel #4 held 11.7s avg watch via parallel brewing/building framing — lean into the same dual-process shape'). Reference metrics and first-10s analyses, not just captions.",
  "experiment_note": "1 sentence on what this reel is an A/B probe for — what's being tested vs the previous reel",
  "avoid": "1 sentence on what NOT to do — specific patterns that killed retention in recent data",
  "mood": "3-5 comma-separated mood/aesthetic words",
  "wardrobe_direction": "1-2 sentences on wardrobe/styling if a human is in frame. Lean K-drama-inspired variety. Say 'N/A' only if the reel genuinely has no human.",
  "suggested_hashtags": ["tag1", "tag2", "tag3", "tag4", "tag5"]
}}

Return ONLY the JSON. No prose, no code fencing."""


WRITER_PROMPT = """You are writing the next Instagram reel script for Swirlie, a latte art practice app.

Brand voice: hand-drawn chalk-coffee-shop aesthetic, cozy, intentional, warm. Indie coffee shop chalkboard meets quiet morning ritual.

The creative strategist handed you this brief:

Theme: {theme}
Angle: {angle}
Informed by: {informed_by}
Experiment note: {experiment_note}
Avoid: {avoid}
Mood: {mood}
Wardrobe direction: {wardrobe_direction}
Suggested hashtags: {suggested_hashtags}

## Variation direction for THIS pass
{variation_hint}

## Fixed constraints
- Hook Type: {hook_type}
- Content Split: {content_split}
- Reel Total Time: 12-18 seconds (shorter end preferred)
- Caption: exactly 5 hashtags, no more, no less. ALWAYS include #swirlie and #buildinginpublic. Use the suggested hashtags from the brief for the other 3 unless you have a strong reason otherwise.
- **Capitalization rule (UPDATED 2026-04-19)**: Use proper sentence case in BOTH the title AND the caption. First letter capitalized, "I" always capitalized as a pronoun, proper nouns (Swirlie, Ginger, place names, brand names) capitalized, acronyms capitalized. Casual conversational tone is fine, but capitalization is standard. The earlier "lowercase sentence starts" rule was wrong — Julie's actually-posted captions use sentence case ("The swirl never looks right.", "Survived week 1!", "Couldn't figure out the pour..."). Do NOT default to all-lowercase.
- **Swirlie threading**: ALWAYS mention Swirlie naturally in the caption. Every reel must have an app connection — even one sentence counts ("tracking my progress in Swirlie"). Never generate a caption with zero app mention.
- **Caption ending style**: Vary between questions, statements, and poetic endings. Check the brief's suggested approach. Questions roughly 1 in 3 reels, never back-to-back with a previous scripted reel that also ended with a question.
- **Save trigger**: Include at least one save-worthy moment (practical tip overlay, aesthetic reference shot, educational beat). Zero saves across all posted reels is the biggest engagement gap.
- NEVER open with an app/product screen (hard rule from Reel 5→6 A/B test)
- Avoid passive wide shots from behind and black text transition cards (killed retention on Reel 5)
- **Soundtrack format (renamed from Suno Prompt)**: Pick EITHER an IG trending audio direction OR a Suno generation prompt — whichever fits the reel's energy. Format the value with an explicit prefix:
  - `IG TRENDING: search "<terms>" — reference accounts: @x, @y, @z. Use if you find a sound with momentum.`
  - `SUNO: <full prompt with BPM, instruments, vibe>`
  - Default to IG trending for high-movement / dance-coded reels; Suno for ambient / narrative / aesthetic-stillness reels. Trending audio gets algorithmic push that custom beats miss.

## Your job
Write the full reel script executing the brief under the variation direction above. Match the mood words exactly. If the brief includes wardrobe direction, bake it into the clip descriptions explicitly (what she's wearing, color palette, styling detail).

Return ONLY a JSON object with these exact keys:
{{
  "title": "short evocative title in sentence case — DO NOT prefix with 'Reel N —', DO NOT use all-lowercase",
  "clip_order": "1. First clip description with specific visual + what Julie is wearing/doing (1-2 sentences each)\\n2. Second clip...\\n(5-8 clips total, weighted toward the first 3 seconds)",
  "on_screen_text": "Overlay 1 | Overlay 2 | Overlay 3 (3-5 short phrases, max ~6 words each — these appear ON the video and may use stylistic lowercase if it fits the chalk aesthetic)",
  "caption": "warm conversational caption in proper sentence case, ending with EXACTLY 5 hashtags",
  "cover_scene": "one sentence describing the thumbnail moment — must be human-first or close-up, never an app screen",
  "soundtrack": "either 'IG TRENDING: ...' or 'SUNO: ...' per the Soundtrack format rule above",
  "transitions": "brief notes on cuts / match cuts / whip pans — avoid black text cards",
  "reel_total_time": "14s",
  "notes": "2-3 sentence rationale. MUST begin by citing the theme and the past reels named in 'informed_by', then state the specific hypothesis this reel is testing.",
  "variation_label": "one of: Text-forward / Visual-only / Humor-led"
}}

Return only the JSON, no prose, no code fencing."""


# Three variation directives used across the writer's three passes. The
# analyst picks a theme; the writer executes it three different ways.
VARIATION_HINTS = [
    (
        "Text-forward",
        "Include 2-3 on-screen text overlays that carry meaning "
        "(title beat, observation, payoff). Text should feel hand-drawn/chalky, never a "
        "plain black card. Let on-screen text do half the storytelling.",
    ),
    (
        "Visual-only",
        "Minimize on-screen text (1 overlay max, often zero). Let composition, motion, "
        "wardrobe, and light carry the entire story. This is the cinematic variation — "
        "should feel like a 14-second short film.",
    ),
    (
        "Humor-led",
        "Inject one unexpected humor beat — a self-deprecating moment, a quiet visual gag, "
        "or a caption that breaks the cozy register briefly before returning to it. The "
        "tone should land somewhere between Reel #4's 'Aphrodite scone' voice and a dry aside.",
    ),
]


def summarize_row_for_prompt(row, full_notes: bool = False):
    """Format a row for the analyst prompt.

    If full_notes=True (used for the top-3 by avg watch time), include the
    FULL Notes text and the full Reel Vision Analysis. Julie's hand-written
    notes on top performers are the highest-signal input — never truncate them.
    Otherwise use compact summaries.

    Off-Script Delta (when present) is always surfaced — it's a load-bearing
    signal about how Julie's actual filming diverges from generated plans.
    Slot Date is shown for Scripted rows so the analyst knows the timing.
    """
    title = read_prop(row, "Title") or "(untitled)"
    cap = read_prop(row, "Caption") or ""
    notes = read_prop(row, "Notes") or ""
    reel_num = read_prop(row, "Reel #")
    hook = read_prop(row, "Hook Type") or ""
    split = read_prop(row, "Content Split") or ""
    status = read_prop(row, "Status") or ""
    views = read_prop(row, "Views")
    reach = read_prop(row, "Reach")
    watch = read_prop(row, "Avg Watch Time") or ""
    frame = read_prop(row, "Reel Vision Analysis") or ""
    delta = read_prop(row, "Off-Script Delta") or ""
    slot = read_prop(row, "Slot Date") or ""
    metric_str = ""
    if views is not None or reach is not None or watch:
        metric_str = f" [views:{views or '?'} reach:{reach or '?'} watch:{watch or '?'}]"
    id_str = f"Reel #{reel_num}" if reel_num else (f"Slot {slot[:10]}" if slot else "Scripted")
    parts = [f"{id_str} [{status} {hook}/{split}]{metric_str} {title}"]
    if cap:
        parts.append(f"  Caption: {cap[:300 if full_notes else 200]}")
    if delta:
        parts.append(f"  Off-Script Delta: {delta[:400]}")
    if notes:
        if full_notes:
            parts.append(f"  Notes (FULL — top performer, high signal):\n    {notes}")
        else:
            parts.append(f"  Notes: {notes[:300]}")
    if frame:
        if full_notes:
            # Include the full frame analysis for top performers
            indented = "\n    ".join(frame.splitlines())
            parts.append(f"  Full frame analysis:\n    {indented}")
        else:
            # Only first line for the rest — keeps token budget in check
            parts.append(f"  Frame analysis: {frame.splitlines()[0] if frame else ''}")
    return "\n".join(parts)


def watch_time_seconds(row):
    """Parse 'Avg Watch Time' text like '4.8s' into float seconds, or 0."""
    s = read_prop(row, "Avg Watch Time")
    if not s:
        return 0.0
    m = re.match(r"([\d.]+)", str(s))
    return float(m.group(1)) if m else 0.0


# ---- Claude call helpers ----
def _call_claude(client, model, prompt, max_tokens):
    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL)
    return json.loads(text)


def analyze_context(client, rows, hook_type, content_split, next_reel_num, slot_date):
    """Pass 1 — Sonnet analyst: strategic reasoning over real performance data.

    Filters Swirl Series rows for the script-gen reasoning loop. Surfaces a
    small sample of Other-tagged rows for cross-category awareness. Top-3
    Swirl performers by avg watch time get their FULL notes + full frame
    analysis. The next ~12 get compact summaries.
    """
    swirl_rows = [r for r in rows if is_swirl_series(r)]
    posted = [r for r in swirl_rows if read_prop(r, "Status") == "Posted"]
    # Sort by avg watch time desc so analyst sees top performers first
    posted.sort(key=watch_time_seconds, reverse=True)
    posted = posted[:15]
    scripted = [r for r in swirl_rows if read_prop(r, "Status") == "Scripted"][:6]
    other_posts = [
        r for r in rows
        if not is_swirl_series(r) and read_prop(r, "Status") == "Posted"
    ][:5]

    # Top 3 get full-fidelity context; everyone else compact
    past_posted_lines = []
    for i, r in enumerate(posted):
        past_posted_lines.append(summarize_row_for_prompt(r, full_notes=(i < 3)))

    frame_lines = []
    for r in posted[:8]:
        fa = read_prop(r, "Reel Vision Analysis")
        if fa:
            frame_lines.append(f"Reel #{read_prop(r, 'Reel #')}:\n{fa}")

    other_lines = []
    for r in other_posts:
        title = read_prop(r, "Title") or "(untitled)"
        cap = (read_prop(r, "Caption") or "")[:160]
        watch = read_prop(r, "Avg Watch Time") or ""
        reach = read_prop(r, "Reach")
        other_lines.append(f"- {title} [reach:{reach or '?'} watch:{watch or '?'}] {cap}")

    prompt = ANALYST_PROMPT.format(
        next_reel_num=next_reel_num,
        slot_date=slot_date,
        hook_type=hook_type,
        content_split=content_split,
        past_posted="\n\n".join(past_posted_lines) or "(none yet)",
        past_scripted="\n".join(summarize_row_for_prompt(r) for r in scripted) or "(none yet)",
        other_posts="\n".join(other_lines) or "(none)",
        frame_analyses="\n\n".join(frame_lines) or "(no frame analyses yet — vision step may still be running)",
    )
    brief = _call_claude(client, ANALYST_MODEL, prompt, max_tokens=900)
    print(
        f"Analyst ({ANALYST_MODEL}): theme='{brief.get('theme')}' "
        f"experiment='{brief.get('experiment_note','')[:80]}'",
        file=sys.stderr,
    )
    return brief


def _enforce_hashtag_cap(data):
    """Backstop: trim any script's caption to MAX_HASHTAGS hashtags."""
    caption = data.get("caption", "")
    tags = re.findall(r"#\w+", caption)
    if len(tags) <= MAX_HASHTAGS:
        return data
    keep = set(tags[:MAX_HASHTAGS])

    def filter_tag(m):
        return m.group(0) if m.group(0) in keep else ""

    caption = re.sub(r"#\w+", filter_tag, caption)
    caption = re.sub(r"\s{2,}", " ", caption).strip()
    missing = [t for t in tags[:MAX_HASHTAGS] if t not in caption]
    if missing:
        caption = caption + "\n\n" + " ".join(missing)
    data["caption"] = caption
    return data


def write_script(client, brief, hook_type, content_split, variation_label, variation_hint):
    """Pass 2 — Sonnet writer: turn the brief into one reel script variation.

    Called 3x per run with different variation_label/hint to produce a
    text-forward, a visual-only, and a humor-led variant.
    """
    suggested = brief.get("suggested_hashtags", [])
    if isinstance(suggested, list):
        suggested_str = ", ".join(suggested)
    else:
        suggested_str = str(suggested)
    prompt = WRITER_PROMPT.format(
        theme=brief.get("theme", ""),
        angle=brief.get("angle", ""),
        informed_by=brief.get("informed_by", ""),
        experiment_note=brief.get("experiment_note", ""),
        avoid=brief.get("avoid", ""),
        mood=brief.get("mood", ""),
        wardrobe_direction=brief.get("wardrobe_direction", "N/A"),
        suggested_hashtags=suggested_str,
        variation_hint=f"[{variation_label}] {variation_hint}",
        hook_type=hook_type,
        content_split=content_split,
    )
    data = _call_claude(client, WRITER_MODEL, prompt, max_tokens=2000)
    data.setdefault("variation_label", variation_label)
    return _enforce_hashtag_cap(data)


def generate_script_variations(client, rows, hook_type, content_split, next_reel_num, slot_date):
    """Two-stage pipeline:
    1. Sonnet analyst produces one creative brief (shared across all variants).
    2. Sonnet writer is called 3x with different variation directives,
       returning (variations_list, brief).

    The main row is written from variations[0] (text-forward), and alts
    from variations[1] (visual-only) + variations[2] (humor-led) get
    serialized into the 'Alt Scripts' column as a compact text blob.

    next_reel_num is passed to the analyst for context (the Reel # the
    next-posted Swirl Series reel will receive). slot_date is the planned
    posting date for the Scripted row being generated.
    """
    brief = analyze_context(client, rows, hook_type, content_split, next_reel_num, slot_date)
    variations = []
    for label, hint in VARIATION_HINTS:
        print(f"  [writer] variation: {label}", file=sys.stderr)
        script = write_script(client, brief, hook_type, content_split, label, hint)
        variations.append(script)
    return variations, brief


def format_alt_scripts(variations):
    """Serialize the non-main variations (B and C) into a compact text blob
    for the Alt Scripts column. Julie can read and swap into main content
    if she prefers one of the alternates."""
    if len(variations) < 2:
        return ""
    lines = []
    for v in variations[1:]:
        label = v.get("variation_label", "(unknown)")
        lines.append(f"=== {label} ===")
        lines.append(f"Title: {v.get('title','')}")
        lines.append(f"Clip order:\n{v.get('clip_order','')}")
        lines.append(f"On-screen text: {v.get('on_screen_text','')}")
        lines.append(f"Caption:\n{v.get('caption','')}")
        lines.append(f"Cover scene: {v.get('cover_scene','')}")
        # Renamed from suno_prompt -> soundtrack as of 2026-04-19
        soundtrack = v.get("soundtrack") or v.get("suno_prompt", "")
        lines.append(f"Soundtrack: {soundtrack}")
        lines.append(f"Transitions: {v.get('transitions','')}")
        lines.append(f"Notes: {v.get('notes','')}")
        lines.append("")  # blank line between variations
    return "\n".join(lines).strip()


def create_script_row(variations, hook_type, content_split, slot_date, schema_props):
    """Create a single Scripted row populated from the first variation, with
    the other two serialized into the 'Alt Scripts' column for review.

    As of 2026-04-19: Scripted rows have NO Reel # (Reel # is only assigned
    when a row becomes Posted AND tagged Swirl Series). Identifier is
    Slot Date instead. Tags Category = Swirl Series automatically.
    """
    if isinstance(variations, dict):
        # Backwards-compat: a single script dict was passed
        variations = [variations]
    main = variations[0]
    alts_blob = format_alt_scripts(variations) if len(variations) > 1 else ""

    title_prop = next((n for n, p in schema_props.items() if p["type"] == "title"), None)
    # Writer returns 'soundtrack' (renamed from suno_prompt). Accept both for
    # backwards compat with any in-flight responses.
    soundtrack = main.get("soundtrack") or main.get("suno_prompt", "")

    updates = {
        title_prop: main["title"],
        "Status": "Scripted",
        "Slot Date": slot_date,
        "Clip Order": main["clip_order"],
        "On-Screen Text": main["on_screen_text"],
        "Caption": main["caption"],
        "Hook Type": hook_type,
        "Content Split": content_split,
        "Cover Scene": main["cover_scene"],
        "Soundtrack": soundtrack,
        "Transitions": main["transitions"],
        "Reel Total Time": main["reel_total_time"],
        "Notes": main["notes"],
    }
    if "Category" in schema_props:
        updates["Category"] = [CATEGORY_SWIRL]
    if alts_blob and "Alt Scripts" in schema_props:
        updates["Alt Scripts"] = alts_blob

    body = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": build_props(schema_props, updates),
    }
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS, json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json()


def should_generate_script_today() -> bool:
    """True if today is Mon/Wed/Fri in America/Chicago local time, or if
    FORCE_REGEN=1 is set in env."""
    if FORCE_REGEN:
        return True
    # America/Chicago is UTC-6 (CST) or UTC-5 (CDT). For simplicity use UTC
    # and accept a ~6h offset at the day boundary. We run at 16:00 UTC which
    # is always mid-morning local, so UTC weekday == local weekday.
    return datetime.now(timezone.utc).weekday() in SCRIPT_GEN_WEEKDAYS


# ---- Main ----
def main():
    run_started = datetime.now(timezone.utc)
    summary = {
        "ig_fetched": 0,
        "metrics_refreshed": [],
        "promoted_to_posted": [],
        "twins_created": [],
        "others_created": [],
        "aged_out_to_skipped": [],
        "unmatched_warnings": [],
        "frames_analyzed": [],
        "new_script_row": None,
        "script_gen_skipped": False,
        "errors": [],
    }
    anthropic_client = Anthropic(api_key=ANTHROPIC_KEY)

    try:
        # Refresh IG token on the 1st of each month (resets 60-day expiry window).
        # Every other day this is a no-op.
        maybe_refresh_ig_token()

        db = notion_get_db()
        schema_props = db["properties"]
        rows = notion_query_all()
        print(f"Notion: {len(rows)} existing rows", file=sys.stderr)

        reels = fetch_ig_reels()
        summary["ig_fetched"] = len(reels)
        print(f"IG: {len(reels)} reels fetched", file=sys.stderr)

        # Snapshot the pre-reconcile IG/Notion delta so the script-gen
        # step later can tell whether this run actually had a new reel
        # to learn from. Reconcile always runs (that's how Notion
        # self-heals toward IG reality — unmatched reels become rows,
        # stale Scripted rows age out). The count check only gates
        # script generation, which is the step that fires "too early"
        # when Julie hasn't posted yet.
        pre_sync_ig_count = len(reels)
        pre_sync_notion_posted = sum(
            1 for r in rows if read_prop(r, "Status") == "Posted"
        )

        # If TARGET_MEDIA_ID is set via workflow_dispatch, narrow the IG
        # candidates to just that one reel (and skip every other path 1/2/3/4
        # check entirely on the rest). Useful for reprocessing a specific
        # off-script reel without touching the others.
        if TARGET_MEDIA_ID:
            reels = [r for r in reels if str(r.get("id")) == TARGET_MEDIA_ID]
            print(
                f"TARGET_MEDIA_ID set — narrowed to {len(reels)} reel(s) matching {TARGET_MEDIA_ID}",
                file=sys.stderr,
            )

        for r in reels:
            r["_insights"] = fetch_insights(r["id"])

        # 4-tier matcher
        refreshes, promotes, twins, others = reconcile(reels, rows, schema_props)

        # Path 1: refresh metrics on already-Posted rows
        for page_id, reel in refreshes:
            try:
                refresh_row_metrics(page_id, reel, schema_props)
                summary["metrics_refreshed"].append(page_id)
            except Exception as e:
                summary["errors"].append(f"refresh {page_id}: {e}")

        # Reel # accounting — assign sequentially as we promote/create new
        # Posted Swirl Series rows in this run. Start from current max + 1.
        running_reel_num = next_swirl_reel_number(rows)

        # Path 2: promote Scripted rows whose caption matched the plan
        for page_id, reel in promotes:
            try:
                promote_row_to_posted(page_id, reel, schema_props, running_reel_num)
                summary["promoted_to_posted"].append(
                    {"page_id": page_id, "reel_num": running_reel_num}
                )
                running_reel_num += 1
            except Exception as e:
                summary["errors"].append(f"promote {page_id}: {e}")

        # Path 3: create Posted twins for off-script reels (with Off-Script Delta)
        for scripted_row, reel in twins:
            try:
                delta = compute_off_script_delta(anthropic_client, scripted_row, reel)
                new_page = create_posted_row(
                    reel,
                    schema_props,
                    CATEGORY_SWIRL,
                    original_plan_id=scripted_row["id"],
                    off_script_delta=delta,
                    reel_num=running_reel_num,
                )
                summary["twins_created"].append({
                    "page_id": new_page["id"],
                    "reel_num": running_reel_num,
                    "original_plan_id": scripted_row["id"],
                })
                running_reel_num += 1
            except Exception as e:
                summary["errors"].append(f"twin {scripted_row.get('id')}: {e}")

        # Path 4: create Other-tagged Posted rows for non-Swirl IG content
        for reel in others:
            try:
                new_page = create_posted_row(
                    reel,
                    schema_props,
                    CATEGORY_OTHER,
                )
                summary["others_created"].append({
                    "page_id": new_page["id"],
                    "ig_media_id": reel.get("id"),
                })
            except Exception as e:
                summary["errors"].append(f"other {reel.get('id')}: {e}")

        # Aging pass: Scripted rows with Slot Date > 7 days past -> Skipped.
        try:
            aged = age_out_scripted_rows(rows, schema_props)
            summary["aged_out_to_skipped"] = aged
        except Exception as e:
            summary["errors"].append(f"age_out: {e}")

        # Re-query so frame analysis + script gen see the fresh state.
        rows = notion_query_all()

        # Frame extraction + vision analysis for Posted rows missing analysis
        reels_by_id = {r["id"]: r for r in reels}
        for row in rows:
            if read_prop(row, "Status") != "Posted":
                continue
            media_id = read_prop(row, "IG Media ID")
            if not media_id:
                continue
            reel = reels_by_id.get(str(media_id))
            if not reel:
                continue
            try:
                if analyze_reel_frames_if_needed(anthropic_client, row, reel, schema_props):
                    summary["frames_analyzed"].append(row["id"])
            except Exception as e:
                summary["errors"].append(f"frame analysis {row['id']}: {e}")

        # Script generation — only on Mon/Wed/Fri (or if FORCE_REGEN=1)
        # Additional gate: at run start, IG must have been exactly one more
        # than Notion's Posted count (the "one new reel just posted" shape).
        # Any other delta means either Julie hasn't posted today yet (cron
        # fired early) or something else is off — generating a new script
        # now would be based on stale data and create a duplicate Scripted
        # row. FORCE_REGEN bypasses this gate for manual dispatch.
        new_reel_pending_at_start = (
            pre_sync_ig_count == pre_sync_notion_posted + 1
        )
        if should_generate_script_today() and not new_reel_pending_at_start and not FORCE_REGEN:
            msg = (
                f"Script-gen day, but at run start IG had {pre_sync_ig_count} "
                f"reels and Notion had {pre_sync_notion_posted} Posted rows "
                f"(expected IG=+1). No fresh reel to analyze — skipping "
                f"script generation. Reconcile still ran. "
                f"Set FORCE_REGEN=1 to override."
            )
            print(msg, file=sys.stderr)
            summary["script_gen_skipped"] = True
        elif should_generate_script_today():
            # Re-query one more time so the analyst sees the frame analyses
            rows = notion_query_all()
            hook_type = pick_next_hook(rows)
            content_split = pick_next_split(rows)
            next_reel_num = next_swirl_reel_number(rows)
            slot_date = next_open_slot_date(rows)
            print(
                f"Next reel plan: Slot {slot_date} (will be #{next_reel_num} when posted) "
                f"hook={hook_type} split={content_split}",
                file=sys.stderr,
            )
            variations, _brief = generate_script_variations(
                anthropic_client, rows, hook_type, content_split, next_reel_num, slot_date
            )
            new_page = create_script_row(
                variations, hook_type, content_split, slot_date, schema_props
            )
            summary["new_script_row"] = {
                "id": new_page["id"],
                "reel_num": next_reel_num,
                "slot_date": slot_date,
                "title": variations[0]["title"],
                "variation_labels": [v.get("variation_label", "?") for v in variations],
            }
        else:
            today = datetime.now(timezone.utc).strftime("%A")
            print(
                f"Not a script-gen day ({today}) — skipping script generation. "
                f"Set FORCE_REGEN=1 to override.",
                file=sys.stderr,
            )
            summary["script_gen_skipped"] = True

    except Exception as e:
        summary["errors"].append(f"fatal: {e}")

    # Summary
    print("\n=== Summary ===")
    print(f"IG reels fetched: {summary['ig_fetched']}")
    print(f"Metrics refreshed on Posted rows (Path 1): {len(summary['metrics_refreshed'])}")
    for pid in summary["metrics_refreshed"]:
        print(f"  - {pid}")
    print(f"Scripted -> Posted promotions (Path 2, caption matched plan): {len(summary['promoted_to_posted'])}")
    for entry in summary["promoted_to_posted"]:
        print(f"  - {entry['page_id']} (Reel #{entry['reel_num']})")
    print(f"Posted twins created (Path 3, off-script): {len(summary['twins_created'])}")
    for entry in summary["twins_created"]:
        print(f"  - {entry['page_id']} (Reel #{entry['reel_num']}, plan={entry['original_plan_id']})")
    print(f"Other rows created (Path 4, non-Swirl): {len(summary['others_created'])}")
    for entry in summary["others_created"]:
        print(f"  - {entry['page_id']} (ig_id={entry['ig_media_id']})")
    print(f"Scripted rows aged to Skipped (Slot Date > {SKIPPED_AGE_DAYS}d past): {len(summary['aged_out_to_skipped'])}")
    for pid in summary["aged_out_to_skipped"]:
        print(f"  - {pid}")
    print(f"Frames analyzed (full-arc vision): {len(summary['frames_analyzed'])}")
    for pid in summary["frames_analyzed"]:
        print(f"  - {pid}")
    if summary["new_script_row"]:
        nsr = summary["new_script_row"]
        print(
            f"New Scripted row: Slot {nsr['slot_date']} (will be #{nsr['reel_num']} when posted) "
            f"— {nsr['title']} ({nsr['id']})"
        )
        if nsr.get("variation_labels"):
            print(f"  Variations generated: {', '.join(nsr['variation_labels'])} (main=first, alts in 'Alt Scripts')")
    elif summary["script_gen_skipped"]:
        print("New Scripted row: (skipped — not a script-gen day)")
    else:
        print("New Scripted row: (not created)")
    # Cost is tracked centrally in JulzOps via the Anthropic Cost API; see
    # julzops/src/app/api/jobs/reconcile-usage/route.ts. Local accounting was
    # removed when the Admin API became available on Organization accounts.

    # Ship run summary to JulzOps (non-fatal — never blocks exit)
    post_run_to_julzops(summary, run_started, datetime.now(timezone.utc))

    if summary["errors"]:
        print(f"\nErrors: {len(summary['errors'])}")
        for e in summary["errors"]:
            print(f"  - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
