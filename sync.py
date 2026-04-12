#!/usr/bin/env python3
"""Swirl Series content automation.

Runs on Mon/Wed/Sat at 10am America/Chicago (15:00 UTC), 2 days before each
Mon/Wed/Fri posting slot.

Each run does three things:
1. Refreshes IG metrics on all existing Posted Notion rows (daily-drift values
   like views/likes/reach/avg_watch_time keep updating for weeks).
2. Promotes any Scripted row that just got posted to IG → Posted, populating
   metrics + permalink + post date + IG media ID.
3. On script-generation days (Mon/Wed/Sat), analyzes recent performance and
   first-10s frame descriptions, then drafts ONE new Scripted row for the next
   posting slot.

HARD RULE: the sync never auto-creates rows for IG posts that don't have a
matching Notion row. Unmatched IG reels get logged as warnings and skipped.
The only row this script creates is the single generated Scripted row in
step 3. Every other row must exist in Notion already (authored by Julie).

All credentials come from environment variables. See README.md.
"""
import difflib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import requests
from anthropic import Anthropic

# ---- Config from env ----
# IG_TOKEN is loaded lazily from the Notion Asset library page (primary) or
# the env var (fallback). See load_ig_token() and the Asset library design.
_IG_TOKEN = None  # set by load_ig_token() at the start of main()
IG_USER_ID = os.environ["IG_USER_ID"]
# The Asset library page in the Swirl Series Content Calendar DB.
# Its body content IS the IG token — nothing else.
ASSET_LIBRARY_PAGE_ID = os.environ.get(
    "ASSET_LIBRARY_PAGE_ID", "33f52fb3-31e4-8114-9809-cadcc5191947"
)
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
# Two-model setup: Opus reasons over past performance, Sonnet turns the brief
# into the final reel script JSON, and Sonnet Vision analyzes first-10s frames.
ANALYST_MODEL = os.environ.get("ANALYST_MODEL", "claude-opus-4-6")
WRITER_MODEL = os.environ.get("WRITER_MODEL", "claude-sonnet-4-6")
VISION_MODEL = os.environ.get("VISION_MODEL", "claude-sonnet-4-6")
# Script generation runs on Sat=5, Mon=0, Wed=2 (Python weekday() numbering).
SCRIPT_GEN_WEEKDAYS = {0, 2, 5}
# Allow overriding via env for manual runs / testing.
FORCE_SCRIPT_GEN = os.environ.get("FORCE_SCRIPT_GEN", "").lower() in ("1", "true", "yes")

# ---- Cost tracking ----
# Per-million-token pricing in USD, from anthropic.com/pricing (2026).
MODEL_PRICING = {
    "claude-opus-4-6": (15.0, 75.0),
    "claude-opus-4-6[1m]": (15.0, 75.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5-20251001": (0.80, 4.0),
}
# Hard abort if a single run would exceed this (safety net against loops/bugs).
MAX_COST_PER_RUN_USD = float(os.environ.get("MAX_COST_PER_RUN_USD", "2.00"))

# Module-level cost tracker. Populated by track_usage() and read in main()'s summary.
_run_usage = {
    "total_usd": 0.0,
    "by_model": {},  # model → {"input_tokens": int, "output_tokens": int, "cost_usd": float, "calls": int}
}


def track_usage(model: str, input_tokens: int, output_tokens: int) -> float:
    """Compute and record the $ cost of one API call. Returns the cost."""
    pricing = MODEL_PRICING.get(model)
    if not pricing:
        # Unknown model — record tokens but zero cost (conservative).
        cost = 0.0
    else:
        in_price, out_price = pricing
        cost = (input_tokens / 1_000_000.0) * in_price + (output_tokens / 1_000_000.0) * out_price
    _run_usage["total_usd"] += cost
    bucket = _run_usage["by_model"].setdefault(
        model, {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0, "calls": 0}
    )
    bucket["input_tokens"] += input_tokens
    bucket["output_tokens"] += output_tokens
    bucket["cost_usd"] += cost
    bucket["calls"] += 1
    return cost


def check_budget():
    """Abort the run if total cost has exceeded MAX_COST_PER_RUN_USD."""
    if _run_usage["total_usd"] >= MAX_COST_PER_RUN_USD:
        raise RuntimeError(
            f"Run cost ${_run_usage['total_usd']:.4f} reached max ${MAX_COST_PER_RUN_USD:.2f} — aborting."
        )

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


# ---- IG token management (Notion Asset library is the source of truth) ----
def _read_notion_page_content(page_id: str) -> str:
    """Read a Notion page's block children and return their plain text."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=30)
    r.raise_for_status()
    texts = []
    for block in r.json().get("results", []):
        btype = block.get("type", "")
        inner = block.get(btype, {})
        for rt in inner.get("rich_text", []):
            texts.append(rt.get("plain_text", ""))
    return "".join(texts).strip()


def _write_notion_page_content(page_id: str, text: str):
    """Replace a Notion page's content with a single paragraph block."""
    # First, delete all existing children
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=30)
    r.raise_for_status()
    for block in r.json().get("results", []):
        requests.delete(
            f"https://api.notion.com/v1/blocks/{block['id']}",
            headers=NOTION_HEADERS, timeout=30,
        )
    # Then append the new content as a single paragraph
    requests.patch(
        url,
        headers=NOTION_HEADERS,
        json={"children": [
            {"type": "paragraph", "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            }}
        ]},
        timeout=30,
    )


def load_ig_token() -> str:
    """Load the IG token from the Notion Asset library page (primary) or the
    env var (fallback). Returns the token string."""
    global _IG_TOKEN
    try:
        token = _read_notion_page_content(ASSET_LIBRARY_PAGE_ID)
        if token and len(token) > 50:
            print(f"  [token] IG token loaded from Notion Asset library ({len(token)} chars)", file=sys.stderr)
            _IG_TOKEN = token
            return token
    except Exception as e:
        print(f"  [warn] couldn't read IG token from Notion: {e}", file=sys.stderr)
    # Fallback to env var
    token = os.environ.get("IG_TOKEN", "")
    if token:
        print("  [token] IG token loaded from env var (fallback)", file=sys.stderr)
        _IG_TOKEN = token
    else:
        raise RuntimeError("No IG token found in Notion Asset library or IG_TOKEN env var")
    return _IG_TOKEN


def refresh_ig_token():
    """Refresh the long-lived IG token and write the new one back to Notion.

    IG long-lived tokens last 60 days. Calling the refresh endpoint every run
    resets the window (idempotent). The refreshed token is written back to the
    Asset library page in Notion (the source of truth) and also to the GH
    Actions secret if gh CLI is available (belt + suspenders).
    """
    global _IG_TOKEN
    url = "https://graph.instagram.com/refresh_access_token"
    params = {"grant_type": "ig_refresh_token", "access_token": _IG_TOKEN}
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
        # Write to Notion (primary)
        try:
            _write_notion_page_content(ASSET_LIBRARY_PAGE_ID, new_token)
            print(f"  [token] Notion Asset library updated with refreshed token", file=sys.stderr)
        except Exception as e:
            print(f"  [warn] Notion token write failed: {e}", file=sys.stderr)
        # Also update GH secret if gh CLI is available (backup)
        try:
            repo = os.environ.get("GITHUB_REPOSITORY", "jw-yue/swirl-series-automation")
            result = subprocess.run(
                ["gh", "secret", "set", "IG_TOKEN", "--body", new_token, "--repo", repo],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                print(f"  [token] GH secret also updated", file=sys.stderr)
        except Exception:
            pass  # gh CLI not available in all environments — Notion is the truth
        _IG_TOKEN = new_token
        days = int(int(expires_in) / 86400) if str(expires_in).isdigit() else "?"
        print(f"  [token] IG token refreshed (expires_in={expires_in}s, ~{days} days)", file=sys.stderr)
        return True
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
        "access_token": _IG_TOKEN,
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


# ---- Matching: IG Media ID first, fuzzy caption fallback ----
def match_by_media_id(reel_id, rows):
    """Exact match on the IG Media ID column. Rock solid, no false positives."""
    for row in rows:
        row_id = read_prop(row, "IG Media ID")
        if row_id and str(row_id).strip() == str(reel_id).strip():
            return row
    return None


def row_posted_date(page, schema_props):
    for field in ("Posted Date", "Post Date", "Date", "Posted", "Scheduled Date"):
        if field in schema_props:
            v = read_prop(page, field)
            if v:
                return parse_ts(v)
    return None


def match_by_fuzzy_caption(reel, rows, schema_props, threshold=0.85):
    """Fuzzy caption match — fallback for rows that don't have an IG Media ID yet.

    Threshold tightened to 0.85 (was 0.7) to eliminate false positives caught
    after Reel 5 double-matched on the loose setting.
    """
    reel_cap = caption_key(reel.get("caption", ""))
    reel_ts = parse_ts(reel.get("timestamp"))
    best = None
    best_score = 0.0
    for row in rows:
        # Skip rows that already have an IG Media ID — they should have matched
        # in the exact pass. If they didn't, they belong to a different reel.
        if read_prop(row, "IG Media ID"):
            continue
        row_cap = ""
        for field in ("Caption", "Title"):
            v = read_prop(row, field)
            if v:
                row_cap = caption_key(v)
                if row_cap:
                    break
        if reel_cap and row_cap:
            score = difflib.SequenceMatcher(None, reel_cap, row_cap).ratio()
            if score >= threshold and score > best_score:
                best_score = score
                best = row
        if reel_ts and best_score < 0.95:
            rd = row_posted_date(row, schema_props)
            if rd and abs((rd - reel_ts).days) <= 2:
                if best is None:
                    best = row
                    best_score = 0.85
    return best


def is_reel_row(row) -> bool:
    """Filter out non-reel pages (e.g. 'Asset library') from matching."""
    if read_prop(row, "Reel #") is not None:
        return True
    if read_prop(row, "Status") in ("Posted", "Scripted"):
        return True
    title = (read_prop(row, "Title") or "").lower()
    return title.startswith("reel ")


def reconcile(reels, rows, schema_props):
    """Match each IG reel to an existing row. Two-bucket outcome:

    - refreshes: already-Posted rows that need metric updates
    - promotes: Scripted rows that just got posted → flip to Posted

    Unmatched IG reels are LOGGED as warnings and skipped. The sync never
    creates rows for unmatched posts.
    """
    refreshes = []
    promotes = []
    unmatched = []
    reel_candidates = [r for r in rows if is_reel_row(r)]
    for reel in reels:
        # Exact ID match first
        match = match_by_media_id(reel["id"], reel_candidates)
        # Fuzzy fallback for rows that don't have an IG Media ID yet
        if match is None:
            match = match_by_fuzzy_caption(reel, reel_candidates, schema_props)
        if match is None:
            unmatched.append(reel)
            continue
        status = read_prop(match, "Status")
        notes = read_prop(match, "Notes") or ""
        if status == "Scripted" and notes and "REGENERATE" not in notes and read_prop(match, "Clip Order"):
            # Protected: user has authored content on this Scripted row.
            # Still flip to Posted because the reel IS live — just don't touch
            # the creative fields.
            promotes.append((match["id"], reel))
            continue
        if status == "Posted":
            refreshes.append((match["id"], reel))
        else:
            promotes.append((match["id"], reel))
    return refreshes, promotes, unmatched


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


def promote_row_to_posted(page_id, reel, schema_props):
    """Flip a row to Posted and populate metrics/permalink/post date/IG ID."""
    updates = {"Status": "Posted"}
    updates.update(_metric_updates(reel, schema_props))
    updates.update(_permalink_updates(reel, schema_props))
    updates.update(_post_date_updates(reel, schema_props))
    return _patch_row(page_id, {"properties": build_props(schema_props, updates)})


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
    """Extract 7 frames at FRAME_TIMESTAMPS using ffmpeg. Returns list of paths."""
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
        check_budget()
        msg = client.messages.create(
            model=VISION_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": content}],
        )
        usage = getattr(msg, "usage", None)
        if usage is not None:
            cost = track_usage(VISION_MODEL, usage.input_tokens, usage.output_tokens)
            print(
                f"  [cost] {VISION_MODEL} (vision): {usage.input_tokens} in + {usage.output_tokens} out = ${cost:.4f}",
                file=sys.stderr,
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


# ---- Rotation helpers ----
def pick_next_hook(rows):
    recent = []
    for row in rows[:6]:
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
    counts = {s: 0 for s in CONTENT_SPLITS}
    for row in rows:
        s = read_prop(row, "Content Split")
        if s in counts:
            counts[s] += 1
    return min(counts, key=counts.get)


def max_reel_number(rows):
    n = 0
    for row in rows:
        v = read_prop(row, "Reel #")
        if isinstance(v, (int, float)) and v > n:
            n = int(v)
    return n


# ---- Prompts ----
ANALYST_PROMPT = """You are the creative strategist for Swirlie, an indie latte art practice app. Brand voice: hand-drawn chalk-coffee-shop aesthetic, cozy, intentional, warm — quiet morning ritual meets indie coffee shop chalkboard.

You're planning the NEXT Instagram reel (Reel #{next_reel_num}). A separate writer will turn your brief into the actual script, so your job is JUDGMENT, not prose. Reason over real performance data and hand the writer a specific direction.

## Fixed constraints for the next reel (chosen by rotation logic)
- Hook Type: {hook_type}
- Content Split: {content_split}

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

## Reel 4 parallel concept (worth iterating)
Reel 4 paired Julie pouring latte with Julie doing dev work — a "two-process-in-parallel" format. 11.7s avg watch (~3x everything else), but only 13 reach because she forgot hashtags. Concept is compelling as a recurring series format: brewing ↔ building, morning ritual ↔ code commit, steam wand ↔ terminal typing. Consider proposing variations.

## Hashtag rule
Exactly 5 hashtags per caption. No more. Writer will enforce but you should suggest the 5 best ones for this reel's angle in your brief (don't always use the same ones — match to theme).

## Recent POSTED reels (sorted by avg watch time desc — emulate the top performers)
{past_posted}

## Recent SCRIPTED reels (avoid duplicating these ideas)
{past_scripted}

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
- Caption: exactly 5 hashtags, no more, no less. Use the suggested hashtags from the brief unless you have a strong reason otherwise.
- **Caption voice rule**: Julie writes captions with LOWERCASE sentence starts — deliberate aesthetic, not a typo. "the hour before anything is asked of you" not "The hour...". Proper nouns (Swirlie, place names, brand names), acronyms, and the word "I" still get capitalized normally. Preserve this voice in every caption you generate.
- NEVER open with an app/product screen (hard rule from Reel 5→6 A/B test)
- Avoid passive wide shots from behind and black text transition cards (killed retention on Reel 5)

## Your job
Write the full reel script executing the brief under the variation direction above. Match the mood words exactly. If the brief includes wardrobe direction, bake it into the clip descriptions explicitly (what she's wearing, color palette, styling detail).

Return ONLY a JSON object with these exact keys:
{{
  "title": "short evocative title — DO NOT prefix with 'Reel N —'",
  "clip_order": "1. First clip description with specific visual + what Julie is wearing/doing (1-2 sentences each)\\n2. Second clip...\\n(5-8 clips total, weighted toward the first 3 seconds)",
  "on_screen_text": "Overlay 1 | Overlay 2 | Overlay 3 (3-5 short phrases, max ~6 words each)",
  "caption": "warm conversational caption ending with EXACTLY 5 hashtags",
  "cover_scene": "one sentence describing the thumbnail moment — must be human-first or close-up, never an app screen",
  "suno_prompt": "lo-fi acoustic cozy music direction",
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
    Otherwise use compact summaries."""
    title = read_prop(row, "Title") or "(untitled)"
    cap = read_prop(row, "Caption") or ""
    notes = read_prop(row, "Notes") or ""
    reel_num = read_prop(row, "Reel #")
    hook = read_prop(row, "Hook Type") or ""
    split = read_prop(row, "Content Split") or ""
    views = read_prop(row, "Views")
    reach = read_prop(row, "Reach")
    watch = read_prop(row, "Avg Watch Time") or ""
    frame = read_prop(row, "Reel Vision Analysis") or ""
    metric_str = ""
    if views is not None or reach is not None or watch:
        metric_str = f" [views:{views or '?'} reach:{reach or '?'} watch:{watch or '?'}]"
    parts = [f"Reel #{reel_num} [{hook}/{split}]{metric_str} {title}"]
    if cap:
        parts.append(f"  Caption: {cap[:300 if full_notes else 200]}")
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
    check_budget()
    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    usage = getattr(msg, "usage", None)
    if usage is not None:
        cost = track_usage(model, usage.input_tokens, usage.output_tokens)
        print(
            f"  [cost] {model}: {usage.input_tokens} in + {usage.output_tokens} out = ${cost:.4f}",
            file=sys.stderr,
        )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL)
    return json.loads(text)


def analyze_context(client, rows, hook_type, content_split, next_reel_num):
    """Pass 1 — Opus analyst: strategic reasoning over real performance data.

    Top-3 performers by avg watch time get their FULL notes + full frame
    analysis in the prompt. The next ~12 get compact summaries.
    """
    posted = [r for r in rows if read_prop(r, "Status") == "Posted"]
    # Sort by avg watch time desc so analyst sees top performers first
    posted.sort(key=watch_time_seconds, reverse=True)
    posted = posted[:15]
    scripted = [r for r in rows if read_prop(r, "Status") == "Scripted"][:6]

    # Top 3 get full-fidelity context; everyone else compact
    past_posted_lines = []
    for i, r in enumerate(posted):
        past_posted_lines.append(summarize_row_for_prompt(r, full_notes=(i < 3)))

    frame_lines = []
    for r in posted[:8]:
        fa = read_prop(r, "Reel Vision Analysis")
        if fa:
            frame_lines.append(f"Reel #{read_prop(r, 'Reel #')}:\n{fa}")

    prompt = ANALYST_PROMPT.format(
        next_reel_num=next_reel_num,
        hook_type=hook_type,
        content_split=content_split,
        past_posted="\n\n".join(past_posted_lines) or "(none yet)",
        past_scripted="\n".join(summarize_row_for_prompt(r) for r in scripted) or "(none yet)",
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


def generate_script_variations(client, rows, hook_type, content_split, next_reel_num):
    """Two-stage pipeline:
    1. Opus analyst produces one creative brief (shared across all variants).
    2. Sonnet writer is called 3x with different variation directives,
       returning (variations_list, brief).

    The main row is written from variations[0] (text-forward), and alts
    from variations[1] (visual-only) + variations[2] (humor-led) get
    serialized into the 'Alt Scripts' column as a compact text blob."""
    brief = analyze_context(client, rows, hook_type, content_split, next_reel_num)
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
        lines.append(f"Suno: {v.get('suno_prompt','')}")
        lines.append(f"Transitions: {v.get('transitions','')}")
        lines.append(f"Notes: {v.get('notes','')}")
        lines.append("")  # blank line between variations
    return "\n".join(lines).strip()


def create_script_row(variations, hook_type, content_split, reel_num, schema_props):
    """Create a single Scripted row populated from the first variation, with
    the other two serialized into the 'Alt Scripts' column for review."""
    if isinstance(variations, dict):
        # Backwards-compat: a single script dict was passed
        variations = [variations]
    main = variations[0]
    alts_blob = format_alt_scripts(variations) if len(variations) > 1 else ""

    title_prop = next((n for n, p in schema_props.items() if p["type"] == "title"), None)
    updates = {
        title_prop: main["title"],
        "Status": "Scripted",
        "Reel #": reel_num,
        "Clip Order": main["clip_order"],
        "On-Screen Text": main["on_screen_text"],
        "Caption": main["caption"],
        "Hook Type": hook_type,
        "Content Split": content_split,
        "Cover Scene": main["cover_scene"],
        "Suno Prompt": main["suno_prompt"],
        "Transitions": main["transitions"],
        "Reel Total Time": main["reel_total_time"],
        "Notes": main["notes"],
    }
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
    """True if today is Mon/Wed/Sat in America/Chicago local time, or if
    FORCE_SCRIPT_GEN=1 is set in env."""
    if FORCE_SCRIPT_GEN:
        return True
    # America/Chicago is UTC-6 (CST) or UTC-5 (CDT). For simplicity use UTC
    # and accept a ~6h offset at the day boundary. We run at 15:00 UTC which
    # is always mid-morning local, so UTC weekday == local weekday.
    return datetime.now(timezone.utc).weekday() in SCRIPT_GEN_WEEKDAYS


# ---- Main ----
def main():
    summary = {
        "ig_fetched": 0,
        "metrics_refreshed": [],
        "promoted_to_posted": [],
        "unmatched_warnings": [],
        "frames_analyzed": [],
        "new_script_row": None,
        "script_gen_skipped": False,
        "errors": [],
    }
    anthropic_client = Anthropic(api_key=ANTHROPIC_KEY)

    try:
        # Load IG token from Notion Asset library (primary) or env var (fallback),
        # then refresh it proactively to reset the 60-day expiry window.
        load_ig_token()
        refresh_ig_token()

        db = notion_get_db()
        schema_props = db["properties"]
        rows = notion_query_all()
        print(f"Notion: {len(rows)} existing rows", file=sys.stderr)

        reels = fetch_ig_reels()
        summary["ig_fetched"] = len(reels)
        print(f"IG: {len(reels)} reels fetched", file=sys.stderr)

        for r in reels:
            r["_insights"] = fetch_insights(r["id"])

        refreshes, promotes, unmatched = reconcile(reels, rows, schema_props)

        # Refresh metrics on already-Posted rows
        for page_id, reel in refreshes:
            try:
                refresh_row_metrics(page_id, reel, schema_props)
                summary["metrics_refreshed"].append(page_id)
            except Exception as e:
                summary["errors"].append(f"refresh {page_id}: {e}")

        # Promote newly-matched Scripted rows to Posted
        for page_id, reel in promotes:
            try:
                promote_row_to_posted(page_id, reel, schema_props)
                summary["promoted_to_posted"].append(page_id)
            except Exception as e:
                summary["errors"].append(f"promote {page_id}: {e}")

        # Log unmatched IG reels as warnings — never create new rows for them
        for reel in unmatched:
            cap = (reel.get("caption") or "").replace("\n", " ")[:80]
            msg = f"unmatched IG reel {reel.get('id')} — '{cap}' — no Notion row; skipped"
            print(f"  [warn] {msg}", file=sys.stderr)
            summary["unmatched_warnings"].append(msg)

        # Re-query so frame analysis sees fresh state
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

        # Script generation — only on Mon/Wed/Sat (or if FORCE_SCRIPT_GEN=1)
        if should_generate_script_today():
            # Re-query one more time so the analyst sees the frame analyses
            rows = notion_query_all()
            hook_type = pick_next_hook(rows)
            content_split = pick_next_split(rows)
            next_reel_num = max_reel_number(rows) + 1
            print(
                f"Next reel plan: #{next_reel_num} hook={hook_type} split={content_split}",
                file=sys.stderr,
            )
            variations, _brief = generate_script_variations(
                anthropic_client, rows, hook_type, content_split, next_reel_num
            )
            new_page = create_script_row(
                variations, hook_type, content_split, next_reel_num, schema_props
            )
            summary["new_script_row"] = {
                "id": new_page["id"],
                "reel_num": next_reel_num,
                "title": variations[0]["title"],
                "variation_labels": [v.get("variation_label", "?") for v in variations],
            }
        else:
            today = datetime.now(timezone.utc).strftime("%A")
            print(
                f"Not a script-gen day ({today}) — skipping script generation. "
                f"Set FORCE_SCRIPT_GEN=1 to override.",
                file=sys.stderr,
            )
            summary["script_gen_skipped"] = True

    except Exception as e:
        summary["errors"].append(f"fatal: {e}")

    # Summary
    print("\n=== Summary ===")
    print(f"IG reels fetched: {summary['ig_fetched']}")
    print(f"Metrics refreshed on Posted rows: {len(summary['metrics_refreshed'])}")
    for pid in summary["metrics_refreshed"]:
        print(f"  - {pid}")
    print(f"Rows promoted Scripted→Posted: {len(summary['promoted_to_posted'])}")
    for pid in summary["promoted_to_posted"]:
        print(f"  - {pid}")
    print(f"Frames analyzed (first 10s vision): {len(summary['frames_analyzed'])}")
    for pid in summary["frames_analyzed"]:
        print(f"  - {pid}")
    print(f"Unmatched IG reels (skipped, no row created): {len(summary['unmatched_warnings'])}")
    for msg in summary["unmatched_warnings"]:
        print(f"  - {msg}")
    if summary["new_script_row"]:
        nsr = summary["new_script_row"]
        print(f"New script row: Reel #{nsr['reel_num']} — {nsr['title']} ({nsr['id']})")
        if nsr.get("variation_labels"):
            print(f"  Variations generated: {', '.join(nsr['variation_labels'])} (main=first, alts in 'Alt Scripts')")
    elif summary["script_gen_skipped"]:
        print("New script row: (skipped — not a script-gen day)")
    else:
        print("New script row: (not created)")
    # Cost accounting
    print(f"\n=== Cost ===")
    print(f"Run total: ${_run_usage['total_usd']:.4f} (cap ${MAX_COST_PER_RUN_USD:.2f})")
    for model, b in sorted(_run_usage["by_model"].items()):
        print(
            f"  {model}: {b['calls']} call(s) | "
            f"{b['input_tokens']} in + {b['output_tokens']} out tokens | "
            f"${b['cost_usd']:.4f}"
        )

    if summary["errors"]:
        print(f"\nErrors: {len(summary['errors'])}")
        for e in summary["errors"]:
            print(f"  - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
