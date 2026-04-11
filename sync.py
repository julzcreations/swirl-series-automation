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
IG_TOKEN = os.environ["IG_TOKEN"]
IG_USER_ID = os.environ["IG_USER_ID"]
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


# Timestamps (seconds) to extract frames at — weighted toward first 3s
# where retention dies.
FRAME_TIMESTAMPS = [0.0, 0.5, 1.0, 1.5, 2.5, 4.0, 7.0]


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


VISION_PROMPT = """You are analyzing the first ~10 seconds of an Instagram reel for a latte art practice app called Swirlie. These frames are in chronological order: 0s, 0.5s, 1s, 1.5s, 2.5s, 4s, 7s.

The creator (Julie) is trying to maximize avg watch time. Specifically track:
- Is there a human in frame? Where (hand, face, full body, shadow)? When do they first appear?
- Is there an app/product screen in frame? If so, when — is it the first frame, or later?
- Is the opener a passive wide shot, a close-up, or a mid-action moment?
- Wardrobe/styling: if a person is visible, describe outfit, styling, color palette. Is it visually distinctive?
- On-screen text: any overlays in these frames? What do they say?
- Transition density: are consecutive frames clearly different scenes (fast cuts) or slight variations (slow / single scene)?
- Overall aesthetic tags: 3-5 descriptors (cozy, chaotic, minimal, warm, chalky, clean, etc.)

Return ONLY a JSON object with these keys:
{
  "first_frame_type": "one of: human-in-frame / app-or-product / wide-scene / close-up-object / text-card",
  "first_frame_description": "one sentence",
  "human_present_in_first_3s": true or false,
  "wardrobe_notes": "brief description or 'N/A' if no human visible",
  "app_appearance_timing": "one of: first_frame / within_3s / middle / end / not_visible",
  "on_screen_text": "quoted text or 'none'",
  "transition_density": "one of: fast / moderate / slow / single_scene",
  "aesthetic_tags": ["tag1", "tag2", "tag3"],
  "retention_concerns": "1 sentence: what in these first frames might cause viewers to scroll past",
  "retention_strengths": "1 sentence: what in these first frames might keep viewers watching"
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
    """If the row doesn't yet have First 10s Analysis, download MP4, extract
    frames, run vision, and write the result. Returns True if analysis was
    written."""
    if "First 10s Analysis" not in schema_props:
        return False
    existing = read_prop(row, "First 10s Analysis")
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
        analysis = analyze_first_10s(client, frames, reel.get("caption", ""))
    if not analysis:
        return False
    # Format as a compact text blob the analyst can read
    summary = (
        f"First frame: {analysis.get('first_frame_type','?')} — {analysis.get('first_frame_description','')}\n"
        f"Human in first 3s: {analysis.get('human_present_in_first_3s','?')}\n"
        f"Wardrobe: {analysis.get('wardrobe_notes','')}\n"
        f"App appears: {analysis.get('app_appearance_timing','?')}\n"
        f"On-screen text: {analysis.get('on_screen_text','none')}\n"
        f"Transition density: {analysis.get('transition_density','?')}\n"
        f"Aesthetic: {', '.join(analysis.get('aesthetic_tags', []))}\n"
        f"Retention concerns: {analysis.get('retention_concerns','')}\n"
        f"Retention strengths: {analysis.get('retention_strengths','')}"
    )
    _patch_row(
        row["id"],
        {"properties": build_props(schema_props, {"First 10s Analysis": summary})},
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

## Fixed constraints
- Hook Type: {hook_type}
- Content Split: {content_split}
- Reel Total Time: 12-18 seconds (shorter end preferred)
- Caption: exactly 5 hashtags, no more, no less. Use the suggested hashtags from the brief unless you have a strong reason otherwise.
- NEVER open with an app/product screen (hard rule from Reel 5→6 A/B test)
- Avoid passive wide shots from behind and black text transition cards (killed retention on Reel 5)

## Your job
Write the full reel script executing the brief. Match the mood words exactly. If the brief includes wardrobe direction, bake it into the clip descriptions explicitly (what she's wearing, color palette, styling detail).

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
  "notes": "2-3 sentence rationale. MUST begin by citing the theme and the past reels named in 'informed_by', then state the specific hypothesis this reel is testing."
}}

Return only the JSON, no prose, no code fencing."""


def summarize_row_for_prompt(row):
    title = read_prop(row, "Title") or "(untitled)"
    cap = read_prop(row, "Caption") or ""
    notes = read_prop(row, "Notes") or ""
    reel_num = read_prop(row, "Reel #")
    hook = read_prop(row, "Hook Type") or ""
    split = read_prop(row, "Content Split") or ""
    views = read_prop(row, "Views")
    reach = read_prop(row, "Reach")
    watch = read_prop(row, "Avg Watch Time") or ""
    frame = read_prop(row, "First 10s Analysis") or ""
    metric_str = ""
    if views is not None or reach is not None or watch:
        metric_str = f" [views:{views or '?'} reach:{reach or '?'} watch:{watch or '?'}]"
    parts = [f"Reel #{reel_num} [{hook}/{split}]{metric_str} {title}"]
    if cap:
        parts.append(f"  Caption: {cap[:200]}")
    if notes:
        parts.append(f"  Notes: {notes[:300]}")
    if frame:
        # Only include first line of frame analysis for the summary
        parts.append(f"  First 10s: {frame.splitlines()[0] if frame else ''}")
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


def analyze_context(client, rows, hook_type, content_split, next_reel_num):
    """Pass 1 — Opus analyst: strategic reasoning over real performance data."""
    posted = [r for r in rows if read_prop(r, "Status") == "Posted"]
    # Sort by avg watch time desc so analyst sees top performers first
    posted.sort(key=watch_time_seconds, reverse=True)
    posted = posted[:15]
    scripted = [r for r in rows if read_prop(r, "Status") == "Scripted"][:6]

    frame_lines = []
    for r in posted[:8]:
        fa = read_prop(r, "First 10s Analysis")
        if fa:
            frame_lines.append(f"Reel #{read_prop(r, 'Reel #')}:\n{fa}")

    prompt = ANALYST_PROMPT.format(
        next_reel_num=next_reel_num,
        hook_type=hook_type,
        content_split=content_split,
        past_posted="\n".join(summarize_row_for_prompt(r) for r in posted) or "(none yet)",
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


def write_script(client, brief, hook_type, content_split):
    """Pass 2 — Sonnet writer: turn the brief into the final reel script JSON."""
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
        hook_type=hook_type,
        content_split=content_split,
    )
    data = _call_claude(client, WRITER_MODEL, prompt, max_tokens=2000)
    # Enforce the 5-hashtag cap as a backstop
    caption = data.get("caption", "")
    tags = re.findall(r"#\w+", caption)
    if len(tags) > MAX_HASHTAGS:
        keep = set(tags[:MAX_HASHTAGS])
        # Remove any hashtags beyond the first 5
        def filter_tag(m):
            return m.group(0) if m.group(0) in keep else ""
        caption = re.sub(r"#\w+", filter_tag, caption)
        caption = re.sub(r"\s{2,}", " ", caption).strip()
        data["caption"] = caption
        # Re-add the kept tags at the end if they got stripped
        missing = [t for t in tags[:MAX_HASHTAGS] if t not in caption]
        if missing:
            data["caption"] = caption + "\n\n" + " ".join(missing)
    return data


def generate_script(client, rows, hook_type, content_split, next_reel_num):
    """Two-pass script generation: Opus analyzes, Sonnet writes."""
    brief = analyze_context(client, rows, hook_type, content_split, next_reel_num)
    return write_script(client, brief, hook_type, content_split)


def create_script_row(script, hook_type, content_split, reel_num, schema_props):
    title_prop = next((n for n, p in schema_props.items() if p["type"] == "title"), None)
    updates = {
        title_prop: script["title"],
        "Status": "Scripted",
        "Reel #": reel_num,
        "Clip Order": script["clip_order"],
        "On-Screen Text": script["on_screen_text"],
        "Caption": script["caption"],
        "Hook Type": hook_type,
        "Content Split": content_split,
        "Cover Scene": script["cover_scene"],
        "Suno Prompt": script["suno_prompt"],
        "Transitions": script["transitions"],
        "Reel Total Time": script["reel_total_time"],
        "Notes": script["notes"],
    }
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
            script = generate_script(
                anthropic_client, rows, hook_type, content_split, next_reel_num
            )
            new_page = create_script_row(
                script, hook_type, content_split, next_reel_num, schema_props
            )
            summary["new_script_row"] = {
                "id": new_page["id"],
                "reel_num": next_reel_num,
                "title": script["title"],
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
    elif summary["script_gen_skipped"]:
        print("New script row: (skipped — not a script-gen day)")
    else:
        print("New script row: (not created)")
    if summary["errors"]:
        print(f"Errors: {len(summary['errors'])}")
        for e in summary["errors"]:
            print(f"  - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
