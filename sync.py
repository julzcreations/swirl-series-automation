#!/usr/bin/env python3
"""Swirl Series content automation.

Runs daily to:
1. Fetch recent IG reels + per-reel insights
2. Reconcile with the Notion content calendar (mark Posted, create missing rows)
3. Generate the next Scripted row via Claude

All credentials come from environment variables. See README.md.
"""
import difflib
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests
from anthropic import Anthropic

# ---- Config from env ----
IG_TOKEN = os.environ["IG_TOKEN"]
IG_USER_ID = os.environ["IG_USER_ID"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
# Two-model setup: Opus reasons over past performance and drafts a creative
# brief; Sonnet turns the brief into the final reel script JSON.
ANALYST_MODEL = os.environ.get("ANALYST_MODEL", "claude-opus-4-6")
WRITER_MODEL = os.environ.get("WRITER_MODEL", "claude-sonnet-4-6")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

REQUIRED_HASHTAGS = ["#latteart", "#swirlie", "#coffeeshop", "#baristalife"]
HOOK_TYPES = ["Curiosity", "Visual Movement", "Unpredictability"]
CONTENT_SPLITS = ["App/Lifestyle Blend", "Lifestyle"]


# ---- Helpers ----
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


# ---- Step 1+2: IG ----
def fetch_ig_reels():
    url = f"https://graph.instagram.com/v21.0/{IG_USER_ID}/media"
    params = {
        "fields": "id,caption,media_type,media_product_type,permalink,timestamp,thumbnail_url",
        "limit": 10,
        "access_token": IG_TOKEN,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    return [m for m in data if m.get("media_product_type") == "REELS"]


def fetch_insights(media_id: str) -> dict:
    url = f"https://graph.instagram.com/v21.0/{media_id}/insights"
    params = {
        "metric": "reach,likes,comments,saved,shares,views",
        "access_token": IG_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=30)
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


# ---- Step 3: Notion read ----
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


# ---- Step 4: Reconcile ----
def row_posted_date(page, schema_props):
    for field in ("Posted Date", "Post Date", "Date", "Posted", "Scheduled Date"):
        if field in schema_props:
            v = read_prop(page, field)
            if v:
                return parse_ts(v)
    return None


def match_reel_to_row(reel, rows, schema_props):
    reel_cap = caption_key(reel.get("caption", ""))
    reel_ts = parse_ts(reel.get("timestamp"))
    best = None
    best_score = 0.0
    for row in rows:
        row_cap = ""
        for field in ("Caption", "Notes", "Title"):
            v = read_prop(row, field)
            if v:
                row_cap = caption_key(v)
                if row_cap:
                    break
        if reel_cap and row_cap:
            score = difflib.SequenceMatcher(None, reel_cap, row_cap).ratio()
            if score >= 0.7 and score > best_score:
                best_score = score
                best = row
        if reel_ts:
            rd = row_posted_date(row, schema_props)
            if rd and abs((rd - reel_ts).days) <= 2:
                if best is None or best_score < 0.9:
                    best = row
                    best_score = max(best_score, 0.75)
    return best


def reconcile(reels, rows, schema_props):
    updates = []
    creates = []
    for reel in reels:
        match = match_reel_to_row(reel, rows, schema_props)
        if match:
            status = read_prop(match, "Status")
            notes = read_prop(match, "Notes") or ""
            if status == "Scripted" and notes and "REGENERATE" not in notes:
                continue  # protected
            if status != "Posted":
                updates.append((match["id"], reel))
        else:
            creates.append(reel)
    return updates, creates


def insights_block(reel):
    ins = reel.get("_insights", {})
    return " | ".join(f"{k}:{v}" for k, v in ins.items()) if ins else ""


def reel_notes_text(reel, prefix=""):
    parts = [prefix] if prefix else []
    parts.append(f"IG caption: {(reel.get('caption') or '').strip()}")
    ib = insights_block(reel)
    if ib:
        parts.append(f"Insights: {ib}")
    return "\n".join(parts)


def patch_row_posted(page_id, reel, schema_props):
    updates = {"Status": "Posted"}
    if "Permalink" in schema_props:
        updates["Permalink"] = reel.get("permalink")
    elif "Link" in schema_props:
        updates["Link"] = reel.get("permalink")
    if "Notes" in schema_props:
        updates["Notes"] = reel_notes_text(reel)
    if "Posted Date" in schema_props and reel.get("timestamp"):
        updates["Posted Date"] = reel["timestamp"][:10]
    body = {"properties": build_props(schema_props, updates)}
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS, json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json()


def create_posted_row(reel, schema_props):
    title_prop = next((n for n, p in schema_props.items() if p["type"] == "title"), None)
    cap = (reel.get("caption") or "Untitled")[:80]
    updates = {title_prop: cap, "Status": "Posted"}
    if "Permalink" in schema_props:
        updates["Permalink"] = reel.get("permalink")
    if "Notes" in schema_props:
        updates["Notes"] = reel_notes_text(reel, prefix="Auto-created from IG sync")
    if "Posted Date" in schema_props and reel.get("timestamp"):
        updates["Posted Date"] = reel["timestamp"][:10]
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


# ---- Step 5/6: Generate next script ----
def pick_next_hook(rows):
    recent = []
    for row in rows[:6]:
        h = read_prop(row, "Hook Type")
        if h:
            recent.append(h)
    for h in HOOK_TYPES:
        if h not in recent:
            return h
    # All three were used in the last 6 — pick the one seen earliest among those recent 6
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


def find_next_scripted_row(rows, schema_props):
    for row in rows:
        status = read_prop(row, "Status")
        if status != "Scripted":
            continue
        title = read_prop(row, "Title")
        clips = read_prop(row, "Clip Order")
        notes = read_prop(row, "Notes") or ""
        if (not title and not clips) or "REGENERATE" in notes:
            return row
    return None


def max_reel_number(rows):
    n = 0
    for row in rows:
        v = read_prop(row, "Reel #")
        if isinstance(v, (int, float)) and v > n:
            n = int(v)
    return n


ANALYST_PROMPT = """You are the creative strategist for Swirlie, an indie latte art practice app. Brand voice: hand-drawn chalk-coffee-shop aesthetic, cozy, intentional, warm — quiet morning ritual meets indie coffee shop chalkboard.

You're planning the NEXT Instagram reel. A separate writer will turn your brief into the actual script, so your job is judgment, not prose. Analyze past performance, decide the angle, and hand the writer a clear direction.

Fixed constraints for the next reel (already chosen by rotation logic — don't override):
- Hook Type: {hook_type}
- Content Split: {content_split}

Recent POSTED reels (with performance notes where available):
{past_posted}

Recent SCRIPTED but not-yet-posted reels (avoid duplication):
{past_scripted}

Return a short creative brief as a JSON object with these keys:
{{
  "theme": "2-4 word theme label for this reel",
  "angle": "1-2 sentences on the specific angle/idea and why it fits the fixed hook type + content split",
  "informed_by": "cite 1-3 past reels by number and what specifically from each is shaping this decision (e.g. 'Reel #12 overperformed on saves for its close-up steam wand shots — lean into ASMR-style detail framing'). If no posted reels exist yet, say 'No performance data yet — establishing baseline.'",
  "avoid": "1 sentence on what NOT to do — duplicate themes, overused visuals, or tone misses",
  "mood": "3-5 comma-separated mood/aesthetic words for the writer"
}}

Return ONLY the JSON object. No prose, no code fencing."""


WRITER_PROMPT = """You are writing the next Instagram reel script for Swirlie, a latte art practice app.

Brand voice: hand-drawn chalk-coffee-shop aesthetic, cozy, intentional, warm. Indie coffee shop chalkboard meets quiet morning ritual.

The creative strategist has already analyzed past performance and handed you this brief:

Theme: {theme}
Angle: {angle}
Informed by: {informed_by}
Avoid: {avoid}
Mood: {mood}

Fixed constraints:
- Hook Type: {hook_type}
- Content Split: {content_split}
- Reel Total Time: 15-30 seconds
- Caption must include these hashtags: #latteart #swirlie #coffeeshop #baristalife (plus 4-8 more relevant ones, 8-12 total)

Your job: write the full reel script that executes the brief. Match the mood words exactly.

Return ONLY a JSON object with these exact keys:
{{
  "title": "short evocative title",
  "clip_order": "1. First clip description (1-2 sentences)\\n2. Second clip...\\n(5-8 clips total)",
  "on_screen_text": "Overlay 1 | Overlay 2 | Overlay 3 (3-5 short phrases, max ~6 words each)",
  "caption": "warm conversational caption ending with 8-12 hashtags",
  "cover_scene": "one sentence describing the thumbnail moment",
  "suno_prompt": "lo-fi acoustic cozy music direction",
  "transitions": "brief notes on cuts / match cuts / whip pans",
  "reel_total_time": "20s",
  "notes": "2-3 sentence rationale. MUST begin by citing the theme and the past reels named in 'informed_by'."
}}

Return only the JSON, no prose, no code fencing."""


def summarize_row_for_prompt(row):
    title = read_prop(row, "Title") or "(untitled)"
    cap = read_prop(row, "Caption") or ""
    notes = read_prop(row, "Notes") or ""
    reel_num = read_prop(row, "Reel #")
    hook = read_prop(row, "Hook Type") or ""
    split = read_prop(row, "Content Split") or ""
    return f"Reel #{reel_num} [{hook}/{split}] {title} — {cap[:100]} | {notes[:200]}"


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


def analyze_context(client, rows, hook_type, content_split):
    """Pass 1 — Opus: strategic analysis over past performance → creative brief."""
    posted = [r for r in rows if read_prop(r, "Status") == "Posted"][:15]
    scripted = [r for r in rows if read_prop(r, "Status") == "Scripted"][:6]
    prompt = ANALYST_PROMPT.format(
        hook_type=hook_type,
        content_split=content_split,
        past_posted="\n".join(summarize_row_for_prompt(r) for r in posted) or "(none yet)",
        past_scripted="\n".join(summarize_row_for_prompt(r) for r in scripted) or "(none yet)",
    )
    brief = _call_claude(client, ANALYST_MODEL, prompt, max_tokens=600)
    print(
        f"Analyst ({ANALYST_MODEL}): theme='{brief.get('theme')}' "
        f"informed_by='{brief.get('informed_by','')[:80]}'",
        file=sys.stderr,
    )
    return brief


def write_script(client, brief, hook_type, content_split):
    """Pass 2 — Sonnet: turn the brief into the final reel script JSON."""
    prompt = WRITER_PROMPT.format(
        theme=brief.get("theme", ""),
        angle=brief.get("angle", ""),
        informed_by=brief.get("informed_by", ""),
        avoid=brief.get("avoid", ""),
        mood=brief.get("mood", ""),
        hook_type=hook_type,
        content_split=content_split,
    )
    data = _call_claude(client, WRITER_MODEL, prompt, max_tokens=2000)
    caption = data.get("caption", "")
    for h in REQUIRED_HASHTAGS:
        if h.lower() not in caption.lower():
            caption += f" {h}"
    data["caption"] = caption
    return data


def generate_script(rows, hook_type, content_split):
    """Two-pass script generation: Opus analyzes, Sonnet writes."""
    client = Anthropic(api_key=ANTHROPIC_KEY)
    brief = analyze_context(client, rows, hook_type, content_split)
    return write_script(client, brief, hook_type, content_split)


def apply_script_to_row(page_id, script, hook_type, content_split, schema_props):
    updates = {
        "Title": script["title"],
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
    body = {"properties": build_props(schema_props, updates)}
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS, json=body, timeout=30,
    )
    r.raise_for_status()
    return r.json()


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


# ---- Main ----
def main():
    summary = {
        "ig_fetched": 0,
        "notion_updated": [],
        "notion_created": [],
        "new_script_row": None,
        "errors": [],
    }
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

        updates, creates = reconcile(reels, rows, schema_props)
        for page_id, reel in updates:
            try:
                patch_row_posted(page_id, reel, schema_props)
                summary["notion_updated"].append(page_id)
            except Exception as e:
                summary["errors"].append(f"patch {page_id}: {e}")
        for reel in creates:
            try:
                new_page = create_posted_row(reel, schema_props)
                summary["notion_created"].append(new_page["id"])
            except Exception as e:
                summary["errors"].append(f"create posted: {e}")

        # Re-query so the next-script logic sees the new rows
        rows = notion_query_all()

        hook_type = pick_next_hook(rows)
        content_split = pick_next_split(rows)
        print(f"Next reel plan: hook={hook_type}, split={content_split}", file=sys.stderr)

        script = generate_script(rows, hook_type, content_split)

        existing = find_next_scripted_row(rows, schema_props)
        if existing:
            apply_script_to_row(existing["id"], script, hook_type, content_split, schema_props)
            summary["new_script_row"] = {"id": existing["id"], "title": script["title"]}
        else:
            reel_num = max_reel_number(rows) + 1
            new_page = create_script_row(script, hook_type, content_split, reel_num, schema_props)
            summary["new_script_row"] = {"id": new_page["id"], "title": script["title"]}

    except Exception as e:
        summary["errors"].append(f"fatal: {e}")

    print("\n=== Summary ===")
    print(f"IG reels fetched: {summary['ig_fetched']}")
    print(f"Notion rows updated to Posted: {len(summary['notion_updated'])}")
    for pid in summary["notion_updated"]:
        print(f"  - {pid}")
    print(f"Notion rows auto-created: {len(summary['notion_created'])}")
    for pid in summary["notion_created"]:
        print(f"  - {pid}")
    if summary["new_script_row"]:
        print(f"New script row: {summary['new_script_row']['id']} — {summary['new_script_row']['title']}")
    else:
        print("New script row: (not created)")
    if summary["errors"]:
        print(f"Errors: {len(summary['errors'])}")
        for e in summary["errors"]:
            print(f"  - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
