#!/usr/bin/env python3
"""
┌─────────────────────────────────────────────────┐
│  VAULTCORD - Discord Server Archive & Exporter  │
└─────────────────────────────────────────────────┘

Vaultcord scrapes all accessible data from a Discord server and
exports it into a single, beautifully formatted HTML archive.

Usage:
    python vaultcord.py

You will be prompted for:
    1. Your Discord token (bot token or user token)
    2. The server (guild) ID to archive

What it collects:
    - Server info (name, description, member count, icon)
    - All accessible channels (text, voice, categories, forums, etc.)
    - Messages from each text channel (configurable limit)
    - Auto-translation of foreign-language messages to English
    - Members list
    - Roles
    - Emojis
    - Attachments & embeds metadata

Output:
    A single self-contained HTML file in the current directory.

IMPORTANT: Using a user token (self-bot) may violate Discord's ToS.
           A bot token with proper permissions is the recommended approach.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from html import escape
from collections import defaultdict
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ─── Configuration ──────────────────────────────────────────────────────────

# Embedded fallback logo (1x1 transparent PNG)
_LOGO_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+a7d0AAAAASUVORK5CYII="
)


MESSAGES_PER_CHANNEL = 500      # Max messages to fetch per channel (set to None for all)
REQUEST_TIMEOUT = 15            # Seconds before a request times out
OUTPUT_DIR = "."                # Where to save the HTML file
MAX_WORKERS_CHANNELS = 4        # Parallel channel fetchers
MAX_WORKERS_TRANSLATE = 8       # Parallel translation requests

# ─── Discord API helpers ────────────────────────────────────────────────────

BASE_URL = "https://discord.com/api/v10"

# Thread-safe rate limit lock
_rate_lock = threading.Lock()

def api_request(endpoint, token, params=None):
    """Make a GET request to the Discord API with rate-limit handling."""
    url = f"{BASE_URL}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    headers = {
        "Authorization": token,
        "User-Agent": "Vaultcord/1.0",
        "Content-Type": "application/json",
    }

    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                # Check remaining rate limit from headers
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if remaining and int(remaining) <= 1:
                    reset_after = float(resp.headers.get("X-RateLimit-Reset-After", 0.5))
                    with _rate_lock:
                        time.sleep(reset_after)
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = float(json.loads(e.read().decode()).get("retry_after", 5))
                print(f"  ⏳ Rate limited, waiting {retry_after:.1f}s...")
                with _rate_lock:
                    time.sleep(retry_after + 0.2)
                continue
            elif e.code in (403, 404):
                return None
            else:
                print(f"  ⚠ HTTP {e.code} for {endpoint}")
                return None
        except Exception as e:
            print(f"  ⚠ Request error: {e}")
            if attempt < 4:
                time.sleep(1)
            else:
                return None
    return None


def fetch_all_messages(channel_id, token, limit=None):
    """Fetch messages from a channel using pagination."""
    messages = []
    before = None
    batch_size = 100

    while True:
        params = {"limit": batch_size}
        if before:
            params["before"] = before

        data = api_request(f"/channels/{channel_id}/messages", token, params)
        if not data or len(data) == 0:
            break

        messages.extend(data)
        before = data[-1]["id"]

        if limit and len(messages) >= limit:
            messages = messages[:limit]
            break

        if len(data) < batch_size:
            break

        print(f"    📨 {len(messages)} messages so far...", end="\r")

    return messages


# ─── Translation helpers ────────────────────────────────────────────────────

# Common English words for fast pre-filtering (skip API call if text is likely English)
_EN_COMMON = frozenset([
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
    "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
    "this", "but", "his", "by", "from", "they", "we", "say", "her",
    "she", "or", "an", "will", "my", "one", "all", "would", "there",
    "their", "what", "so", "up", "out", "if", "about", "who", "get",
    "which", "go", "me", "when", "make", "can", "like", "time", "no",
    "just", "him", "know", "take", "people", "into", "year", "your",
    "good", "some", "could", "them", "see", "other", "than", "then",
    "now", "look", "only", "come", "its", "over", "think", "also",
    "back", "after", "use", "how", "our", "well", "way", "want",
    "because", "any", "these", "give", "day", "most", "us", "is",
    "was", "are", "been", "has", "had", "did", "got", "am", "were",
    "yes", "yeah", "yep", "nah", "nope", "okay", "ok", "lol", "lmao",
    "bruh", "bro", "dude", "man", "guys", "hey", "hi", "hello", "bye",
    "thanks", "thank", "please", "sorry", "sure", "right", "oh", "wow",
    "nice", "cool", "great", "awesome", "damn", "shit", "fuck", "wtf",
    "why", "how", "where", "when", "what", "don't", "doesn't", "didn't",
    "isn't", "aren't", "wasn't", "weren't", "won't", "can't", "couldn't",
    "shouldn't", "wouldn't", "haven't", "hasn't", "hadn't", "don",
])


def _is_likely_english(text):
    """Fast heuristic: check if text is likely English based on common words."""
    words = re.findall(r"[a-zA-Z']+", text.lower())
    if len(words) == 0:
        return False
    if len(words) <= 2:
        # Very short - check if all words are ASCII letters (common in English)
        return all(w.isascii() for w in words) and any(w in _EN_COMMON for w in words)
    en_count = sum(1 for w in words if w in _EN_COMMON)
    return (en_count / len(words)) >= 0.3


def _clean_for_detection(text):
    """Strip Discord markup before language detection."""
    clean = re.sub(r'<[^>]+>', '', text)
    clean = re.sub(r'https?://\S+', '', clean)
    clean = re.sub(r'```[\s\S]*?```', '', clean)
    clean = re.sub(r'`[^`]+`', '', clean)
    return clean.strip()


def detect_and_translate(text):
    """
    Detect the language of text and translate to English if foreign.
    Returns (detected_lang, translated_text) or (None, None) if English or empty.
    """
    if not text or len(text.strip()) < 3:
        return None, None

    clean = _clean_for_detection(text)
    if len(clean) < 3:
        return None, None

    alpha_chars = [c for c in clean if c.isalpha()]
    if len(alpha_chars) < 2:
        return None, None

    # Fast path: skip API call if text looks English
    if _is_likely_english(clean):
        return None, None

    try:
        params = urllib.parse.urlencode({
            "client": "gtx",
            "sl": "auto",
            "tl": "en",
            "dt": "t",
            "q": clean[:1500],
        })
        url = f"https://translate.googleapis.com/translate_a/single?{params}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
        })
        with urllib.request.urlopen(req, timeout=8) as resp:
            result = json.loads(resp.read().decode())

        detected_lang = result[2] if len(result) > 2 else None

        if detected_lang and detected_lang.startswith("en"):
            return None, None

        translated_parts = []
        if result and result[0]:
            for segment in result[0]:
                if segment and segment[0]:
                    translated_parts.append(segment[0])

        translated = "".join(translated_parts).strip()

        if translated.lower() == clean.lower():
            return None, None

        return detected_lang, translated

    except Exception:
        return None, None


TRANSLATION_CACHE = {}
_cache_lock = threading.Lock()

def get_translation(text):
    """Thread-safe cached wrapper around detect_and_translate."""
    with _cache_lock:
        if text in TRANSLATION_CACHE:
            return TRANSLATION_CACHE[text]
    result = detect_and_translate(text)
    with _cache_lock:
        TRANSLATION_CACHE[text] = result
    return result


# Language code to human-readable name (common ones)
LANG_NAMES = {
    "af": "Afrikaans", "sq": "Albanian", "ar": "Arabic", "hy": "Armenian",
    "az": "Azerbaijani", "eu": "Basque", "be": "Belarusian", "bn": "Bengali",
    "bs": "Bosnian", "bg": "Bulgarian", "ca": "Catalan", "zh": "Chinese",
    "zh-CN": "Chinese", "zh-TW": "Chinese (Traditional)", "hr": "Croatian",
    "cs": "Czech", "da": "Danish", "nl": "Dutch", "et": "Estonian",
    "fi": "Finnish", "fr": "French", "gl": "Galician", "ka": "Georgian",
    "de": "German", "el": "Greek", "gu": "Gujarati", "ht": "Haitian Creole",
    "he": "Hebrew", "hi": "Hindi", "hu": "Hungarian", "is": "Icelandic",
    "id": "Indonesian", "ga": "Irish", "it": "Italian", "ja": "Japanese",
    "kn": "Kannada", "kk": "Kazakh", "ko": "Korean", "ky": "Kyrgyz",
    "lo": "Lao", "la": "Latin", "lv": "Latvian", "lt": "Lithuanian",
    "mk": "Macedonian", "ms": "Malay", "ml": "Malayalam", "mt": "Maltese",
    "mn": "Mongolian", "ne": "Nepali", "no": "Norwegian", "fa": "Persian",
    "pl": "Polish", "pt": "Portuguese", "pa": "Punjabi", "ro": "Romanian",
    "ru": "Russian", "sr": "Serbian", "sk": "Slovak", "sl": "Slovenian",
    "es": "Spanish", "sw": "Swahili", "sv": "Swedish", "ta": "Tamil",
    "te": "Telugu", "th": "Thai", "tr": "Turkish", "uk": "Ukrainian",
    "ur": "Urdu", "uz": "Uzbek", "vi": "Vietnamese", "cy": "Welsh",
    "yi": "Yiddish",
}

def lang_name(code):
    """Get readable language name from code."""
    if not code:
        return "Unknown"
    return LANG_NAMES.get(code, LANG_NAMES.get(code.split("-")[0], code.upper()))


def translate_messages(all_messages):
    """
    Pre-translate all foreign messages using parallel threads.
    Adds '_translation' and '_detected_lang' keys to message dicts.
    """
    # Collect all messages that need checking
    candidates = []
    for ch_id, msgs in all_messages.items():
        for msg in msgs:
            content = msg.get("content", "")
            if content and len(content.strip()) >= 3:
                clean = _clean_for_detection(content)
                if len(clean) >= 3 and not _is_likely_english(clean):
                    candidates.append(msg)

    total_all = sum(len(msgs) for msgs in all_messages.values())
    skipped = total_all - len(candidates)
    print(f"\n🌐 Translating: {len(candidates)} candidates out of {total_all} messages ({skipped} skipped as English)")

    if not candidates:
        print("   ✅ No foreign messages detected")
        return all_messages

    translated_count = 0
    processed = 0
    _progress_lock = threading.Lock()

    def _translate_one(msg):
        nonlocal translated_count, processed
        content = msg.get("content", "")
        lang, translation = get_translation(content)
        with _progress_lock:
            processed += 1
            if lang and translation:
                msg["_detected_lang"] = lang
                msg["_translation"] = translation
                translated_count += 1
            if processed % 25 == 0 or processed == len(candidates):
                print(f"   🔄 {processed}/{len(candidates)} checked, {translated_count} translated...", end="\r")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRANSLATE) as pool:
        futures = [pool.submit(_translate_one, msg) for msg in candidates]
        for f in as_completed(futures):
            f.result()  # Raise any exceptions

    print(f"   ✅ {translated_count} messages translated out of {len(candidates)} checked      ")
    return all_messages


# ─── Data collection ────────────────────────────────────────────────────────

def collect_server_data(token, guild_id, translate=False):
    """Collect all accessible data from a Discord server."""
    data = {}

    # 1. Guild info
    print("📋 Fetching server info...")
    guild = api_request(f"/guilds/{guild_id}", token, {"with_counts": "true"})
    if not guild:
        print("❌ Could not access this server. Check your token and guild ID.")
        sys.exit(1)
    data["guild"] = guild
    print(f"   ✅ Server: {guild['name']}")

    # 2. Channels
    print("📂 Fetching channels...")
    channels = api_request(f"/guilds/{guild_id}/channels", token) or []
    data["channels"] = channels
    print(f"   ✅ {len(channels)} channels found")

    # 3. Roles
    print("🎭 Fetching roles...")
    roles = api_request(f"/guilds/{guild_id}/roles", token) or []
    data["roles"] = sorted(roles, key=lambda r: r.get("position", 0), reverse=True)
    print(f"   ✅ {len(roles)} roles found")

    # 4. Emojis
    print("😀 Fetching emojis...")
    emojis = api_request(f"/guilds/{guild_id}/emojis", token) or []
    data["emojis"] = emojis
    print(f"   ✅ {len(emojis)} emojis found")

    # 5. Members (paginated, may be limited without GUILD_MEMBERS intent)
    print("👥 Fetching members...")
    members = []
    after = "0"
    for _ in range(20):  # Safety cap at ~2000 members
        batch = api_request(f"/guilds/{guild_id}/members", token, {"limit": 100, "after": after})
        if not batch or len(batch) == 0:
            break
        members.extend(batch)
        after = batch[-1]["user"]["id"]
        if len(batch) < 100:
            break
    data["members"] = members
    print(f"   ✅ {len(members)} members fetched")

    # 6. Messages from text channels (parallel)
    print("💬 Fetching messages from text channels...")
    text_channel_types = {0, 5, 15, 16}  # text, announcement, forum, media
    text_channels = [c for c in channels if c.get("type") in text_channel_types]
    data["messages"] = {}

    _fetch_counter = {"done": 0}
    _fetch_lock = threading.Lock()
    total_ch = len(text_channels)

    def _fetch_channel(ch):
        ch_id = ch["id"]
        ch_name = ch.get("name", "unknown")
        msgs = fetch_all_messages(ch_id, token, MESSAGES_PER_CHANNEL)
        with _fetch_lock:
            _fetch_counter["done"] += 1
            n = _fetch_counter["done"]
            if msgs:
                print(f"   [{n}/{total_ch}] #{ch_name} - {len(msgs)} messages ✅")
            else:
                print(f"   [{n}/{total_ch}] #{ch_name} - ⛔ no access or empty")
        return ch_id, msgs

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
        futures = {pool.submit(_fetch_channel, ch): ch for ch in text_channels}
        for future in as_completed(futures):
            ch_id, msgs = future.result()
            if msgs:
                data["messages"][ch_id] = msgs

    # 7. Translate foreign messages (if enabled)
    if translate:
        translate_messages(data["messages"])
    else:
        print("\n  ⏩ Translation skipped")

    return data


# ─── HTML generation ────────────────────────────────────────────────────────

def role_color_css(color_int):
    """Convert Discord role color int to CSS hex."""
    if not color_int:
        return "#99aab5"
    return f"#{color_int:06x}"


def format_timestamp(iso_str):
    """Format an ISO timestamp into a readable string."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso_str[:16]


def channel_type_label(t):
    labels = {
        0: "Text", 2: "Voice", 4: "Category", 5: "Announcement",
        10: "Thread", 11: "Thread", 12: "Thread", 13: "Stage",
        14: "Directory", 15: "Forum", 16: "Media"
    }
    return labels.get(t, f"Type {t}")


def channel_type_icon(t):
    icons = {
        0: "#", 2: "🔊", 4: "📁", 5: "📢",
        10: "🧵", 11: "🧵", 12: "🧵", 13: "🎙",
        15: "💬", 16: "🖼"
    }
    return icons.get(t, "•")


def build_html(data):
    """Build a self-contained HTML document from the collected data."""
    guild = data["guild"]
    channels = data["channels"]
    roles = data["roles"]
    emojis = data["emojis"]
    members = data["members"]
    messages = data["messages"]

    guild_name = escape(guild.get("name", "Unknown Server"))
    guild_desc = escape(guild.get("description") or "")
    member_count = guild.get("approximate_member_count", len(members))
    online_count = guild.get("approximate_presence_count", "?")
    guild_id = guild.get("id", "")
    icon_hash = guild.get("icon")
    icon_url = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.png?size=128" if icon_hash else ""

    # Organize channels by category
    categories = {}
    uncategorized = []
    cat_names = {}
    for ch in channels:
        if ch["type"] == 4:
            cat_names[ch["id"]] = ch["name"]
            categories[ch["id"]] = []

    for ch in channels:
        if ch["type"] == 4:
            continue
        parent = ch.get("parent_id")
        if parent and parent in categories:
            categories[parent].append(ch)
        else:
            uncategorized.append(ch)

    for cat_id in categories:
        categories[cat_id].sort(key=lambda c: c.get("position", 0))
    uncategorized.sort(key=lambda c: c.get("position", 0))

    ch_lookup = {ch["id"]: ch["name"] for ch in channels}

    user_lookup = {}
    for m in members:
        u = m.get("user", {})
        uid = u.get("id", "")
        display = m.get("nick") or u.get("global_name") or u.get("username", "Unknown")
        user_lookup[uid] = display

    total_messages = sum(len(v) for v in messages.values())
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Embedded logo
    LOGO_DATA_URI = "data:image/png;base64," + _LOGO_B64

    # ─── Start building HTML ─────────────────────────────────────────────

    html_parts = []

    html_parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{guild_name} - Vaultcord Archive</title>
<style>
:root {{
    --bg: #2b2b2b;
    --bg-side: #242424;
    --bg-card: #333;
    --bg-hover: #3a3a3a;
    --text: #eee;
    --text-dim: #aaa;
    --text-muted: #777;
    --accent: #7b6cf6;
    --green: #5cb85c;
    --blue: #5bc0de;
    --border: #444;
    --radius: 6px;
}}

* {{ margin:0; padding:0; box-sizing:border-box; }}

body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
}}

::-webkit-scrollbar {{ width:6px; }}
::-webkit-scrollbar-track {{ background:var(--bg); }}
::-webkit-scrollbar-thumb {{ background:var(--border); border-radius:3px; }}

.layout {{ display:grid; grid-template-columns:260px 1fr; min-height:100vh; }}

/* Sidebar */
.sidebar {{
    background:var(--bg-side);
    border-right:1px solid var(--border);
    position:sticky; top:0; height:100vh;
    overflow-y:auto; display:flex; flex-direction:column;
}}

.brand-bar {{
    padding:14px 16px;
    border-bottom:1px solid var(--border);
    display:flex; align-items:center; gap:10px;
}}
.brand-bar img {{ width:28px; height:28px; border-radius:6px; }}
.brand-name {{ font-size:0.9rem; font-weight:700; color:var(--accent); letter-spacing:0.04em; text-transform:uppercase; }}
.brand-tag {{ font-size:0.6rem; color:var(--text-muted); margin-left:auto; }}

.server-header {{
    padding:18px 16px;
    border-bottom:1px solid var(--border);
}}
.server-header img {{ width:48px; height:48px; border-radius:12px; margin-bottom:10px; }}
.server-header h1 {{ font-size:1.05rem; font-weight:700; }}
.server-header .meta {{ font-size:0.75rem; color:var(--text-dim); }}
.server-header .meta span {{ margin-right:10px; }}
.server-header .meta .dot {{ display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:3px; vertical-align:middle; }}
.server-header .meta .dot.green {{ background:var(--green); }}
.server-header .meta .dot.gray {{ background:var(--text-muted); }}

.sidebar-nav {{ padding:10px 6px; flex:1; overflow-y:auto; }}
.nav-section-title {{ font-size:0.65rem; font-weight:700; text-transform:uppercase; letter-spacing:0.06em; color:var(--text-muted); padding:8px 10px 4px; }}
.nav-link {{ display:flex; align-items:center; gap:7px; padding:5px 10px; border-radius:4px; color:var(--text-dim); text-decoration:none; font-size:0.82rem; cursor:pointer; }}
.nav-link:hover {{ background:var(--bg-hover); color:var(--text); }}
.nav-link .icon {{ width:18px; text-align:center; flex-shrink:0; }}
.nav-link .badge {{ margin-left:auto; font-size:0.68rem; background:var(--bg-card); color:var(--text-muted); padding:1px 6px; border-radius:8px; }}

/* Main */
.main {{ padding:28px 36px 50px; max-width:940px; }}
.section {{ margin-bottom:40px; }}
.section-title {{ font-size:1.3rem; font-weight:700; margin-bottom:6px; }}
.section-subtitle {{ font-size:0.82rem; color:var(--text-muted); margin-bottom:20px; }}

/* Stats */
.stats-bar {{ display:flex; gap:12px; margin-bottom:30px; flex-wrap:wrap; }}
.stat-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:14px 18px; flex:1; min-width:120px; }}
.stat-card .value {{ font-size:1.5rem; font-weight:800; color:var(--text); }}
.stat-card .label {{ font-size:0.72rem; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.05em; margin-top:2px; }}

/* Channels */
.channel-group-title {{ font-size:0.7rem; font-weight:700; text-transform:uppercase; letter-spacing:0.06em; color:var(--text-muted); margin:20px 0 8px; padding-left:4px; }}
.channel-list {{ display:flex; flex-direction:column; gap:2px; }}
.channel-row {{ display:flex; align-items:center; gap:7px; padding:6px 12px; background:var(--bg-card); border-radius:4px; font-size:0.85rem; }}
.channel-row .ch-icon {{ color:var(--text-muted); width:18px; text-align:center; flex-shrink:0; }}
.channel-row .ch-name {{ font-weight:500; }}
.channel-row .ch-topic {{ margin-left:auto; font-size:0.72rem; color:var(--text-muted); max-width:280px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}

/* Roles */
.roles-grid {{ display:flex; flex-wrap:wrap; gap:6px; }}
.role-chip {{ display:inline-flex; align-items:center; gap:5px; padding:4px 12px; background:var(--bg-card); border:1px solid var(--border); border-radius:16px; font-size:0.8rem; }}
.role-chip .dot {{ width:10px; height:10px; border-radius:50%; flex-shrink:0; }}

/* Members */
.members-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(220px,1fr)); gap:6px; }}
.member-card {{ display:flex; align-items:center; gap:9px; padding:8px 12px; background:var(--bg-card); border-radius:4px; }}
.member-avatar {{ width:30px; height:30px; border-radius:50%; background:var(--border); display:flex; align-items:center; justify-content:center; font-size:0.75rem; font-weight:700; color:var(--text-dim); flex-shrink:0; }}
.member-avatar img {{ width:30px; height:30px; border-radius:50%; }}
.member-info .name {{ font-size:0.82rem; font-weight:500; }}
.member-info .tag {{ font-size:0.7rem; color:var(--text-muted); }}

/* Messages */
.channel-messages {{ margin-bottom:40px; }}
.channel-msg-header {{ display:flex; align-items:center; gap:8px; margin-bottom:14px; padding-bottom:8px; border-bottom:1px solid var(--border); }}
.channel-msg-header .ch-hash {{ font-size:1.3rem; color:var(--text-muted); font-weight:300; }}
.channel-msg-header h3 {{ font-size:1.05rem; font-weight:600; }}
.channel-msg-header .count {{ margin-left:auto; font-size:0.72rem; color:var(--text-muted); }}

.message {{ display:flex; gap:12px; padding:6px 4px; border-radius:4px; }}
.message:hover {{ background:var(--bg-hover); }}
.msg-avatar {{ width:34px; height:34px; border-radius:50%; background:var(--border); display:flex; align-items:center; justify-content:center; font-size:0.72rem; font-weight:700; color:var(--text-muted); flex-shrink:0; margin-top:2px; }}
.msg-avatar img {{ width:34px; height:34px; border-radius:50%; }}
.msg-body {{ flex:1; min-width:0; }}
.msg-header {{ display:flex; align-items:baseline; gap:7px; margin-bottom:1px; }}
.msg-author {{ font-weight:600; font-size:0.85rem; }}
.msg-timestamp {{ font-size:0.68rem; color:var(--text-muted); }}
.msg-content {{ font-size:0.85rem; color:var(--text-dim); line-height:1.5; word-wrap:break-word; white-space:pre-wrap; }}

.msg-attachments {{ margin-top:5px; display:flex; flex-wrap:wrap; gap:5px; }}
.msg-attachment {{ display:inline-flex; align-items:center; gap:4px; padding:3px 8px; background:var(--bg-card); border-radius:4px; font-size:0.72rem; color:var(--blue); text-decoration:none; border:1px solid var(--border); }}
.msg-attachment:hover {{ border-color:var(--blue); }}

.msg-embed {{ margin-top:5px; padding:8px 12px; background:var(--bg-card); border-left:3px solid var(--accent); border-radius:0 4px 4px 0; font-size:0.8rem; }}
.msg-embed .embed-title {{ font-weight:600; color:var(--text); margin-bottom:3px; }}
.msg-embed .embed-desc {{ color:var(--text-dim); }}

/* Translation */
.msg-translation {{ margin-top:5px; padding:6px 10px; background:rgba(91,192,222,0.08); border-left:2px solid var(--blue); border-radius:0 4px 4px 0; font-size:0.8rem; color:var(--text-dim); }}
.msg-translation .tr-label {{ font-size:0.65rem; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; color:var(--blue); margin-bottom:2px; }}
.msg-translation .tr-text {{ color:var(--text); font-style:italic; white-space:pre-wrap; word-wrap:break-word; }}

/* Emojis */
.emoji-grid {{ display:flex; flex-wrap:wrap; gap:8px; }}
.emoji-item {{ display:flex; flex-direction:column; align-items:center; gap:3px; padding:8px; background:var(--bg-card); border-radius:4px; min-width:66px; }}
.emoji-item img {{ width:30px; height:30px; }}
.emoji-item .emoji-name {{ font-size:0.65rem; color:var(--text-muted); max-width:66px; text-align:center; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}

/* Footer */
.footer {{ margin-top:50px; padding:20px 0; border-top:1px solid var(--border); font-size:0.72rem; color:var(--text-muted); text-align:center; line-height:1.7; }}
.footer img {{ width:18px; height:18px; vertical-align:middle; margin-right:4px; border-radius:4px; }}

/* Search */
.search-box {{ width:100%; padding:8px 12px; background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-family:inherit; font-size:0.82rem; outline:none; margin-bottom:16px; }}
.search-box:focus {{ border-color:var(--accent); }}
.search-box::placeholder {{ color:var(--text-muted); }}

@media (max-width:800px) {{
    .layout {{ grid-template-columns:1fr; }}
    .sidebar {{ position:static; height:auto; max-height:50vh; }}
    .main {{ padding:16px 14px 36px; }}
    .stats-bar {{ flex-direction:column; }}
}}
</style>
</head>
<body>
<div class="layout">
""")

    # ─── Sidebar ─────────────────────────────────────────────────────────

    html_parts.append(f"""
<aside class="sidebar">
  <div class="brand-bar">
    <span class="brand-name">Vaultcord</span>
    <span class="brand-tag">🔑</span>
  </div>
  <div class="server-header">
    {"<img src='" + icon_url + "' alt='icon'>" if icon_url else ""}
    <h1>{guild_name}</h1>
    {"<p style='font-size:0.78rem;color:var(--text-dim);margin:4px 0;'>" + guild_desc + "</p>" if guild_desc else ""}
    <div class="meta">
      <span><span class="dot green"></span> {online_count} online</span>
      <span><span class="dot gray"></span> {member_count} members</span>
    </div>
  </div>
  <nav class="sidebar-nav">
    <a class="nav-link" href="#overview"><span class="icon">📊</span> Overview</a>
    <a class="nav-link" href="#channels"><span class="icon">📂</span> Channels <span class="badge">{len(channels)}</span></a>
    <a class="nav-link" href="#roles"><span class="icon">🎭</span> Roles <span class="badge">{len(roles)}</span></a>
    <a class="nav-link" href="#members"><span class="icon">👥</span> Members <span class="badge">{len(members)}</span></a>
    <a class="nav-link" href="#emojis"><span class="icon">😀</span> Emojis <span class="badge">{len(emojis)}</span></a>
    <a class="nav-link" href="#messages"><span class="icon">💬</span> Messages <span class="badge">{total_messages}</span></a>
""")

    text_ch_with_msgs = [(ch_id, ch_lookup.get(ch_id, ch_id)) for ch_id in messages if messages[ch_id]]
    if text_ch_with_msgs:
        html_parts.append('    <div class="nav-section-title" style="margin-top:10px;">Channels</div>')
        for ch_id, ch_name in sorted(text_ch_with_msgs, key=lambda x: x[1]):
            count = len(messages[ch_id])
            html_parts.append(
                f'    <a class="nav-link" href="#ch-{ch_id}">'
                f'<span class="icon">#</span> {escape(ch_name)} '
                f'<span class="badge">{count}</span></a>'
            )

    html_parts.append("  </nav>\n</aside>")

    # ─── Main content ────────────────────────────────────────────────────

    html_parts.append('<main class="main">')

    text_ch_count = sum(1 for c in channels if c.get("type") in {0, 5, 15})
    voice_ch_count = sum(1 for c in channels if c.get("type") in {2, 13})

    html_parts.append(f"""
<section id="overview" class="section">
  <h2 class="section-title">📊 Overview</h2>
  <p class="section-subtitle">Archived on {scraped_at}</p>
  <div class="stats-bar">
    <div class="stat-card"><div class="value">{member_count}</div><div class="label">Members</div></div>
    <div class="stat-card"><div class="value">{text_ch_count}</div><div class="label">Text Channels</div></div>
    <div class="stat-card"><div class="value">{voice_ch_count}</div><div class="label">Voice Channels</div></div>
    <div class="stat-card"><div class="value">{total_messages}</div><div class="label">Messages</div></div>
    <div class="stat-card"><div class="value">{len(roles)}</div><div class="label">Roles</div></div>
    <div class="stat-card"><div class="value">{len(emojis)}</div><div class="label">Emojis</div></div>
  </div>
</section>
""")

    # Channels
    html_parts.append("""
<section id="channels" class="section">
  <h2 class="section-title">📂 Channels</h2>
  <p class="section-subtitle">All server channels organized by category</p>
""")

    if uncategorized:
        html_parts.append('<div class="channel-group-title">Uncategorized</div><div class="channel-list">')
        for ch in uncategorized:
            topic = escape(ch.get("topic") or "")[:80]
            html_parts.append(
                f'<div class="channel-row"><span class="ch-icon">{channel_type_icon(ch["type"])}</span>'
                f'<span class="ch-name">{escape(ch["name"])}</span><span class="ch-topic">{topic}</span></div>'
            )
        html_parts.append('</div>')

    for cat_id in sorted(categories.keys(), key=lambda cid: next((c["position"] for c in channels if c["id"] == cid), 0)):
        cat_chs = categories[cat_id]
        html_parts.append(f'<div class="channel-group-title">{escape(cat_names.get(cat_id, ""))}</div><div class="channel-list">')
        for ch in cat_chs:
            topic = escape(ch.get("topic") or "")[:80]
            html_parts.append(
                f'<div class="channel-row"><span class="ch-icon">{channel_type_icon(ch["type"])}</span>'
                f'<span class="ch-name">{escape(ch["name"])}</span><span class="ch-topic">{topic}</span></div>'
            )
        html_parts.append('</div>')

    html_parts.append('</section>')

    # Roles
    html_parts.append("""
<section id="roles" class="section">
  <h2 class="section-title">🎭 Roles</h2>
  <p class="section-subtitle">Server roles sorted by hierarchy</p>
  <div class="roles-grid">
""")
    for role in roles:
        rname = escape(role.get("name", ""))
        if rname == "@everyone":
            continue
        rcolor = role_color_css(role.get("color", 0))
        html_parts.append(f'<div class="role-chip"><span class="dot" style="background:{rcolor}"></span>{rname}</div>')
    html_parts.append('</div></section>')

    # Members
    html_parts.append("""
<section id="members" class="section">
  <h2 class="section-title">👥 Members</h2>
  <p class="section-subtitle">Server members</p>
  <input type="text" class="search-box" placeholder="Filter members…" oninput="filterMembers(this.value)">
  <div class="members-grid" id="members-grid">
""")
    for m in members:
        u = m.get("user", {})
        username = escape(u.get("username", "unknown"))
        display = escape(m.get("nick") or u.get("global_name") or username)
        avatar = u.get("avatar")
        uid = u.get("id", "")
        avatar_url = f"https://cdn.discordapp.com/avatars/{uid}/{avatar}.png?size=64" if avatar else ""
        avatar_html = f'<img src="{avatar_url}" alt="">' if avatar_url else display[0].upper()
        html_parts.append(
            f'<div class="member-card" data-name="{display.lower()} {username.lower()}">'
            f'<div class="member-avatar">{avatar_html}</div>'
            f'<div class="member-info"><div class="name">{display}</div><div class="tag">@{username}</div></div></div>'
        )
    html_parts.append('</div></section>')

    # Emojis
    if emojis:
        html_parts.append("""
<section id="emojis" class="section">
  <h2 class="section-title">😀 Custom Emojis</h2>
  <p class="section-subtitle">Server custom emojis</p>
  <div class="emoji-grid">
""")
        for e in emojis:
            eid = e.get("id", "")
            ename = escape(e.get("name", ""))
            ext = "gif" if e.get("animated", False) else "png"
            url = f"https://cdn.discordapp.com/emojis/{eid}.{ext}?size=64"
            html_parts.append(f'<div class="emoji-item"><img src="{url}" alt=":{ename}:" loading="lazy"><span class="emoji-name">:{ename}:</span></div>')
        html_parts.append('</div></section>')

    # Messages
    html_parts.append("""
<section id="messages" class="section">
  <h2 class="section-title">💬 Messages</h2>
  <p class="section-subtitle">Message history from accessible text channels</p>
  <input type="text" class="search-box" placeholder="Search messages…" oninput="filterMessages(this.value)">
""")

    for ch_id, msgs in messages.items():
        if not msgs:
            continue
        ch_name = escape(ch_lookup.get(ch_id, ch_id))
        msgs_sorted = sorted(msgs, key=lambda m: m.get("timestamp", ""))

        html_parts.append(
            f'<div class="channel-messages" id="ch-{ch_id}">'
            f'<div class="channel-msg-header"><span class="ch-hash">#</span>'
            f'<h3>{ch_name}</h3><span class="count">{len(msgs_sorted)} messages</span></div>'
        )

        for msg in msgs_sorted:
            author = msg.get("author", {})
            author_name = escape(author.get("global_name") or author.get("username", "Unknown"))
            author_id = author.get("id", "")
            avatar = author.get("avatar")
            avatar_url = f"https://cdn.discordapp.com/avatars/{author_id}/{avatar}.png?size=64" if avatar else ""
            timestamp = format_timestamp(msg.get("timestamp"))
            content = escape(msg.get("content", ""))

            avatar_html = f'<img src="{avatar_url}" alt="">' if avatar_url else (author_name[0].upper() if author_name else "?")

            html_parts.append(
                f'<div class="message" data-content="{escape(content.lower())}">'
                f'<div class="msg-avatar">{avatar_html}</div><div class="msg-body">'
                f'<div class="msg-header"><span class="msg-author">{author_name}</span>'
                f'<span class="msg-timestamp">{timestamp}</span></div>'
            )

            if content:
                html_parts.append(f'<div class="msg-content">{content}</div>')

            if msg.get("_translation"):
                t_lang = escape(lang_name(msg.get("_detected_lang", "")))
                t_text = escape(msg["_translation"])
                html_parts.append(
                    f'<div class="msg-translation">'
                    f'<div class="tr-label">🌐 Translated from {t_lang}</div>'
                    f'<div class="tr-text">{t_text}</div></div>'
                )

            attachments = msg.get("attachments", [])
            if attachments:
                html_parts.append('<div class="msg-attachments">')
                for att in attachments:
                    fname = escape(att.get("filename", "file"))
                    aurl = att.get("url", "#")
                    size = att.get("size", 0)
                    size_str = f"{size/1024:.0f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
                    html_parts.append(f'<a class="msg-attachment" href="{aurl}" target="_blank">📎 {fname} ({size_str})</a>')
                html_parts.append('</div>')

            embeds = msg.get("embeds", [])
            for emb in embeds:
                etitle = escape(emb.get("title") or "")
                edesc = escape(emb.get("description") or "")
                if etitle or edesc:
                    ecolor = emb.get("color")
                    border_css = f"border-left-color:{role_color_css(ecolor)}" if ecolor else ""
                    html_parts.append(
                        f'<div class="msg-embed" style="{border_css}">'
                        f'{"<div class=embed-title>" + etitle + "</div>" if etitle else ""}'
                        f'{"<div class=embed-desc>" + edesc + "</div>" if edesc else ""}</div>'
                    )

            html_parts.append('</div></div>')

        html_parts.append('</div>')

    html_parts.append('</section>')

    # Footer
    html_parts.append(f"""
<div class="footer">
  <div style="margin-bottom:6px;">
    <img src="{LOGO_DATA_URI}" alt="">
    <span style="font-weight:700;color:var(--accent);letter-spacing:0.04em;">VAULTCORD</span>
  </div>
  {guild_name} &middot; {scraped_at}<br>
  {total_messages} messages &middot; {len(messages)} channels &middot; {len(members)} members &middot; {len(roles)} roles
</div>
""")

    html_parts.append('</main></div>')

    html_parts.append("""
<script>
function filterMembers(q) {
  q = q.toLowerCase();
  document.querySelectorAll('.member-card').forEach(el => {
    el.style.display = el.dataset.name.includes(q) ? '' : 'none';
  });
}
function filterMessages(q) {
  q = q.toLowerCase();
  document.querySelectorAll('.message').forEach(el => {
    const content = (el.dataset.content || '') + ' ' + (el.querySelector('.msg-author')?.textContent || '').toLowerCase();
    el.style.display = content.includes(q) ? '' : 'none';
  });
}
document.querySelectorAll('.nav-link[href^="#"]').forEach(a => {
  a.addEventListener('click', e => {
    e.preventDefault();
    const target = document.querySelector(a.getAttribute('href'));
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});
</script>
</body>
</html>
""")

    return "".join(html_parts)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print()
    print("  \033[38;5;141m╭─────────────────────────────────────────────────╮\033[0m")
    print("  \033[38;5;141m│\033[0m                                                 \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;141m██╗   ██╗ █████╗ ██╗   ██╗██╗  ████████╗\033[0m      \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;141m██║   ██║██╔══██╗██║   ██║██║  ╚══██╔══╝\033[0m      \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;147m██║   ██║███████║██║   ██║██║     ██║\033[0m         \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;153m╚██╗ ██╔╝██╔══██║██║   ██║██║     ██║\033[0m         \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;159m ╚████╔╝ ██║  ██║╚██████╔╝███████╗██║\033[0m         \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[1m\033[38;5;159m  ╚═══╝  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝\033[0m         \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m                \033[38;5;80m── cord ──\033[0m                       \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m                                                 \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m│\033[0m   \033[2mDiscord Server Archive & Exporter    v1.0\033[0m      \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m╰─────────────────────────────────────────────────╯\033[0m")
    print()

    # Token
    print("  Enter your Discord token.")
    print("    • Bot token: prefix with 'Bot ' (e.g. 'Bot abc123...')")
    print("    • User token: paste as-is (note: No tokens are stored, sent or transfered anywhere other than the Discord API)")
    token = input("\n  🔑 Token: ").strip()
    if not token:
        print("  ❌ No token provided.")
        sys.exit(1)

    # Validate token
    print("\n  ⏳ Validating token...")
    me = api_request("/users/@me", token)
    if not me:
        print("  ❌ Invalid token or cannot connect to Discord.")
        sys.exit(1)
    print(f"  ✅ Authenticated as: {me.get('username')}#{me.get('discriminator', '0')}")

    # Guild ID
    guild_id = input("\n  🏠 Server (Guild) ID: ").strip()
    if not guild_id:
        print("  ❌ No guild ID provided.")
        sys.exit(1)

    # Message limit
    global MESSAGES_PER_CHANNEL
    limit_input = input(f"\n  📨 Messages per channel (default {MESSAGES_PER_CHANNEL}, 0 for all): ").strip()
    if limit_input:
        val = int(limit_input)
        MESSAGES_PER_CHANNEL = val if val > 0 else None

    # Translation toggle
    translate_input = input("\n  🌐 Translate foreign messages to English? (y/N): ").strip().lower()
    enable_translation = translate_input in ("y", "yes")

    print()
    print("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  Starting archive...\n")

    # Collect data
    data = collect_server_data(token, guild_id, enable_translation)

    # Build HTML
    print("\n🎨 Building Vaultcord archive...")
    html = build_html(data)

    # Save
    safe_name = "".join(c if c.isalnum() or c in "-_ " else "" for c in data["guild"]["name"]).strip()
    filename = f"vaultcord_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = os.path.getsize(filepath) / 1024
    msg_total = sum(len(v) for v in data['messages'].values())
    ch_total = len(data['messages'])
    print()
    print("  \033[38;5;141m╭─────────────────────────────────────────────────╮\033[0m")
    print("  \033[38;5;141m│\033[0m  \033[32m✓\033[0m  \033[1mArchive complete\033[0m                              \033[38;5;141m│\033[0m")
    print("  \033[38;5;141m╰─────────────────────────────────────────────────╯\033[0m")
    print(f"    \033[38;5;245m📄  File ·\033[0m  {filepath}")
    print(f"    \033[38;5;245m📦  Size ·\033[0m  {file_size:.0f} KB")
    print(f"    \033[38;5;245m💬  Data ·\033[0m  {msg_total} msgs / {ch_total} channels")
    print(f"\n  \033[2mOpen in any browser to explore your archive.\033[0m\n")
    input("  Press Enter to exit...")


if __name__ == "__main__":
    main()