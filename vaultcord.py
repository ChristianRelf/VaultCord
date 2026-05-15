#!/usr/bin/env python3
"""
┌─────────────────────────────────────────────────┐
│  VAULTCORD v2.0 - Discord Archive Toolkit       │
└─────────────────────────────────────────────────┘

High-performance Discord server archiver with 13 export tools.

Tools:
     1. Full Server Archive → HTML
     2. Messages Only → JSON
     3. Audit Log → JSON
     4. Bans List → JSON
     5. Invites → JSON
     6. Pinned Messages → JSON
     7. Webhooks → JSON
     8. Server Emojis → download all
     9. Threads + Messages → JSON
    10. Stickers → JSON
    11. Scheduled Events → JSON
    12. Channel Permissions → JSON
    13. Server Assets → download icon/banner/splash
     0. Run All

Performance:
    - Persistent HTTPS connection pool (24 keep-alive sockets)
    - Per-route rate limit buckets (parallel channels run independently)
    - 15 parallel channel fetchers, 16 parallel translators
    - orjson fast JSON parsing when available
    - ~500-1000+ messages/second depending on server/connection
"""

import json
import os
import sys
import time
import http.client
import ssl
import urllib.parse
from datetime import datetime, timezone
from html import escape
from collections import defaultdict
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Use orjson if available (3-10x faster JSON parsing), fallback to stdlib
try:
    import orjson
    def _json_loads(b):
        return orjson.loads(b)
    def _json_dumps(o):
        return orjson.dumps(o, option=orjson.OPT_INDENT_2).decode()
except ImportError:
    def _json_loads(b):
        return json.loads(b)
    def _json_dumps(o):
        return json.dumps(o, indent=2, ensure_ascii=False, default=str)

# Precompiled regexes for translation cleaning
_RE_TAGS = re.compile(r'<[^>]+>')
_RE_URLS = re.compile(r'https?://\S+')
_RE_CODEBLOCK = re.compile(r'```[\s\S]*?```')
_RE_INLINE = re.compile(r'`[^`]+`')
_RE_ALPHA = re.compile(r"[a-zA-Z']+")

# ─── Configuration ──────────────────────────────────────────────────────────

MESSAGES_PER_CHANNEL = None      # None = fetch ALL messages (no limit)
REQUEST_TIMEOUT = 10
OUTPUT_DIR = "."
MAX_WORKERS_CHANNELS = 15       # Discord allows ~50 req/s global; 15 channels × ~3 req/s each
MAX_WORKERS_TRANSLATE = 16      # Google Translate is more lenient


# ─── High-performance HTTP connection pool ──────────────────────────────────

_ssl_ctx = ssl.create_default_context()

class ConnectionPool:
    """Thread-safe persistent HTTPS connection pool with keep-alive."""

    def __init__(self, host="discord.com", pool_size=24):
        self._host = host
        self._pool = []
        self._lock = threading.Lock()
        self._pool_size = pool_size
        self._created = 0

    def _new_conn(self):
        conn = http.client.HTTPSConnection(self._host, timeout=REQUEST_TIMEOUT, context=_ssl_ctx)
        conn.connect()
        self._created += 1
        return conn

    def get(self):
        with self._lock:
            while self._pool:
                conn = self._pool.pop()
                # Quick health check - discard stale connections
                if conn.sock is not None:
                    return conn
                try: conn.close()
                except: pass
        return self._new_conn()

    def put(self, conn):
        if conn.sock is None:
            return
        with self._lock:
            if len(self._pool) < self._pool_size:
                self._pool.append(conn)
            else:
                try: conn.close()
                except: pass

    def close_all(self):
        with self._lock:
            for c in self._pool:
                try: c.close()
                except: pass
            self._pool.clear()


_pool = ConnectionPool()

# Per-route rate limit buckets
_route_limits = {}       # bucket_id -> (remaining, reset_time)
_route_buckets = {}      # route_key -> bucket_id
_rl_lock = threading.Lock()

# Cached headers per token (avoid dict creation on every request)
_headers_cache = {}
_headers_lock = threading.Lock()

def _get_headers(token):
    with _headers_lock:
        if token not in _headers_cache:
            _headers_cache[token] = {
                "Authorization": token,
                "User-Agent": "Vaultcord/2.0",
                "Connection": "keep-alive",
                "Accept-Encoding": "identity",
            }
        return _headers_cache[token]


# Precompiled regex for extracting major route params
_RE_MAJOR = re.compile(r'/(channels|guilds)/(\d+)')

def _route_key(method, endpoint):
    """Discord rate limits are per major-parameter (channel/guild id)."""
    m = _RE_MAJOR.search(endpoint)
    if m:
        return f"{method}:{m.group(1)}:{m.group(2)}"
    return f"{method}:{endpoint}"


def _check_rate_limit(route):
    """Wait if this route's bucket is exhausted. Non-blocking for other routes."""
    wait = 0
    with _rl_lock:
        bucket = _route_buckets.get(route)
        if bucket and bucket in _route_limits:
            remaining, reset_time = _route_limits[bucket]
            if remaining <= 0:
                wait = reset_time - time.monotonic()
    # Sleep OUTSIDE the lock so other routes aren't blocked
    if wait > 0:
        time.sleep(wait + 0.02)


# Sentinel for permission denied (distinct from None which means other errors)
FORBIDDEN = "FORBIDDEN"


def api_request(endpoint, token, params=None, method="GET"):
    """High-performance Discord API request with connection pooling + keep-alive."""
    path = f"/api/v10{endpoint}"
    if params:
        path += "?" + urllib.parse.urlencode(params)

    route = _route_key(method, endpoint)

    for attempt in range(6):
        _check_rate_limit(route)
        conn = _pool.get()
        try:
            conn.request(method, path, headers=_get_headers(token))
            resp = conn.getresponse()
            body = resp.read()

            # Update rate limits from response headers (avoid dict creation)
            bucket = resp.getheader("X-RateLimit-Bucket")
            remaining = resp.getheader("X-RateLimit-Remaining")
            reset_after = resp.getheader("X-RateLimit-Reset-After")
            if bucket and remaining is not None and reset_after is not None:
                with _rl_lock:
                    _route_buckets[route] = bucket
                    _route_limits[bucket] = (int(remaining), time.monotonic() + float(reset_after))

            if resp.status == 200:
                _pool.put(conn)
                if not body:
                    return None
                try:
                    return _json_loads(body)
                except (json.JSONDecodeError, ValueError):
                    return None
            elif resp.status == 204:
                _pool.put(conn)
                return []
            elif resp.status == 429:
                _pool.put(conn)
                try:
                    retry_after = float(_json_loads(body).get("retry_after", 2)) if body else 2
                except (json.JSONDecodeError, ValueError):
                    retry_after = 2
                time.sleep(retry_after + 0.05)
                continue
            elif resp.status == 403:
                _pool.put(conn)
                return FORBIDDEN
            elif resp.status == 404:
                _pool.put(conn)
                return None
            else:
                _pool.put(conn)
                return None

        except (http.client.HTTPException, OSError, ConnectionError, TimeoutError):
            try: conn.close()
            except: pass
            if attempt < 5:
                time.sleep(0.2 * (attempt + 1))
            else:
                return None

    return None


def fetch_all_messages(channel_id, token, limit=None):
    """Fetch messages from a channel using pagination."""
    messages = []
    before = None
    endpoint = f"/channels/{channel_id}/messages"
    consecutive_fails = 0

    while True:
        params = {"limit": 100}
        if before:
            params["before"] = before

        data = api_request(endpoint, token, params)

        if data is FORBIDDEN:
            break

        if not data or not isinstance(data, list):
            consecutive_fails += 1
            if consecutive_fails >= 3:
                break  # Give up after 3 consecutive failures
            time.sleep(0.5)
            continue  # Retry the same page

        n = len(data)
        if n == 0:
            break

        consecutive_fails = 0  # Reset on success
        messages.extend(data)
        before = data[-1]["id"]

        if limit and len(messages) >= limit:
            del messages[limit:]
            break

        if n < 100:
            break

    return messages


# ─── Scrape tools ───────────────────────────────────────────────────────────

def scrape_audit_log(token, guild_id):
    """Scrape the server audit log (requires VIEW_AUDIT_LOG permission)."""
    print("\n📜 Fetching audit log...")
    entries = []
    before = None
    for _ in range(20):
        params = {"limit": 100}
        if before:
            params["before"] = before
        data = api_request(f"/guilds/{guild_id}/audit-logs", token, params)
        if data is FORBIDDEN:
            print("   ❌ Missing permission: VIEW_AUDIT_LOG")
            return None
        if not isinstance(data, dict) or not data.get("audit_log_entries"):
            break
        batch = data["audit_log_entries"]
        entries.extend(batch)
        before = batch[-1]["id"]
        print(f"   📜 {len(entries)} entries...", end="\r")
        if len(batch) < 100:
            break
    print(f"   ✅ {len(entries)} audit log entries fetched       ")
    return {"audit_log_entries": entries, "users": data.get("users", []) if isinstance(data, dict) else []}


def scrape_bans(token, guild_id):
    """Scrape the ban list (requires BAN_MEMBERS permission)."""
    print("\n🔨 Fetching ban list...")
    bans = []
    after = None
    for _ in range(50):
        params = {"limit": 1000}
        if after:
            params["after"] = after
        data = api_request(f"/guilds/{guild_id}/bans", token, params)
        if data is FORBIDDEN:
            print("   ❌ Missing permission: BAN_MEMBERS — cannot access ban list")
            return None
        if not data or len(data) == 0:
            break
        bans.extend(data)
        after = data[-1]["user"]["id"]
        print(f"   🔨 {len(bans)} bans...", end="\r")
        if len(data) < 1000:
            break
    print(f"   ✅ {len(bans)} bans fetched       ")
    return bans


def scrape_invites(token, guild_id):
    """Scrape active invites (requires MANAGE_GUILD permission)."""
    print("\n🔗 Fetching invites...")
    data = api_request(f"/guilds/{guild_id}/invites", token)
    if data is FORBIDDEN:
        print("   ❌ Missing permission: MANAGE_GUILD")
        return None
    invites = _as_list(data)
    print(f"   ✅ {len(invites)} invites fetched")
    return invites


def scrape_webhooks(token, guild_id):
    """Scrape all webhooks (requires MANAGE_WEBHOOKS permission)."""
    print("\n🪝 Fetching webhooks...")
    data = api_request(f"/guilds/{guild_id}/webhooks", token)
    if data is FORBIDDEN:
        print("   ❌ Missing permission: MANAGE_WEBHOOKS")
        return None
    webhooks = _as_list(data)
    print(f"   ✅ {len(webhooks)} webhooks fetched")
    return webhooks


def scrape_pins(token, channels):
    """Scrape pinned messages from all text channels (parallel)."""
    print("\n📌 Fetching pinned messages...")
    text_types = {0, 5, 15, 16}
    text_chs = [c for c in channels if c.get("type") in text_types]
    all_pins = {}
    _lock = threading.Lock()
    _count = {"n": 0, "pins": 0}

    def _fetch_pins(ch):
        ch_id, ch_name = ch["id"], ch.get("name", "?")
        data = api_request(f"/channels/{ch_id}/pins", token)
        pins = _as_list(data)
        with _lock:
            _count["n"] += 1
            _count["pins"] += len(pins)
            if pins:
                print(f"   [{_count['n']}/{len(text_chs)}] #{ch_name} — {len(pins)} pins", end="\r")
        return ch_id, ch_name, pins

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
        futures = [pool.submit(_fetch_pins, ch) for ch in text_chs]
        for f in as_completed(futures):
            ch_id, ch_name, pins = f.result()
            if pins:
                all_pins[ch_name] = pins

    print(f"   ✅ {_count['pins']} pinned messages from {len(all_pins)} channels       ")
    return all_pins


def scrape_emojis_download(token, guild_id):
    """Download all custom emojis to a folder."""
    import urllib.request
    print("\n😀 Downloading emojis...")
    emojis = _as_list(api_request(f"/guilds/{guild_id}/emojis", token))
    if not emojis:
        print("   ✅ No custom emojis")
        return

    folder = os.path.join(OUTPUT_DIR, f"vaultcord_emojis_{guild_id}")
    os.makedirs(folder, exist_ok=True)

    _lock = threading.Lock()
    _count = {"n": 0}

    def _dl(emoji):
        eid = emoji["id"]
        name = emoji.get("name", eid)
        ext = "gif" if emoji.get("animated") else "png"
        url = f"https://cdn.discordapp.com/emojis/{eid}.{ext}?size=128"
        filepath = os.path.join(folder, f"{name}.{ext}")
        try:
            urllib.request.urlretrieve(url, filepath)
        except: pass
        with _lock:
            _count["n"] += 1
            if _count["n"] % 10 == 0:
                print(f"   🔄 {_count['n']}/{len(emojis)} downloaded...", end="\r")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_dl, emojis))

    print(f"   ✅ {len(emojis)} emojis saved to {folder}       ")
    return folder


def scrape_threads(token, guild_id, channels=None):
    """Scrape all active and archived threads."""
    print("\n🧵 Fetching threads...")
    # Active threads (guild-wide endpoint)
    active = api_request(f"/guilds/{guild_id}/threads/active", token)
    active_threads = active.get("threads", []) if isinstance(active, dict) else []
    print(f"   ✅ {len(active_threads)} active threads found")

    # Archived threads (per-channel, parallel)
    if channels:
        text_types = {0, 5, 15}
        text_chs = [c for c in channels if c.get("type") in text_types]
        archived = []
        _lock = threading.Lock()

        def _fetch_archived(ch):
            ch_id = ch["id"]
            # Public archived
            pub = api_request(f"/channels/{ch_id}/threads/archived/public", token, {"limit": 100})
            threads = pub.get("threads", []) if isinstance(pub, dict) else []
            # Private archived
            priv = api_request(f"/channels/{ch_id}/threads/archived/private", token, {"limit": 100})
            threads += priv.get("threads", []) if isinstance(priv, dict) else []
            if threads:
                with _lock:
                    archived.extend(threads)
            return threads

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
            list(pool.map(_fetch_archived, text_chs))

        print(f"   ✅ {len(archived)} archived threads found")
        active_threads.extend(archived)

    # Optionally fetch messages from threads
    if active_threads:
        thread_msgs = {}
        _lock2 = threading.Lock()
        _c = {"n": 0}

        def _fetch_thread_msgs(thread):
            t_id = thread["id"]
            t_name = thread.get("name", "?")
            msgs = fetch_all_messages(t_id, token, 100)  # Cap at 100 per thread
            with _lock2:
                _c["n"] += 1
                if _c["n"] % 10 == 0:
                    print(f"   🧵 {_c['n']}/{len(active_threads)} thread messages...", end="\r")
            return t_id, t_name, msgs

        print(f"   💬 Fetching messages from {len(active_threads)} threads...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
            for t_id, t_name, msgs in pool.map(_fetch_thread_msgs, active_threads):
                if msgs:
                    thread_msgs[t_name] = {"thread_info": {"id": t_id, "name": t_name}, "messages": msgs}

        print(f"   ✅ {sum(len(v['messages']) for v in thread_msgs.values())} thread messages       ")
        return {"threads": active_threads, "thread_messages": thread_msgs}

    return {"threads": active_threads}


def scrape_stickers(token, guild_id):
    """Scrape all server stickers."""
    print("\n🏷️ Fetching stickers...")
    data = api_request(f"/guilds/{guild_id}/stickers", token)
    stickers = _as_list(data)
    print(f"   ✅ {len(stickers)} stickers found")
    return stickers


def scrape_scheduled_events(token, guild_id):
    """Scrape all scheduled events."""
    print("\n📅 Fetching scheduled events...")
    data = api_request(f"/guilds/{guild_id}/scheduled-events", token, {"with_user_count": "true"})
    events = _as_list(data)
    print(f"   ✅ {len(events)} scheduled events found")
    return events


def scrape_channels_permissions(token, channels):
    """Export channel permission overwrites for all channels."""
    print("\n🔒 Extracting channel permissions...")
    result = []
    for ch in channels:
        result.append({
            "id": ch["id"],
            "name": ch.get("name", "?"),
            "type": ch.get("type"),
            "position": ch.get("position"),
            "parent_id": ch.get("parent_id"),
            "nsfw": ch.get("nsfw", False),
            "topic": ch.get("topic"),
            "slowmode": ch.get("rate_limit_per_user", 0),
            "permission_overwrites": ch.get("permission_overwrites", []),
        })
    print(f"   ✅ {len(result)} channels exported with permissions")
    return result


def scrape_server_assets(token, guild_id, guild_info):
    """Download server icon, banner, splash, and discovery splash."""
    import urllib.request as urlreq
    print("\n🖼️ Downloading server assets...")
    folder = os.path.join(OUTPUT_DIR, f"vaultcord_assets_{guild_id}")
    os.makedirs(folder, exist_ok=True)

    assets = {
        "icon": guild_info.get("icon"),
        "banner": guild_info.get("banner"),
        "splash": guild_info.get("splash"),
        "discovery_splash": guild_info.get("discovery_splash"),
    }

    downloaded = 0
    for kind, hash_val in assets.items():
        if not hash_val:
            continue
        ext = "gif" if hash_val.startswith("a_") else "png"
        url = f"https://cdn.discordapp.com/{kind}s/{guild_id}/{hash_val}.{ext}?size=1024"
        filepath = os.path.join(folder, f"{kind}.{ext}")
        try:
            urlreq.urlretrieve(url, filepath)
            downloaded += 1
            print(f"   ✅ {kind}.{ext}")
        except:
            print(f"   ⚠ Failed: {kind}")

    print(f"   ✅ {downloaded} assets saved to {folder}")
    return folder


def scrape_reactions(token, channels_messages):
    """Scrape all reactions on messages that have them (parallel)."""
    print("\n😂 Fetching reactions...")
    candidates = []
    for ch_id, msgs in channels_messages.items():
        for msg in msgs:
            if msg.get("reactions"):
                candidates.append((ch_id, msg))

    if not candidates:
        print("   ✅ No reactions found")
        return {}

    all_reactions = {}
    _lock = threading.Lock()
    _c = {"n": 0}

    def _fetch_reactions(item):
        ch_id, msg = item
        msg_id = msg["id"]
        msg_reactions = {}
        for r in msg.get("reactions", []):
            emoji = r.get("emoji", {})
            e_name = emoji.get("name", "?")
            e_id = emoji.get("id")
            # URL-encode the emoji identifier
            if e_id:
                e_str = f"{e_name}:{e_id}"
            else:
                e_str = urllib.parse.quote(e_name)

            users = []
            after = None
            for _ in range(5):  # Cap at 500 users per reaction
                params = {"limit": 100}
                if after:
                    params["after"] = after
                data = api_request(f"/channels/{ch_id}/messages/{msg_id}/reactions/{e_str}", token, params)
                if not data or len(data) == 0:
                    break
                users.extend(data)
                after = data[-1]["id"]
                if len(data) < 100:
                    break
            msg_reactions[e_name] = {"count": r.get("count", 0), "users": users}

        with _lock:
            _c["n"] += 1
            if _c["n"] % 5 == 0:
                print(f"   😂 {_c['n']}/{len(candidates)} messages processed...", end="\r")

        return msg_id, msg_reactions

    print(f"   Scanning {len(candidates)} messages with reactions...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
        for msg_id, reactions in pool.map(_fetch_reactions, candidates):
            if reactions:
                all_reactions[msg_id] = reactions

    total_r = sum(sum(r["count"] for r in v.values()) for v in all_reactions.values())
    print(f"   ✅ {total_r} reactions from {len(all_reactions)} messages       ")
    return all_reactions


def scrape_user_avatars(token, members, guild_id):
    """Download all member avatars to a folder (parallel)."""
    import urllib.request as urlreq
    print("\n🧑 Downloading user avatars...")
    folder = os.path.join(OUTPUT_DIR, f"vaultcord_avatars_{guild_id}")
    os.makedirs(folder, exist_ok=True)

    _lock = threading.Lock()
    _c = {"n": 0, "ok": 0}

    def _dl_avatar(m):
        u = m.get("user", {})
        uid = u.get("id", "")
        username = u.get("username", "unknown")
        # Prefer guild avatar, fallback to global
        avatar_hash = m.get("avatar") or u.get("avatar")
        if not avatar_hash:
            return

        ext = "gif" if avatar_hash.startswith("a_") else "png"
        if m.get("avatar"):
            url = f"https://cdn.discordapp.com/guilds/{guild_id}/users/{uid}/avatars/{avatar_hash}.{ext}?size=256"
        else:
            url = f"https://cdn.discordapp.com/avatars/{uid}/{avatar_hash}.{ext}?size=256"

        filepath = os.path.join(folder, f"{username}_{uid}.{ext}")
        try:
            urlreq.urlretrieve(url, filepath)
            with _lock:
                _c["ok"] += 1
        except:
            pass
        with _lock:
            _c["n"] += 1
            if _c["n"] % 20 == 0:
                print(f"   🧑 {_c['n']}/{len(members)} processed...", end="\r")

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(_dl_avatar, members))

    print(f"   ✅ {_c['ok']} avatars saved to {folder}       ")
    return folder


def scrape_attachments_download(token, channels_messages, guild_id):
    """Download all message attachments to a folder (parallel)."""
    import urllib.request as urlreq
    print("\n📎 Downloading attachments...")
    folder = os.path.join(OUTPUT_DIR, f"vaultcord_attachments_{guild_id}")
    os.makedirs(folder, exist_ok=True)

    # Collect all attachments
    items = []
    for ch_id, msgs in channels_messages.items():
        for msg in msgs:
            for att in msg.get("attachments", []):
                url = att.get("url")
                fname = att.get("filename", "unknown")
                if url:
                    items.append((url, fname, msg["id"]))

    if not items:
        print("   ✅ No attachments found")
        return folder

    _lock = threading.Lock()
    _c = {"n": 0, "ok": 0, "bytes": 0}

    def _dl(item):
        url, fname, msg_id = item
        safe_name = f"{msg_id}_{fname}"
        filepath = os.path.join(folder, safe_name)
        try:
            urlreq.urlretrieve(url, filepath)
            size = os.path.getsize(filepath)
            with _lock:
                _c["ok"] += 1
                _c["bytes"] += size
        except:
            pass
        with _lock:
            _c["n"] += 1
            if _c["n"] % 10 == 0:
                mb = _c["bytes"] / 1048576
                print(f"   📎 {_c['n']}/{len(items)} files ({mb:.1f} MB)...", end="\r")

    print(f"   Downloading {len(items)} files...")
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_dl, items))

    mb = _c["bytes"] / 1048576
    print(f"   ✅ {_c['ok']} files ({mb:.1f} MB) saved to {folder}       ")
    return folder


def scrape_automod(token, guild_id):
    """Scrape auto-moderation rules."""
    print("\n🛡️ Fetching auto-moderation rules...")
    data = api_request(f"/guilds/{guild_id}/auto-moderation/rules", token)
    if data is FORBIDDEN:
        print("   ❌ Missing permission: MANAGE_GUILD")
        return None
    rules = _as_list(data)
    print(f"   ✅ {len(rules)} auto-mod rules found")
    return rules


def scrape_integrations(token, guild_id):
    """Scrape server integrations (bots, apps, connected services)."""
    print("\n🔌 Fetching integrations...")
    data = api_request(f"/guilds/{guild_id}/integrations", token)
    if data is FORBIDDEN:
        print("   ❌ Missing permission: MANAGE_GUILD")
        return None
    integrations = _as_list(data)
    print(f"   ✅ {len(integrations)} integrations found")
    return integrations


def scrape_guild_profile(token, guild_id):
    """Full guild profile dump with all available metadata."""
    print("\n🏛️ Fetching full server profile...")
    guild = api_request(f"/guilds/{guild_id}", token, {"with_counts": "true"})
    if not guild or guild is FORBIDDEN:
        print("   ⚠ Could not fetch guild profile")
        return None

    preview = api_request(f"/guilds/{guild_id}/preview", token)
    if preview is FORBIDDEN:
        preview = None

    profile = {
        "guild": guild,
        "preview": preview,
        "features": guild.get("features", []),
        "verification_level": guild.get("verification_level"),
        "mfa_level": guild.get("mfa_level"),
        "explicit_content_filter": guild.get("explicit_content_filter"),
        "default_message_notifications": guild.get("default_message_notifications"),
        "system_channel_id": guild.get("system_channel_id"),
        "rules_channel_id": guild.get("rules_channel_id"),
        "vanity_url_code": guild.get("vanity_url_code"),
        "premium_tier": guild.get("premium_tier"),
        "premium_subscription_count": guild.get("premium_subscription_count"),
        "preferred_locale": guild.get("preferred_locale"),
        "nsfw_level": guild.get("nsfw_level"),
    }
    print(f"   ✅ Server profile exported (boost tier {guild.get('premium_tier', 0)}, "
          f"{guild.get('premium_subscription_count', 0)} boosts)")
    return profile


def export_messages_csv(channels_messages, ch_lookup, guild_name):
    """Export all messages as a CSV file for spreadsheet analysis."""
    import csv
    print("\n📊 Exporting messages to CSV...")
    safe = "".join(c if c.isalnum() or c in "-_ " else "" for c in guild_name).strip()
    filename = f"vaultcord_{safe}_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    total = 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["channel", "timestamp", "author_id", "author_name", "content",
                         "attachments", "embeds", "reactions", "reply_to"])
        for ch_id, msgs in channels_messages.items():
            ch_name = ch_lookup.get(ch_id, ch_id)
            sorted_msgs = sorted(msgs, key=lambda m: m.get("timestamp", ""))
            for msg in sorted_msgs:
                author = msg.get("author", {})
                att_count = len(msg.get("attachments", []))
                emb_count = len(msg.get("embeds", []))
                react_count = sum(r.get("count", 0) for r in msg.get("reactions", []))
                ref = msg.get("referenced_message", {})
                reply_to = ref.get("id", "") if ref else ""
                writer.writerow([
                    ch_name,
                    msg.get("timestamp", ""),
                    author.get("id", ""),
                    author.get("global_name") or author.get("username", "?"),
                    msg.get("content", ""),
                    att_count,
                    emb_count,
                    react_count,
                    reply_to,
                ])
                total += 1

    size = os.path.getsize(filepath) / 1024
    print(f"   ✅ {total} messages → {filepath} ({size:.0f} KB)")
    return filepath


def save_json(data, name, guild_name):
    """Save data as a JSON file using fastest available serializer."""
    if data is None:
        print(f"   ⏩ Skipping {name} export (no data / no permission)")
        return None
    safe = "".join(c if c.isalnum() or c in "-_ " else "" for c in guild_name).strip()
    filename = f"vaultcord_{safe}_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(_json_dumps(data))
    size = os.path.getsize(filepath) / 1024
    print(f"   📄 Saved: {filepath} ({size:.0f} KB)")
    return filepath


# ─── Translation helpers ────────────────────────────────────────────────────

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
    words = _RE_ALPHA.findall(text.lower())
    if len(words) == 0:
        return False
    if len(words) <= 2:
        return all(w.isascii() for w in words) and any(w in _EN_COMMON for w in words)
    en_count = sum(1 for w in words if w in _EN_COMMON)
    return (en_count / len(words)) >= 0.3


def _clean_for_detection(text):
    clean = _RE_TAGS.sub('', text)
    clean = _RE_URLS.sub('', clean)
    clean = _RE_CODEBLOCK.sub('', clean)
    clean = _RE_INLINE.sub('', clean)
    return clean.strip()


# Persistent connection for Google Translate
_translate_pool = ConnectionPool(host="translate.googleapis.com", pool_size=20)

def detect_and_translate(text):
    if not text or len(text.strip()) < 3:
        return None, None

    clean = _clean_for_detection(text)
    if len(clean) < 3:
        return None, None

    alpha_chars = [c for c in clean if c.isalpha()]
    if len(alpha_chars) < 2:
        return None, None

    if _is_likely_english(clean):
        return None, None

    try:
        params = urllib.parse.urlencode({
            "client": "gtx", "sl": "auto", "tl": "en", "dt": "t",
            "q": clean[:1500],
        })
        path = f"/translate_a/single?{params}"

        conn = _translate_pool.get()
        try:
            conn.request("GET", path, headers={"User-Agent": "Mozilla/5.0"})
            resp = conn.getresponse()
            body = resp.read()
            if resp.status == 200:
                _translate_pool.put(conn)
                result = json.loads(body)
            elif resp.status == 429:
                _translate_pool.put(conn)
                time.sleep(1)
                return None, None
            else:
                _translate_pool.put(conn)
                return None, None
        except:
            try: conn.close()
            except: pass
            return None, None

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
    except:
        return None, None


TRANSLATION_CACHE = {}
_cache_lock = threading.Lock()

def get_translation(text):
    # Use hash as cache key to save memory (messages can be long)
    key = hash(text)
    with _cache_lock:
        if key in TRANSLATION_CACHE:
            return TRANSLATION_CACHE[key]
    result = detect_and_translate(text)
    with _cache_lock:
        TRANSLATION_CACHE[key] = result
    return result


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
    if not code:
        return "Unknown"
    return LANG_NAMES.get(code, LANG_NAMES.get(code.split("-")[0], code.upper()))


def translate_messages(all_messages):
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
    print(f"\n🌐 Translating: {len(candidates)} candidates ({skipped} skipped as English)")

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
            if processed % 50 == 0 or processed == len(candidates):
                print(f"   🔄 {processed}/{len(candidates)} checked, {translated_count} translated...", end="\r")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRANSLATE) as pool:
        list(pool.map(_translate_one, candidates))

    print(f"   ✅ {translated_count} messages translated                            ")
    return all_messages


# ─── Data collection ────────────────────────────────────────────────────────

def _as_list(result):
    """Convert API result to list, treating FORBIDDEN/None/non-list as empty."""
    if result is FORBIDDEN or result is None or not isinstance(result, list):
        return []
    return result


def collect_server_data(token, guild_id, translate=False):
    """Collect all accessible data from a Discord server."""
    data = {}

    # Fetch guild info first (needed to confirm access)
    print("📋 Fetching server info...")
    guild = api_request(f"/guilds/{guild_id}", token, {"with_counts": "true"})
    if not guild or guild is FORBIDDEN:
        print("❌ Could not access this server. Check your token and guild ID.")
        sys.exit(1)
    data["guild"] = guild
    print(f"   ✅ Server: {guild['name']}")

    # Fetch channels, roles, emojis in parallel (they're independent)
    print("📂 Fetching channels, roles, emojis in parallel...")
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_channels = pool.submit(api_request, f"/guilds/{guild_id}/channels", token)
        f_roles = pool.submit(api_request, f"/guilds/{guild_id}/roles", token)
        f_emojis = pool.submit(api_request, f"/guilds/{guild_id}/emojis", token)

    channels = _as_list(f_channels.result())
    roles = _as_list(f_roles.result())
    emojis = _as_list(f_emojis.result())

    data["channels"] = channels
    data["roles"] = sorted(roles, key=lambda r: r.get("position", 0), reverse=True)
    data["emojis"] = emojis
    print(f"   ✅ {len(channels)} channels, {len(roles)} roles, {len(emojis)} emojis")

    # Members (paginated, can't easily parallelize)
    print("👥 Fetching members...")
    members = []
    after = "0"
    for _ in range(50):
        batch = api_request(f"/guilds/{guild_id}/members", token, {"limit": 1000, "after": after})
        if batch is FORBIDDEN:
            print("   ⚠ Missing permission: SERVER MEMBERS INTENT")
            break
        if not batch or not isinstance(batch, list) or len(batch) == 0:
            break
        members.extend(batch)
        after = batch[-1]["user"]["id"]
        print(f"   👥 {len(members)} members...", end="\r")
        if len(batch) < 1000:
            break
    data["members"] = members
    print(f"   ✅ {len(members)} members fetched       ")

    # Messages (parallel across channels)
    print("💬 Fetching messages from text channels...")
    t_start = time.time()
    text_channel_types = {0, 5, 15, 16}
    text_channels = [c for c in channels if c.get("type") in text_channel_types]
    data["messages"] = {}

    _fetch_counter = {"done": 0, "total_msgs": 0}
    _fetch_lock = threading.Lock()
    total_ch = len(text_channels)

    def _fetch_channel(ch):
        ch_id = ch["id"]
        ch_name = ch.get("name", "unknown")
        msgs = fetch_all_messages(ch_id, token, MESSAGES_PER_CHANNEL)
        with _fetch_lock:
            _fetch_counter["done"] += 1
            _fetch_counter["total_msgs"] += len(msgs) if msgs else 0
            n = _fetch_counter["done"]
            tm = _fetch_counter["total_msgs"]
            if msgs:
                print(f"   [{n}/{total_ch}] #{ch_name} — {len(msgs)} msgs (total: {tm}) ✅")
            else:
                print(f"   [{n}/{total_ch}] #{ch_name} — ⛔ no access")
        return ch_id, msgs

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
        futures = {pool.submit(_fetch_channel, ch): ch for ch in text_channels}
        for future in as_completed(futures):
            ch_id, msgs = future.result()
            if msgs:
                data["messages"][ch_id] = msgs

    elapsed = time.time() - t_start
    total_msgs = sum(len(v) for v in data["messages"].values())
    rate = total_msgs / elapsed if elapsed > 0 else 0
    print(f"   ⚡ {total_msgs} messages in {elapsed:.1f}s ({rate:.0f} msg/s)")

    if translate:
        translate_messages(data["messages"])
    else:
        print("\n  ⏩ Translation skipped")

    return data


# ─── HTML generation ────────────────────────────────────────────────────────

def role_color_css(color_int):
    if not color_int:
        return "#99aab5"
    return f"#{color_int:06x}"


def format_timestamp(iso_str):
    if not iso_str:
        return ""
    # Fast path: Discord timestamps are always ISO format like "2024-01-15T12:30:45.123456+00:00"
    # Just slice the first 16 chars for "YYYY-MM-DD HH:MM" — no parsing needed
    try:
        return iso_str[:10] + " " + iso_str[11:16]
    except (IndexError, TypeError):
        return str(iso_str)[:16]


def channel_type_icon(t):
    icons = {0: "#", 2: "🔊", 4: "📁", 5: "📢", 10: "🧵", 11: "🧵",
             12: "🧵", 13: "🎙", 15: "💬", 16: "🖼"}
    return icons.get(t, "•")

_LOGO_B64 = "/9j/4AAQSkZJRgABAQAAAQABAAD/4gHYSUNDX1BST0ZJTEUAAQEAAAHIAAAAAAQwAABtbnRyUkdCIFhZWiAH4AABAAEAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAACRyWFlaAAABFAAAABRnWFlaAAABKAAAABRiWFlaAAABPAAAABR3dHB0AAABUAAAABRyVFJDAAABZAAAAChnVFJDAAABZAAAAChiVFJDAAABZAAAAChjcHJ0AAABjAAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAAgAAAAcAHMAUgBHAEJYWVogAAAAAAAAb6IAADj1AAADkFhZWiAAAAAAAABimQAAt4UAABjaWFlaIAAAAAAAACSgAAAPhAAAts9YWVogAAAAAAAA9tYAAQAAAADTLXBhcmEAAAAAAAQAAAACZmYAAPKnAAANWQAAE9AAAApbAAAAAAAAAABtbHVjAAAAAAAAAAEAAAAMZW5VUwAAACAAAAAcAEcAbwBvAGcAbABlACAASQBuAGMALgAgADIAMAAxADb/2wBDAAUDBAQEAwUEBAQFBQUGBwwIBwcHBw8LCwkMEQ8SEhEPERETFhwXExQaFRERGCEYGh0dHx8fExciJCIeJBweHx7/2wBDAQUFBQcGBw4ICA4eFBEUHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh7/wAARCAQABAADASIAAhEBAxEB/8QAHAABAQEAAgMBAAAAAAAAAAAAAAcBBggDBAUC/8QATRABAAEDAgQCBQYJCgUDBAMAAAECAxEEIQUGMUEHEiIyUWFxE3JzgbGzCBQ0NTZCYqHDIyQmUmODkbK0wRUzU8TRQ4KjosLh8BYlRP/EABsBAQABBQEAAAAAAAAAAAAAAAABAgMEBQcG/8QANhEBAAECAgcFCAICAwEBAAAAAAECAwQRBQYxMkFRcRIhIoGxNDVhkaHB0fATslLhFCOCkkL/2gAMAwEAAhEDEQA/AOmQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD//2Q=="


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
<title>{guild_name} — Vaultcord Archive</title>
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
.msg-translation {{ margin-top:3px; font-size:0.85rem; color:var(--text-dim); white-space:pre-wrap; word-wrap:break-word; }}

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

/* Virtual list */
#vl-viewport {{ height:calc(100vh - 220px); min-height:300px; overflow-y:auto; position:relative; border:1px solid var(--border); border-radius:var(--radius); }}
#vl-inner    {{ position:relative; }}
#vl-empty    {{ padding:40px; text-align:center; color:var(--text-muted); font-size:0.9rem; }}
.vl-item     {{ position:absolute; left:0; right:0; padding:0 2px; }}

/* Channel msg header (override existing to add date input layout) */
.channel-msg-header {{ display:flex; align-items:center; gap:8px; margin-bottom:10px; padding-bottom:8px; border-bottom:1px solid var(--border); }}
#jump-date-input {{ margin-left:auto; padding:4px 8px; background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-size:0.75rem; color-scheme:dark; cursor:pointer; }}
#jump-date-input:focus {{ border-color:var(--accent); outline:none; }}

/* Collapsible sidebar categories */
.cat-header  {{ display:flex; align-items:center; gap:5px; padding:4px 10px; margin-top:6px; cursor:pointer; user-select:none; font-size:0.65rem; font-weight:700; text-transform:uppercase; letter-spacing:.06em; color:var(--text-muted); }}
.cat-header:hover {{ color:var(--text); }}
.cat-arrow   {{ font-size:0.55rem; transition:transform .15s; display:inline-block; flex-shrink:0; }}
.cat-section.collapsed .cat-arrow    {{ transform:rotate(-90deg); }}
.cat-section.collapsed .cat-channels {{ display:none; }}
.nav-link.active {{ background:var(--bg-hover); color:var(--text); }}

/* Global search overlay */
#search-overlay {{ position:fixed; inset:0; background:rgba(0,0,0,.65); z-index:1000; display:flex; align-items:flex-start; justify-content:center; padding-top:80px; }}
#search-modal   {{ background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); width:min(700px,90vw); max-height:70vh; display:flex; flex-direction:column; overflow:hidden; box-shadow:0 8px 32px rgba(0,0,0,.5); }}
#search-input   {{ padding:12px 16px; background:transparent; border:none; border-bottom:1px solid var(--border); color:var(--text); font-size:.9rem; outline:none; font-family:inherit; }}
#search-results {{ overflow-y:auto; flex:1; }}
.search-result  {{ padding:10px 16px; border-bottom:1px solid var(--border); cursor:pointer; font-size:.82rem; }}
.search-result:hover {{ background:var(--bg-hover); }}
.sr-channel {{ color:var(--accent); font-size:.72rem; margin-bottom:2px; }}
.sr-author  {{ font-weight:600; }}
.sr-ts      {{ color:var(--text-muted); font-size:.68rem; margin-left:6px; }}
.sr-content {{ color:var(--text-dim); margin-top:2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.sr-empty   {{ padding:24px; text-align:center; color:var(--text-muted); font-size:0.85rem; }}
</style>
</head>
<body>
<div class="layout">
""")

    # ─── Sidebar ─────────────────────────────────────────────────────────

    html_parts.append(f"""
<aside class="sidebar">
  <div class="brand-bar">
    <img src="{LOGO_DATA_URI}" alt="Vaultcord">
    <span class="brand-name">Vaultcord</span>
    <span class="brand-tag">v1.0</span>
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
    <a class="nav-link" onclick="openSearch();return false;" href="#" style="cursor:pointer;"><span class="icon">🔍</span> Search</a>
""")

    msgs_ch_ids = {ch_id for ch_id, msgs_list in messages.items() if msgs_list}
    if msgs_ch_ids:
        html_parts.append('    <div class="nav-section-title" style="margin-top:6px;">Channels</div>')

        def _sidebar_ch_link(ch):
            ch_id = ch["id"]
            count = len(messages.get(ch_id, []))
            icon = "#" if ch.get("type") in {0, 5, 15} else "🔊"
            safe_id = ch_id.replace("'", "\\'")
            return (
                f'    <a class="nav-link ch-link" data-ch-id="{ch_id}"'
                f' onclick="switchChannel(\'{safe_id}\');return false;" href="#">'
                f'<span class="icon">{icon}</span> {escape(ch["name"])}'
                f' <span class="badge">{count}</span></a>'
            )

        sorted_cat_ids = sorted(
            categories.keys(),
            key=lambda cid: next((c["position"] for c in channels if c["id"] == cid), 0)
        )

        unc_with_msgs = [ch for ch in uncategorized if ch["id"] in msgs_ch_ids]
        if unc_with_msgs:
            html_parts.append('<div class="cat-section" data-cat-id="__unc__">')
            html_parts.append('  <div class="cat-header" onclick="toggleCat(\'__unc__\')">')
            html_parts.append('    <span class="cat-arrow">▾</span><span>No Category</span>')
            html_parts.append('  </div><div class="cat-channels">')
            for ch in unc_with_msgs:
                html_parts.append(_sidebar_ch_link(ch))
            html_parts.append('  </div></div>')

        for cat_id in sorted_cat_ids:
            cat_chs_with_msgs = [ch for ch in categories[cat_id] if ch["id"] in msgs_ch_ids]
            if not cat_chs_with_msgs:
                continue
            safe_cat = cat_id.replace("'", "\\'")
            html_parts.append(f'<div class="cat-section" data-cat-id="{cat_id}">')
            html_parts.append(f'  <div class="cat-header" onclick="toggleCat(\'{safe_cat}\')">')
            html_parts.append(f'    <span class="cat-arrow">▾</span><span>{escape(cat_names.get(cat_id, ""))}</span>')
            html_parts.append('  </div><div class="cat-channels">')
            for ch in cat_chs_with_msgs:
                html_parts.append(_sidebar_ch_link(ch))
            html_parts.append('  </div></div>')

    html_parts.append(
        '  </nav>\n</aside>\n'
        '<div id="search-overlay" style="display:none" onclick="if(event.target===this)closeSearch()">'
        '<div id="search-modal">'
        '<input id="search-input" type="text" placeholder="Search all channels…" autocomplete="off">'
        '<div id="search-results"><div class="sr-empty">Type to search messages…</div></div>'
        '</div></div>'
    )

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

    # Messages — static shell (content rendered by JS virtual list)
    html_parts.append("""
<section id="messages" class="section">
  <h2 class="section-title">💬 Messages</h2>
  <p class="section-subtitle">Message history from accessible text channels</p>
  <div id="msg-channel-header" class="channel-msg-header" style="display:none">
    <span class="ch-hash">#</span>
    <h3 id="msg-channel-name"></h3>
    <span id="msg-channel-count" class="count"></span>
    <input type="date" id="jump-date-input" title="Jump to date">
  </div>
  <div id="vl-viewport"><div id="vl-inner"></div></div>
  <div id="vl-empty">Select a channel from the sidebar</div>
</section>
""")

    # Build VDATA — users dict (deduplicated), channels list, slim messages dict
    vd_users = {}
    for _msgs_list in messages.values():
        for _msg in _msgs_list:
            _au = _msg.get("author") or {}
            _uid = _au.get("id", "")
            if _uid and _uid not in vd_users:
                vd_users[_uid] = {
                    "n": _au.get("global_name") or _au.get("username", "?"),
                    "av": _au.get("avatar") or ""
                }

    sorted_cat_ids_vd = sorted(
        categories.keys(),
        key=lambda cid: next((c["position"] for c in channels if c["id"] == cid), 0)
    )
    vd_channels = []
    for _ch in uncategorized:
        if _ch["id"] in messages and messages[_ch["id"]]:
            vd_channels.append({"id": _ch["id"], "name": _ch["name"], "cat": "", "catId": ""})
    for _cid in sorted_cat_ids_vd:
        for _ch in categories[_cid]:
            if _ch["id"] in messages and messages[_ch["id"]]:
                vd_channels.append({"id": _ch["id"], "name": _ch["name"],
                                    "cat": cat_names.get(_cid, ""), "catId": _cid})

    vd_messages = {}
    for ch_id, msgs_list in messages.items():
        if not msgs_list:
            continue
        msgs_sorted = sorted(msgs_list, key=lambda m: m.get("timestamp", ""))
        entries = []
        for _msg in msgs_sorted:
            _au = _msg.get("author") or {}
            _entry = {
                "id": _msg.get("id", ""),
                "uid": _au.get("id", ""),
                "ts": format_timestamp(_msg.get("timestamp")),
                "c": _msg.get("content", ""),
            }
            _atts = _msg.get("attachments")
            if _atts:
                _entry["a"] = [{"url": a.get("url", ""), "fn": a.get("filename", "file"),
                                 "sz": a.get("size", 0)} for a in _atts]
            _embs = _msg.get("embeds")
            if _embs:
                _el = [{"t": e.get("title", ""), "d": e.get("description", ""),
                        "col": e.get("color", 0)}
                       for e in _embs if e.get("title") or e.get("description")]
                if _el:
                    _entry["e"] = _el
            _tr = _msg.get("_translation")
            if _tr:
                _entry["tr"] = _tr
                _entry["tl"] = _msg.get("_detected_lang", "")
            entries.append(_entry)
        vd_messages[ch_id] = entries

    _vdata_json = json.dumps(
        {"users": vd_users, "channels": vd_channels, "messages": vd_messages},
        ensure_ascii=False, separators=(',', ':')
    )
    html_parts.append(f'<script>const VDATA={_vdata_json};</script>')

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
// ── Utilities ────────────────────────────────────────────────────────────────
function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function fmtSize(b) {
  return b < 1048576 ? Math.round(b/1024) + ' KB' : (b/1048576).toFixed(1) + ' MB';
}
const LANG_NAMES = {
  af:'Afrikaans',sq:'Albanian',ar:'Arabic',hy:'Armenian',az:'Azerbaijani',eu:'Basque',
  be:'Belarusian',bn:'Bengali',bs:'Bosnian',bg:'Bulgarian',ca:'Catalan',zh:'Chinese',
  hr:'Croatian',cs:'Czech',da:'Danish',nl:'Dutch',et:'Estonian',fi:'Finnish',fr:'French',
  gl:'Galician',ka:'Georgian',de:'German',el:'Greek',gu:'Gujarati',ht:'Haitian Creole',
  he:'Hebrew',hi:'Hindi',hu:'Hungarian',is:'Icelandic',id:'Indonesian',ga:'Irish',
  it:'Italian',ja:'Japanese',kn:'Kannada',kk:'Kazakh',ko:'Korean',ky:'Kyrgyz',lo:'Lao',
  la:'Latin',lv:'Latvian',lt:'Lithuanian',mk:'Macedonian',ms:'Malay',ml:'Malayalam',
  mt:'Maltese',mn:'Mongolian',ne:'Nepali',no:'Norwegian',fa:'Persian',pl:'Polish',
  pt:'Portuguese',pa:'Punjabi',ro:'Romanian',ru:'Russian',sr:'Serbian',sk:'Slovak',
  sl:'Slovenian',es:'Spanish',sw:'Swahili',sv:'Swedish',ta:'Tamil',te:'Telugu',
  th:'Thai',tr:'Turkish',uk:'Ukrainian',ur:'Urdu',uz:'Uzbek',vi:'Vietnamese',
  cy:'Welsh',yi:'Yiddish'
};
function langName(code) {
  if (!code) return 'Unknown';
  return LANG_NAMES[code] || LANG_NAMES[code.split('-')[0]] || code.toUpperCase();
}

// ── Virtual list state ───────────────────────────────────────────────────────
let activeCh = null;
const vlHeights = {};
const vlOffsets = {};
const rendered  = new Map(); // idx -> DOM element
const OVERSCAN  = 6;
const BASE_H    = 58;
const LINE_H    = 19;
const CPL       = 72;   // chars per line estimate
const ATT_H     = 24;
const EMB_H     = 46;
const TR_H      = 22;

const vlViewport = document.getElementById('vl-viewport');
const vlInner    = document.getElementById('vl-inner');
const vlEmpty    = document.getElementById('vl-empty');

function estimateH(msg) {
  let h = BASE_H;
  const c = msg.c || '';
  if (c) h += Math.max(0, Math.ceil(c.length / CPL) - 1) * LINE_H;
  if (msg.a && msg.a.length) h += ATT_H * Math.ceil(msg.a.length / 3);
  if (msg.e && msg.e.length) {
    for (const e of msg.e) h += EMB_H + Math.ceil(((e.t||'').length + (e.d||'').length) / CPL) * LINE_H;
  }
  if (msg.tr) h += TR_H + Math.ceil(msg.tr.length / CPL) * LINE_H;
  return h;
}

function buildOffsets(chId) {
  const msgs = VDATA.messages[chId];
  if (!msgs) return;
  const n = msgs.length;
  const h = new Float32Array(n);
  const o = new Float32Array(n + 1);
  for (let i = 0; i < n; i++) { h[i] = estimateH(msgs[i]); o[i+1] = o[i] + h[i]; }
  vlHeights[chId] = h;
  vlOffsets[chId] = o;
}

function bsFirst(offsets, scrollTop) {
  let lo = 0, hi = offsets.length - 2;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (offsets[mid + 1] <= scrollTop) lo = mid + 1; else hi = mid;
  }
  return lo;
}
function bsLast(offsets, scrollBottom) {
  let lo = 0, hi = offsets.length - 2;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (offsets[mid] < scrollBottom) lo = mid; else hi = mid - 1;
  }
  return lo;
}

function renderMsgHtml(msg) {
  const u = VDATA.users[msg.uid] || {n:'Unknown', av:''};
  const name = escapeHtml(u.n);
  const av = u.av
    ? '<img src="https://cdn.discordapp.com/avatars/' + escapeHtml(msg.uid) + '/' + escapeHtml(u.av) + '.png?size=64" alt="">'
    : escapeHtml((u.n||'?')[0].toUpperCase());
  let h = '<div class="msg-avatar">' + av + '</div><div class="msg-body">';
  h += '<div class="msg-header"><span class="msg-author">' + name + '</span>'
     + '<span class="msg-timestamp">' + escapeHtml(msg.ts) + '</span></div>';
  if (msg.c) h += '<div class="msg-content">' + escapeHtml(msg.c) + '</div>';
  if (msg.tr) h += '<div class="msg-translation">(Translated from ' + escapeHtml(langName(msg.tl)) + ': ' + escapeHtml(msg.tr) + ')</div>';
  if (msg.a && msg.a.length) {
    h += '<div class="msg-attachments">';
    for (const a of msg.a) h += '<a class="msg-attachment" href="' + escapeHtml(a.url) + '" target="_blank">📎 ' + escapeHtml(a.fn) + ' (' + fmtSize(a.sz) + ')</a>';
    h += '</div>';
  }
  if (msg.e && msg.e.length) {
    for (const e of msg.e) {
      const col = e.col ? ' style="border-left-color:#' + (e.col >>> 0).toString(16).padStart(6,'0') + '"' : '';
      h += '<div class="msg-embed"' + col + '>';
      if (e.t) h += '<div class="embed-title">' + escapeHtml(e.t) + '</div>';
      if (e.d) h += '<div class="embed-desc">' + escapeHtml(e.d) + '</div>';
      h += '</div>';
    }
  }
  h += '</div>';
  return h;
}

function measureItem(idx, el) {
  if (!activeCh) return;
  const h = vlHeights[activeCh], o = vlOffsets[activeCh];
  if (!h || idx >= h.length) return;
  const actual = el.getBoundingClientRect().height;
  if (actual <= 1 || Math.abs(actual - h[idx]) <= 2) return;
  const firstVis = bsFirst(o, vlViewport.scrollTop);
  const topOff   = vlViewport.scrollTop - o[firstVis];
  h[idx] = actual;
  const n = h.length;
  for (let i = idx; i < n; i++) o[i+1] = o[i] + h[i];
  vlInner.style.height = o[n] + 'px';
  for (const [ri, rel] of rendered) { if (ri >= idx) rel.style.top = o[ri] + 'px'; }
  if (idx < firstVis) vlViewport.scrollTop = o[firstVis] + topOff;
}

let rafPending = false;
function scheduleRender() {
  if (!rafPending) { rafPending = true; requestAnimationFrame(() => { rafPending = false; renderWindow(); }); }
}

function renderWindow() {
  if (!activeCh) return;
  const msgs = VDATA.messages[activeCh];
  if (!msgs || !msgs.length) return;
  const o = vlOffsets[activeCh];
  const scrollTop = vlViewport.scrollTop;
  const viewH    = vlViewport.clientHeight;
  const first = Math.max(0, bsFirst(o, scrollTop) - OVERSCAN);
  const last  = Math.min(msgs.length - 1, bsLast(o, scrollTop + viewH) + OVERSCAN);

  for (const [idx, el] of rendered) {
    if (idx < first || idx > last) { el.remove(); rendered.delete(idx); }
  }
  for (let i = first; i <= last; i++) {
    if (rendered.has(i)) continue;
    const el = document.createElement('div');
    el.className = 'message vl-item';
    el.style.top = o[i] + 'px';
    el.innerHTML = renderMsgHtml(msgs[i]);
    vlInner.appendChild(el);
    rendered.set(i, el);
    const idx = i;
    requestAnimationFrame(() => measureItem(idx, el));
  }
}

vlViewport.addEventListener('scroll', scheduleRender);

// ── Channel switching ────────────────────────────────────────────────────────
function switchChannel(chId) {
  if (!chId || !VDATA.messages[chId]) return;
  document.querySelectorAll('.ch-link').forEach(a => a.classList.remove('active'));
  const link = document.querySelector('.ch-link[data-ch-id="' + chId + '"]');
  if (link) link.classList.add('active');
  for (const [, el] of rendered) el.remove();
  rendered.clear();
  activeCh = chId;
  if (!vlOffsets[chId]) buildOffsets(chId);
  const msgs = VDATA.messages[chId];
  const chInfo = VDATA.channels.find(c => c.id === chId);
  document.getElementById('msg-channel-name').textContent = chInfo ? chInfo.name : chId;
  document.getElementById('msg-channel-count').textContent = msgs.length.toLocaleString() + ' messages';
  document.getElementById('msg-channel-header').style.display = '';
  vlEmpty.style.display = 'none';
  vlViewport.style.display = '';
  vlInner.style.height = vlOffsets[chId][msgs.length] + 'px';
  vlViewport.scrollTop = 0;
  renderWindow();
  document.getElementById('messages').scrollIntoView({behavior:'smooth', block:'start'});
}

function jumpToIndex(chId, idx) {
  if (chId !== activeCh) switchChannel(chId);
  const o = vlOffsets[chId];
  if (o) { vlViewport.scrollTop = o[idx]; renderWindow(); }
}

// ── Jump to date ─────────────────────────────────────────────────────────────
function jumpToDate(dateStr) {
  if (!activeCh || !dateStr) return;
  const msgs = VDATA.messages[activeCh];
  if (!msgs || !msgs.length) return;
  let lo = 0, hi = msgs.length - 1, found = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (msgs[mid].ts.slice(0, 10) < dateStr) lo = mid + 1;
    else { found = mid; hi = mid - 1; }
  }
  jumpToIndex(activeCh, found);
}
document.getElementById('jump-date-input').addEventListener('change', e => jumpToDate(e.target.value));

// ── Collapsible categories ───────────────────────────────────────────────────
function toggleCat(catId) {
  const el = document.querySelector('.cat-section[data-cat-id="' + catId + '"]');
  if (!el) return;
  el.classList.toggle('collapsed');
  try {
    const s = JSON.parse(localStorage.getItem('vc_cats') || '{}');
    s[catId] = el.classList.contains('collapsed');
    localStorage.setItem('vc_cats', JSON.stringify(s));
  } catch(e) {}
}

// ── Global search ────────────────────────────────────────────────────────────
let searchTimer = null;
function openSearch() {
  document.getElementById('search-overlay').style.display = '';
  setTimeout(() => document.getElementById('search-input').focus(), 50);
}
function closeSearch() { document.getElementById('search-overlay').style.display = 'none'; }
document.getElementById('search-input').addEventListener('input', e => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => runSearch(e.target.value), 280);
});
function runSearch(q) {
  const res = document.getElementById('search-results');
  q = q.trim().toLowerCase();
  if (q.length < 2) { res.innerHTML = '<div class="sr-empty">Type at least 2 characters…</div>'; return; }
  let html = '', count = 0;
  const MAX = 200;
  outer: for (const ch of VDATA.channels) {
    const msgs = VDATA.messages[ch.id];
    if (!msgs) continue;
    for (let i = 0; i < msgs.length; i++) {
      const msg = msgs[i], u = VDATA.users[msg.uid] || {};
      if (!(msg.c||'').toLowerCase().includes(q) && !(u.n||'').toLowerCase().includes(q)) continue;
      count++;
      html += '<div class="search-result" onclick="closeSearch();jumpToIndex(\'' + ch.id + '\',' + i + ')">'
            + '<div class="sr-channel">#' + escapeHtml(ch.name) + '</div>'
            + '<div><span class="sr-author">' + escapeHtml(u.n||'Unknown') + '</span>'
            + '<span class="sr-ts">' + escapeHtml(msg.ts) + '</span></div>'
            + '<div class="sr-content">' + escapeHtml((msg.c||'').slice(0, 140)) + '</div></div>';
      if (count >= MAX) break outer;
    }
  }
  if (!html) html = '<div class="sr-empty">No results found</div>';
  else if (count >= MAX) html = '<div class="sr-empty">Showing first ' + MAX + ' results</div>' + html;
  res.innerHTML = html;
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeSearch(); });

// ── Members filter (pre-rendered) ────────────────────────────────────────────
function filterMembers(q) {
  q = q.toLowerCase();
  document.querySelectorAll('.member-card').forEach(el => {
    el.style.display = el.dataset.name.includes(q) ? '' : 'none';
  });
}

// ── Smooth scroll for section nav links ──────────────────────────────────────
document.querySelectorAll('.nav-link[href^="#"]').forEach(a => {
  a.addEventListener('click', e => {
    e.preventDefault();
    const t = document.querySelector(a.getAttribute('href'));
    if (t) t.scrollIntoView({behavior:'smooth', block:'start'});
  });
});

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  try {
    const s = JSON.parse(localStorage.getItem('vc_cats') || '{}');
    for (const [catId, collapsed] of Object.entries(s)) {
      if (collapsed) {
        const el = document.querySelector('.cat-section[data-cat-id="' + catId + '"]');
        if (el) el.classList.add('collapsed');
      }
    }
  } catch(e) {}
  const first = VDATA.channels.find(c => VDATA.messages[c.id] && VDATA.messages[c.id].length);
  if (first) switchChannel(first.id);
});
</script>
</body>
</html>
""")

    return html_parts  # Return list of parts, caller joins or writes directly



# ─── Main ────────────────────────────────────────────────────────────────────

def print_banner():
    P = "\033[38;5;141m"
    C = "\033[38;5;80m"
    R = "\033[0m"
    B = "\033[1m"
    D = "\033[2m"
    print()
    print(f"  {P}╭─────────────────────────────────────────────────╮{R}")
    print(f"  {P}│{R}                                                 {P}│{R}")
    print(f"  {P}│{R}   {B}{P}██╗   ██╗ █████╗ ██╗   ██╗██╗  ████████╗{R}      {P}│{R}")
    print(f"  {P}│{R}   {B}{P}██║   ██║██╔══██╗██║   ██║██║  ╚══██╔══╝{R}      {P}│{R}")
    print(f"  {P}│{R}   {B}\033[38;5;147m██║   ██║███████║██║   ██║██║     ██║{R}         {P}│{R}")
    print(f"  {P}│{R}   {B}\033[38;5;153m╚██╗ ██╔╝██╔══██║██║   ██║██║     ██║{R}         {P}│{R}")
    print(f"  {P}│{R}   {B}\033[38;5;159m ╚████╔╝ ██║  ██║╚██████╔╝███████╗██║{R}         {P}│{R}")
    print(f"  {P}│{R}   {B}\033[38;5;159m  ╚═══╝  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝{R}         {P}│{R}")
    print(f"  {P}│{R}                {C}── cord ──{R}                       {P}│{R}")
    print(f"  {P}│{R}                                                 {P}│{R}")
    print(f"  {P}│{R}   {D}Discord Archive Toolkit             v1.2{R}      {P}│{R}")
    print(f"  {P}╰─────────────────────────────────────────────────╯{R}")
    print()


def print_menu():
    P = "\033[38;5;141m"
    R = "\033[0m"
    print(f"  {P}┌─ Tools ──────────────────────────────────────────┐{R}")
    print(f"  {P}│{R}  {P}ARCHIVE{R}                                         {P}│{R}")
    print(f"  {P}│{R}   {P} 1{R}  📦 Full Server Archive → HTML              {P}│{R}")
    print(f"  {P}│{R}   {P} 2{R}  💬 Messages Only → JSON                    {P}│{R}")
    print(f"  {P}│{R}   {P} 3{R}  📊 Messages → CSV (spreadsheet)            {P}│{R}")
    print(f"  {P}│{R}  {P}SERVER DATA{R}                                     {P}│{R}")
    print(f"  {P}│{R}   {P} 4{R}  📜 Audit Log → JSON                        {P}│{R}")
    print(f"  {P}│{R}   {P} 5{R}  🔨 Ban List → JSON                         {P}│{R}")
    print(f"  {P}│{R}   {P} 6{R}  🔗 Active Invites → JSON                   {P}│{R}")
    print(f"  {P}│{R}   {P} 7{R}  📌 Pinned Messages → JSON                  {P}│{R}")
    print(f"  {P}│{R}   {P} 8{R}  🪝 Webhooks → JSON                         {P}│{R}")
    print(f"  {P}│{R}   {P} 9{R}  🧵 Threads + Messages → JSON               {P}│{R}")
    print(f"  {P}│{R}   {P}10{R}  🏷️ Stickers → JSON                         {P}│{R}")
    print(f"  {P}│{R}   {P}11{R}  📅 Scheduled Events → JSON                 {P}│{R}")
    print(f"  {P}│{R}   {P}12{R}  🔒 Channel Permissions → JSON              {P}│{R}")
    print(f"  {P}│{R}   {P}13{R}  🛡️ Auto-Mod Rules → JSON                   {P}│{R}")
    print(f"  {P}│{R}   {P}14{R}  🔌 Integrations (bots/apps) → JSON         {P}│{R}")
    print(f"  {P}│{R}   {P}15{R}  🏛️ Full Server Profile → JSON             {P}│{R}")
    print(f"  {P}│{R}   {P}16{R}  😂 Reactions (who reacted) → JSON          {P}│{R}")
    print(f"  {P}│{R}  {P}DOWNLOADS{R}                                        {P}│{R}")
    print(f"  {P}│{R}   {P}17{R}  😀 Download All Emojis                     {P}│{R}")
    print(f"  {P}│{R}   {P}18{R}  🖼️ Download Server Assets                  {P}│{R}")
    print(f"  {P}│{R}   {P}19{R}  🧑 Download User Avatars                   {P}│{R}")
    print(f"  {P}│{R}   {P}20{R}  📎 Download All Attachments                {P}│{R}")
    print(f"  {P}│{R}                                                  {P}│{R}")
    print(f"  {P}│{R}   {P} 0{R}  🔄 Run All (no downloads)                 {P}│{R}")
    print(f"  {P}│{R}                                                 {P}│{R}")
    print(f"  {P}└─────────────────────────────────────────────────┘{R}")


def main():
    print_banner()
    D = "\033[2m"
    R = "\033[0m"
    print(" Welcome to the VaultCord Archive Toolkit")
    print(" ")
    print(f"   {D}TERMS AND INFORMATION{R}")
    print(" ")
    print(f"   {D}§1 - NO USER TOKENS, PASSWORDS, OR PRIVATE CREDENTIALS ARE STORED, SOLD, OR TRANSMITTED TO ANY THIRD-PARTY SERVER OUTSIDE OF DISCORD'S OFFICIAL API.{R} ")
    print(f"   {D}§2 - VAULTCORD DOES NOT ENCOURAGE HARASSMENT, UNAUTHORIZED ACCESS, INVASION OF PRIVACY, DATA ABUSE, OR MALICIOUS ACTIVITY OF ANY KIND.{R} ")
    print(f"   {D}§3 - THE DEVELOPERS AND CONTRIBUTORS OF VAULTCORD SHALL NOT BE HELD LIABLE FOR ACCOUNT RESTRICTIONS, DATA LOSS, SERVICE LIMITATIONS, OR DAMAGES RESULTING FROM MISUSE OF THE SOFTWARE.{R}")
    print(f"   {D}§4 - USERS ACCEPT FULL RESPONSIBILITY FOR ANY CONTENT PROCESSED, INDEXED, ARCHIVED, OR ACCESSED THROUGH THE SOFTWARE.{R}")
    print(f"   {D}§5 - VAULTCORD DOES NOT GUARANTEE CONTINUOUS ACCESS, ACCURACY OF DATA, OR UNINTERRUPTED FUNCTIONALITY.{R}")
    print(" ")
    print("  ")
    print("  📋 By continuing to use this application you agree to the terms above. ")
    print("  Enter your Discord token...")
    token = input("\n  🔑 Token: ").strip()
    if not token:
        print("  ❌ No token provided.")
        input("  Press Enter to exit...")
        sys.exit(1)

    import itertools
    print()
    me = [None]
    done = threading.Event()

    def _validate():
        me[0] = api_request("/users/@me", token)
        done.set()

    t = threading.Thread(target=_validate, daemon=True)
    t.start()

    spinner = itertools.cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
    tick = 0
    while not done.is_set():
        print(f"\r  \033[38;5;141m{next(spinner)}\033[0m  Validating token...", end="", flush=True)
        time.sleep(0.1)
        tick += 1

    elapsed = tick * 0.1
    if elapsed < 1.2:
        remaining = 1.2 - elapsed
        while remaining > 0:
            print(f"\r  \033[38;5;141m{next(spinner)}\033[0m  Validating token...", end="", flush=True)
            time.sleep(0.1)
            remaining -= 0.1

    t.join()
    print("\r" + " " * 50 + "\r", end="")  # Clear spinner line

    if not me[0] or me[0] is FORBIDDEN:
        print("  ❌ Invalid token or cannot connect.")
        input("  Press Enter to exit...")
        sys.exit(1)
    print(f"  ✅ Authenticated as: {me[0].get('username')}#{me[0].get('discriminator', '0')}")

    guild_id = input("\n  🏠 Server (Guild) ID: ").strip()
    if not guild_id:
        print("  ❌ No guild ID provided.")
        input("  Press Enter to exit...")
        sys.exit(1)

    guild_info = api_request(f"/guilds/{guild_id}", token, {"with_counts": "true"})
    if not guild_info:
        print("  ❌ Cannot access that server.")
        input("  Press Enter to exit...")
        sys.exit(1)
    guild_name = guild_info.get("name", "Unknown")
    print(f"  ✅ Server: {guild_name}\n")

    while True:
        print_menu()
        choice = input("\n  Select tool (1-20, 0=all data, q=quit): ").strip().lower()

        if choice == "q":
            break

        t_start = time.time()
        run_all = (choice == "0")

        # Lazy caches so we don't re-fetch channels/messages/members across tools
        _ch_cache = [None]
        _msg_cache = [None]
        _mem_cache = [None]

        def get_channels():
            if _ch_cache[0] is None:
                _ch_cache[0] = _as_list(api_request(f"/guilds/{guild_id}/channels", token))
            return _ch_cache[0]

        def get_ch_lookup():
            return {ch["id"]: ch.get("name", ch["id"]) for ch in get_channels()}

        def get_messages():
            if _msg_cache[0] is not None:
                return _msg_cache[0]
            channels = get_channels()
            text_types = {0, 5, 15, 16}
            text_chs = [c for c in channels if c.get("type") in text_types]
            all_msgs = {}
            _lock = threading.Lock()
            _c = [0]

            def _f(ch):
                msgs = fetch_all_messages(ch["id"], token, MESSAGES_PER_CHANNEL)
                with _lock:
                    _c[0] += 1
                    if msgs:
                        print(f"   [{_c[0]}/{len(text_chs)}] #{ch.get('name','?')} — {len(msgs)} msgs ✅")
                return ch["id"], msgs

            print("\n  💬 Fetching messages...")
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHANNELS) as pool:
                futures = {pool.submit(_f, ch): ch for ch in text_chs}
                for future in as_completed(futures):
                    ch_id, msgs = future.result()
                    if msgs:
                        all_msgs[ch_id] = msgs
            elapsed = time.time() - t0
            total = sum(len(v) for v in all_msgs.values())
            rate = total / elapsed if elapsed > 0 else 0
            print(f"   ⚡ {total} messages in {elapsed:.1f}s ({rate:.0f} msg/s)")
            _msg_cache[0] = all_msgs
            return all_msgs

        def get_members():
            if _mem_cache[0] is not None:
                return _mem_cache[0]
            print("  👥 Fetching members...")
            members = []
            after = "0"
            for _ in range(50):
                batch = api_request(f"/guilds/{guild_id}/members", token, {"limit": 1000, "after": after})
                if not batch or len(batch) == 0:
                    break
                members.extend(batch)
                after = batch[-1]["user"]["id"]
                if len(batch) < 1000:
                    break
            print(f"   ✅ {len(members)} members")
            _mem_cache[0] = members
            return members

        try:
            # 1 — Full HTML archive
            if choice == "1" or run_all:
                global MESSAGES_PER_CHANNEL
                if choice == "1":
                    li = input(f"\n  📨 Messages per channel (default ALL, or enter a number): ").strip()
                    if li:
                        v = int(li)
                        MESSAGES_PER_CHANNEL = v if v > 0 else None
                    ti = input("  🌐 Translate foreign messages? (y/N): ").strip().lower()
                    enable_trans = ti in ("y", "yes")
                else:
                    enable_trans = False
                print("\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                data = collect_server_data(token, guild_id, enable_trans)
                _msg_cache[0] = data["messages"]
                _mem_cache[0] = data["members"]
                _ch_cache[0] = data["channels"]
                print("\n  🎨 Building Vaultcord archive...")
                html_parts = build_html(data)
                safe = "".join(c if c.isalnum() or c in "-_ " else "" for c in guild_name).strip()
                fn = f"vaultcord_{safe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                fp = os.path.join(OUTPUT_DIR, fn)
                with open(fp, "w", encoding="utf-8") as f:
                    f.writelines(html_parts)
                sz = os.path.getsize(fp) / 1024
                tot = sum(len(v) for v in data["messages"].values())
                print(f"   📄 {fn} ({sz:.0f} KB, {tot} messages)")

            # 2 — Messages → JSON
            if choice == "2" or run_all:
                if choice == "2":
                    li = input(f"\n  📨 Messages per channel (default ALL, or enter a number): ").strip()
                    if li:
                        v = int(li)
                        MESSAGES_PER_CHANNEL = v if v > 0 else None
                msgs = get_messages()
                named = {get_ch_lookup().get(k, k): v for k, v in msgs.items()}
                save_json(named, "messages", guild_name)

            # 3 — Messages → CSV
            if choice == "3" or run_all:
                if choice == "3" and _msg_cache[0] is None:
                    li = input(f"\n  📨 Messages per channel (default ALL, or enter a number): ").strip()
                    if li:
                        v = int(li)
                        MESSAGES_PER_CHANNEL = v if v > 0 else None
                msgs = get_messages()
                export_messages_csv(msgs, get_ch_lookup(), guild_name)

            # 4 — Audit log
            if choice == "4" or run_all:
                save_json(scrape_audit_log(token, guild_id), "audit_log", guild_name)

            # 5 — Bans
            if choice == "5" or run_all:
                save_json(scrape_bans(token, guild_id), "bans", guild_name)

            # 6 — Invites
            if choice == "6" or run_all:
                save_json(scrape_invites(token, guild_id), "invites", guild_name)

            # 7 — Pinned messages
            if choice == "7" or run_all:
                save_json(scrape_pins(token, get_channels()), "pins", guild_name)

            # 8 — Webhooks
            if choice == "8" or run_all:
                save_json(scrape_webhooks(token, guild_id), "webhooks", guild_name)

            # 9 — Threads
            if choice == "9" or run_all:
                save_json(scrape_threads(token, guild_id, get_channels()), "threads", guild_name)

            # 10 — Stickers
            if choice == "10" or run_all:
                save_json(scrape_stickers(token, guild_id), "stickers", guild_name)

            # 11 — Scheduled events
            if choice == "11" or run_all:
                save_json(scrape_scheduled_events(token, guild_id), "events", guild_name)

            # 12 — Channel permissions
            if choice == "12" or run_all:
                save_json(scrape_channels_permissions(token, get_channels()), "permissions", guild_name)

            # 13 — Auto-mod rules
            if choice == "13" or run_all:
                save_json(scrape_automod(token, guild_id), "automod", guild_name)

            # 14 — Integrations
            if choice == "14" or run_all:
                save_json(scrape_integrations(token, guild_id), "integrations", guild_name)

            # 15 — Full server profile
            if choice == "15" or run_all:
                save_json(scrape_guild_profile(token, guild_id), "profile", guild_name)

            # 16 — Reactions
            if choice == "16" or run_all:
                msgs = get_messages()
                save_json(scrape_reactions(token, msgs), "reactions", guild_name)

            # 17 — Download emojis (not in run_all)
            if choice == "17":
                scrape_emojis_download(token, guild_id)

            # 18 — Download server assets (not in run_all)
            if choice == "18":
                scrape_server_assets(token, guild_id, guild_info)

            # 19 — Download user avatars (not in run_all)
            if choice == "19":
                scrape_user_avatars(token, get_members(), guild_id)

            # 20 — Download attachments (not in run_all)
            if choice == "20":
                msgs = get_messages()
                scrape_attachments_download(token, msgs, guild_id)

        except Exception as e:
            print(f"\n  ❌ Error: {e}")

        elapsed = time.time() - t_start
        print(f"\n  ⏱ Completed in {elapsed:.1f}s\n")

        if run_all:
            break

    _pool.close_all()
    _translate_pool.close_all()
    print("\n  👋 Done. Files saved to current directory.\n")
    input("  Press Enter to exit...")


if __name__ == "__main__":
    main()
