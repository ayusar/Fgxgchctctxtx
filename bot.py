"""
Telegram Channel Forwarder — Final Version
===========================================
• Forwards every message SOURCE → DEST (oldest first)
• START_FROM_LINK  : resume from a specific message link
• Photo thumbnail  : sends photo to Saved Messages, downloads back, replaces thumb
• Video/Audio thumb: replaced with custom image
• Blocked phrases  : messages dropped silently
• 5s delay         : between every send
• Auto-reconnect   : if connection drops mid-fetch
• Flask /SECRET    : live server + forward stats in JSON
• Flask /          : health check (required by Koyeb)
"""

import asyncio
import io
import os
import re
import sys
import threading
import time

import psutil
import requests
from flask import Flask, jsonify
from PIL import Image
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)

# ──────────────────────────────────────────────────────────────
#  Config
# ──────────────────────────────────────────────────────────────

def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"❌  Missing required env var: {name}")
        sys.exit(1)
    return val


API_ID         = int(_require("API_ID"))
API_HASH       = _require("API_HASH")
SESSION_STRING = _require("SESSION_STRING")
SOURCE_CHANNEL = _require("SOURCE_CHANNEL")
DEST_CHANNEL   = _require("DEST_CHANNEL")
SECRET_PATH    = os.environ.get("SECRET_PATH", "secret").strip("/")

MESSAGE_DELAY  = int(os.environ.get("MESSAGE_DELAY", "5"))
FLASK_PORT     = int(os.environ.get("FLASK_PORT", "8080"))
THUMBNAIL_URL  = os.environ.get("THUMBNAIL_URL", "https://files.catbox.moe/eoa2a9.jpg")

# Optional: t.me/c/CHANNEL_ID/MESSAGE_ID  — start forwarding from this message
START_FROM_LINK = os.environ.get("START_FROM_LINK", "").strip()

RAW_BLOCKED = os.environ.get(
    "BLOCKED_PHRASES",
    "Join now|Sorry\nfor\nthat|Sorry for that",
)
BLOCKED_PHRASES = [p for p in RAW_BLOCKED.split("|") if p.strip()]

# ──────────────────────────────────────────────────────────────
#  Shared live state
# ──────────────────────────────────────────────────────────────

state = {
    "fetched":      0,
    "sent":         0,
    "skipped":      0,
    "errors":       0,
    "total":        0,
    "status":       "idle",
    "start_from":   START_FROM_LINK or "beginning",
    "started_at":   None,
}
state_lock = threading.Lock()

def update_state(**kwargs):
    with state_lock:
        state.update(kwargs)

def read_state() -> dict:
    with state_lock:
        return dict(state)

# ──────────────────────────────────────────────────────────────
#  Flask
# ──────────────────────────────────────────────────────────────

app = Flask(__name__)

@app.route("/")
def health():
    return "OK", 200

@app.route(f"/{SECRET_PATH}")
def stats():
    s = read_state()

    disk      = psutil.disk_usage("/")
    total_gb  = round(disk.total / (1024 ** 3), 2)
    used_gb   = round(disk.used  / (1024 ** 3), 2)
    free_gb   = round(disk.free  / (1024 ** 3), 2)
    cpu_pct   = psutil.cpu_percent(interval=0.5)
    ram_pct   = psutil.virtual_memory().percent

    server_banner = (
        "╔════❰ sᴇʀᴠᴇʀ sᴛᴀᴛs  ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        f"║┣⪼ ᴛᴏᴛᴀʟ ᴅɪsᴋ sᴘᴀᴄᴇ:  {total_gb} GB\n"
        f"║┣⪼ ᴜsᴇᴅ: {used_gb} GB\n"
        f"║┣⪼ ꜰʀᴇᴇ: {free_gb} GB\n"
        f"║┣⪼ ᴄᴘᴜ: {cpu_pct}%\n"
        f"║┣⪼ ʀᴀᴍ: {ram_pct}%\n"
        "║╰━━━━━━━━━━━━━━━➣\n"
        "╚══════════════════❍⊱❁۪۪"
    )

    fetched   = s["fetched"]
    sent      = s["sent"]
    skipped   = s["skipped"]
    errors    = s["errors"]
    total     = s["total"]
    processed = sent + skipped + errors
    pct       = round((sent / total) * 100, 1) if total > 0 else 0.0

    fwd_banner = (
        "╔════❰ ғᴏʀᴡᴀʀᴅ sᴛᴀᴛᴜs  ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        f"║┣⪼🕵 ғᴇᴄʜᴇᴅ Msɢ : {fetched}\n"
        "║┃\n"
        f"║┣⪼✅ sᴜᴄᴄᴇғᴜʟʟʏ Fᴡᴅ : {sent}\n"
        "║┃\n"
        f"║┣⪼📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs: {s['status'].upper()}\n"
        "║┃\n"
        f"║┣⪼𖨠 Pᴇʀᴄᴇɴᴛᴀɢᴇ: {pct}%\n"
        "║╰━━━━━━━━━━━━━━━➣ \n"
        "╚════❰ Current Status ❱══❍⊱❁۪۪"
    )

    return jsonify({
        "server_stats": {
            "banner":        server_banner,
            "disk_total_gb": total_gb,
            "disk_used_gb":  used_gb,
            "disk_free_gb":  free_gb,
            "cpu_percent":   cpu_pct,
            "ram_percent":   ram_pct,
        },
        "forward_stats": {
            "banner":                 fwd_banner,
            "start_from":             s["start_from"],
            "fetched_messages":       fetched,
            "total_messages":         total,
            "successfully_forwarded": sent,
            "skipped":                skipped,
            "errors":                 errors,
            "processed":              processed,
            "percentage":             pct,
            "current_status":         s["status"],
        },
    })

def run_flask():
    print(f"🌐  Flask listening on port {FLASK_PORT}  →  /{SECRET_PATH}")
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)

# ──────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────

def _parse_channel(val: str):
    """Return int for numeric IDs, str for @usernames."""
    return int(val) if val.lstrip("-").isdigit() else val


def parse_start_link(link: str) -> int | None:
    """
    Extract message ID from a Telegram link.
    Supports:
      https://t.me/c/1234567890/42
      https://t.me/somechannel/42
    Returns the message ID as int, or None if not parseable.
    """
    if not link:
        return None
    m = re.search(r"/(\d+)$", link.strip())
    return int(m.group(1)) if m else None


def contains_blocked_phrase(text: str) -> bool:
    if not text:
        return False
    norm = re.sub(r"\s+", " ", text).strip().lower()
    for phrase in BLOCKED_PHRASES:
        p = re.sub(r"\s+", " ", phrase).strip().lower()
        if p and p in norm:
            return True
    return False


def build_thumbnail() -> bytes | None:
    """
    Download THUMBNAIL_URL and return JPEG bytes.
    Falls back to a solid dark image if URL is missing or broken.
    """
    if THUMBNAIL_URL:
        try:
            resp = requests.get(THUMBNAIL_URL, timeout=15)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            img.thumbnail((320, 320))
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=85)
            print("✅  Custom thumbnail ready.")
            return buf.getvalue()
        except Exception as exc:
            print(f"⚠️   Custom thumbnail failed ({exc}) — using fallback.")

    # Fallback: dark gradient image
    img = Image.new("RGB", (320, 320), color=(15, 23, 42))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    print("✅  Fallback thumbnail generated.")
    return buf.getvalue()


async def ensure_connected(client: TelegramClient):
    """Reconnect if the client has dropped."""
    if not client.is_connected():
        print("   🔌  Reconnecting…")
        await client.connect()


# ──────────────────────────────────────────────────────────────
#  Fetch
# ──────────────────────────────────────────────────────────────

async def fetch_all_messages(client: TelegramClient, channel, start_msg_id: int | None):
    """
    Fetch all messages oldest-first.
    If start_msg_id is set, only messages with ID >= start_msg_id are kept.
    """
    msgs      = []
    offset_id = 0

    print("📥  Fetching full history…")
    while True:
        await ensure_connected(client)

        for attempt in range(6):
            try:
                history = await client(GetHistoryRequest(
                    peer=channel, offset_id=offset_id, offset_date=None,
                    add_offset=0, limit=100, max_id=0, min_id=0, hash=0,
                ))
                break
            except Exception as exc:
                wait = 5 * (attempt + 1)
                print(f"   ⚠️  Attempt {attempt+1}/6 failed: {exc} — retry in {wait}s")
                await asyncio.sleep(wait)
                await ensure_connected(client)
        else:
            print("❌  Fetch failed after 6 attempts. Aborting.")
            update_state(status="error")
            sys.exit(1)

        if not history.messages:
            break

        msgs.extend(history.messages)
        offset_id = history.messages[-1].id
        update_state(fetched=len(msgs))
        print(f"   …{len(msgs)} fetched")

        if len(history.messages) < 100:
            break

    # Oldest first
    msgs.reverse()

    # Slice from start_msg_id if given
    if start_msg_id:
        before = len(msgs)
        msgs = [m for m in msgs if m.id >= start_msg_id]
        print(f"   ✂️   Sliced to {len(msgs)} messages (from ID {start_msg_id}, skipped {before - len(msgs)})")

    update_state(fetched=len(msgs), total=len(msgs))
    print(f"✅  {len(msgs)} messages to process.\n")
    return msgs


# ──────────────────────────────────────────────────────────────
#  Send one message
# ──────────────────────────────────────────────────────────────

async def send_one(client: TelegramClient, msg, dest, thumb_bytes: bytes) -> bool:
    text = msg.message or ""

    if contains_blocked_phrase(text):
        print(f"   ⛔  Blocked phrase  — ID {msg.id}")
        return False

    await ensure_connected(client)

    # ── Plain text ──
    if not msg.media:
        if text.strip():
            await client.send_message(dest, text)
            print(f"   ✉️   Text            — ID {msg.id}")
            return True
        return False

    # ── Photo ──
    if isinstance(msg.media, MessageMediaPhoto):
        photo_bytes = await client.download_media(msg.media, bytes)
        await client.send_file(
            dest,
            file=io.BytesIO(photo_bytes),
            caption=text or None,
            thumb=io.BytesIO(thumb_bytes),
        )
        print(f"   🖼️   Photo           — ID {msg.id}")
        return True

    # ── Document / Video / Audio ──
    if isinstance(msg.media, MessageMediaDocument):
        doc      = msg.media.document
        data     = await client.download_media(msg.media, bytes)
        is_video = any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)
        is_audio = any(isinstance(a, DocumentAttributeAudio) for a in doc.attributes)

        await client.send_file(
            dest,
            file=io.BytesIO(data),
            caption=text or None,
            thumb=io.BytesIO(thumb_bytes) if (is_video or is_audio) else None,
            force_document=False,
            attributes=doc.attributes,
            mime_type=doc.mime_type,
        )
        label = "🎬 Video" if is_video else ("🎵 Audio" if is_audio else "📎 Doc  ")
        print(f"   {label}           — ID {msg.id}")
        return True

    # ── Fallback ──
    try:
        await client.forward_messages(dest, msg)
        print(f"   ➡️   Forwarded       — ID {msg.id}")
        return True
    except Exception as exc:
        print(f"   ⚠️   Failed          — ID {msg.id}: {exc}")
        return False


# ──────────────────────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("  Telegram Channel Forwarder  •  session-string mode")
    print("=" * 60)
    print(f"  Source     : {SOURCE_CHANNEL}")
    print(f"  Dest       : {DEST_CHANNEL}")
    print(f"  Delay      : {MESSAGE_DELAY}s")
    print(f"  Start from : {START_FROM_LINK or 'beginning'}")
    print(f"  Stats at   : /{SECRET_PATH}")
    print("=" * 60 + "\n")

    update_state(status="connecting", started_at=time.time())

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        print("❌  Session string invalid or expired.")
        update_state(status="error")
        sys.exit(1)

    me = await client.get_me()
    print(f"👤  Logged in as: {me.first_name} (@{me.username})\n")

    print("🔄  Loading dialogs…")
    await client.get_dialogs()
    print("✅  Dialogs loaded.\n")

    source = await client.get_entity(_parse_channel(SOURCE_CHANNEL))
    dest   = await client.get_entity(_parse_channel(DEST_CHANNEL))
    print(f"📡  {getattr(source, 'title', SOURCE_CHANNEL)}  →  {getattr(dest, 'title', DEST_CHANNEL)}\n")

    # Parse start link
    start_msg_id = parse_start_link(START_FROM_LINK)
    if start_msg_id:
        print(f"🔗  Will start from message ID {start_msg_id} ({START_FROM_LINK})\n")

    # Build thumbnail
    thumb_bytes = build_thumbnail()

    update_state(status="fetching")
    messages = await fetch_all_messages(client, source, start_msg_id)

    total  = len(messages)
    sent   = skipped = errors = 0
    update_state(status="running", total=total)

    for i, msg in enumerate(messages, 1):
        print(f"[{i:>6}/{total}] ID {msg.id}", end=" — ")

        # Skip service messages (pinned, joined, etc.)
        if msg.action is not None:
            print("service, skip")
            skipped += 1
            update_state(skipped=skipped)
            await asyncio.sleep(0.5)
            continue

        try:
            ok = await send_one(client, msg, dest, thumb_bytes)
            if ok:
                sent += 1
            else:
                skipped += 1
        except Exception as exc:
            print(f"❌  {exc}")
            errors += 1

        update_state(sent=sent, skipped=skipped, errors=errors)
        await asyncio.sleep(MESSAGE_DELAY)

    update_state(status="done")
    print("\n" + "=" * 60)
    print(f"  ✅ Sent: {sent}   ⛔ Skipped: {skipped}   ❌ Errors: {errors}")
    print("=" * 60)
    await client.disconnect()


# ──────────────────────────────────────────────────────────────
#  Entry point
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main())
