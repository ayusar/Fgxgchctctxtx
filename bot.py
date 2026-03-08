"""
Telegram Channel Forwarder — Session String + Flask Stats
==========================================================
• Forwards every message from SOURCE → DEST (oldest first)
• Replaces all media thumbnails with a custom image
• Drops messages that contain blocked phrases
• 5-second delay between messages
• Exposes a secret Flask endpoint with live server + forward stats

Env vars required:
    API_ID, API_HASH, SESSION_STRING
    SOURCE_CHANNEL, DEST_CHANNEL
    SECRET_PATH        (e.g. "mysecret"  → GET /mysecret)
    FLASK_PORT         (default 8080)
    MESSAGE_DELAY      (default 5)
    THUMBNAIL_URL      (default: google share link)
    BLOCKED_PHRASES    (pipe-separated, default: "Join now|Sorry for that|…")
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
THUMBNAIL_URL  = os.environ.get(
    "THUMBNAIL_URL",
    "https://share.google/images/eRIk0kTqmVKMcCKYY",
)

RAW_BLOCKED = os.environ.get(
    "BLOCKED_PHRASES",
    "Join now|Sorry\nfor\nthat|Sorry for that",
)
BLOCKED_PHRASES = [p for p in RAW_BLOCKED.split("|") if p.strip()]

# ──────────────────────────────────────────────────────────────
#  Shared live state  (written by forwarder, read by Flask)
# ──────────────────────────────────────────────────────────────

state = {
    "fetched":    0,
    "sent":       0,
    "skipped":    0,
    "errors":     0,
    "total":      0,
    "status":     "idle",       # idle | running | done | error
    "started_at": None,
}
state_lock = threading.Lock()


def update_state(**kwargs):
    with state_lock:
        state.update(kwargs)


def read_state() -> dict:
    with state_lock:
        return dict(state)

# ──────────────────────────────────────────────────────────────
#  Flask app
# ──────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route(f"/{SECRET_PATH}")
def stats():
    s = read_state()

    # ── Server stats ──
    disk   = psutil.disk_usage("/")
    total_gb = round(disk.total / (1024 ** 3), 2)
    used_gb  = round(disk.used  / (1024 ** 3), 2)
    free_gb  = round(disk.free  / (1024 ** 3), 2)
    cpu_pct  = psutil.cpu_percent(interval=0.5)
    ram_pct  = psutil.virtual_memory().percent

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

    # ── Forward stats ──
    fetched  = s["fetched"]
    sent     = s["sent"]
    skipped  = s["skipped"]
    errors   = s["errors"]
    total    = s["total"]                          # real total, 0 until fetching done
    processed = sent + skipped + errors            # messages actually attempted
    pct      = round((sent / total) * 100, 1) if total > 0 else 0.0
    status   = s["status"].upper()

    fwd_banner = (
        "╔════❰ ғᴏʀᴡᴀʀᴅ sᴛᴀᴛᴜs  ❱═❍⊱❁۪۪\n"
        "║╭━━━━━━━━━━━━━━━➣\n"
        f"║┣⪼🕵 ғᴇᴄʜᴇᴅ Msɢ : {fetched}\n"
        "║┃\n"
        f"║┣⪼✅ sᴜᴄᴄᴇғᴜʟʟʏ Fᴡᴅ : {sent}\n"
        "║┃\n"
        f"║┣⪼📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs: {status}\n"
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
    """Run Flask in its own thread (non-blocking)."""
    print(f"🌐  Flask listening on port {FLASK_PORT}  →  /{SECRET_PATH}")
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)


# ──────────────────────────────────────────────────────────────
#  Telegram helpers
# ──────────────────────────────────────────────────────────────

def contains_blocked_phrase(text: str) -> bool:
    if not text:
        return False
    norm = re.sub(r"\s+", " ", text).strip().lower()
    for phrase in BLOCKED_PHRASES:
        p = re.sub(r"\s+", " ", phrase).strip().lower()
        if p and p in norm:
            return True
    return False


def download_thumbnail():
    try:
        resp = requests.get(THUMBNAIL_URL, timeout=15)
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        img.thumbnail((320, 320))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        print("✅  Thumbnail ready.")
        return buf.getvalue()
    except Exception as exc:
        print(f"⚠️   Thumbnail download failed: {exc}")
        return None


async def fetch_all_messages(client, channel):
    msgs      = []
    offset_id = 0

    print("📥  Fetching full history…")
    while True:
        history = await client(GetHistoryRequest(
            peer=channel, offset_id=offset_id, offset_date=None,
            add_offset=0, limit=100, max_id=0, min_id=0, hash=0,
        ))
        if not history.messages:
            break
        msgs.extend(history.messages)
        offset_id = history.messages[-1].id
        update_state(fetched=len(msgs))
        print(f"   …{len(msgs)} fetched")
        if len(history.messages) < 100:
            break

    msgs.reverse()
    update_state(fetched=len(msgs), total=len(msgs))
    print(f"✅  {len(msgs)} messages to process.\n")
    return msgs


async def send_one(client, msg, dest, thumb_bytes) -> bool:
    text = msg.message or ""

    if contains_blocked_phrase(text):
        print(f"   ⛔  Blocked — ID {msg.id}")
        return False

    if not msg.media:
        if text.strip():
            await client.send_message(dest, text)
            print(f"   ✉️   Text          — ID {msg.id}")
            return True
        return False

    if isinstance(msg.media, MessageMediaPhoto):
        data = await client.download_media(msg.media, bytes)
        await client.send_file(
            dest, file=io.BytesIO(data), caption=text or None,
            thumb=io.BytesIO(thumb_bytes) if thumb_bytes else None,
        )
        print(f"   🖼️   Photo         — ID {msg.id}")
        return True

    if isinstance(msg.media, MessageMediaDocument):
        doc      = msg.media.document
        data     = await client.download_media(msg.media, bytes)
        is_video = any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)
        is_audio = any(isinstance(a, DocumentAttributeAudio) for a in doc.attributes)
        await client.send_file(
            dest, file=io.BytesIO(data), caption=text or None,
            thumb=io.BytesIO(thumb_bytes) if thumb_bytes and (is_video or is_audio) else None,
            force_document=False, attributes=doc.attributes, mime_type=doc.mime_type,
        )
        label = "🎬 Video" if is_video else ("🎵 Audio" if is_audio else "📎 Doc  ")
        print(f"   {label}         — ID {msg.id}")
        return True

    try:
        await client.forward_messages(dest, msg)
        print(f"   ➡️   Forwarded     — ID {msg.id}")
        return True
    except Exception as exc:
        print(f"   ⚠️   Failed        — ID {msg.id}: {exc}")
        return False


# ──────────────────────────────────────────────────────────────
#  Main async loop
# ──────────────────────────────────────────────────────────────

async def main():
    print("=" * 58)
    print("  Telegram Channel Forwarder  •  session-string mode")
    print("=" * 58)
    print(f"  Source   : {SOURCE_CHANNEL}")
    print(f"  Dest     : {DEST_CHANNEL}")
    print(f"  Delay    : {MESSAGE_DELAY}s")
    print(f"  Stats at : /{SECRET_PATH}")
    print("=" * 58 + "\n")

    update_state(status="connecting", started_at=time.time())

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        print("❌  Session string invalid/expired. Run generate_session.py.")
        update_state(status="error")
        await client.disconnect()
        sys.exit(1)

    me = await client.get_me()
    print(f"👤  Logged in as: {me.first_name} (@{me.username})\n")

    # Populate Telethon entity cache so numeric IDs can be resolved
    print("🔄  Loading dialogs to resolve channel IDs…")
    await client.get_dialogs()
    print("✅  Dialogs loaded.\n")

    def _parse_channel(val: str):
        return int(val) if val.lstrip("-").isdigit() else val

    source = await client.get_entity(_parse_channel(SOURCE_CHANNEL))
    dest   = await client.get_entity(_parse_channel(DEST_CHANNEL))
    print(f"📡  {getattr(source,'title',SOURCE_CHANNEL)}  →  {getattr(dest,'title',DEST_CHANNEL)}\n")

    thumb_bytes = download_thumbnail()

    update_state(status="fetching")
    messages = await fetch_all_messages(client, source)

    total   = len(messages)
    sent    = skipped = errors = 0
    update_state(status="running", total=total)

    for i, msg in enumerate(messages, 1):
        print(f"[{i:>6}/{total}] ID {msg.id}", end=" — ")

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
            print(f"❌ {exc}")
            errors += 1

        update_state(sent=sent, skipped=skipped, errors=errors)
        await asyncio.sleep(MESSAGE_DELAY)

    update_state(status="done")
    print("\n" + "=" * 58)
    print(f"  ✅ Sent: {sent}  ⛔ Skipped: {skipped}  ❌ Errors: {errors}")
    print("=" * 58)
    await client.disconnect()


# ──────────────────────────────────────────────────────────────
#  Entry point — Flask in thread, Telethon in main
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main())
