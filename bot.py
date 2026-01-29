# ===============================
# IMPORTS
# ===============================
import os
import time
import asyncio
import contextlib
from html import escape

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatPermissions,
)
from telegram.error import RetryAfter, Forbidden, BadRequest, ChatMigrated
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ChatMemberHandler,
)

from psycopg_pool import ConnectionPool  # ✅ ONLY THIS (Supabase safe)

# ===============================
# GLOBAL CACHES
# ===============================
STATS_CACHE = {
    "users": 0,
    "groups": 0,
    "admin_groups": 0,
    "last_update": 0
}
STATS_TTL = 300  # 5 minutes

BOT_ADMIN_CACHE: set[int] = set()
USER_ADMIN_CACHE: dict[int, set[int]] = {}
REMINDER_MESSAGES: dict[int, list[int]] = {}
PENDING_BROADCAST = {}
BOT_START_TIME = int(time.time())

LINK_SPAM_CACHE = {
    # (chat_id, user_id): {
    #   "count": int,
    #   "last_time": int
    # }
}
LINK_SPAM_CACHE_TTL = 7200  # 2 hours (recommend)

# ===============================
# CONFIG
# ===============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
START_IMAGE = "https://i.postimg.cc/q7PtfZYj/Untitled-design-(16).png"

DB_HOST = os.getenv("SUPABASE_HOST")
DB_NAME = os.getenv("SUPABASE_DB")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_PORT = int(os.getenv("SUPABASE_PORT", "6543"))

# =====================================
# DB POOL (RAILWAY SAFE)
# =====================================
pool = None

async def db_execute(query, params=None, fetch=False):
    loop = asyncio.get_running_loop()

    def _run():
        if pool is None:
            raise RuntimeError("DB pool not initialized")

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    cols = [d.name for d in cur.description]
                    return [dict(zip(cols, r)) for r in cur.fetchall()]
                conn.commit()

    return await loop.run_in_executor(None, _run)

# ===============================
# INIT DB
# ===============================
async def init_db():
    await db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY
        )
    """)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id BIGINT PRIMARY KEY,
            is_admin_cached BOOLEAN DEFAULT FALSE,
            last_checked_at BIGINT
        )
    """)

    await db_execute("""
        CREATE TABLE IF NOT EXISTS link_spam (
            chat_id BIGINT,
            user_id BIGINT,
            count INT,
            last_time BIGINT,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

# ===============================
# /start
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.message

    if not chat or not user or not msg:
        return

    bot = context.bot
    bot_username = bot.username or ""

    # ===============================
    # 🔒 PRIVATE CHAT (/start)
    # ===============================
    if chat.type == "private":

        # ---------------------------
        # ✅ Deep-link flows (no extra handlers needed)
        # t.me/<bot>?start=donate
        # t.me/<bot>?start=donate_bot
        # t.me/<bot>?start=donate_dev
        # t.me/<bot>?start=donate_ton
        # ---------------------------
        arg = (context.args[0] if getattr(context, "args", None) else "").strip().lower()

        # ===============================
        # 💖 DONATE MENU
        # ===============================
        if arg == "donate":
            donate_text = (
                "<b>💖 Support Us</b>\n\n"
                "မင်းအတွက် အလုပ်ကောင်းကောင်းလုပ်နေတဲ့ Bot ကို Support ပေးနိုင်ပါတယ်။\n\n"
                "👇 အောက်ကနေ ရွေးပါ"
            )
            donate_buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("⭐️ Support Bot (5 Stars)", url=f"https://t.me/{bot_username}?start=donate_bot")
                ],
                [
                    InlineKeyboardButton("💸 Support Developer", url=f"https://t.me/{bot_username}?start=donate_dev")
                ],
                [
                    InlineKeyboardButton("⬅️ Back", url=f"https://t.me/{bot_username}")
                ],
            ])
            await msg.reply_text(donate_text, parse_mode="HTML", reply_markup=donate_buttons)
            return

        # ===============================
        # ⭐️ Support Bot (Telegram Stars)
        # ===============================
        if arg == "donate_bot":
            # NOTE: Stars donate = Stars balance goes to "this bot" (bot owner can withdraw/claim via Telegram tools)
            # Local import to avoid changing global imports
            from telegram import LabeledPrice

            try:
                await context.bot.send_invoice(
                    chat_id=chat.id,
                    title="Support Bot",
                    description="Donate 5 Telegram Stars ⭐️",
                    payload=f"donate_bot_5_{user.id}",
                    currency="XTR",  # Telegram Stars currency
                    prices=[LabeledPrice("Support", 5)],  # 5 Stars
                    provider_token="",  # Stars usually use empty provider_token
                )
            except Exception as e:
                await msg.reply_text(f"❌ Donate မလုပ်နိုင်ပါ: {e}")
            return

        # ===============================
        # 🟦 TON Donate details
        # ===============================
        if arg in ("donate_dev", "donate_ton"):
            TON_ADDRESS = os.getenv("TON_ADDRESS", "PUT_YOUR_TON_ADDRESS_HERE")
            ton_text = (
                "<b>🟦 Support Developer (TON)</b>\n\n"
                f"<b>TON Address:</b>\n<code>{escape(TON_ADDRESS)}</code>\n\n"
                "Address ကို copy လုပ်ပြီး TON ပို့ပါ ✅"
            )
            ton_buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("⬅️ Back", url=f"https://t.me/{bot_username}?start=donate"),
                ],
            ])
            await msg.reply_text(ton_text, parse_mode="HTML", reply_markup=ton_buttons)
            return

        # save user
        context.application.create_task(
            db_execute(
                "INSERT INTO users VALUES (%s) ON CONFLICT DO NOTHING",
                (user.id,)
            )
        )

        user_name = escape(user.first_name or "User")
        bot_name = escape(bot.first_name or "Bot")

        user_mention = f"<a href='tg://user?id={user.id}'>{user_name}</a>"
        bot_mention = (
            f"<a href='https://t.me/{bot_username}'>{bot_name}</a>"
            if bot_username else bot_name
        )

        text = (
            f"<b>────「 {bot_mention} 」────</b>\n\n"
            f"<b>ဟယ်လို {user_mention} ! 👋</b>\n\n"
            "<b>ငါသည် Group များအတွက် Link ဖျက် Bot တစ်ခုဖြစ်တယ်။</b>\n"
            "<b>ငါ၏လုပ်နိုင်စွမ်းကို ကောင်းကောင်းအသုံးချပါ။</b>\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📌 ငါ၏လုပ်နိုင်စွမ်း</b>\n\n"
            "✅ Auto Link Delete ( Setting ချိန်းစရာမလိုပဲ ချက်ချင်း အလုပ်လုပ်။ )\n"
            "✅ Spam Link Mute ( Link 3 ခါ ပို့ရင် 10 မိနစ် Auto Mute ပေး။ )\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📥 ငါ့ကိုအသုံးပြုရန်</b>\n\n"
            "➕ ငါ့ကို Group ထဲထည့်ပါ\n"
            "⭐️ ငါ့ကို Admin ပေးပါ"
        )

        buttons = []

        if bot_username:
            buttons.append([
                InlineKeyboardButton(
                    "➕ ADD ME TO YOUR GROUP",
                    url=f"https://t.me/{bot_username}?startgroup=true"
                )
            ])

        # ✅ Donate Us button
        if bot_username:
            buttons.append([
                InlineKeyboardButton(
                    "💖 DONATE US",
                    url=f"https://t.me/{bot_username}?start=donate"
                )
            ])

        buttons.append([
            InlineKeyboardButton("👨‍💻 𝐃𝐞𝐯𝐞𝐥𝐨𝐩𝐞𝐫", url="tg://user?id=5942810488"),
            InlineKeyboardButton("📢 𝐂𝐡𝐚𝐧𝐧𝐞𝐥", url="https://t.me/MMTelegramBotss"),
        ])

        await msg.reply_photo(
            photo=START_IMAGE,
            caption=text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # ===============================
    # 👥 GROUP / SUPERGROUP (/start)
    # ===============================
    if chat.type in ("group", "supergroup"):

        # 🔐 Check bot status
        try:
            me = await bot.get_chat_member(chat.id, bot.id)
        except:
            return  # cannot access → silent

        # 🔕 No send permission → SILENT
        if me.status in ("member", "restricted"):
            if not getattr(me, "can_send_messages", True):
                return  # silent

        # ---------------------------
        # ✅ BOT IS ADMIN
        # ---------------------------
        if me.status in ("administrator", "creator"):
            await bot.send_message(
                chat.id,
                "✅ Bot ကို Admin အဖြစ်ခန့်ထားပြီးသားပါ။\n\n"
                "🔗 <b>Auto Link Delete</b>\n"
                "🔇 <b>Spam Link Mute</b>\n\n"
                "🤖 Bot က လက်ရှိ Group မှာ ကောင်းကောင်းအလုပ်လုပ်နေပါပြီး။",
                parse_mode="HTML"
            )
            return

        # ---------------------------
        # ❌ BOT IS NOT ADMIN
        # ---------------------------
        await bot.send_message(
            chat.id,
            "⚠️ <b>Bot သည် Admin မဟုတ်သေးပါ</b>\n\n"
            "🤖 <b>Bot ကို အလုပ်လုပ်စေရန်</b>\n"
            "⭐️ <b>Admin Permission ပေးပါ</b>\n\n"
            "Required: Delete messages",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⭐️ GIVE ADMIN PERMISSION",
                    url=f"https://t.me/{bot_username}?startgroup=true"
                )
            ]])
        )
        return
 
# ===============================
# /stats (OWNER ONLY - PRIVATE)
# ===============================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if (
        not chat
        or chat.type != "private"
        or not user
        or user.id != OWNER_ID
        or not msg
    ):
        return

    now = time.time()

    # 🔒 Cache valid → DB မထိ
    if now - STATS_CACHE["last_update"] > STATS_TTL:
        try:
            users = await db_execute(
                "SELECT COUNT(*) AS c FROM users",
                fetch=True
            )
            groups = await db_execute(
                "SELECT COUNT(*) AS c FROM groups",
                fetch=True
            )
            admin_groups = await db_execute(
                "SELECT COUNT(*) AS c FROM groups WHERE is_admin_cached = TRUE",
                fetch=True
            )

            STATS_CACHE["users"] = users[0]["c"] if users else 0
            STATS_CACHE["groups"] = groups[0]["c"] if groups else 0
            STATS_CACHE["admin_groups"] = admin_groups[0]["c"] if admin_groups else 0
            STATS_CACHE["last_update"] = now

        except Exception as e:
            print("❌ STATS DB ERROR:", e)

    no_admin = max(
        0,
        STATS_CACHE["groups"] - STATS_CACHE["admin_groups"]
    )

    uptime = int(time.time()) - BOT_START_TIME
    h, m = divmod(uptime // 60, 60)

    await msg.reply_text(
        "📊 <b>Bot Statistics</b>\n\n"
        f"👤 Users: <b>{STATS_CACHE['users']}</b>\n"
        f"👥 Groups: <b>{STATS_CACHE['groups']}</b>\n\n"
        f"🔐 Admin Groups: <b>{STATS_CACHE['admin_groups']}</b>\n"
        f"⚠️ No Admin Groups: <b>{no_admin}</b>\n\n"
        f"⏱️ Uptime: <b>{h}h {m}m</b>",
        parse_mode="HTML"
    )

# ===============================
# LINK + MUTE CONFIG
# ===============================
LINK_LIMIT = 3          # links before mute
MUTE_SECONDS = 600      # 10 minutes
SPAM_RESET_SECONDS = 3600  # 1 hour

# ===============================
# AUTO LINK DELETE (OPTION A CORE)
# ===============================
async def auto_delete_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user

    if not chat or not msg or not user:
        return
    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    user_id = user.id

    # ===============================
    # 🔥 STEP 1: LINK DETECT FIRST (NO API)
    # ===============================
    has_link = False
    entities = []

    if msg.entities:
        entities.extend(msg.entities)
    if msg.caption_entities:
        entities.extend(msg.caption_entities)

    for e in entities:
        if e.type in ("url", "text_link"):
            has_link = True
            break

    text = (msg.text or msg.caption or "").lower()
    if "http://" in text or "https://" in text or "t.me/" in text:
        has_link = True

    # 🚀 NO LINK → EXIT IMMEDIATELY (BIG SPEED BOOST)
    if not has_link:
        return

    # ===============================
    # 🔐 STEP 2: BOT ADMIN CHECK (ONLY IF LINK)
    # ===============================
    if chat_id not in BOT_ADMIN_CACHE:
        return

    # ===============================
    # 👮 STEP 3: ADMIN BYPASS
    # ===============================
    if await is_user_admin(chat_id, user_id, context):
        return

    # ===============================
    # 🗑️ STEP 4: DELETE MESSAGE
    # ===============================
    try:
        await msg.delete()
    except BadRequest as e:
        print("ℹ️ Delete skipped:", e)
        return
    except Exception as e:
        print("❌ Delete failed:", e)
        return

    # 🔢 STEP 5: COUNT + MUTE FIRST (IMPORTANT FIX)
    muted = await link_spam_control(chat_id, user_id, context)

    user_mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # ⚠️ ONLY WARN IF NOT MUTED
    if not muted:
        try:
            await context.bot.send_message(
                chat_id,
                f"⚠️ <b>{user_mention}</b> မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီး။\n"
                "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူး။",
                parse_mode="HTML"
            )
        except:
            pass
    else:
        # 🔇 MUTE MESSAGE ONLY
        try:
            await context.bot.send_message(
                chat_id,
                f"🔇 <b>{user_mention}</b>\n"
                f"🔗 Link {LINK_LIMIT} ကြိမ် ပို့လို့\n"
                f"⏰ 10 မိနစ် mute လုပ်လိုက်ပါပြီး",
                parse_mode="HTML"
            )
        except:
            pass

# ===============================
# LINK COUNT + MUTE (RAM-FIRST ⭐ 90+)
# ===============================
async def link_spam_control(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    now = int(time.time())
    key = (chat_id, user_id)

    # -------------------------------------------------
    # 🔥 STEP 1: RAM-FIRST CHECK (NO DB, NO API)
    # -------------------------------------------------
    data = LINK_SPAM_CACHE.get(key)

    if data:
        # ⛔ still muted window → nothing to do
        if now - data["last_time"] < MUTE_SECONDS:
            return False

        # 🔢 count logic
        if now - data["last_time"] > SPAM_RESET_SECONDS:
            data["count"] = 1
        else:
            data["count"] += 1

        data["last_time"] = now
    else:
        # -------------------------------------------------
        # 🐢 STEP 2: FALLBACK DB (FIRST TIME ONLY)
        # -------------------------------------------------
        try:
            rows = await asyncio.wait_for(
                db_execute(
                    """
                    SELECT count, last_time
                    FROM link_spam
                    WHERE chat_id=%s AND user_id=%s
                    """,
                    (chat_id, user_id),
                    fetch=True
                ),
                timeout=2
            )
        except:
            rows = None

        if rows:
            last_time = rows[0]["last_time"]

            if now - last_time < MUTE_SECONDS:
                return False

            count = (
                1
                if now - last_time > SPAM_RESET_SECONDS
                else rows[0]["count"] + 1
            )
        else:
            count = 1

        data = {
            "count": count,
            "last_time": now
        }
        LINK_SPAM_CACHE[key] = data

    # -------------------------------------------------
    # 🔢 STEP 3: LIMIT NOT REACHED
    # -------------------------------------------------
    if data["count"] < LINK_LIMIT:
        return False

    # -------------------------------------------------
    # 🔒 STEP 4: SUPERGROUP ONLY (NO API)
    # -------------------------------------------------
    if chat_id > 0:  # normal group → skip mute
        return False

    # -------------------------------------------------
    # 👮 STEP 5: BOT PERMISSION CHECK
    # -------------------------------------------------
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if not me.can_restrict_members:
            return False
    except:
        return False

    # -------------------------------------------------
    # 🔇 STEP 6: MUTE USER
    # -------------------------------------------------
    try:
        await context.bot.restrict_chat_member(
            chat_id,
            user_id,
            ChatPermissions(can_send_messages=False),
            until_date=now + MUTE_SECONDS
        )
    except:
        return False

    # -------------------------------------------------
    # 🧹 STEP 7: CLEANUP (RAM + DB ASYNC)
    # -------------------------------------------------
    LINK_SPAM_CACHE.pop(key, None)

    context.application.create_task(
        db_execute(
            "DELETE FROM link_spam WHERE chat_id=%s AND user_id=%s",
            (chat_id, user_id)
        )
    )

    return True

# ===============================
# RAM CACHE CLEANUP JOB (FIXED)
# ===============================
async def cleanup_link_spam_cache(context: ContextTypes.DEFAULT_TYPE):
    now = int(time.time())
    removed = 0

    for key, data in list(LINK_SPAM_CACHE.items()):
        if now - data["last_time"] > LINK_SPAM_CACHE_TTL:
            LINK_SPAM_CACHE.pop(key, None)
            removed += 1

    if removed:
        print(f"🧹 RAM cache cleaned: {removed} entries")

# ===============================
# 📢 BROADCAST (OWNER ONLY)
# ===============================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.effective_message
    if not msg:
        return

    text = msg.text or msg.caption
    if text and text.startswith("/broadcast"):
        text = text.replace("/broadcast", "", 1).strip()

    content = {
        "text": text,
        "photo": msg.photo[-1].file_id if msg.photo else None,
        "video": msg.video.file_id if msg.video else None,
        "audio": msg.audio.file_id if msg.audio else None,
        "document": msg.document.file_id if msg.document else None,
    }

    if not any(v for v in content.values() if v):
        await msg.reply_text("❌ Broadcast လုပ်ရန် content မတွေ့ပါ")
        return

    PENDING_BROADCAST[OWNER_ID] = content

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ CONFIRM", callback_data="broadcast_confirm"),
        InlineKeyboardButton("❌ CANCEL", callback_data="broadcast_cancel")
    ]])

    await msg.reply_text(
        "📢 <b>Broadcast Confirm လုပ်ပါ</b>",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# ===============================
# Broadcast Confirm → Choose Target
# ===============================
async def broadcast_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if OWNER_ID not in PENDING_BROADCAST:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Users only", callback_data="bc_target_users")],
        [InlineKeyboardButton("👥 Groups only", callback_data="bc_target_groups")],
        [InlineKeyboardButton("👥👤 Users + Groups", callback_data="bc_target_all")],
        [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
    ])

    await query.edit_message_text(
        "📢 <b>Broadcast Target ကိုရွေးပါ</b>",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# ===============================
# Progress Bar Helper 
# ===============================
def render_progress(done, total):
    if total <= 0:
        return "██████████ 100%"
    percent = int((done / total) * 100)
    blocks = min(10, percent // 10)
    bar = "█" * blocks + "░" * (10 - blocks)
    return f"{bar} {percent}%"

# ===============================
# update progress 
# ===============================
async def update_progress(msg, sent, total):
    if total <= 0:
        percent = 100
    else:
        percent = int((sent / total) * 100)

    bar_blocks = min(10, percent // 10)
    bar = "█" * bar_blocks + "░" * (10 - bar_blocks)

    try:
        await msg.edit_text(
            "📢 <b>Broadcasting...</b>\n\n"
            f"⏳ Progress: {bar} {percent}%",
            parse_mode="HTML"
        )
    except:
        pass

# ===============================
# Broadcast flood-safe 
# ===============================
async def safe_send(func, *args, **kwargs):
    for _ in range(5):
        try:
            return await func(*args, **kwargs)

        except ChatMigrated as e:
            # args = (context, chat_id, data) ဆိုတဲ့ pattern ဖြစ်နေလို့
            try:
                context = args[0]
                old_chat_id = args[1]
                new_chat_id = e.new_chat_id

                # DB update (groups table)
                context.application.create_task(
                    db_execute(
                        "UPDATE groups SET group_id=%s WHERE group_id=%s",
                        (new_chat_id, old_chat_id)
                    )
                )

                # retry with new chat id
                new_args = (args[0], new_chat_id, *args[2:])
                args = new_args
                continue
            except Exception:
                return None

        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)

        except (Forbidden, BadRequest):
            return None

    return None

# ===============================
# BATCH DB READ (10k+ SAFE)
# ===============================
async def iter_db_ids(query, batch_size=500):
    offset = 0
    while True:
        rows = await db_execute(
            f"{query} LIMIT %s OFFSET %s",
            (batch_size, offset),
            fetch=True
        )
        if not rows:
            break
        yield rows
        offset += batch_size

# ===============================
# Broadcast Target Handler
# ===============================
async def broadcast_target_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = PENDING_BROADCAST.pop(OWNER_ID, None)
    if not data:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    target_type = query.data  # bc_target_users / bc_target_groups / bc_target_all

    progress_msg = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )
    
    sent = 0
    start_time = time.time()
    
    total = 0

    if target_type in ("bc_target_users", "bc_target_all"):
        rows = await db_execute("SELECT COUNT(*) AS c FROM users", fetch=True)
        total += rows[0]["c"] if rows else 0

    if target_type in ("bc_target_groups", "bc_target_all"):
        rows = await db_execute(
            "SELECT COUNT(*) AS c FROM groups WHERE is_admin_cached = TRUE",
            fetch=True
        )
        total += rows[0]["c"] if rows else 0

    async def send_batch(ids):
        nonlocal sent
        for cid in ids:
            await safe_send(send_content, context, cid, data)
            sent += 1

            # 🔄 update every 50 messages (SAFE)
            if sent % 50 == 0 or sent == total:
                await update_progress(progress_msg, sent, total)

    # 👤 USERS
    if target_type in ("bc_target_users", "bc_target_all"):
        async for rows in iter_db_ids(
            "SELECT user_id FROM users ORDER BY user_id"
        ):
            await send_batch([r["user_id"] for r in rows])

    # 👥 GROUPS (ADMIN ONLY)
    if target_type in ("bc_target_groups", "bc_target_all"):
        async for rows in iter_db_ids(
            "SELECT group_id FROM groups WHERE is_admin_cached = TRUE ORDER BY group_id"
        ):
            await send_batch([r["group_id"] for r in rows])

    elapsed = int(time.time() - start_time)

    await progress_msg.edit_text(
        "✅ <b>Broadcast Completed</b>\n\n"
        f"📨 Sent: <b>{sent}</b>\n"
        f"⏱️ Time: <b>{elapsed // 60}m {elapsed % 60}s</b>",
        parse_mode="HTML"
    )

# ===============================
# Cancel Button 
# ===============================
async def broadcast_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    PENDING_BROADCAST.pop(OWNER_ID, None)

    await query.edit_message_text("❌ Broadcast Cancel လုပ်လိုက်ပါပြီ")

# ===============================
# Media / Text 
# ===============================
async def send_content(context, chat_id, data):
    text = data.get("text") or ""

    try:
        if data.get("photo"):
            await context.bot.send_photo(
                chat_id,
                data["photo"],
                caption=text,
                parse_mode="HTML"
            )

        elif data.get("video"):
            await context.bot.send_video(
                chat_id,
                data["video"],
                caption=text,
                parse_mode="HTML"
            )

        elif data.get("audio"):
            await context.bot.send_audio(
                chat_id,
                data["audio"],
                caption=text,
                parse_mode="HTML"
            )

        elif data.get("document"):
            await context.bot.send_document(
                chat_id,
                data["document"],
                caption=text,
                parse_mode="HTML"
            )

        else:
            await context.bot.send_message(
                chat_id,
                text,
                parse_mode="HTML"
            )

    except Exception:
        # let caller (safe_send) + broadcast logic handle cleanup
        raise

# ===============================
# Auto leave job (FIXED)
# ===============================
async def leave_if_not_admin(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return

    chat_id = context.job.data.get("chat_id")
    if not chat_id:
        return

    # 🔎 ALWAYS verify with Telegram (cache is NOT source of truth)
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat_id)
            return
    except:
        # cannot access chat → treat as removed
        pass

    # ❌ bot is NOT admin → cleanup
    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)
    REMINDER_MESSAGES.pop(chat_id, None)

    # 🧹 Supabase cleanup (background, non-blocking)
    context.application.create_task(
        db_execute(
            """
            UPDATE groups
            SET is_admin_cached = FALSE,
                last_checked_at = %s
            WHERE group_id = %s
            """,
            (int(time.time()), chat_id)
        )
    )
    context.application.create_task(
        db_execute("DELETE FROM link_spam WHERE chat_id=%s", (chat_id,))
    )

    # 🚪 Leave group
    try:
        await context.bot.leave_chat(chat_id)
    except Exception as e:
        print(f"⚠️ Leave chat failed ({chat_id}):", e)

# ===============================
# Helper: Clear all reminder jobs (SAFE)
# ===============================
def clear_reminders(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    job_queue = context.job_queue

    # ❗ JobQueue မရှိရင် ဘာမှမလုပ်
    if job_queue is None:
        return

    for job in list(job_queue.jobs()):
        data = job.data or {}

        # only jobs for this chat
        if data.get("chat_id") != chat_id:
            continue

        name = job.name or ""

        if (
            name.startswith("auto_leave_")
            or data.get("type") == "admin_reminder"
        ):
            job.schedule_removal()

# ===============================
# Admin Permission + ThankYou (SAFE FIX)
# ===============================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.my_chat_member:
        return

    chat = update.effective_chat
    if not chat:
        return

    USER_ADMIN_CACHE.pop(chat.id, None)

    old = update.my_chat_member.old_chat_member
    new = update.my_chat_member.new_chat_member
    if not old or not new:
        return

    bot_id = context.bot.id

    # ===============================
    # 🟢 BOT PROMOTED TO ADMIN
    # ===============================
    if (
        new.user.id == bot_id
        and new.status == "administrator"
        and old.status != "administrator"
    ):
        BOT_ADMIN_CACHE.add(chat.id)
        clear_reminders(context, chat.id)

        # 🔥 delete admin request / reminder messages
        for mid in REMINDER_MESSAGES.pop(chat.id, []):
            with contextlib.suppress(Exception):
                await context.bot.delete_message(chat.id, mid)

        # 💾 save group admin status
        context.application.create_task(
            db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET
                    is_admin_cached = TRUE,
                    last_checked_at = EXCLUDED.last_checked_at
                """,
                (chat.id, int(time.time()))
            )
        )

        # ✅ thank you message (KEEP FOREVER)
        try:
            await context.bot.send_message(
                chat.id,
                "✅ <b>Thank you!</b>\n\n"
                "🤖 Bot ကို <b>Admin</b> အဖြစ် ခန့်ထားပြီးပါပြီး။\n"
                "🔗 Auto Link Delete & Spam Link Mute စနစ် စတင်အလုပ်လုပ်နေပါပြီး.........!",
                parse_mode="HTML"
            )
        except:
            pass

        return

    # ===============================
    # 🔴 BOT DEMOTED OR REMOVED
    # ===============================
    if (
        old.user.id == bot_id
        and old.status in ("administrator", "creator")
        and new.status in ("member", "left", "kicked")
    ):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)

        if context.job_queue:
            context.job_queue.run_once(
                leave_if_not_admin,
                when=60,
                data={"chat_id": chat.id},
                name=f"auto_leave_{chat.id}"
            )
        return

    # ===============================
    # 🟡 BOT ADDED BUT NOT ADMIN
    # ===============================
    if (
        new.user.id == bot_id
        and new.status == "member"
        and old.status in ("left", "kicked")
    ):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)

        try:
            me = await context.bot.get_me()
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⭐️ GIVE ADMIN PERMISSION",
                    url=f"https://t.me/{me.username}?startgroup=true"
                )
            ]])

            msg = await context.bot.send_message(
                chat.id,
                "⚠️ <b>Admin Permission Required</b>\n\n"
                "🤖 Bot ကို အလုပ်လုပ်နိုင်ရန်\n"
                "⭐️ <b>Admin အဖြစ် ခန့်ထားပေးပါ</b>",
                parse_mode="HTML",
                reply_markup=keyboard
            )

            REMINDER_MESSAGES.setdefault(chat.id, []).append(msg.message_id)

            if context.job_queue:
                for i in range(1, 6):
                    context.job_queue.run_once(
                        admin_reminder,
                        when=300 * i,
                        data={
                            "chat_id": chat.id,
                            "count": i,
                            "total": 5,
                            "type": "admin_reminder"
                        }
                    )

                context.job_queue.run_once(
                    leave_if_not_admin,
                    when=1510,
                    data={"chat_id": chat.id},
                    name=f"auto_leave_{chat.id}"
                )
        except:
            pass

# ===============================
# Admin Reminder (SAFE FIXED)
# ===============================
async def admin_reminder(context: ContextTypes.DEFAULT_TYPE):

    if not context.job or not context.job.data:
        return

    chat_id = context.job.data.get("chat_id")
    count = context.job.data.get("count")
    total = context.job.data.get("total")

    if not chat_id:
        return

    # ✅ already cached as admin → stop everything
    if chat_id in BOT_ADMIN_CACHE:
        clear_reminders(context, chat_id)
        return

    # 🔐 STEP 1: Check bot still in group
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
    except Exception:
        # ❌ bot kicked / group deleted
        clear_reminders(context, chat_id)
        BOT_ADMIN_CACHE.discard(chat_id)
        REMINDER_MESSAGES.pop(chat_id, None)
        return

    # ✅ STEP 2: Bot is admin now → stop reminders
    if me.status in ("administrator", "creator"):
        BOT_ADMIN_CACHE.add(chat_id)
        clear_reminders(context, chat_id)
        return

    # ❌ STEP 3: Bot still member → send reminder
    try:
        bot = await context.bot.get_me()

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "⭐️ GIVE ADMIN PERMISSION",
                url=f"https://t.me/{bot.username}?startgroup=true"
            )
        ]])

        msg = await context.bot.send_message(
            chat_id,
            f"⏰ <b>Reminder ({count}/{total})</b>\n\n"
            "🤖 Bot ကို အလုပ်လုပ်နိုင်ရန်\n"
            "⭐️ <b>Admin Permission ပေးပါ</b>\n\n"
            "⚠️ Required: Delete messages",
            parse_mode="HTML",
            reply_markup=keyboard
        )

        REMINDER_MESSAGES.setdefault(chat_id, []).append(msg.message_id)

    except Exception:
        # ❌ any unexpected error → stop future reminders
        clear_reminders(context, chat_id)
        BOT_ADMIN_CACHE.discard(chat_id)
        REMINDER_MESSAGES.pop(chat_id, None)

# ===============================
# bot admin check
# ===============================
async def is_bot_admin(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if chat_id in BOT_ADMIN_CACHE:
        return True

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat_id)
            return True
        return False
    except:
        return False

# ===============================
# USER ADMIN CHECK
# ===============================
async def is_user_admin(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    admins = USER_ADMIN_CACHE.setdefault(chat_id, set())

    if user_id in admins:
        return True

    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if member.status in ("administrator", "creator"):
            admins.add(user_id)
            return True
        return False
    except:
        return False

# ===============================
# /refresh (ADMIN ONLY - FAST) ✅ FIXED
# ===============================
async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if not chat or not user or not msg:
        return

    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    user_id = user.id

    # 👮 USER ADMIN CHECK (SAFE)
    if not await is_user_admin(chat_id, user_id, context):
        return

    # 🔄 Clear caches
    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)

    # 🤖 Re-check bot admin (STRICT)
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator") and me.can_delete_messages:
            BOT_ADMIN_CACHE.add(chat_id)

            context.application.create_task(
                db_execute(
                    """
                    INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                    VALUES (%s, TRUE, %s)
                    ON CONFLICT (group_id)
                    DO UPDATE SET
                        is_admin_cached = TRUE,
                        last_checked_at = EXCLUDED.last_checked_at
                    """,
                    (chat_id, int(time.time()))
                )
            )
        else:
            await msg.reply_text(
                "⚠️ <b>Bot မှာ Delete permission မရှိပါ</b>\n\n"
                "🔧 Admin setting ထဲမှာ\n"
                "✅ <b>Delete Messages</b> ကို ဖွင့်ပေးပါ",
                parse_mode="HTML"
            )
            return
    except:
        return

    await msg.reply_text(
        "🔄 <b>Refresh completed!</b>\n\n"
        "✅ Admin cache updated\n"
        "✅ Bot permission re-checked",
        parse_mode="HTML"
    )

# ===============================
# 🔄 AUTO REFRESH ADMIN CACHE ON START (SAFE)
# ===============================
async def refresh_admin_cache(app):
    rows = await db_execute(
        "SELECT group_id FROM groups",
        fetch=True
    ) or []

    BOT_ADMIN_CACHE.clear()
    verified = 0
    skipped = 0

    now = time.time_ns()

    for row in rows:
        gid = row["group_id"]

        try:
            me = await app.bot.get_chat_member(gid, app.bot.id)

            if me.status in ("administrator", "creator"):
                # ✅ ADMIN
                BOT_ADMIN_CACHE.add(gid)
                verified += 1

                await db_execute(
                    """
                    UPDATE groups
                    SET is_admin_cached = TRUE,
                        last_checked_at = %s
                    WHERE group_id = %s
                    """,
                    (now, gid)
                )

            else:
                # ❌ NOT ADMIN (IMPORTANT FIX)
                skipped += 1
                await db_execute(
                    """
                    UPDATE groups
                    SET is_admin_cached = FALSE,
                        last_checked_at = %s
                    WHERE group_id = %s
                    """,
                    (now, gid)
                )

        except Exception as e:
            # ✅ API error -> DO NOT TOUCH DB (keeps old values)
            print(f"⚠️ Skip admin check for {gid}: {e}", flush=True)

        await asyncio.sleep(0.1)

    print(f"✅ Admin cache verified: {verified}", flush=True)
    print(f"⚠️ Non-admin groups marked: {skipped}", flush=True)

    return now  # ✅ IMPORTANT: return this run's timestamp

# ===============================
# purge non admin groups verified
# ===============================
async def purge_non_admin_groups_verified(now: int):
    # Only delete rows that were VERIFIED in this startup run
    await db_execute(
        """
        DELETE FROM groups
        WHERE is_admin_cached = FALSE
          AND last_checked_at = %s
        """,
        (now,)
    )
    print("🧹 Startup purge: verified non-admin groups removed", flush=True)

# ===============================
# /refresh_all (OWNER ONLY - FINAL SAFE VERSION)
# ===============================
async def refresh_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.effective_message

    rows = await db_execute(
        "SELECT group_id FROM groups",
        fetch=True
    ) or []

    BOT_ADMIN_CACHE.clear()

    verified = 0
    skipped = 0
    failed = 0

    for row in rows:
        gid = row["group_id"]

        try:
            me = await context.bot.get_chat_member(gid, context.bot.id)

            # ✅ Bot admin ဖြစ်ရင် cache ထဲထည့်
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(gid)
                verified += 1
            else:
                skipped += 1

        except Exception as e:
            # ❗ API error / private group / rate limit
            # ❌ DB မဖျက် ❌
            print(f"⚠️ refresh_all skip {gid}: {e}")
            failed += 1

        await asyncio.sleep(0.1)  # rate-limit safe

    await msg.reply_text(
        "🔄 <b>Refresh All Completed (SAFE)</b>\n\n"
        f"✅ Admin groups (active): {verified}\n"
        f"⚠️ Non-admin groups (kept): {skipped}\n"
        f"❗ API skipped: {failed}\n\n"
        "🛡️ <i>DB was NOT modified</i>",
        parse_mode="HTML"
    )

# ===============================
# MAIN (FINAL CORRECT VERSION)
# ===============================
def main():
    global pool

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # -------------------------------
    # Commands
    # -------------------------------
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("refresh", refresh))
    app.add_handler(CommandHandler("refresh_all", refresh_all))

    # -------------------------------
    # Chat Member
    # -------------------------------
    app.add_handler(
        ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
    )

    # -------------------------------
    # Auto link delete
    # -------------------------------
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP,
            auto_delete_links
        ),
        group=0
    )

    # -------------------------------
    # Broadcast
    # -------------------------------
    app.add_handler(
        MessageHandler(
            filters.User(OWNER_ID)
            & (filters.TEXT | filters.CAPTION)
            & filters.Regex(r"^/broadcast"),
            broadcast
        )
    )

    app.add_handler(CallbackQueryHandler(
        broadcast_confirm_handler,
        pattern="broadcast_confirm"
    ))
    app.add_handler(CallbackQueryHandler(
        broadcast_target_handler,
        pattern="^bc_target_"
    ))
    app.add_handler(CallbackQueryHandler(
        broadcast_cancel_handler,
        pattern="broadcast_cancel"
    ))

    # -------------------------------
    # STARTUP HOOK (CORRECT)
    # -------------------------------
    async def on_startup(app):
        global pool
        print("🟡 Starting bot...", flush=True)

        await app.bot.delete_webhook(drop_pending_updates=True)

        try:
            pool = ConnectionPool(
                conninfo=(
                    f"host={DB_HOST} "
                    f"dbname={DB_NAME} "
                    f"user={DB_USER} "
                    f"password={DB_PASS} "
                    f"port={DB_PORT} "
                    f"sslmode=require"
                ),
                min_size=1,
                max_size=5,
                timeout=5,
                kwargs={"prepare_threshold": None}
            )
            print("✅ DB pool created", flush=True)
        except Exception as e:
            print("❌ DB pool creation failed:", e, flush=True)
            raise

        await init_db()
        print("✅ DB init done", flush=True)

        now = await refresh_admin_cache(app)
        print("✅ Admin cache refreshed", flush=True)
        
        await purge_non_admin_groups_verified(now)
        
        # 🔄 schedule RAM cache cleanup (every 30 minutes) ✅ CORRECT PLACE
        if app.job_queue:
            app.job_queue.run_repeating(
                cleanup_link_spam_cache,
                interval=1800,   # 30 minutes
                first=1800
            )
            print("🧹 RAM cache cleanup job scheduled", flush=True)

        print("🤖 Link Delete Bot running (PRODUCTION READY)", flush=True)

    # ✅ IMPORTANT
    app.post_init = on_startup

    try:
        app.run_polling(close_loop=False)
    finally:
        if pool:
            pool.close()


if __name__ == "__main__":
    main()