# ===============================
# IMPORTS
# ===============================
import os
import time
import asyncio
import contextlib
from html import escape
from concurrent.futures import ThreadPoolExecutor  # ✅ REQUIRED

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatPermissions,
)
from telegram.error import BadRequest
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)

from psycopg_pool import ConnectionPool  # ✅ ONLY THIS (Supabase safe)

# ===============================
# GLOBAL CACHES
# ===============================
BOT_ADMIN_CACHE: set[int] = set()
USER_ADMIN_CACHE: dict[int, set[int]] = {}
REMINDER_MESSAGES: dict[int, list[int]] = {}
PENDING_BROADCAST = {}
BOT_START_TIME = int(time.time())

# 🔥 LINK SPAM CACHE (OPTION A CORE)
LINK_SPAM_CACHE: dict[int, dict[int, tuple[int, int]]] = {}

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
    kwargs={                      
        "prepare_threshold": None 
    }
)

async def db_execute(query, params=None, fetch=False):
    loop = asyncio.get_running_loop()

    def _run():
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
            group_id BIGINT PRIMARY KEY
        )
    """)

# ===============================
# /start (PRIVATE)
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.message

    if not chat or chat.type != "private" or not user or not msg:
        return

    context.application.create_task(
        db_execute(
            "INSERT INTO users VALUES (%s) ON CONFLICT DO NOTHING",
            (user.id,)
        )
    )

    bot = context.bot
    bot_username = bot.username or ""

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

    try:
        users_task = db_execute(
            "SELECT COUNT(*) AS c FROM users",
            fetch=True
        )
        groups_task = db_execute(
            "SELECT COUNT(*) AS c FROM groups",
            fetch=True
        )

        users, groups = await asyncio.gather(
            users_task,
            groups_task,
            return_exceptions=True
        )

        if isinstance(users, Exception):
            print("⚠️ STATS users query failed:", users)
            user_count = 0
        else:
            user_count = users[0]["c"] if users else 0

        if isinstance(groups, Exception):
            print("⚠️ STATS groups query failed:", groups)
            group_count = 0
        else:
            group_count = groups[0]["c"] if groups else 0

    except Exception as e:
        print("❌ STATS FAILED:", e)
        await msg.reply_text("❌ Stats temporarily unavailable")
        return

    # ✅ CACHE-SAFE COUNT
    admin_groups = len(BOT_ADMIN_CACHE)
    no_admin_groups = max(0, group_count - admin_groups)

    uptime = int(time.time()) - BOT_START_TIME
    h, m = divmod(uptime // 60, 60)

    await msg.reply_text(
        "📊 <b>Bot Statistics</b>\n\n"
        f"👤 Users: <b>{user_count}</b>\n"
        f"👥 Groups: <b>{group_count}</b>\n\n"
        f"🔐 Admin Groups: <b>{admin_groups}</b>\n"
        f"⚠️ No Admin Groups: <b>{no_admin_groups}</b>\n\n"
        f"⏱️ Uptime: <b>{h}h {m}m</b>",
        parse_mode="HTML",
    )

# ===============================
# ⏱️ DELETE JOB CONFIG
# ===============================
DELETE_AFTER = 18000  # 5 hour 

# ===============================
# schedule delete message (SAFE)
# ===============================
async def schedule_delete_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    delay: int
):
    run_at = int(time.time()) + delay

    # DB မှာပဲ save (restart အတွက်)
    context.application.create_task(
        db_execute(
            "INSERT INTO delete_jobs VALUES (%s,%s,%s)",
            (chat_id, message_id, run_at)
        )
    )

    # ❗ JobQueue မသုံးတော့ဘူး
    # delete job ကို schedule မလုပ်

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

    # BOT ADMIN CHECK
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status not in ("administrator", "creator"):
            return
    except:
        return

    # ADMIN BYPASS
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if member.status in ("administrator", "creator"):
            return
    except:
        return

    # LINK DETECT
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

    if not has_link:
        return

    # ---- DELETE
    try:
        await msg.delete()
    except BadRequest as e:
        print("ℹ️ Delete skipped:", e)
        return
    except Exception as e:
        print("❌ Delete failed:", e)
        return
    
    user_mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
    # ---- DELETE WARN
    try:
        warn = await context.bot.send_message(
            chat_id,
            f"⚠️ <b>{user_mention}</b> မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီ။\n"
            "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူး။",
            parse_mode="HTML"
        )
    except:
        warn = None

    # ---- COUNT + MUTE (SYNC)
    muted = await link_spam_control(chat_id, user_id, context)

    # ---- MUTE WARN 🔥 (FIX)
    if muted:
        try:
            await context.bot.send_message(
                chat_id,
                f"🔇 <b>{user_mention}</b> ကို\n"
                f"🔗 Link {LINK_LIMIT} ကြိမ် ပို့လို့\n"
                f"⏰ 10 မိနစ် mute လုပ်လိုက်ပါပြီ",
                parse_mode="HTML"
            )
        except:
            pass

    # ---- SAVE GROUP
    context.application.create_task(
        db_execute(
            "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
            (chat_id,)
        )
    )

    # ---- AUTO DELETE WARN
    if warn:
        await schedule_delete_message(
            context,
            chat_id,
            warn.message_id,
            DELETE_AFTER
        )

# ===============================
# LINK COUNT + MUTE (RETURN RESULT)
# ===============================
async def link_spam_control(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    now = int(time.time())

    try:
        rows = await asyncio.wait_for(
            db_execute(
                "SELECT count, last_time FROM link_spam WHERE chat_id=%s AND user_id=%s",
                (chat_id, user_id),
                fetch=True
            ),
            timeout=2
        )
    except:
        return False

    # ---- COUNT
    if rows:
        last = rows[0]
        count = 1 if now - last["last_time"] > SPAM_RESET_SECONDS else last["count"] + 1
        await db_execute(
            "UPDATE link_spam SET count=%s, last_time=%s WHERE chat_id=%s AND user_id=%s",
            (count, now, chat_id, user_id)
        )
    else:
        count = 1
        await db_execute(
            "INSERT INTO link_spam VALUES (%s,%s,%s,%s)",
            (chat_id, user_id, count, now)
        )

    # ---- NOT REACHED LIMIT
    if count < LINK_LIMIT:
        return False

    # ---- SUPERGROUP ONLY
    try:
        chat = await context.bot.get_chat(chat_id)
        if chat.type != "supergroup":
            return False
    except:
        return False

    # ---- BOT PERMISSION
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if not me.can_restrict_members:
            return False
    except:
        return False

    # ---- MUTE
    try:
        await context.bot.restrict_chat_member(
            chat_id,
            user_id,
            ChatPermissions(can_send_messages=False),
            until_date=now + MUTE_SECONDS
        )
    except:
        return False

    # ---- RESET COUNTER
    await db_execute(
        "DELETE FROM link_spam WHERE chat_id=%s AND user_id=%s",
        (chat_id, user_id)
    )

    return True  # 🔥 IMPORTANT

# ===============================
# 🔄 RESTORE JOBS ON START (SAFE – NO JOBQUEUE)
# ===============================
async def restore_jobs(app):
    now = int(time.time())

    try:
        rows = await db_execute(
            "SELECT chat_id, message_id, run_at FROM delete_jobs",
            fetch=True
        )
    except Exception as e:
        print("⚠️ restore_jobs DB error:", e)
        return

    if not rows:
        return

    for row in rows:
        run_at = row["run_at"]

        # expired → clean DB only
        if run_at <= now:
            await db_execute(
                "DELETE FROM delete_jobs WHERE chat_id=%s AND message_id=%s",
                (row["chat_id"], row["message_id"])
            )
            continue

        # ❗ JobQueue မရှိတဲ့အတွက်
        # Bot restart ပြီးတဲ့ message delete ကို SKIP
        # (Bot crash မဖြစ်အောင်)
        print(
            f"ℹ️ Skip restore delete job "
            f"(chat={row['chat_id']}, msg={row['message_id']})"
        )

# ===============================
# Save Group (ADMIN ONLY)
# ===============================
async def save_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        return

    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status not in ("administrator", "creator"):
            return
    except:
        return

    # ✅ cache update
    BOT_ADMIN_CACHE.add(chat.id)

    # ✅ DB save (background, never block)
    context.application.create_task(
        db_execute(
            "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
            (chat.id,)
        )
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
# Broadcast flood-safe 
# ===============================
async def safe_send(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except (Forbidden, BadRequest):
            return None

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

    users = await db_execute("SELECT user_id FROM users", fetch=True) or []
    groups = await db_execute("SELECT group_id FROM groups", fetch=True) or []

    if target_type == "bc_target_users":
        targets = [u["user_id"] for u in users]
    elif target_type == "bc_target_groups":
        targets = [g["group_id"] for g in groups]
    else:
        targets = list(set(
            [u["user_id"] for u in users] +
            [g["group_id"] for g in groups]
        ))

    total = len(targets)
    sent = 0

    progress_msg = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )

    async def progress_updater():
        while sent < total:
            try:
                await progress_msg.edit_text(
                    f"📢 <b>Broadcasting...</b>\n\n⏳ Progress: {render_progress(sent, total)}",
                    parse_mode="HTML"
                )
            except:
                pass
            await asyncio.sleep(2)

    progress_task = asyncio.create_task(progress_updater())

    BATCH_SIZE = 10
    start_time = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = targets[i:i + BATCH_SIZE]

        results = await asyncio.gather(
            *[safe_send(send_content, context, cid, data) for cid in batch],
            return_exceptions=True
        )

        for cid, result in zip(batch, results):
            sent += 1
            # ❌ DB delete logic REMOVED
            if result is None or isinstance(result, Exception):
                pass   # error ဖြစ်ရင် ignore only

    progress_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await progress_task

    elapsed = int(time.time() - start_time)

    await progress_msg.edit_text(
        "✅ <b>Broadcast Completed</b>\n\n"
        f"🎯 Target: {total}\n"
        f"⏱️ Time: {elapsed // 60}m {elapsed % 60}s",
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
        db_execute("DELETE FROM groups WHERE group_id=%s", (chat_id,))
    )
    context.application.create_task(
        db_execute("DELETE FROM link_spam WHERE chat_id=%s", (chat_id,))
    )
    context.application.create_task(
        db_execute("DELETE FROM delete_jobs WHERE chat_id=%s", (chat_id,))
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

        # 🔥 delete admin request messages
        for mid in REMINDER_MESSAGES.pop(chat.id, []):
            with contextlib.suppress(Exception):
                await context.bot.delete_message(chat.id, mid)

        context.application.create_task(
            db_execute(
                "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
                (chat.id,)
            )
        )

        try:
            thank = await context.bot.send_message(
                chat.id,
                "✅ <b>Thank you!</b>\n\n"
                "🤖 Bot ကို <b>Admin</b> အဖြစ် ခန့်ထားပြီးပါပြီး။\n"
                "🔗 Auto Link Delete & Spam Link Mute စနစ် စတင်အလုပ်လုပ်နေပါပြီ..........!",
                parse_mode="HTML"
            )

            # schedule delete (only if job_queue exists)
            if context.job_queue:
                await schedule_delete_message(
                    context,
                    chat.id,
                    thank.message_id,
                    300
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
# Admin Reminder (FIXED)
# ===============================
async def admin_reminder(context: ContextTypes.DEFAULT_TYPE):

    if not context.job or not context.job.data:
        return

    chat_id = context.job.data["chat_id"]
    count = context.job.data["count"]
    total = context.job.data["total"]

    # ✅ Already admin → stop reminders
    if chat_id in BOT_ADMIN_CACHE:
        clear_reminders(context, chat_id)
        return

    try:
        # 🔍 check bot permission again
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat_id)
            clear_reminders(context, chat_id)
            return

        # ✅ correct username source
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

    except Exception as e:
        print(f"❌ admin_reminder error in {chat_id}:", e)

# ===============================
# delete message job (FIXED)
# ===============================
async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return

    chat_id = context.job.data.get("chat_id")
    message_id = context.job.data.get("message_id")

    if not chat_id or not message_id:
        return

    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception as e:
        # message already deleted / no permission → ignore but log
        print(f"⚠️ delete_message_job failed {chat_id}:{message_id} →", e)

    # 🧹 cleanup DB (ALWAYS)
    try:
        await db_execute(
            "DELETE FROM delete_jobs WHERE chat_id=%s AND message_id=%s",
            (chat_id, message_id)
        )
    except Exception as e:
        print(f"⚠️ delete_jobs DB cleanup failed:", e)

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
                    "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
                    (chat_id,)
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

    for row in rows:
        gid = row["group_id"]

        try:
            me = await app.bot.get_chat_member(gid, app.bot.id)

            # ✅ Admin ဖြစ်ရင် cache ထဲထည့်
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(gid)
                verified += 1
            else:
                # ❌ NOT ADMIN → cache မထည့်ပဲ skip
                skipped += 1

        except Exception as e:
            # ❗ API error / private group / rate limit
            # ❌ DB မဖျက် ❌
            print(f"⚠️ Skip admin check for {gid}: {e}")
            skipped += 1

        await asyncio.sleep(0.1)  # rate-limit safe

    print(f"✅ Admin cache verified: {verified}")
    print(f"⚠️ Skipped (kept in DB): {skipped}")

# ===============================
# /refresh_all (OWNER ONLY - SAVE ADMIN GROUPS)
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

    verified_groups = []   # 🔥 admin groups to save
    skipped = 0

    for row in rows:
        gid = row["group_id"]

        try:
            me = await context.bot.get_chat_member(gid, context.bot.id)

            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(gid)
                verified_groups.append(gid)   # ✅ collect
            else:
                skipped += 1

        except Exception as e:
            print(f"⚠️ refresh_all skip {gid}: {e}")
            skipped += 1

        await asyncio.sleep(0.1)  # rate-limit safe

    # ===============================
    # 🔥 BULK SAVE (ONE SHOT)
    # ===============================
    if verified_groups:
        values = ",".join(["(%s)"] * len(verified_groups))
        query = f"""
            INSERT INTO groups (group_id)
            VALUES {values}
            ON CONFLICT DO NOTHING
        """
        await db_execute(query, tuple(verified_groups))

    await msg.reply_text(
        "🔄 <b>Refresh All Completed</b>\n\n"
        f"✅ Admin groups saved: {len(verified_groups)}\n"
        f"⚠️ Skipped (kept in DB): {skipped}",
        parse_mode="HTML"
    )

# ===============================
# MAIN (FINAL FIXED)
# ===============================
def main():
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
    # Auto link delete (GROUP + SUPERGROUP ONLY)
    # -------------------------------
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP,
            auto_delete_links
        ),
        group=0
    )

    # -------------------------------
    # Broadcast (OWNER ONLY)
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
    # Startup jobs (ORDER IS CRITICAL)
    # -------------------------------
    async def on_startup(app):
        try:
            await init_db()
        except Exception as e:
            print("⚠️ DB init failed:", e)

        await restore_jobs(app)
        await refresh_admin_cache(app)

    app.post_init = on_startup

    print("🤖 Link Delete Bot running (PRODUCTION READY)")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()