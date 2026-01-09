# ===============================
# IMPORTS
# ===============================
import os
import time
import asyncio
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
from telegram.error import RetryAfter
from html import escape
import psycopg
from psycopg_pool import ConnectionPool
import contextlib
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatPermissions,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)

# ===============================
# GLOBAL CACHES
# ===============================
BOT_ADMIN_CACHE: set[int] = set()
USER_ADMIN_CACHE: dict[int, set[int]] = {}
REMINDER_MESSAGES: dict[int, list[int]] = {}
PENDING_BROADCAST = {}
BOT_START_TIME = int(time.time())

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
)

# =====================================
# DB EXECUTOR (ASYNC SAFE)
# =====================================
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


# =====================================
# INIT DB
# =====================================
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
     await db_execute("""
        CREATE TABLE IF NOT EXISTS link_spam (
            chat_id BIGINT,
            user_id BIGINT,
            count INT,
            last_time BIGINT,
            PRIMARY KEY (chat_id, user_id)
        )
    """)
     await db_execute("""
        CREATE TABLE IF NOT EXISTS delete_jobs (
            chat_id BIGINT,
            message_id BIGINT,
            run_at BIGINT
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

    # 🔥 DB save in background (NO DELAY)
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
        "✅ Spam Link Mute ( Link 3 ခါ ပိုရင် 10 မိနစ် Auto Mute )\n\n"
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
        InlineKeyboardButton("👨‍💻 DEVELOPER", url="https://t.me/callmeoggy"),
        InlineKeyboardButton("📢 CHANNEL", url="https://t.me/MMTelegramBotss"),
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

    users = await db_execute("SELECT COUNT(*) AS c FROM users", fetch=True)
    groups = await db_execute("SELECT COUNT(*) AS c FROM groups", fetch=True)

    user_count = users[0]["c"] if users else 0
    group_count = groups[0]["c"] if groups else 0

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
        f"⏱ Uptime: <b>{h}h {m}m</b>",
        parse_mode="HTML",
    )

# ===============================
# ⏱️ DELETE JOB CONFIG
# ===============================
DELETE_AFTER = 10800  # 3 hour (warn delete faster)

# ===============================
# 🔗 AUTO LINK DELETE (FAST + SAFE)
# ===============================
async def auto_delete_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user

    if not chat or not msg or not user:
        return

    # skip commands
    if msg.text and msg.text.startswith("/"):
        return

    chat_id = chat.id
    user_id = user.id

    # ===========================
    # 🔗 FAST LINK DETECT
    # ===========================
    text = (msg.text or msg.caption or "").lower()
    has_link = False

    for e in (msg.entities or []) + (msg.caption_entities or []):
        if e.type in ("url", "text_link"):
            has_link = True
            break

    if not has_link and ("http://" in text or "https://" in text or "t.me/" in text):
        has_link = True

    # 🔗 detect link
    if not has_link:
        return

    # 🔥 DELETE FIRST (FAST)
    try:
        await msg.delete()
    except:
        return

    # 🤖 BOT ADMIN CACHE (soft)
    if chat_id not in BOT_ADMIN_CACHE:
        try:
            me = await context.bot.get_chat_member(chat_id, context.bot.id)
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(chat_id)
            else:
                return
        except:
            return

    # 💾 DB save → background
    context.application.create_task(
        db_execute(
            "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
            (chat.id,)
        )
    )

    # ⚠️ spam control → background
    context.application.create_task(
        link_spam_control(update, context)
    )

    # ===========================
    # 👤 USER ADMIN CHECK (CACHE)
    # ===========================
    admins = USER_ADMIN_CACHE.setdefault(chat_id, set())
    if user_id not in admins:
        try:
            member = await context.bot.get_chat_member(chat_id, user_id)
            if member.status in ("administrator", "creator"):
                admins.add(user_id)
                return
        except:
            return

    # ===========================
    # ⚠️ SPAM CONTROL (BACKGROUND)
    # ===========================
    context.application.create_task(
        link_spam_control(update, context)
    )

    # ===========================
    # ⚠️ WARN
    # ===========================
    warn = await context.bot.send_message(
        chat_id,
        f"⚠️ <b>{user.first_name}</b> မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီ။\n"
        "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူး။",
        parse_mode="HTML"
    )

    context.job_queue.run_once(
        delete_message_job,
        when=DELETE_AFTER,
        data={"chat_id": chat_id, "message_id": warn.message_id}
    )

# ===============================
# 🔄 RESTORE JOBS ON START (OK)
# ===============================
async def restore_jobs(app):
    now = int(time.time())
    rows = await db_execute(
        "SELECT chat_id, message_id, run_at FROM delete_jobs",
        fetch=True
    ) or []

    for row in rows:
        delay = max(0, row["run_at"] - now)
        app.job_queue.run_once(
            delete_message_job,
            when=delay,
            data={
                "chat_id": row["chat_id"],
                "message_id": row["message_id"]
            }
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
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat.id)
    except:
        pass

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

    if not any(content.values()):
        await msg.reply_text("❌ Broadcast လုပ်ရန် content မတွေ့ပါ")
        return

    PENDING_BROADCAST[msg.from_user.id] = content

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
# Broadcast Confirm
# ===============================
async def broadcast_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = PENDING_BROADCAST.pop(query.from_user.id, None)
    if not data:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    users = await db_execute("SELECT user_id FROM users", fetch=True) or []
    groups = await db_execute("SELECT group_id FROM groups", fetch=True) or []

    targets = [u["user_id"] for u in users] + [g["group_id"] for g in groups]
    total = len(targets)
    sent = 0

    progress_msg = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )

    start_time = time.time()

    # 🔄 progress updater (every 2 sec)
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

    # 🚀 FAST SEND (batch)
    BATCH_SIZE = 10  # Railway safe

    for i in range(0, total, BATCH_SIZE):
        batch = targets[i:i + BATCH_SIZE]

        tasks = [
            safe_send(send_content, context, chat_id, data)
            for chat_id in batch
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for chat_id, result in zip(batch, results):
            sent += 1
            if isinstance(result, Exception):
                context.application.create_task(
                    db_execute("DELETE FROM users WHERE user_id=%s", (chat_id,))
                )
                context.application.create_task(
                    db_execute("DELETE FROM groups WHERE group_id=%s", (chat_id,))
                )

    progress_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await progress_task

    elapsed = int(time.time() - start_time)
    await progress_msg.edit_text(
        "✅ <b>Broadcast Completed</b>\n\n"
        f"👤 Users: {len(users)}\n"
        f"👥 Groups: {len(groups)}\n"
        f"⏱ Time: {elapsed // 60}m {elapsed % 60}s",
        parse_mode="HTML"
    )

# ===============================
# Cancel Button 
# ===============================
async def broadcast_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    PENDING_BROADCAST.pop(query.from_user.id, None)
    await query.edit_message_text("❌ Broadcast Cancel လုပ်လိုက်ပါပြီ")

# ===============================
# Media / Text 
# ===============================
async def send_content(context, chat_id, data):
    if data["photo"]:
        await context.bot.send_photo(chat_id, data["photo"], caption=data["text"])
    elif data["video"]:
        await context.bot.send_video(chat_id, data["video"], caption=data["text"])
    elif data["audio"]:
        await context.bot.send_audio(chat_id, data["audio"], caption=data["text"])
    elif data["document"]:
        await context.bot.send_document(chat_id, data["document"], caption=data["text"])
    else:
        await context.bot.send_message(chat_id, data["text"])

# ===============================
# Auto leave job
# ===============================
async def leave_if_not_admin(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return

    chat_id = context.job.data["chat_id"]

    # ✅ CACHE FIRST (NO API CALL)
    if chat_id in BOT_ADMIN_CACHE:
        return

    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)
    REMINDER_MESSAGES.pop(chat_id, None)

    try:
        await context.bot.leave_chat(chat_id)
    except:
        pass

# ===============================
# Helper: Clear all reminder jobs
# ===============================
def clear_reminders(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    for job in context.job_queue.jobs():
        data = job.data
        if not data:
            continue
        if data.get("chat_id") == chat_id:
            job.schedule_removal()

# ===============================
# Admin Permission + ThankYou
# ===============================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.my_chat_member:
        return

    chat = update.effective_chat
    if not chat:
        return

    # reset cached user-admins on any change
    USER_ADMIN_CACHE.pop(chat.id, None)

    old = update.my_chat_member.old_chat_member
    new = update.my_chat_member.new_chat_member
    if not old or not new:
        return

    # Save group whenever bot appears
    if new.user.id == context.bot.id:
        context.application.create_task(
            db_execute(
                "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
                (chat.id,)
            )
        )

    # ===============================
    # 🔴 BOT DEMOTED OR REMOVED
    # ===============================
    if (
        old.user.id == context.bot.id
        and old.status in ("administrator", "creator")
        and new.status == "member"
    ):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)
        
        # cancel auto-leave
        for job in context.job_queue.get_jobs_by_name(f"auto_leave_{chat.id}"):
            job.schedule_removal()

        context.job_queue.run_once(
            leave_if_not_admin,
            when=60,
            data={"chat_id": chat.id},
            name=f"auto_leave_{chat.id}"
        )
        return

    # ===============================
    # 🟢 BOT PROMOTED TO ADMIN
    # ===============================
    if (
        new.user.id == context.bot.id
        and new.status == "administrator"
        and old.status != "administrator"
    ):
        
        # cancel auto-leave
        for job in context.job_queue.get_jobs_by_name(f"auto_leave_{chat.id}"):
            job.schedule_removal()
        
        BOT_ADMIN_CACHE.add(chat.id)
        clear_reminders(context, chat.id)

        thank = await context.bot.send_message(
            chat.id,
            "✅ <b>Thank you!</b>\n\n"
            "🤖 Bot ကို <b>Admin</b> အဖြစ် ခန့်ထားပြီးပါပြီ။\n"
            "🔗 Auto Link Delete & Spam Link Mute စနစ် စတင်အလုပ်လုပ်နေပါပြီ..........!",
            parse_mode="HTML"
        )

        context.job_queue.run_once(
            delete_message_job,
            when=300,
            data={"chat_id": chat.id, "message_id": thank.message_id}
        )
        return

    # ===============================
    # 🟡 BOT ADDED BUT NOT ADMIN
    # ===============================
    if (
        new.user.id == context.bot.id
        and new.status == "member"
        and old.status in ("left", "kicked")
    ):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)

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

        for i in range(1, 6):
            context.job_queue.run_once(
                admin_reminder,
                when=300 * i,
                data={"chat_id": chat.id, "count": i, "total": 5}
            )

        context.job_queue.run_once(
            leave_if_not_admin,
            when=300 * 5 + 10,
            data={"chat_id": chat.id},
            name=f"auto_leave_{chat.id}"
        )


# ===============================
# Admin Reminder
# ===============================
async def admin_reminder(context: ContextTypes.DEFAULT_TYPE):

    if not context.job or not context.job.data:
        return

    chat_id = context.job.data["chat_id"]
    count = context.job.data["count"]
    total = context.job.data["total"]
      
    if chat_id in BOT_ADMIN_CACHE:
        clear_reminders(context, chat_id)
        return
   
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat_id)
            clear_reminders(context, chat_id)
            return

        keyboard = InlineKeyboardMarkup([[  
            InlineKeyboardButton(
                "⭐️ GIVE ADMIN PERMISSION",
                url=f"https://t.me/{me.username}?startgroup=true"
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

    except:
        pass

# ===============================
# delete message job
# ===============================
async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return

    chat_id = context.job.data["chat_id"]
    message_id = context.job.data["message_id"]

    try:
        await context.bot.delete_message(chat_id, message_id)
    except:
        pass

    context.application.create_task(
        db_execute(
            "DELETE FROM delete_jobs WHERE chat_id=%s AND message_id=%s",
            (chat_id, message_id)
        )
    )

# ===============================
# admin check
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
# Link Detect + Count + Mute Code
# ===============================
LINK_LIMIT = 3
MUTE_SECONDS = 600
SPAM_RESET_SECONDS = 3600

async def link_spam_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message

    if not chat or not user or not message:
        return

    if chat.type != "supergroup":   # ⚡ mute only needs supergroup
        return

    chat_id = chat.id
    user_id = user.id

    # =========================
    # 👤 Admin bypass (CACHE FIRST)
    # =========================
    admins = USER_ADMIN_CACHE.setdefault(chat_id, set())
    if user_id in admins:
        return

    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if member.status in ("administrator", "creator"):
            admins.add(user_id)
            return
    except:
        return

    # =========================
    # ⚡ DB ACCESS (ONLY HERE)
    # =========================
    now = int(time.time())

    rows = await db_execute(
        "SELECT count, last_time FROM link_spam WHERE chat_id=%s AND user_id=%s",
        (chat_id, user_id),
        fetch=True
    ) or []

    if rows:
        row = rows[0]
        if now - row["last_time"] > SPAM_RESET_SECONDS:
            count = 1
        else:
            count = row["count"] + 1

        context.application.create_task(
            db_execute(
                "UPDATE link_spam SET count=%s, last_time=%s WHERE chat_id=%s AND user_id=%s",
                (count, now, chat_id, user_id)
            )
        )
    else:
        count = 1
        context.application.create_task(
            db_execute(
                "INSERT INTO link_spam VALUES (%s,%s,%s,%s)",
                (chat_id, user_id, count, now)
            )
        )

    # =========================
    # 🚨 MUTE (INSTANT)
    # =========================
    if count >= LINK_LIMIT:
        until = now + MUTE_SECONDS

        await context.bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until
        )

        await context.bot.send_message(
            chat_id,
            f"🔇 <b>{user.first_name}</b>ကို\n"
            f"🔗 Link {LINK_LIMIT} ကြိမ် ပို့လို့\n"
            f"⏰ 10 မိနစ် mute လုပ်လိုက်ပါပြီ",
            parse_mode="HTML"
        )

        context.application.create_task(
            db_execute(
                "DELETE FROM link_spam WHERE chat_id=%s AND user_id=%s",
                (chat_id, user_id)
            )
        )

# ===============================
# /refresh (ADMIN ONLY - FAST)
# ===============================
async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if not chat or not user or chat.type not in ("group", "supergroup"):
        return

    # 👮 User admin check
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator"):
            return
    except:
        return

    # 🔄 Clear caches
    BOT_ADMIN_CACHE.discard(chat.id)
    USER_ADMIN_CACHE.pop(chat.id, None)

    # 🤖 Re-check bot admin (ONLY ONCE)
    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat.id)
            context.application.create_task(
                db_execute(
                    "INSERT INTO groups VALUES (%s) ON CONFLICT DO NOTHING",
                    (chat.id,)
                )
            )
    except:
        pass

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
    
    added = 0

    for row in rows:
        gid = row["group_id"]
        try:
            me = await app.bot.get_chat_member(gid, app.bot.id)
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(gid)
                added += 1
        except:
            pass

        await asyncio.sleep(0.1)  # safer for large groups

    print(f"✅ Admin cache loaded: {added}")

# ===============================
# /refresh_all (OWNER ONLY - SAFE)
# ===============================
async def refresh_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.effective_message
    groups = await db_execute("SELECT group_id FROM groups", fetch=True) or []

    refreshed = 0
    removed = 0

    for row in groups:
        gid = row["group_id"]
        try:
            me = await context.bot.get_chat_member(gid, context.bot.id)
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(gid)
                refreshed += 1
            else:
                context.application.create_task(
                    db_execute(
                        "DELETE FROM groups WHERE group_id=%s",
                        (gid,)
                    )
                )
                removed += 1
        except:
                context.application.create_task(
                    db_execute(
                        "DELETE FROM groups WHERE group_id=%s",
                        (gid,)
                    )
                )
                removed += 1

        await asyncio.sleep(0.1)  

    await msg.reply_text(
        "🔄 <b>Refresh All Completed</b>\n\n"
        f"✅ Active groups: {refreshed}\n"
        f"❌ Removed groups: {removed}",
        parse_mode="HTML"
    )

# ===============================
# MAIN
# ===============================
def main():
    
    # ✅ FIX 1: token check BEFORE build
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
    # Auto link delete (groups only)
    # -------------------------------
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION),
            auto_delete_links
        ),
        group=1
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

    app.add_handler(
        CallbackQueryHandler(
            broadcast_confirm_handler,
            pattern="broadcast_confirm"
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            broadcast_cancel_handler,
            pattern="broadcast_cancel"
        )
    )

    # -------------------------------
    # Bot admin / permission tracking
    # -------------------------------
    app.add_handler(
        ChatMemberHandler(
            on_my_chat_member,
            ChatMemberHandler.MY_CHAT_MEMBER
        )
    )

    # -------------------------------
    # Startup jobs
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

