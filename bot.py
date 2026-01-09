# ===============================
# IMPORTS
# ===============================
import os
import psycopg
import time
import asyncio
from typing import Optional

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
BOT_ADMIN_CACHE = set()
USER_ADMIN_CACHE = {}
REMINDER_MESSAGES = {}
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

# ===============================
# DATABASE POOL (SAFE)
# ===============================
def get_conn():
    return psycopg.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=int(DB_PORT),
        sslmode="require",
    )

# ===============================
# SAFE DB EXECUTOR (FIXED)
# ===============================
def db_execute(query, params=None, fetch=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            conn.commit()

# ===============================
# TABLE CREATE
# ===============================
db_execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY
)
""")

db_execute("""
CREATE TABLE IF NOT EXISTS groups (
    group_id BIGINT PRIMARY KEY
)
""")

db_execute("""
CREATE TABLE IF NOT EXISTS delete_jobs (
    chat_id BIGINT,
    message_id BIGINT,
    run_at BIGINT
)
""")

db_execute("""
CREATE TABLE IF NOT EXISTS link_spam (
    chat_id BIGINT,
    user_id BIGINT,
    count INT,
    last_time BIGINT,
    PRIMARY KEY (chat_id, user_id)
)
""")

# ===============================
# DB HELPERS
# ===============================
def save_user_db(user_id: int):
    db_execute(
        "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (user_id,)
    )

def save_group_db(group_id: int):
    db_execute(
        "INSERT INTO groups (group_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (group_id,)
    )

# ===============================
# /start (PRIVATE)
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_chat or update.effective_chat.type != "private":
        return

    if not update.message or not update.effective_user:
        return

    from html import escape

    user = update.effective_user
    save_user_db(user.id)

    bot = await context.bot.get_me()
    bot_username = bot.username

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
        InlineKeyboardButton("📢 CHANNEL", url="https://t.me/MMTelegramBotss")
    ])

    keyboard = InlineKeyboardMarkup(buttons)

    await update.message.reply_photo(
        photo=START_IMAGE,
        caption=text,
        parse_mode="HTML",
        reply_markup=keyboard
    )


# ===============================
# /stats (OWNER ONLY - PRIVATE)
# ===============================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_chat or update.effective_chat.type != "private":
        return

    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    rows = db_execute("SELECT count(*) AS c FROM users", fetch=True) or []
    user_count = rows[0]["c"] if rows else 0

    rows = db_execute("SELECT count(*) AS c FROM groups", fetch=True) or []
    group_count = rows[0]["c"] if rows else 0

    admin_groups = len(BOT_ADMIN_CACHE)
    no_admin_groups = max(0, group_count - admin_groups)

    uptime = int(time.time()) - BOT_START_TIME
    h, m = divmod(uptime // 60, 60)

    await update.effective_message.reply_text(
        "📊 <b>Bot Statistics</b>\n\n"
        f"👤 Users: <b>{user_count}</b>\n"
        f"👥 Groups: <b>{group_count}</b>\n\n"
        f"🔐 Admin Groups: <b>{admin_groups}</b>\n"
        f"⚠️ No Admin Groups: <b>{no_admin_groups}</b>\n\n"
        f"⏱ Uptime: <b>{h}h {m}m</b>",
        parse_mode="HTML"
    )

# ===============================
# ⏱️ DELETE JOB CONFIG
# ===============================
DELETE_AFTER = 10800  # 3 hour

# ===============================
# 🔗 AUTO LINK DELETE (OPTIMIZED)
# ===============================
async def auto_delete_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message
    user = update.effective_user

    if not chat or not message or not user:
        return

    # Skip commands
    if message.text and message.text.startswith("/"):
        return

    chat_id = chat.id
    admins = USER_ADMIN_CACHE.setdefault(chat_id, set())

    # ===============================
    # 🤖 BOT ADMIN CHECK (CACHE)
    # ===============================
    if chat_id not in BOT_ADMIN_CACHE:
        try:
            me = await context.bot.get_chat_member(chat_id, context.bot.id)
            if me.status not in ("administrator", "creator"):
                return
            BOT_ADMIN_CACHE.add(chat_id)
            save_group_db(chat_id)   # ✅ admin ဖြစ်မှ save
        except:
            return

    # ===============================
    # 👤 USER ADMIN BYPASS (CACHE)
    # ===============================
    if user.id in admins:
        return

    try:
        member = await context.bot.get_chat_member(chat_id, user.id)
        if member.status in ("administrator", "creator"):
            admins.add(user.id)
            return
    except:
        return

    # ==============================
    # 🔗 Link detect
    # ==============================
    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)

    text = (message.text or message.caption or "").lower()

    has_link = any(e.type in ("url", "text_link") for e in entities)

    if not has_link and any(x in text for x in ("http://", "https://", "t.me/")):
        has_link = True

    if not has_link:
        return

    # ==============================
    # 🗑 DELETE + SPAM CONTROL
    # ==============================
    try:
        await message.delete()
    except:
        return  # ❗ delete မရရင် spam count မလုပ်

    await link_spam_control(update, context)

    warn = await context.bot.send_message(
        chat_id,
        f"⚠️ ({user.first_name}) မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီ။\n"
        "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူး။"
    )

    run_at = int(time.time()) + DELETE_AFTER

    db_execute(
        "INSERT INTO delete_jobs (chat_id, message_id, run_at) VALUES (%s,%s,%s)",
        (chat_id, warn.message_id, run_at)
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
    rows = db_execute(
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
            save_group_db(chat.id)
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

    start_time = time.time()

    users = db_execute("SELECT user_id FROM users", fetch=True) or []
    groups = db_execute("SELECT group_id FROM groups", fetch=True) or []

    total = len(users) + len(groups)
    sent = 0

    progress = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )

    async def update_progress():
        try:
            await safe_send(
                progress.edit_text,
                f"📢 <b>Broadcasting...</b>\n\n⏳ Progress: {render_progress(sent, total)}",
                parse_mode="HTML"
            )
        except:
            pass

    # 👤 USERS
    for row in users:
        uid = row["user_id"]
        try:
            await safe_send(send_content, context, uid, data)
        except:
            db_execute("DELETE FROM users WHERE user_id=%s", (uid,))

        sent += 1
        if sent % 30 == 0 or sent == total:
            await update_progress()

        await asyncio.sleep(0.08)

    # 👥 GROUPS
    for row in groups:
        gid = row["group_id"]
        try:
            await safe_send(send_content, context, gid, data)
        except:
            db_execute("DELETE FROM groups WHERE group_id=%s", (gid,))

        sent += 1
        if sent % 30 == 0 or sent == total:
            await update_progress()

        await asyncio.sleep(0.08)

    elapsed = int(time.time() - start_time)
    await progress.edit_text(
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

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            return  # ✅ still admin → do nothing
    except:
        pass

    def clear_group_memory(chat_id):
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
        if job.data and job.data.get("chat_id") == chat_id:
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
        save_group_db(chat.id)

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
        BOT_ADMIN_CACHE.add(chat.id)
        clear_reminders(context, chat.id)

        # cancel auto-leave
        for job in context.job_queue.get_jobs_by_name(f"auto_leave_{chat.id}"):
            job.schedule_removal()

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

    if chat_id in BOT_ADMIN_CACHE:
        clear_reminders(context, chat_id)
        return

    count = context.job.data["count"]
    total = context.job.data["total"]

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

    db_execute(
        "DELETE FROM delete_jobs WHERE chat_id=%s AND message_id=%s",
        (chat_id, message_id)
    )


# ===============================
# admin check
# ===============================
async def is_bot_admin(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        return me.status in ("administrator", "creator")
    except:
        return False


# ===============================
# Link Detect + Count + Mute Code
# ===============================
LINK_LIMIT = 3
MUTE_SECONDS = 600  # 10 minutes
SPAM_RESET_SECONDS = 3600  # 1 hour reset

async def link_spam_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message

    if not chat or not user or not message:
        return

    if chat.type not in ("group", "supergroup"):
        return

    # ==============================
    # Link detect
    # ==============================
    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)

    text = (message.text or message.caption or "").lower()

    has_link = any(e.type in ("url", "text_link") for e in entities)
    if not has_link and ("http://" in text or "https://" in text or "t.me/" in text):
        has_link = True

    if not has_link:
        return

    # ==============================
    # Admin bypass
    # ==============================
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status in ("administrator", "creator"):
            return
    except:
        return

    is_supergroup = (chat.type == "supergroup")
    now = int(time.time())

    # ==============================
    # DB fetch
    # ==============================
    rows = db_execute(
        "SELECT count, last_time FROM link_spam WHERE chat_id=%s AND user_id=%s",
        (chat.id, user.id),
        fetch=True
    ) or []

    if rows:
        row = rows[0]

        # 🔄 reset if inactive > 1 hour
        if now - row["last_time"] > SPAM_RESET_SECONDS:
            count = 1
        else:
            count = row["count"] + 1

        db_execute(
            "UPDATE link_spam SET count=%s, last_time=%s WHERE chat_id=%s AND user_id=%s",
            (count, now, chat.id, user.id)
        )
    else:
        count = 1
        db_execute(
            "INSERT INTO link_spam (chat_id, user_id, count, last_time) VALUES (%s,%s,%s,%s)",
            (chat.id, user.id, count, now)
        )

    # ==============================
    # 🚨 Limit reached → mute
    # ==============================
    if count >= LINK_LIMIT and is_supergroup:
        until = now + MUTE_SECONDS

        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=user.id,
            permissions=ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False
            ),
            until_date=until
        )

        await context.bot.send_message(
            chat.id,
            f"🔇 <b>{user.first_name}</b> ကို\n"
            f"🔗 Link {LINK_LIMIT} ကြိမ် ပို့လို့\n"
            f"⏰ 10 မိနစ် mute လုပ်လိုက်ပါပြီ",
            parse_mode="HTML"
        )

        db_execute(
            "DELETE FROM link_spam WHERE chat_id=%s AND user_id=%s",
            (chat.id, user.id)
        )

# ===============================
# /refresh (ADMIN ONLY)
# ===============================
async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if not chat or not user or chat.type not in ("group", "supergroup"):
        return

    save_group_db(chat.id)

    # 👮 Admin only
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ("administrator", "creator"):
            return
    except:
        return

    # 🔄 Clear caches
    BOT_ADMIN_CACHE.discard(chat.id)
    USER_ADMIN_CACHE.pop(chat.id, None)

    # 🔁 Re-check bot admin
    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat.id)
            save_group_db(chat.id)
    except:
        pass

    await msg.reply_text(
        "🔄 <b>Refresh completed!</b>\n\n"
        "✅ Admin cache updated\n"
        "✅ Bot permission re-checked",
        parse_mode="HTML"
    )

# ===============================
# 🔄 AUTO REFRESH ADMIN CACHE ON START
# ===============================
async def refresh_admin_cache(app):
    rows = db_execute(
        "SELECT group_id FROM groups",
        fetch=True
    ) or []

    for row in rows:
        chat_id = row["group_id"]
        try:
            me = await app.bot.get_chat_member(chat_id, app.bot.id)
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(chat_id)
        except:
            pass

    print(f"✅ Admin cache loaded: {len(BOT_ADMIN_CACHE)}")

# ===============================
# /refresh_all (Owner only)
# ===============================
async def refresh_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.effective_message
    groups = db_execute("SELECT group_id FROM groups", fetch=True) or []

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
                db_execute(
                    "DELETE FROM groups WHERE group_id=%s",
                    (gid,)
                )
                removed += 1
        except:
            db_execute(
                "DELETE FROM groups WHERE group_id=%s",
                (gid,)
            )
            removed += 1

    await msg.reply_text(
        f"🔄 <b>Refresh All Completed</b>\n\n"
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
        group=0
    )

    # -------------------------------
    # Broadcast (OWNER ONLY)
    # -------------------------------
    app.add_handler(
        MessageHandler(
            filters.User(OWNER_ID) & filters.Regex(r"^/broadcast"),
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
        await refresh_admin_cache(app)
        await restore_jobs(app)


    app.post_init = on_startup

    print("🤖 Link Delete Bot running (PRODUCTION READY)")
    app.run_polling()


if __name__ == "__main__":
    main()

