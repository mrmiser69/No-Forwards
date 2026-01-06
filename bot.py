# ===============================
# IMPORTS
# ===============================
import os
import sqlite3
import time
import asyncio
from datetime import timedelta

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions
)

from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    ChatMemberHandler,
    CallbackQueryHandler
)

# ===============================
# GLOBAL CACHES (10K+ GROUP OPTIMIZATION)
# ===============================
BOT_ADMIN_CACHE = set()        # (chat_id) → bot admin cache
REMINDER_MESSAGES = {}
PENDING_BROADCAST = {}
USER_ADMIN_CACHE = {}  # {chat_id: set(user_id)}

# ===============================
# CONFIG
# ===============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
START_IMAGE = "https://i.postimg.cc/q7PtfZYj/Untitled-design-(16).png"

# ===============================
# MAIN DATABASE (users / groups)
# ===============================
db_conn = sqlite3.connect(
    "database.db",
    check_same_thread=False,
    isolation_level=None   # ✅ auto-commit (SQLite lock issue fix)
)
db_cur = db_conn.cursor()

db_cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY
)
""")

db_cur.execute("""
CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY
)
""")

def save_user_db(user_id: int):
    try:
        db_cur.execute(
            "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
            (user_id,)
        )
    except:
        pass

def save_group_db(group_id: int):
    try:
        db_cur.execute(
            "INSERT OR IGNORE INTO groups (group_id) VALUES (?)",
            (group_id,)
        )
    except:
        pass

# ===============================
# JOB DATABASE (delete jobs / spam counter)
# ===============================
job_conn = sqlite3.connect(
    "jobs.db",
    check_same_thread=False,
    isolation_level=None   # ✅ prevent database locked (important for 10K groups)
)
job_cur = job_conn.cursor()

job_cur.execute("""
CREATE TABLE IF NOT EXISTS delete_jobs (
    chat_id INTEGER,
    message_id INTEGER,
    run_at INTEGER
)
""")

job_cur.execute("""
CREATE TABLE IF NOT EXISTS link_spam (
    chat_id INTEGER,
    user_id INTEGER,
    count INTEGER,
    last_time INTEGER,
    PRIMARY KEY (chat_id, user_id)
)
""")

# ===============================
# /start (PRIVATE)
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_chat or update.effective_chat.type != "private":
        return

    if not update.message or not update.effective_user:
        return

    user = update.effective_user
    save_user_db(user.id)   # ✅ safe (already fixed DB)

    bot = await context.bot.get_me()

    bot_username = bot.username or ""

    user_name = user.first_name or "User"
    user_mention = f"<a href='tg://user?id={user.id}'>{user_name}</a>"
    bot_mention = (
        f"<a href='https://t.me/{bot_username}'>{bot.first_name}</a>"
        if bot_username else bot.first_name
    )

    text = (
        f"<b>────「 {bot_mention} 」────</b>\n\n"
        f"<b>ဟယ်လို {user_mention} ! 👋</b>\n\n"
        "<b>ငါသည် Group များအတွက် Link ဖျက် Bot တစ်ခုဖြစ်တယ်။</b>\n"
        "<b>ငါ၏လုပ်နိုင်စွမ်းကို ကောင်းကောင်းအသုံးချပါ။</b>\n\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        "<b>📌 ငါ၏လုပ်နိုင်စွမ်း</b>\n\n"
        "✅ Auto Link Delete ( Setting ချိန်းစရာမလိုပဲ ချက်ချင်း အလုပ်လုပ်။ )\n"
        "✅ Spam Link Mute ( Link 3 ခါ ပို့ရင် 10 မိနစ် Auto Mute )\n\n"
        "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
        "<b>📥 ငါ့ကိုအသုံးပြုရန်</b>\n\n"
        "➕ ငါ့ကို Group ထဲထည့်ပါ\n"
        "⭐️ ငါ့ကို Admin ပေးပါ"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ ADD ME TO YOUR GROUP",
                    url=f"https://t.me/{bot_username}?startgroup=true"
                )
            ],
            [
                InlineKeyboardButton(
                    "👨‍💻 DEVELOPER",
                    url="https://t.me/callmeoggy"
                ),
                InlineKeyboardButton(
                    "📢 CHANNEL",
                    url="https://t.me/MMTelegramBotss"
                )
            ]
        ]
    )

    await update.message.reply_photo(
        photo=START_IMAGE,
        caption=text,
        parse_mode="HTML",
        reply_markup=keyboard
    )

# ===============================
# ⏱️ DELETE JOB CONFIG
# ===============================
DELETE_AFTER = 600  # 10 minutes


# ===============================
# 🧹 JOB FUNCTION
# ===============================
async def delete_warn_job(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return

    data = context.job.data
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    try:
        await context.bot.delete_message(chat_id, message_id)
    except:
        pass

    try:
        job_cur.execute(
            "DELETE FROM delete_jobs WHERE chat_id=? AND message_id=?",
            (chat_id, message_id)
        )
    except:
        pass

# ===============================
# 🔗 AUTO LINK DELETE (OPTIMIZED)
# ===============================
async def auto_delete_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message
    user = update.effective_user

    if not chat or not message or not user:
        return

    # 🔥 IMPORTANT FIX
    if message.text and message.text.startswith("/"):
        return

    if chat.type not in ("group", "supergroup"):
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
            BOT_ADMIN_CACHE.add(chat_id)   # ✅ cache admin group
        except:
            return

    # ===============================
    # 👤 USER ADMIN BYPASS
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
    # Link detect (improved)
    # ==============================

    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)

    text = (message.text or message.caption or "").lower()

    has_link = False

    for e in entities:
        # Detect real URLs & text_link entities
        if e.type in ("url", "text_link"):
            has_link = True
            break

    # Fallback: catch t.me, http(s) even without entity
    if not has_link:
        if "http://" in text or "https://" in text or "t.me/" in text:
            has_link = True

    if not has_link:
        return


    try:
        # 🔗 spam counter (no return impact)
        asyncio.create_task(link_spam_control(update, context))

        # 🗑 delete MUST always run
        await message.delete()




        warn = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⚠️ ({user.first_name}) မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီ။\n"
                "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူ။"
            )
        )

        run_at = int(time.time()) + DELETE_AFTER

        # 💾 save delete job (single connection)
        job_cur.execute(
            "INSERT INTO delete_jobs VALUES (?, ?, ?)",
            (chat_id, warn.message_id, run_at)
        )
        job_conn.commit()

        context.job_queue.run_once(
            delete_warn_job,
            when=DELETE_AFTER,
            data={
                "chat_id": chat_id,
                "message_id": warn.message_id
            }
        )

    except:
        pass

# ===============================
# 🔄 RESTORE JOBS ON START
# ===============================
async def restore_jobs(app):
    now = int(time.time())

    rows = job_cur.execute(
        "SELECT chat_id, message_id, run_at FROM delete_jobs"
    ).fetchall()

    for chat_id, message_id, run_at in rows:
        delay = max(0, run_at - now)

        app.job_queue.run_once(
            delete_warn_job,
            when=delay,
            data={
                "chat_id": chat_id,
                "message_id": message_id
            }
        )

# ===============================
# Save Group (OPTIMIZED)
# ===============================
async def save_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        return

    if chat.id in BOT_ADMIN_CACHE:
        save_group_db(chat.id)
        return

    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status in ("administrator", "creator"):
            BOT_ADMIN_CACHE.add(chat.id)
            save_group_db(chat.id)
    except:
        pass


# ===============================
# 📢 BROADCAST 
# ===============================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.effective_message   # ✅ FIX HERE
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
# Confirm Button 
# ===============================
async def broadcast_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data = PENDING_BROADCAST.pop(user_id, None)

    if not data:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    sent_users = 0
    sent_groups = 0

    users = db_cur.execute("SELECT user_id FROM users").fetchall()
    for (uid,) in users:
        try:
            await send_content(context, uid, data)
            sent_users += 1
            await asyncio.sleep(0.05)
        except:
            db_cur.execute("DELETE FROM users WHERE user_id=?", (uid,))
            db_conn.commit()

    groups = db_cur.execute("SELECT group_id FROM groups").fetchall()
    for (gid,) in groups:
        try:
            await send_content(context, gid, data)
            sent_groups += 1
            await asyncio.sleep(0.05)
        except:
            db_cur.execute("DELETE FROM groups WHERE group_id=?", (gid,))
            db_conn.commit()

    await query.edit_message_text(
        f"✅ <b>Broadcast Done</b>\n\n"
        f"👤 Users: {sent_users}\n"
        f"👥 Groups: {sent_groups}",
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
# Admin Permission + ThankYou
# ===============================
async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.my_chat_member:
        return

    chat = update.effective_chat
    if not chat:                     # ✅ FIX 1
        return

    old = update.my_chat_member.old_chat_member
    new = update.my_chat_member.new_chat_member
    if not old or not new or not new.user:   # ✅ FIX 2
        return

    # ✅ FAST PATH (cache hit)
    if chat.id in BOT_ADMIN_CACHE:
        save_group_db(chat.id)
        is_admin = True
    else:
        try:
            me = await context.bot.get_chat_member(chat.id, context.bot.id)
            is_admin = me.status in ("administrator", "creator")
            if is_admin:
                BOT_ADMIN_CACHE.add(chat.id)
                save_group_db(chat.id)
        except:
            is_admin = False

    # ===============================
    # 🟢 1) BOT PROMOTED TO ADMIN → THANK YOU
    # ===============================
    if (
        new.user.id == context.bot.id
        and new.status == "administrator"
        and old.status != "administrator"
    ):
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

        for msg_id in REMINDER_MESSAGES.get(chat.id, []):
            try:
                await context.bot.delete_message(chat.id, msg_id)
            except:
                pass

        REMINDER_MESSAGES.pop(chat.id, None)
        return

    # ===============================
    # 🟡 2) BOT ADDED BUT NOT ADMIN → ASK PERMISSION
    # ===============================
    if (
        not is_admin
        and new.user.id == context.bot.id
        and old.status in ("left", "kicked")
        and new.status == "member"
    ):
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

# ===============================
# Admin Reminder
# ===============================
async def admin_reminder(context: ContextTypes.DEFAULT_TYPE):

    # ✅ FIX: job / data မရှိရင် stop (Error fix only)
    if not context.job or not context.job.data:
        return

    chat_id = context.job.data["chat_id"]
    count = context.job.data["count"]
    total = context.job.data["total"]

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator"):
            return  # ✅ Admin ဖြစ်ပြီးသား → Reminder မပို့

        msg = await context.bot.send_message(
            chat_id,
            f"⏰ <b>Reminder ({count}/{total})</b>\n\n"
            "🤖 Bot ကို အလုပ်လုပ်နိုင်ရန်\n"
            "⭐️ <b>Admin Permission ပေးပါ</b>\n\n"
            "⚠️ Required: Delete messages",
            parse_mode="HTML"
        )

        REMINDER_MESSAGES.setdefault(chat_id, []).append(msg.message_id)

    except:
        pass


# ===============================
# delete message job
# ===============================
async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):

    # ✅ FIX: job / data guard
    if not context.job or not context.job.data:
        return

    data = context.job.data
    try:
        await context.bot.delete_message(
            data["chat_id"],
            data["message_id"]
        )
    except:
        pass


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
LINK_KEYWORDS = ("http://", "https://", "t.me/")

async def link_spam_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message  # ✅ FIX

    if not chat or not user or not message:
        return

    if chat.type not in ("group", "supergroup"):
        return

    # ==============================
    # Link detect (improved)
    # ==============================

    entities = []
    if message.entities:
        entities.extend(message.entities)
    if message.caption_entities:
        entities.extend(message.caption_entities)

    text = (message.text or message.caption or "").lower()

    has_link = False

    for e in entities:
        # Detect real URLs & text_link entities
        if e.type in ("url", "text_link"):
            has_link = True
            break

    # Fallback: catch t.me, http(s) even without entity
    if not has_link:
        if "http://" in text or "https://" in text or "t.me/" in text:
            has_link = True

    if not has_link:
        return

    # Admin bypass
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status in ("administrator", "creator"):
            return
    except:
        return

    # ⚠️ mute is supergroup-only, but DO NOT return
    is_supergroup = (chat.type == "supergroup")


    now = int(time.time())

    row = job_cur.execute(
        "SELECT count FROM link_spam WHERE chat_id=? AND user_id=?",
        (chat.id, user.id)
    ).fetchone()

    if row:
        count = row[0] + 1
        job_cur.execute(
            "UPDATE link_spam SET count=?, last_time=? WHERE chat_id=? AND user_id=?",
            (count, now, chat.id, user.id)
        )
    else:
        count = 1
        job_cur.execute(
            "INSERT INTO link_spam VALUES (?, ?, ?, ?)",
            (chat.id, user.id, count, now)
        )

    job_conn.commit()

    # 🚨 Limit reached → mute
    if count >= LINK_LIMIT and is_supergroup:
        until = now + MUTE_SECONDS

        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until
        )

        await context.bot.send_message(
            chat.id,
            f"🔇 <b>{user.first_name}</b> ကို\n"
            f"🔗 Link {LINK_LIMIT} ကြိမ် ပို့လို့\n"
            f"⏰ 10 မိနစ် mute လုပ်လိုက်ပါပြီ",
            parse_mode="HTML"
        )

        job_cur.execute(
            "DELETE FROM link_spam WHERE chat_id=? AND user_id=?",
            (chat.id, user.id)
        )
        job_conn.commit()

# ===============================
# /refresh (ADMIN ONLY)
# ===============================
async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if not chat or not user or chat.type not in ("group", "supergroup"):
        return

    # Admin only
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
    print("🔄 Refreshing admin cache...")

    rows = db_cur.execute(
        "SELECT group_id FROM groups"
    ).fetchall()

    for (chat_id,) in rows:
        try:
            me = await app.bot.get_chat_member(chat_id, app.bot.id)
            if me.status in ("administrator", "creator"):
                BOT_ADMIN_CACHE.add(chat_id)
        except:
            pass

    print(f"✅ Admin cache loaded: {len(BOT_ADMIN_CACHE)} groups")

# ===============================
# MAIN
# ===============================
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    # 🔗 Auto delete + spam control (combined logic)
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION),
            auto_delete_links
        ),
        group=0
    )

    app.add_handler(
        MessageHandler(
            filters.User(OWNER_ID)
            & (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL),
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

    app.add_handler(
        ChatMemberHandler(
            on_my_chat_member,
            ChatMemberHandler.MY_CHAT_MEMBER
        )
    )
    
    app.add_handler(CommandHandler("refresh", refresh))
    
    async def on_startup(app):
      await restore_jobs(app)
      await refresh_admin_cache(app)

    app.post_init = on_startup

    print("🤖 Link Delete Bot running.....")
    app.run_polling()


if __name__ == "__main__":
    main()
