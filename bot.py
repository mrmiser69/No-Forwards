# ===============================
# IMPORTS
# ===============================
import sqlite3
import time
from datetime import timedelta
import asyncio

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions   # ✅ FIX HERE
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
# MAIN DATABASE (users / groups)
# ===============================
db_conn = sqlite3.connect("database.db", check_same_thread=False)
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

db_conn.commit()

def save_user_db(user_id: int):
    db_cur.execute(
        "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
        (user_id,)
    )
    db_conn.commit()

def save_group_db(group_id: int):
    db_cur.execute(
        "INSERT OR IGNORE INTO groups (group_id) VALUES (?)",
        (group_id,)
    )
    db_conn.commit()


# ===============================
# JOB DATABASE (delete jobs)
# ===============================
job_conn = sqlite3.connect("jobs.db", check_same_thread=False)
job_cur = job_conn.cursor()

job_cur.execute("""
CREATE TABLE IF NOT EXISTS delete_jobs (
    chat_id INTEGER,
    message_id INTEGER,
    run_at INTEGER
)
""")

job_conn.commit()

# ===============================
# Database Table (link spam counter)
# ===============================
job_cur.execute("""
CREATE TABLE IF NOT EXISTS link_spam (
    chat_id INTEGER,
    user_id INTEGER,
    count INTEGER,
    last_time INTEGER,
    PRIMARY KEY (chat_id, user_id)
)
""")
job_conn.commit()

# ===============================
# SAVE
# ===============================
REMINDER_MESSAGES = {}
PENDING_BROADCAST = {}

# ===============================
# CONFIG
# ===============================
BOT_TOKEN = "8583592706:AAH8yCrsLXr5dDE0O8_BMdSSfbMNoJLkv4E"
OWNER_ID = 5942810488
START_IMAGE = "https://i.postimg.cc/q7PtfZYj/Untitled-design-(16).png"

# ===============================
# /start (PRIVATE)
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_chat or update.effective_chat.type != "private":
        return

    if not update.message or not update.effective_user:
        return

    user = update.effective_user          # ✅ အရင် define
    save_user_db(user.id)                 # ✅ အခုမှ safe

    bot = await context.bot.get_me()

    # ✅ SAFE username
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
# 🔗 AUTO LINK DELETE (JobQueue + SQLite)
# ===============================
import sqlite3
import time
from telegram import Update
from telegram.ext import ContextTypes

# ⏱️ 10 minutes
DELETE_AFTER = 600

# ===============================
# 🗄 DATABASE SETUP
# ===============================
conn = sqlite3.connect("jobs.db", check_same_thread=False)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS delete_jobs (
    chat_id INTEGER,
    message_id INTEGER,
    run_at INTEGER
)
""")
conn.commit()

# ===============================
# 🧹 JOB FUNCTION
# ===============================
async def delete_warn_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id = data["chat_id"]
    message_id = data["message_id"]

    try:
        await context.bot.delete_message(chat_id, message_id)
    except:
        pass

    cur.execute(
        "DELETE FROM delete_jobs WHERE chat_id=? AND message_id=?",
        (chat_id, message_id)
    )
    conn.commit()

# ===============================
# 🔗 AUTO LINK DELETE
# ===============================
async def auto_delete_links(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat = update.effective_chat
    message = update.message
    user = update.effective_user

    if not chat or not message or not user:
        return

    # 🧠 BOT admin auto-check
    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status not in ("administrator", "creator"):
            return
    except:
        return

    if chat.type not in ("group", "supergroup"):
        return

    # admin bypass
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status in ("administrator", "creator"):
            return
    except:
        return

    text = (message.text or message.caption or "").lower()
    if not text:
        return

    if "http://" in text or "https://" in text or "t.me/" in text:
        try:
            await link_spam_control(update, context)  # ✅ ERROR FIX (ဒီလိုင်းပဲ ထည့်)

            await message.delete()

            warn = await context.bot.send_message(
            chat_id=chat.id,
            text=(
                f"⚠️ ({user.first_name}) မင်းရဲ့စာကို ဖျက်လိုက်ပါပြီ။\n"
                "အကြောင်းပြချက်: 🔗 Link ပို့လို့ မရပါဘူ။"
            )
        )

            run_at = int(time.time()) + DELETE_AFTER

            # ✅ FIXED HERE
            cur.execute(
                "INSERT INTO delete_jobs VALUES (?, ?, ?)",
                (chat.id, warn.message_id, run_at)
            )
            conn.commit()

            context.job_queue.run_once(
                delete_warn_job,
                when=DELETE_AFTER,
                data={
                    "chat_id": chat.id,
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

    rows = cur.execute(
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
# Save Group
# ===============================
async def save_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        return

    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        if me.status in ("administrator", "creator"):
            save_group_db(chat.id)  # ✅ DB ထဲ သိမ်း
    except:
        pass


# ===============================
# 📢 BROADCAST 
# ===============================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return

    msg = update.message
    if not msg:
        return

    text = msg.text or msg.caption
    if text and text.startswith("/broadcast"):
        text = text.replace("/broadcast", "", 1).strip()

    # ✅ FIX: indentation only
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

    # ✅ FIX: guard clause (Error fix only)
    if not update.my_chat_member:
        return

    chat = update.effective_chat
    old = update.my_chat_member.old_chat_member
    new = update.my_chat_member.new_chat_member

    # 🧠 Admin auto-check (တစ်ခါတည်း)
    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
        is_admin = me.status in ("administrator", "creator")
    except:
        is_admin = False

    if is_admin:
        save_group_db(chat.id)

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
    message = update.message

    if not chat or not user or not message:
        return

    if chat.type not in ("group", "supergroup"):
        return

    text = (message.text or message.caption or "").lower()
    if not any(k in text for k in LINK_KEYWORDS):
        return

    # Admin bypass
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status in ("administrator", "creator"):
            return
    except:
        return

    # ✅ ADD THIS
    if chat.type != "supergroup":
        return

    now = int(time.time())

    row = cur.execute(
        "SELECT count FROM link_spam WHERE chat_id=? AND user_id=?",
        (chat.id, user.id)
    ).fetchone()

    if row:
        count = row[0] + 1
        cur.execute(
            "UPDATE link_spam SET count=?, last_time=? WHERE chat_id=? AND user_id=?",
            (count, now, chat.id, user.id)
        )
    else:
        count = 1
        cur.execute(
            "INSERT INTO link_spam VALUES (?, ?, ?, ?)",
            (chat.id, user.id, count, now)
        )

    conn.commit()

    # 🚨 Limit reached → mute
    if count >= LINK_LIMIT:
        print("DEBUG: MUTE TRIGGERED", chat.id, user.id, count)  # ✅ ဒီလိုင်း

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

        # reset counter after mute
        cur.execute(
            "DELETE FROM link_spam WHERE chat_id=? AND user_id=?",
            (chat.id, user.id)
        )
        conn.commit()

# ===============================
# MAIN
# ===============================
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    # 1️⃣ link spam control အရင်
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION),
            link_spam_control
        ),
        group=0
)

    # 2️⃣ auto delete နောက်
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION),
            auto_delete_links
        ),
        group=1
)

    app.add_handler(
    MessageHandler(
        filters.User(OWNER_ID)
        & (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL),
        broadcast
    )
)
    app.add_handler(CallbackQueryHandler(broadcast_confirm_handler, pattern="broadcast_confirm"))
    app.add_handler(CallbackQueryHandler(broadcast_cancel_handler, pattern="broadcast_cancel"))

    app.add_handler(
    ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
)
    
    app.post_init = restore_jobs

    print("🤖 Link Delete Bot running.....")
    app.run_polling()

if __name__ == "__main__":
    main()