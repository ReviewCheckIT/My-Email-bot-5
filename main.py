# -*- coding: utf-8 -*-
import logging
import os
import json
import asyncio
import random
import string
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    ContextTypes, 
    CallbackQueryHandler,
    filters
)
import firebase_admin
from firebase_admin import credentials, db

# --- Load Environment Variables ---
load_dotenv()

# --- Logging Setup ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Environment Variables ---
TOKEN = os.environ.get('EMAIL_BOT_TOKEN')
OWNER_ID = os.environ.get('BOT_OWNER_ID')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '10000'))
GAS_URL_ENV = os.environ.get('GAS_URL')

# Gemini API Keys (Comma separated)
GEMINI_KEYS_STR = os.environ.get('GEMINI_API_KEYS', '') 
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS_STR.split(',') if k.strip()]

# --- Global Control ---
IS_SENDING = False
CURRENT_KEY_INDEX = 0
BOT_ID_PREFIX = TOKEN.split(':')[0] if TOKEN else "bot"

# --- Firebase Initialization ---
try:
    if not firebase_admin._apps:
        if FB_JSON:
            try:
                if os.path.exists(FB_JSON):
                    cred = credentials.Certificate(FB_JSON)
                else:
                    cred_dict = json.loads(FB_JSON)
                    cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
                logger.info("🔥 Firebase Connected!")
            except Exception as e:
                logger.error(f"❌ Firebase Auth Error: {e}")
except Exception as e:
    logger.error(f"❌ Firebase Init Error: {e}")

def is_owner(uid):
    return str(uid) == str(OWNER_ID)

# --- AI Helper Functions ---
def get_next_api_key():
    global CURRENT_KEY_INDEX
    if not GEMINI_KEYS: return None
    key = GEMINI_KEYS[CURRENT_KEY_INDEX % len(GEMINI_KEYS)]
    CURRENT_KEY_INDEX += 1
    return key

async def rewrite_email_with_ai(original_sub, original_body, app_name):
    if not GEMINI_KEYS: return original_sub, original_body
    for _ in range(len(GEMINI_KEYS)):
        api_key = get_next_api_key()
        if not api_key: break
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        prompt = f"""
        As a professional App Growth Specialist, rewrite the following email for the app "{app_name}".
        RULES:
        1. Keep the CORE message: Organic installs, real reviews, and ranking growth.
        2. Change the sentences, structure, and greetings to make it unique every time.
        3. Output format must be EXACTLY: Subject: [New Subject] ||| Body: [New Body]
        Original Subject: {original_sub}
        Original Body: {original_body}
        """
        try:
            response = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30)
            if response.status_code == 200:
                text = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                if "|||" in text:
                    parts = text.split("|||")
                    return parts[0].replace("Subject:", "").strip(), parts[1].replace("Body:", "").strip().replace('\n', '<br>')
        except: pass
        await asyncio.sleep(1)
    return original_sub, original_body

# --- Helper Functions ---
def get_gas_url():
    try:
        stored_url = db.reference(f'bot_configs/{BOT_ID_PREFIX}/gas_url').get()
        return stored_url if stored_url else GAS_URL_ENV
    except: return GAS_URL_ENV

def generate_random_id(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def call_gas_api(payload):
    url = get_gas_url()
    try:
        response = requests.post(url, json=payload, timeout=60, allow_redirects=True)
        return response.json() if response.status_code == 200 else {"status": "error"}
    except: return {"status": "error"}

def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🚀 Start Sending", callback_data='btn_start_send')],
        [InlineKeyboardButton("🛑 Stop", callback_data='btn_stop_send')],
        [InlineKeyboardButton("📊 Report", callback_data='btn_stats'),
         InlineKeyboardButton("📝 Set Email", callback_data='btn_set_content')],
        [InlineKeyboardButton("🔄 Reset DB", callback_data='btn_reset_all')]
    ]
    return InlineKeyboardMarkup(keyboard)

def back_button():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='btn_main_menu')]])

# --- Background Worker (Stable Version) ---
async def email_worker(context: ContextTypes.DEFAULT_TYPE):
    global IS_SENDING
    chat_id = context.job.chat_id
    
    config = db.reference('shared_config/email_template').get()
    leads_ref = db.reference('scraped_emails')
    if not config:
        await context.bot.send_message(chat_id, "⚠️ ইমেইল টেম্পলেট নেই!")
        IS_SENDING = False
        return

    count = 0
    await context.bot.send_message(chat_id, f"🤖 Bot {BOT_ID_PREFIX} কাজ শুরু করেছে...")

    while IS_SENDING:
        # মাদারচোদ, এখানে order_by_key() যোগ করা হয়েছে যেন limit কাজ করে
        # এবং equal_to(None) সরায়ে পাইথন দিয়ে ফিল্টার করা হচ্ছে
        try:
            all_leads = leads_ref.order_by_key().limit_to_first(100).get()
        except Exception as e:
            logger.error(f"❌ DB Fetch Error: {e}")
            await asyncio.sleep(30)
            continue

        if not all_leads:
            await context.bot.send_message(chat_id, "ℹ️ ডাটাবেজে নতুন কোনো ইমেইল পাওয়া যায়নি।")
            break
        
        target_key = None
        target_data = None
        now = datetime.now()
        
        # ডাটা ফিল্টারিং লজিক (Manual filter to avoid SDK issues)
        for key, val in all_leads.items():
            if val.get('status') is not None:
                continue
                
            proc_by = val.get('processing_by')
            last_ping = val.get('last_ping')
            
            is_locked = False
            if proc_by and last_ping:
                try:
                    last_ping_dt = datetime.fromisoformat(last_ping)
                    if (now - last_ping_dt) < timedelta(minutes=5):
                        is_locked = True
                except: pass

            if not is_locked:
                target_key = key
                target_data = val
                break
        
        if not target_key:
            await asyncio.sleep(30)
            continue

        # Lock the lead
        leads_ref.child(target_key).update({
            'processing_by': BOT_ID_PREFIX,
            'last_ping': now.isoformat()
        })
        
        email = target_data.get('email')
        app_name = target_data.get('app_name', 'your app')
        
        orig_sub = config.get('subject', '').replace('{app_name}', app_name)
        orig_body = config.get('body', '').replace('{app_name}', app_name)
        
        final_subject, ai_body = await rewrite_email_with_ai(orig_sub, orig_body, app_name)
        unique_id = generate_random_id()
        final_body = f"{ai_body}<br><br><span style='color:transparent;display:none;'>Ref: {unique_id}</span>"

        res = call_gas_api({"action": "sendEmail", "to": email, "subject": final_subject, "body": final_body})
        
        if res.get("status") == "success":
            leads_ref.child(target_key).update({
                'status': 'sent', 
                'sent_at': now.isoformat(), 
                'sent_by': BOT_ID_PREFIX, 
                'processing_by': None
            })
            count += 1
            if count % 10 == 0:
                await context.bot.send_message(chat_id, f"📊 রিপোর্ট: {count}টি সম্পন্ন।")
            # ৩ থেকে ৫ মিনিট বিরতি (সেফটি)
            await asyncio.sleep(random.randint(180, 300))
        else:
            leads_ref.child(target_key).update({'processing_by': None, 'last_ping': None})
            await asyncio.sleep(60)

    IS_SENDING = False
    await context.bot.send_message(chat_id, f"✅ প্রসেস শেষ। পাঠানো হয়েছে: {count}")

# --- Handlers ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if is_owner(u.effective_user.id):
        await u.message.reply_text("🤖 **AI Email Sender**\nStatus: Online", reply_markup=main_menu_keyboard())

async def button_tap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global IS_SENDING
    query = update.callback_query
    await query.answer()
    
    if query.data == 'btn_main_menu':
        await query.edit_message_text("🤖 **Main Menu**", reply_markup=main_menu_keyboard())
    elif query.data == 'btn_start_send' and not IS_SENDING:
        IS_SENDING = True
        context.job_queue.run_once(email_worker, 1, chat_id=query.message.chat_id)
        await query.edit_message_text("🚀 AI প্রসেস শুরু হচ্ছে...", reply_markup=back_button())
    elif query.data == 'btn_stop_send':
        IS_SENDING = False
        await query.edit_message_text("🛑 Stopping...", reply_markup=back_button())
    elif query.data == 'btn_stats':
        leads = db.reference('scraped_emails').get() or {}
        sent = sum(1 for v in leads.values() if v.get('status') == 'sent')
        await query.edit_message_text(f"📊 Stats: {sent}/{len(leads)}", reply_markup=back_button())
    elif query.data == 'btn_set_content':
        await query.edit_message_text("ব্যবহার:\n`/set_email Subject | Body`", reply_markup=back_button())
    elif query.data == 'btn_reset_all':
        await query.edit_message_text("Type `/confirm_reset` to clear DB.", reply_markup=back_button())

async def set_email_cmd(u, c):
    if is_owner(u.effective_user.id):
        try:
            content = u.message.text.split('/set_email ', 1)[1]
            sub, body = content.split('|', 1)
            db.reference('shared_config/email_template').set({'subject': sub.strip(), 'body': body.strip()})
            await u.message.reply_text("✅ টেম্পলেট সেভ হয়েছে।")
        except: await u.message.reply_text("❌ ফরম্যাট ভুল।")

async def confirm_reset_cmd(u, c):
    if is_owner(u.effective_user.id):
        leads = db.reference('scraped_emails').get() or {}
        for k in leads:
            db.reference(f'scraped_emails/{k}').update({'status': None, 'processing_by': None, 'last_ping': None})
        await u.message.reply_text("🔄 ডাটাবেজ রিসেট সম্পন্ন।")

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set_email", set_email_cmd))
    app.add_handler(CommandHandler("confirm_reset", confirm_reset_cmd))
    app.add_handler(CallbackQueryHandler(button_tap))

    if RENDER_URL:
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], 
                        webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}", allowed_updates=Update.ALL_TYPES)
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
