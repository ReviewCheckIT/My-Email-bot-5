# -*- coding: utf-8 -*-
import logging
import os
import json
import asyncio
import random
import string
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    ContextTypes, 
    CallbackQueryHandler
)
import firebase_admin
from firebase_admin import credentials, db

# --- Load Environment Variables ---
load_dotenv()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get('EMAIL_BOT_TOKEN')
OWNER_ID = os.environ.get('BOT_OWNER_ID')
FB_JSON = os.environ.get('FIREBASE_CREDENTIALS_JSON')
FB_URL = os.environ.get('FIREBASE_DATABASE_URL')
RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL')
PORT = int(os.environ.get('PORT', '10000'))
GAS_URL_ENV = os.environ.get('GAS_URL')

GEMINI_KEYS_STR = os.environ.get('GEMINI_API_KEYS', '') 
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS_STR.split(',') if k.strip()]

IS_SENDING = False
CURRENT_KEY_INDEX = 0
BOT_UNIQUE_ID = TOKEN.split(':')[0] # বটের টোকেন থেকে আইডি আলাদা করা

# --- Firebase Init ---
try:
    if not firebase_admin._apps:
        if FB_JSON:
            if os.path.exists(FB_JSON):
                cred = credentials.Certificate(FB_JSON)
            else:
                cred = credentials.Certificate(json.loads(FB_JSON))
            firebase_admin.initialize_app(cred, {'databaseURL': FB_URL})
            logger.info(f"🔥 Bot {BOT_UNIQUE_ID} Connected to Firebase!")
except Exception as e:
    logger.error(f"❌ Firebase Init Error: {e}")

# --- AI & Helper Functions ---
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
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        prompt = f"As a professional App Growth Specialist, rewrite the following email for the app '{app_name}'. Output format: Subject: [New Subject] ||| Body: [New Body]\nOriginal Subject: {original_sub}\nOriginal Body: {original_body}"
        try:
            res = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30)
            if res.status_code == 200:
                text = res.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                if "|||" in text:
                    parts = text.split("|||")
                    return parts[0].replace("Subject:", "").strip(), parts[1].replace("Body:", "").strip().replace('\n', '<br>')
        except: pass
        await asyncio.sleep(1)
    return original_sub, original_body

def call_gas_api(payload):
    url = GAS_URL_ENV # সরাসরি ENV থেকে অথবা DB থেকে নিতে পারেন
    try:
        response = requests.post(url, json=payload, timeout=60)
        return response.json() if response.status_code == 200 else {"status": "error"}
    except: return {"status": "error"}

# --- Distributed Worker Logic ---
async def email_worker(context: ContextTypes.DEFAULT_TYPE):
    global IS_SENDING
    chat_id = context.job.chat_id
    
    await context.bot.send_message(chat_id, f"🚀 Worker Bot ({BOT_UNIQUE_ID}) started...")

    while IS_SENDING:
        # ১. ছোট একটি র‍্যান্ডম ডিলে যাতে একাধিক বট একসাথে ডাটাবেজে হিট না করে
        await asyncio.sleep(random.uniform(1, 5))
        
        leads_ref = db.reference('scraped_emails')
        all_leads = leads_ref.order_by_child('status').equal_to(None).limit_to_first(20).get()
        
        if not all_leads:
            await context.bot.send_message(chat_id, "🏁 No more leads to process.")
            break

        target_key = None
        target_data = None

        # ২. মাল্টি-বট সেফ লিড সিলেকশন
        now = datetime.now()
        for key, data in all_leads.items():
            proc_by = data.get('processing_by')
            last_ping = data.get('last_ping')
            
            # যদি কেউ না ধরে থাকে অথবা ৫ মিনিটের বেশি সময় ধরে পেন্ডিং থাকে
            is_locked = False
            if proc_by and last_ping:
                last_ping_dt = datetime.fromisoformat(last_ping)
                if (now - last_ping_dt) < timedelta(minutes=5):
                    is_locked = True

            if not is_locked:
                target_key = key
                target_data = data
                break

        if not target_key:
            await asyncio.sleep(10) # সবাই বিজি থাকলে একটু অপেক্ষা
            continue

        # ৩. লিড লক করা (Atomic Update simulation)
        leads_ref.child(target_key).update({
            'processing_by': BOT_UNIQUE_ID,
            'last_ping': now.isoformat()
        })

        # ৪. ইমেইল প্রসেসিং
        try:
            config = db.reference('shared_config/email_template').get()
            app_name = target_data.get('app_name', 'your app')
            email = target_data.get('email')

            sub, body = await rewrite_email_with_ai(
                config['subject'].replace('{app_name}', app_name),
                config['body'].replace('{app_name}', app_name),
                app_name
            )

            res = call_gas_api({
                "action": "sendEmail", 
                "to": email, 
                "subject": sub, 
                "body": f"{body}<br><br><small>ID: {random.randint(1000,9999)}</small>"
            })

            if res.get("status") == "success":
                leads_ref.child(target_key).update({
                    'status': 'sent',
                    'sent_at': now.isoformat(),
                    'sent_by': BOT_UNIQUE_ID
                })
                # ইমেইল পাঠানোর পর বড় গ্যাপ (৩-৫ মিনিট)
                await asyncio.sleep(random.randint(180, 300))
            else:
                # ব্যর্থ হলে লক ছেড়ে দেওয়া
                leads_ref.child(target_key).update({'processing_by': None, 'last_ping': None})
                await asyncio.sleep(30)

        except Exception as e:
            logger.error(f"Worker Error: {e}")
            leads_ref.child(target_key).update({'processing_by': None})

    IS_SENDING = False

# --- Handlers ---
async def start(u, c):
    if str(u.effective_user.id) == OWNER_ID:
        kb = [[InlineKeyboardButton("🚀 Start", callback_data='btn_start_send'),
               InlineKeyboardButton("🛑 Stop", callback_data='btn_stop_send')]]
        await u.message.reply_text(f"🤖 Bot ID: {BOT_UNIQUE_ID}\nStatus: Online", reply_markup=InlineKeyboardMarkup(kb))

async def button_tap(update, context):
    global IS_SENDING
    query = update.callback_query
    await query.answer()
    if query.data == 'btn_start_send' and not IS_SENDING:
        IS_SENDING = True
        context.job_queue.run_once(email_worker, 1, chat_id=query.message.chat_id)
        await query.edit_message_text("🚀 Sending Started...")
    elif query.data == 'btn_stop_send':
        IS_SENDING = False
        await query.edit_message_text("🛑 Stopping...")

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_tap))

    if RENDER_URL:
        # Webhook with automated health check response for UptimeRobot
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN[-10:], 
                        webhook_url=f"{RENDER_URL}/{TOKEN[-10:]}")
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
