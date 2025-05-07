import asyncio
from telegram import Bot
import requests

from config import arbi_alarm, tele_chatid
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils

TELEGRAM_BOT_TOKEN = arbi_alarm
TELEGRAM_CHAT_ID = tele_chatid
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def send_alert_sync(message: str, bot_token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print("✅ Message sent")
    except Exception as e:
        print(f"❌ Failed to send message: {e}")

if __name__ == '__main__':
    # send_alert(message=f"test message at {datetime.datetime.now()}")
    send_alert_sync(message='test2')

