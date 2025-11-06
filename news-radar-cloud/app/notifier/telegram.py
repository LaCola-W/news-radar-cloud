import os, httpx

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

async def send_telegram(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": text}
        await client.post(url, data=data)
