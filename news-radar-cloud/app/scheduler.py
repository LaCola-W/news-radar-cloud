import asyncio
from datetime import datetime, timezone
from app.fetchers.yahoo import YahooFetcher
from app.fetchers.cnyes import CnyesFetcher
from app.notifier.telegram import send_telegram
from app.db import upsert_articles, list_undelivered, mark_delivered, init_db

FETCHERS = [YahooFetcher(), CnyesFetcher()]

def render_message(art: dict) -> str:
    return f"【新聞】{art['title']}\n{art['url']}"

async def kick_once():
    await init_db()
    collected = []
    for f in FETCHERS:
        try:
            items = await f.fetch()
            collected.extend(items)
        except Exception as e:
            print("Fetcher error:", e)
    await upsert_articles(collected)
    to_send = await list_undelivered(limit=10)
    for art in to_send:
        await send_telegram(render_message(art))
        await mark_delivered(art["url"])
    return {"ingested": len(collected), "delivered": len(to_send), "ts": datetime.now(timezone.utc).isoformat()}
