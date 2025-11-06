import asyncio
from datetime import datetime, timezone
from app.fetchers.yahoo import YahooFetcher
from app.fetchers.cnyes import CnyesFetcher
from app.notifier.telegram import send_telegram
from app.db import upsert_articles, list_undelivered, mark_delivered, init_db
from app.ranker import score_articles

FETCHERS = [YahooFetcher(), CnyesFetcher()]

def render_message(art: dict) -> str:
    # very simple message composer (can be extended later)
    head = f"【{art.get('category', 'news')} / 分數 {art.get('score', 0)}】{art['title']}\n{art['url']}"
    tip  = "\n建議：若量價共振可小量分批；若跌破月線建議先觀望。"
    tp   = "\n停利：+5%~+12% 分段；停損：跌破關鍵均線。"
    alloc= "\n資金：40%/30%/30% 分批規劃，保持現金彈性。"
    return head + tip + tp + alloc

async def kick_once():
    await init_db()
    # 1) fetch
    collected = []
    for f in FETCHERS:
        try:
            items = await f.fetch()
            collected.extend(items)
        except Exception as e:
            print("Fetcher error:", f.__class__.__name__, e)
    # 2) upsert
    await upsert_articles(collected)
    # 3) score
    await score_articles()
    # 4) undelivered
    to_send = await list_undelivered(limit=20)
    # 5) notify
    for art in to_send:
        text = render_message(art)
        await send_telegram(text)
        await mark_delivered(art["url"])
    return {"ingested": len(collected), "delivered": len(to_send), "ts": datetime.now(timezone.utc).isoformat()}
