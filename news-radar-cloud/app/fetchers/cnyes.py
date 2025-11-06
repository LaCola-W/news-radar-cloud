import asyncio, feedparser
from app.fetchers.base import Article

FEEDS = [
    "https://news.cnyes.com/rss/cat/tw_stock",  # 鉅亨台股 RSS（示例）
]

class CnyesFetcher:
    async def fetch(self):
        out = []
        loop = asyncio.get_event_loop()
        for url in FEEDS:
            feed = await loop.run_in_executor(None, feedparser.parse, url)
            for e in feed.entries:
                out.append({
                    "source": "cnyes",
                    "title": e.title,
                    "url": e.link,
                    "published_at": getattr(e, "published", None)
                })
        return out
