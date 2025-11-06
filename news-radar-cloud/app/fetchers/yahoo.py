import asyncio, feedparser

FEEDS = ["https://tw.stock.yahoo.com/rss"]

class YahooFetcher:
    async def fetch(self):
        out = []
        loop = asyncio.get_event_loop()
        for url in FEEDS:
            feed = await loop.run_in_executor(None, feedparser.parse, url)
            for e in feed.entries:
                out.append({
                    "source": "yahoo",
                    "title": e.title,
                    "url": e.link,
                    "published_at": getattr(e, "published", None)
                })
        return out
