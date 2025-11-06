import os
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

_DB_URL = os.getenv("DB_URL")
if _DB_URL and not _DB_URL.startswith("postgresql+psycopg"):
    # allow raw postgresql:// to be passed; upgrade to async driver
    _DB_URL = _DB_URL.replace("postgresql://", "postgresql+psycopg://")

engine: AsyncEngine | None = None

async def get_engine() -> AsyncEngine:
    global engine
    if engine is None:
        if not _DB_URL:
            raise RuntimeError("DB_URL not set")
        engine = create_async_engine(_DB_URL, pool_pre_ping=True)
    return engine

async def init_db():
    eng = await get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("""        CREATE TABLE IF NOT EXISTS articles (
          id SERIAL PRIMARY KEY,
          source TEXT NOT NULL,
          title TEXT NOT NULL,
          url TEXT UNIQUE NOT NULL,
          published_at TIMESTAMPTZ,
          ticker TEXT[],
          category TEXT,
          sentiment INT,
          exposure INT DEFAULT 1,
          score INT,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """))
        await conn.execute(text("""        CREATE TABLE IF NOT EXISTS deliveries (
          id SERIAL PRIMARY KEY,
          article_url TEXT UNIQUE NOT NULL,
          delivered_at TIMESTAMPTZ DEFAULT now()
        );
        """))

async def upsert_articles(items: list[dict]):
    if not items:
        return
    eng = await get_engine()
    async with eng.begin() as conn:
        for it in items:
            await conn.execute(
                text("""                INSERT INTO articles(source,title,url,published_at)
                VALUES (:source,:title,:url,:published_at)
                ON CONFLICT (url) DO UPDATE SET title=EXCLUDED.title
                """),
                {
                    "source": it.get("source",""),
                    "title": it.get("title",""),
                    "url": it.get("url",""),
                    "published_at": it.get("published_at")
                }
            )

async def score_articles():
    # trivial baseline scoring; extend later
    eng = await get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("""            UPDATE articles
            SET score = COALESCE(score, 0) + 1,
                category = COALESCE(category, 'news')
            WHERE score IS NULL OR category IS NULL;
        """))

async def list_undelivered(limit: int = 20) -> list[dict]:
    eng = await get_engine()
    async with eng.connect() as conn:
        res = await conn.execute(text("""            SELECT a.source, a.title, a.url, a.published_at, a.category, a.score
            FROM articles a
            LEFT JOIN deliveries d ON d.article_url = a.url
            WHERE d.id IS NULL
            ORDER BY a.published_at DESC NULLS LAST, a.id DESC
            LIMIT :limit
        """), {"limit": limit})
        rows = res.mappings().all()
        return [dict(r) for r in rows]

async def mark_delivered(url: str):
    eng = await get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("""            INSERT INTO deliveries(article_url) VALUES (:url)
            ON CONFLICT (article_url) DO NOTHING;
        """), {"url": url})
