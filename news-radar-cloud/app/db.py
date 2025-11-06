import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_DB_URL = os.getenv("DB_URL")
if _DB_URL and not _DB_URL.startswith("postgresql+psycopg"):
    _DB_URL = _DB_URL.replace("postgresql://", "postgresql+psycopg://")

engine = None

async def get_engine():
    global engine
    if engine is None:
        if not _DB_URL:
            raise RuntimeError("DB_URL not set")
        engine = create_async_engine(_DB_URL, pool_pre_ping=True)
    return engine

async def init_db():
    eng = await get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            published_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """))
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS deliveries (
            id SERIAL PRIMARY KEY,
            article_url TEXT UNIQUE NOT NULL,
            delivered_at TIMESTAMPTZ DEFAULT now()
        );
        """))

async def upsert_articles(items):
    eng = await get_engine()
    async with eng.begin() as conn:
        for it in items:
            await conn.execute(
                text("""
                INSERT INTO articles(source,title,url,published_at)
                VALUES (:source,:title,:url,:published_at)
                ON CONFLICT (url) DO NOTHING
                """),
                it
            )

async def list_undelivered(limit=10):
    eng = await get_engine()
    async with eng.connect() as conn:
        res = await conn.execute(text("""
            SELECT a.source, a.title, a.url, a.published_at FROM articles a
            LEFT JOIN deliveries d ON d.article_url = a.url
            WHERE d.id IS NULL
            ORDER BY a.id DESC LIMIT :limit
        """), {"limit": limit})
        return [dict(r) for r in res.mappings().all()]

async def mark_delivered(url):
    eng = await get_engine()
    async with eng.begin() as conn:
        await conn.execute(text("""
            INSERT INTO deliveries(article_url) VALUES (:url)
            ON CONFLICT (article_url) DO NOTHING;
        """), {"url": url})
