from dataclasses import dataclass

@dataclass
class Article:
    source: str
    title: str
    url: str
    published_at: str | None = None
