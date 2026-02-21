# add_news.py
from Backend.Scrapers.Setup.db import execute

execute("""
    CREATE TABLE IF NOT EXISTS news (
        id SERIAL PRIMARY KEY,
        facility_id INTEGER REFERENCES facilities(id),
        title TEXT NOT NULL,
        source TEXT,
        url TEXT,
        published_date DATE,
        snippet TEXT,
        last_synced TIMESTAMP DEFAULT NOW(),
        UNIQUE(facility_id, url)
    )
""")
print("News table created.")
