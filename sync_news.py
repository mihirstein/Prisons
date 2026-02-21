# sync_news.py
# Pulls news from Google News RSS for each facility

import xml.etree.ElementTree as ET
import requests
import time
import html
import re
from datetime import datetime
from setup_db import query, execute

HEADERS = {
    "User-Agent": "GroundTruth Legal Research (educational project)"
}

def clean_html(text):
    """Strip HTML tags from snippet."""
    return re.sub(r'<[^>]+>', '', html.unescape(text or ""))

def parse_date(date_str):
    """Parse RSS date format."""
    try:
        # Google News RSS format: "Mon, 10 Feb 2025 08:00:00 GMT"
        return datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %Z").date()
    except:
        try:
            return datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %z").date()
        except:
            return None

def fetch_news_for_facility(facility):
    """Fetch news from Google News RSS."""
    name = facility["name"]
    # Build search query — use facility name + prison/jail context
    search_terms = f'"{name}" prison OR jail OR inmates OR corrections'
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(search_terms)}&hl=en-US&gl=US&ceid=US:en"

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"  Failed to fetch news for {name}: {e}")
        return []

    articles = []
    try:
        root = ET.fromstring(response.content)
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            description = item.findtext("description", "")
            source = item.findtext("source", "")

            if not title or not link:
                continue

            articles.append({
                "title": clean_html(title),
                "url": link,
                "source": source or "",
                "published_date": parse_date(pub_date),
                "snippet": clean_html(description)[:500],
            })
    except ET.ParseError as e:
        print(f"  XML parse error for {name}: {e}")

    return articles

def sync():
    facilities = query("SELECT * FROM facilities")
    print(f"Syncing news for {len(facilities)} facilities...\n")

    total = 0
    for f in facilities:
        print(f"  {f['name']}...")
        articles = fetch_news_for_facility(f)
        stored = 0

        for a in articles:
            try:
                execute(
                    """INSERT INTO news (facility_id, title, source, url, published_date, snippet, last_synced)
                       VALUES (%s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (facility_id, url) DO UPDATE SET
                         title = EXCLUDED.title,
                         snippet = EXCLUDED.snippet,
                         last_synced = NOW()""",
                    (f["id"], a["title"], a["source"], a["url"], a["published_date"], a["snippet"])
                )
                stored += 1
            except Exception as e:
                print(f"    Error: {e}")

        print(f"    {stored} articles stored ({len(articles)} found)")
        total += stored
        time.sleep(1)  # be polite

    print(f"\nDone: {total} total articles stored")

    # Summary
    counts = query("""
        SELECT f.name, COUNT(n.id) as count
        FROM facilities f
        LEFT JOIN news n ON n.facility_id = f.id
        GROUP BY f.name
        HAVING COUNT(n.id) > 0
        ORDER BY count DESC
    """)
    if counts:
        print("\nNews articles by facility:")
        for row in counts:
            print(f"  {row['name']}: {row['count']}")

if __name__ == "__main__":
    sync()