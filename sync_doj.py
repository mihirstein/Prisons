# sync_doj.py
import time
import requests
from bs4 import BeautifulSoup
from setup_db import query, execute

# Polite headers so DOJ doesn't block you
HEADERS = {
    "User-Agent": "GroundTruth Legal Research Tool (educational project)"
}


def scrape_doj_special_litigation():
    """Scrape the DOJ Special Litigation Section cases page."""
    url = "https://www.justice.gov/crt/special-litigation-section-cases-and-matters"
    print(f"Fetching: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch DOJ page: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    cases = []

    # DOJ pages use various structures — try multiple selectors
    # You WILL need to adjust these after inspecting the actual page
    for link in soup.find_all("a"):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        # Filter for prison/jail related links
        prison_keywords = [
            "prison", "jail", "correctional", "penitentiary",
            "detention", "incarcerat", "inmate", "CRIPA",
            "conditions of confinement"
        ]

        if any(kw in text.lower() for kw in prison_keywords) and len(text) > 20:
            full_url = href if href.startswith("http") else f"https://www.justice.gov{href}"
            cases.append({
                "title": text,
                "url": full_url,
            })

    print(f"  Found {len(cases)} prison-related links")
    return cases


def scrape_doj_press_releases(pages=3):
    """Scrape DOJ Civil Rights Division press releases."""
    all_releases = []

    for page in range(pages):
        url = f"https://www.justice.gov/crt/press-releases?page={page}"
        print(f"Fetching press releases page {page}...")

        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"  Failed: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # Find press release entries
        for item in soup.select(".views-row"):
            title_el = item.select_one("h2 a, .field-content a, a")
            date_el = item.select_one(".date-display-single, .datetime, time")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            href = title_el.get("href", "")
            date = date_el.get_text(strip=True) if date_el else ""

            # Filter for prison/jail related
            prison_keywords = [
                "prison", "jail", "correctional", "penitentiary",
                "detention", "inmate", "CRIPA", "incarcerat"
            ]

            if any(kw in title.lower() for kw in prison_keywords):
                full_url = href if href.startswith("http") else f"https://www.justice.gov{href}"
                all_releases.append({
                    "title": title,
                    "url": full_url,
                    "date": date,
                })

        time.sleep(2)  # be polite

    print(f"  Found {len(all_releases)} prison-related press releases")
    return all_releases


def match_to_facility(text):
    """Try to match text content to a facility in our database."""
    facilities = query("SELECT * FROM facilities")
    text_lower = text.lower()

    for facility in facilities:
        # Check primary name
        if facility["name"].lower() in text_lower:
            return facility

        # Check aliases
        for alias in (facility.get("aliases") or []):
            if alias.lower() in text_lower:
                return facility

    return None


def infer_action_type(title):
    """Determine the type of DOJ action from its title."""
    t = title.lower()
    if "findings" in t or "investigation finds" in t:
        return "findings_letter"
    if "consent decree" in t or "settlement" in t or "agreement" in t:
        return "consent_decree"
    if "opens investigation" in t or "announces investigation" in t:
        return "investigation_opening"
    if "compliance" in t or "monitor" in t:
        return "compliance_report"
    return "press_release"


def sync_doj():
    """Main DOJ sync function."""
    print("=== DOJ SYNC STARTING ===\n")

    # Get cases from the Special Litigation page
    sl_cases = scrape_doj_special_litigation()
    time.sleep(2)

    # Get press releases
    press_releases = scrape_doj_press_releases(pages=3)

    # Combine all DOJ items
    all_items = []
    for case in sl_cases:
        all_items.append(case)
    for release in press_releases:
        all_items.append(release)

    # Deduplicate by title
    seen = set()
    unique_items = []
    for item in all_items:
        if item["title"] not in seen:
            seen.add(item["title"])
            unique_items.append(item)

    print(f"\n{len(unique_items)} unique DOJ items to process")

    # Match each item to a facility and store
    matched = 0
    for item in unique_items:
        facility = match_to_facility(item["title"])

        if facility:
            print(f"  MATCH: '{item['title'][:60]}...' -> {facility['name']}")

            try:
                execute(
                    """INSERT INTO doj_actions 
                       (facility_id, title, action_date, action_type, agency, summary, pdf_url, last_synced)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (facility_id, title) DO UPDATE SET
                         last_synced = NOW()""",
                    (
                        facility["id"],
                        item["title"],
                        item.get("date"),
                        infer_action_type(item["title"]),
                        "DOJ Civil Rights Division — Special Litigation Section",
                        item["title"],  # use title as summary for now
                        item["url"],
                    )
                )
                matched += 1
            except Exception as e:
                print(f"  Error storing: {e}")

    # Print summary
    print(f"\n=== DOJ SYNC COMPLETE ===")
    print(f"Matched {matched} items to facilities")

    counts = query("""
        SELECT f.name, COUNT(d.id) as action_count
        FROM facilities f
        LEFT JOIN doj_actions d ON d.facility_id = f.id
        GROUP BY f.name
        HAVING COUNT(d.id) > 0
        ORDER BY action_count DESC
    """)
    for row in counts:
        print(f"  {row['name']}: {row['action_count']} DOJ actions")

if __name__ == "__main__":
    sync_doj()