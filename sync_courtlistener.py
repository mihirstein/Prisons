# sync_courtlistener.py
import os
import time
import requests
from setup_db import query, execute
from dotenv import load_dotenv

load_dotenv()

CL_TOKEN = os.getenv("COURTLISTENER_TOKEN")
CL_BASE = "https://www.courtlistener.com/api/rest/v4"

HEADERS = {
    "Authorization": f"Token {CL_TOKEN}",
}

# Prison civil rights search terms
LEGAL_TERMS = '("section 1983" OR "eighth amendment" OR "deliberate indifference" OR "excessive force" OR "conditions of confinement" OR "cruel and unusual" OR "PLRA")'


def search_courtlistener(facility_name):
    """Search CourtListener for cases mentioning this facility."""
    search_query = f'"{facility_name}" AND {LEGAL_TERMS}'
    
    try:
        response = requests.get(
            f"{CL_BASE}/search/",
            params={
                "q": search_query,
                "type": "o",  # opinions
                "order_by": "dateFiled desc",
                "format": "json",
            },
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  Error searching for '{facility_name}': {e}")
        return []


def infer_case_type(text):
    """Classify the case type based on keywords in the text."""
    if not text:
        return "Section 1983"
    t = text.lower()
    if "excessive force" in t or "use of force" in t:
        return "Excessive Force"
    if "medical" in t or "deliberate indifference" in t:
        return "Medical Care"
    if "solitary" in t or "segregation" in t or "shu" in t:
        return "Solitary Confinement"
    if "sexual" in t or "prea" in t or "rape" in t:
        return "Sexual Assault / PREA"
    if "covid" in t or "pandemic" in t:
        return "COVID-19 Conditions"
    if "death" in t or "wrongful death" in t or "died" in t:
        return "Wrongful Death"
    if "conditions" in t:
        return "Conditions of Confinement"
    if "access to court" in t or "legal mail" in t:
        return "Access to Courts"
    return "Section 1983"


def sync_facility(facility):
    """Sync all cases for one facility."""
    facility_id = facility["id"]
    name = facility["name"]
    aliases = facility.get("aliases", [])

    print(f"\nSyncing: {name}")

    # Search by primary name
    all_results = search_courtlistener(name)
    time.sleep(1)  # respect rate limits

    # Also search by aliases
    for alias in aliases:
        if alias.lower() != name.lower():
            alias_results = search_courtlistener(alias)
            all_results.extend(alias_results)
            time.sleep(1)

    # Deduplicate by case name
    seen = set()
    unique = []
    for r in all_results:
        case_name = r.get("caseName", r.get("case_name", ""))
        if case_name and case_name not in seen:
            seen.add(case_name)
            unique.append(r)

    print(f"  Found {len(unique)} unique cases")

    # Store each case
    stored = 0
    for r in unique:
        case_name = r.get("caseName", r.get("case_name", "Unknown"))
        docket_number = r.get("docketNumber", r.get("docket_number", ""))
        court = r.get("court", "")
        date_filed = r.get("dateFiled", r.get("date_filed", None))
        snippet = r.get("snippet", "")
        absolute_url = r.get("absolute_url", "")
        url = f"https://www.courtlistener.com{absolute_url}" if absolute_url else ""
        cl_id = str(r.get("id", ""))

        try:
            execute(
                """INSERT INTO cases 
                   (facility_id, courtlistener_id, docket_number, case_name, court, date_filed, case_type, summary, url, last_synced)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (facility_id, case_name) DO UPDATE SET
                     summary = EXCLUDED.summary,
                     last_synced = NOW()""",
                (facility_id, cl_id, docket_number, case_name, court, date_filed,
                 infer_case_type(snippet), snippet, url)
            )
            stored += 1
        except Exception as e:
            print(f"  Error storing case '{case_name}': {e}")

    print(f"  Stored {stored} cases")


def sync_all():
    """Sync all facilities."""
    facilities = query("SELECT * FROM facilities ORDER BY id")
    print(f"Syncing {len(facilities)} facilities from CourtListener...")

    for facility in facilities:
        sync_facility(facility)
        time.sleep(2)  # pause between facilities

    # Print summary
    counts = query("""
        SELECT f.name, COUNT(c.id) as case_count 
        FROM facilities f 
        LEFT JOIN cases c ON c.facility_id = f.id 
        GROUP BY f.name 
        ORDER BY case_count DESC
    """)
    print("\n=== SYNC COMPLETE ===")
    for row in counts:
        print(f"  {row['name']}: {row['case_count']} cases")


if __name__ == "__main__":
    sync_all()