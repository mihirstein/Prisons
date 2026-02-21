# sync_recap.py
# Pulls docket data from CourtListener's RECAP archive.
# Gets active docket entries, attorneys, judges, and document metadata
# for cases linked to facilities in our database.
#
# Uses the same CourtListener API token as sync_courtlistener.py.
# Free tier: 5,000 requests/day.

import os
import re
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from setup_db import execute, query

load_dotenv()

CL_TOKEN = os.getenv("COURTLISTENER_TOKEN")
CL_BASE = "https://www.courtlistener.com/api/rest/v3"
HEADERS = {
    "Authorization": f"Token {CL_TOKEN}",
    "Content-Type": "application/json",
}


def setup_tables():
    """Create docket tables if they don't exist."""
    execute("""
        CREATE TABLE IF NOT EXISTS dockets (
            id SERIAL PRIMARY KEY,
            facility_id INTEGER REFERENCES facilities(id),
            courtlistener_docket_id INTEGER UNIQUE,
            case_name TEXT NOT NULL,
            docket_number TEXT,
            court TEXT,
            court_id TEXT,
            date_filed DATE,
            date_terminated DATE,
            date_last_filing DATE,
            assigned_to TEXT,
            referred_to TEXT,
            nature_of_suit TEXT,
            cause TEXT,
            jury_demand TEXT,
            parties JSONB,
            attorneys JSONB,
            source_url TEXT,
            last_synced TIMESTAMP DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS docket_entries (
            id SERIAL PRIMARY KEY,
            docket_id INTEGER REFERENCES dockets(id),
            entry_number INTEGER,
            date_filed DATE,
            description TEXT,
            document_count INTEGER DEFAULT 0,
            recap_documents JSONB,
            last_synced TIMESTAMP DEFAULT NOW(),
            UNIQUE(docket_id, entry_number)
        )
    """)
    print("  Tables ready: dockets, docket_entries")


def cl_get(endpoint, params=None):
    """Make a rate-limited GET request to CourtListener API."""
    url = f"{CL_BASE}/{endpoint}/"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        time.sleep(1)  # rate limit
        return resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"    API error: {e.response.status_code} — {e.response.text[:200]}")
        return None
    except Exception as e:
        print(f"    Request failed: {e}")
        return None


def search_dockets(facility_name, court=None):
    """Search RECAP archive for dockets related to a facility."""
    # Build search query
    search_terms = f'"{facility_name}"'

    params = {
        "q": search_terms,
        "type": "r",  # RECAP
        "order_by": "dateFiled desc",
    }
    if court:
        params["court"] = court

    print(f"    Searching RECAP: {search_terms}")
    data = cl_get("search", params)
    if not data:
        return []

    results = data.get("results", [])
    print(f"    Found {data.get('count', 0)} total, fetched {len(results)}")
    return results


def get_docket_detail(docket_id):
    """Get full docket details including parties and attorneys."""
    data = cl_get("dockets", {"id": docket_id})
    if not data or not data.get("results"):
        # Try direct fetch
        try:
            url = f"{CL_BASE}/dockets/{docket_id}/"
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            time.sleep(1)
            return resp.json()
        except:
            return None
    return data["results"][0] if data.get("results") else None


def get_docket_entries(docket_id, limit=50):
    """Get recent docket entries for a docket."""
    params = {
        "docket": docket_id,
        "order_by": "-date_filed",
        "page_size": limit,
    }
    data = cl_get("docket-entries", params)
    if not data:
        return []
    return data.get("results", [])


def extract_attorneys(parties_data):
    """Extract attorney names and firms from party data."""
    attorneys = []
    if not parties_data:
        return attorneys

    for party in parties_data:
        party_name = party.get("name", "")
        party_type = party.get("party_type", {})
        if isinstance(party_type, dict):
            party_type = party_type.get("name", "")

        for atty in party.get("attorneys", []):
            attorneys.append({
                "name": atty.get("attorney_name", ""),
                "firm": atty.get("attorney_firm", ""),
                "roles": [r.get("role_name", "") if isinstance(r, dict) else str(r)
                          for r in atty.get("roles", [])],
                "representing": party_name,
                "party_type": party_type,
            })

    return attorneys


def store_docket(facility_id, docket_data, cl_docket_id):
    """Store a docket in the database."""
    try:
        # Extract parties and attorneys
        parties_raw = docket_data.get("parties", [])
        attorneys = extract_attorneys(parties_raw)

        # Simplify parties for storage
        parties = []
        for p in (parties_raw or []):
            parties.append({
                "name": p.get("name", ""),
                "type": p.get("party_type", {}).get("name", "") if isinstance(p.get("party_type"), dict) else str(p.get("party_type", "")),
            })

        execute("""
            INSERT INTO dockets (facility_id, courtlistener_docket_id, case_name,
                docket_number, court, court_id, date_filed, date_terminated,
                date_last_filing, assigned_to, referred_to, nature_of_suit,
                cause, jury_demand, parties, attorneys, source_url, last_synced)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (courtlistener_docket_id) DO UPDATE SET
                date_last_filing=EXCLUDED.date_last_filing,
                parties=EXCLUDED.parties,
                attorneys=EXCLUDED.attorneys,
                last_synced=NOW()
        """, (
            facility_id,
            cl_docket_id,
            docket_data.get("case_name", ""),
            docket_data.get("docket_number", ""),
            docket_data.get("court_citation_string", docket_data.get("court", "")),
            docket_data.get("court_id", ""),
            docket_data.get("date_filed"),
            docket_data.get("date_terminated"),
            docket_data.get("date_last_filing"),
            docket_data.get("assigned_to_str", ""),
            docket_data.get("referred_to_str", ""),
            docket_data.get("nature_of_suit", ""),
            docket_data.get("cause", ""),
            docket_data.get("jury_demand", ""),
            json.dumps(parties),
            json.dumps(attorneys),
            f"https://www.courtlistener.com{docket_data.get('absolute_url', '')}",
        ))
        return True
    except Exception as e:
        print(f"    Error storing docket: {e}")
        return False


def store_entry(docket_db_id, entry):
    """Store a docket entry."""
    entry_num = entry.get("entry_number")
    if not entry_num:
        return False

    # Extract recap document info
    recap_docs = []
    for doc in entry.get("recap_documents", []):
        recap_docs.append({
            "id": doc.get("id"),
            "description": doc.get("description", ""),
            "document_type": doc.get("document_type", ""),
            "page_count": doc.get("page_count"),
            "is_available": doc.get("is_available", False),
            "filepath": doc.get("filepath_local", ""),
        })

    try:
        execute("""
            INSERT INTO docket_entries (docket_id, entry_number, date_filed,
                description, document_count, recap_documents, last_synced)
            VALUES (%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (docket_id, entry_number) DO UPDATE SET
                description=EXCLUDED.description,
                recap_documents=EXCLUDED.recap_documents,
                last_synced=NOW()
        """, (
            docket_db_id,
            entry_num,
            entry.get("date_filed"),
            entry.get("description", ""),
            len(recap_docs),
            json.dumps(recap_docs),
        ))
        return True
    except Exception as e:
        if "duplicate" not in str(e).lower():
            print(f"    Error storing entry #{entry_num}: {e}")
        return False


def sync_facility_dockets(facility_id, facility_name, search_terms=None):
    """Sync RECAP docket data for a facility."""
    print(f"\n  {facility_name}")
    print(f"  " + "-" * 40)

    if not search_terms:
        search_terms = [facility_name]

    total_dockets = 0
    total_entries = 0

    for term in search_terms:
        results = search_dockets(term)
        if not results:
            continue

        for result in results[:10]:  # Top 10 per search term
            cl_docket_id = result.get("docket_id")
            if not cl_docket_id:
                continue

            case_name = result.get("caseName", result.get("case_name", ""))
            docket_number = result.get("docketNumber", result.get("docket_number", ""))
            print(f"    Docket: {docket_number} — {case_name[:60]}")

            # Get full docket details
            detail = get_docket_detail(cl_docket_id)
            if detail:
                stored = store_docket(facility_id, detail, cl_docket_id)
                if stored:
                    total_dockets += 1

                    # Get our DB id for this docket
                    db_row = query(
                        "SELECT id FROM dockets WHERE courtlistener_docket_id = %s",
                        (cl_docket_id,)
                    )
                    if db_row:
                        db_id = db_row[0]["id"]

                        # Get docket entries
                        entries = get_docket_entries(cl_docket_id, limit=30)
                        for entry in entries:
                            if store_entry(db_id, entry):
                                total_entries += 1
            else:
                # Store from search result (less detail but still useful)
                store_docket(facility_id, {
                    "case_name": case_name,
                    "docket_number": docket_number,
                    "court_citation_string": result.get("court_citation_string", ""),
                    "court_id": result.get("court_id", ""),
                    "date_filed": result.get("dateFiled"),
                    "date_last_filing": result.get("dateLastFiling"),
                    "assigned_to_str": result.get("assignedTo", ""),
                    "absolute_url": result.get("docket_absolute_url", ""),
                }, cl_docket_id)
                total_dockets += 1

    print(f"    Total: {total_dockets} dockets, {total_entries} entries stored")
    return total_dockets, total_entries


def get_facility_id(name):
    result = query("SELECT id FROM facilities WHERE name = %s", (name,))
    return result[0]["id"] if result else None


def run():
    print("=" * 60)
    print("GROUNDTRUTH — RECAP Docket Sync")
    print("=" * 60)

    if not CL_TOKEN:
        print("ERROR: COURTLISTENER_TOKEN not set in .env")
        return

    setup_tables()

    # Facilities and their search terms
    facilities = [
        ("Rikers Island", [
            "Rikers Island",
            "Nunez v. City of New York",
        ]),
        ("Mississippi State Penitentiary", [
            "Mississippi State Penitentiary",
            "Parchman",
        ]),
        ("Angola", [
            "Louisiana State Penitentiary",
            "Angola prison",
        ]),
        ("Cook County Jail", [
            "Cook County Jail",
            "Cook County Department of Corrections",
        ]),
        ("Attica Correctional Facility", [
            "Attica Correctional Facility",
        ]),
        ("San Quentin State Prison", [
            "San Quentin",
        ]),
    ]

    total_d = 0
    total_e = 0
    for name, terms in facilities:
        fid = get_facility_id(name)
        if not fid:
            print(f"\n  {name}: not in database, skipping")
            continue
        d, e = sync_facility_dockets(fid, name, terms)
        total_d += d
        total_e += e

    print("\n" + "=" * 60)
    print(f"RECAP SYNC COMPLETE")
    print(f"Total: {total_d} dockets, {total_e} entries")
    print("=" * 60)

    # Summary
    docket_counts = query("""
        SELECT f.name, COUNT(d.id) as dockets,
            (SELECT COUNT(*) FROM docket_entries de WHERE de.docket_id = ANY(ARRAY_AGG(d.id))) as entries
        FROM facilities f
        JOIN dockets d ON d.facility_id = f.id
        GROUP BY f.name
        ORDER BY dockets DESC
    """)
    if docket_counts:
        print("\nDockets by facility:")
        for row in docket_counts:
            print(f"  {row['name']}: {row['dockets']} dockets, {row['entries']} entries")

    # Show sample attorneys
    atty_sample = query("""
        SELECT case_name, attorneys
        FROM dockets
        WHERE attorneys IS NOT NULL AND attorneys != '[]'::jsonb
        LIMIT 3
    """)
    if atty_sample:
        print("\nSample attorneys found:")
        for row in atty_sample:
            attys = row.get("attorneys", [])
            if attys:
                print(f"  {row['case_name'][:50]}:")
                for a in attys[:3]:
                    print(f"    {a.get('name','')} — {a.get('firm','')} (representing {a.get('representing','')})")


if __name__ == "__main__":
    run()