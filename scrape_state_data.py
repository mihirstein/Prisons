# scrape_state_data.py
# Scrapes LIVE state corrections data from official government sources.
# Downloads PDFs, extracts facility-level data, stores in database.
#
# Sources:
#   - MDOC Monthly Fact Sheets (mdoc.ms.gov) — population by facility, demographics
#   - NYC Comptroller DOC Dashboard — Rikers population, staffing, use of force
#   - MDOC Daily Inmate Population reports
#   - MDOC PREA Audit Reports

import os
import pwd
import re
import json
import time
import requests
import pdfplumber
from datetime import datetime
from setup_db import execute, query

DATA_DIR = "./data/state_corrections"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "GroundTruth Legal Research (educational project)"
}


def get_facility_id(name):
    result = query("SELECT id FROM facilities WHERE name = %s", (name,))
    return result[0]["id"] if result else None


def store_stat(facility_id, year, stat_type, value_dict, source):
    """Store a stat, skip if duplicate."""
    try:
        execute(
            """INSERT INTO facility_stats (facility_id, year, stat_type, value, source, last_synced)
               VALUES (%s, %s, %s, %s, %s, NOW())""",
            (facility_id, year, stat_type, json.dumps(value_dict), source)
        )
        return True
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return False
        print(f"    Error storing stat: {e}")
        return False


def download_pdf(url, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        print(f"    Already downloaded: {filename}")
        return filepath
    try:
        print(f"    Downloading: {url[:80]}...")
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(resp.content)
        print(f"    Saved: {filename} ({len(resp.content) // 1024}KB)")
        return filepath
    except Exception as e:
        print(f"    Download failed: {e}")
        return None


def extract_pdf_text(filepath):
    try:
        text = ""
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text += t + "\n"
        return text
    except Exception as e:
        print(f"    PDF extraction failed: {e}")
        return None


# ════════════════════════════════════════════════════════════
# MISSISSIPPI DOC — Monthly Fact Sheets
# URL pattern: mdoc.ms.gov/sites/default/files/Monthly_Fact_Sheets/YYYY_Monthly%20Fact%20Sheet_Month.pdf
# ════════════════════════════════════════════════════════════

MDOC_MONTHS = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

def scrape_mdoc_fact_sheet(year, month_name):
    """Download and parse an MDOC monthly fact sheet PDF."""
    url = f"https://www.mdoc.ms.gov/sites/default/files/Monthly_Fact_Sheets/{year}_Monthly%20Fact%20Sheet_{month_name}.pdf"
    filename = f"mdoc_fact_sheet_{year}_{month_name}.pdf"

    filepath = download_pdf(url, filename)
    if not filepath:
        return None

    text = extract_pdf_text(filepath)
    if not text:
        return None

    return parse_mdoc_fact_sheet(text, year, month_name)


def parse_mdoc_fact_sheet(text, year, month_name):
    """Extract facility-level data from MDOC fact sheet text.
    
    Actual PDF format (verified Sep 2025):
      Mississippi State Penitentiary 2,690 2,466 11.50%
      Central Mississippi Correctional Facility 3,983 3,740 17.44%
      South Mississippi Correctional Institution 2,882 2,817 13.13%
      INMATE TOTAL 21,447 33.80%
      Black 12,382 719
      White 6,884 ...
    """
    data = {
        "year": year,
        "month": month_name,
        "facilities": {},
        "system_totals": {},
    }

    # Facility patterns — full names as they appear in PDF
    # Format: "Facility Name capacity population pct%"
    facility_patterns = {
        "MSP": r'Mississippi State Penitentiary\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "CMCF": r'Central Mississippi Correctional Facility\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "SMCI": r'South Mississippi Correctional Institution\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "WGCF": r'Walnut Grove Corr(?:ec)?tional Facility\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "MCCF": r'Marshall County Correctional Facility\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "EMCF": r'East MS Correctional Facility\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
        "WCCF": r'Wilkinson County Correctional Facility\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)%',
    }

    for key, pattern in facility_patterns.items():
        match = re.search(pattern, text)
        if match:
            data["facilities"][key] = {
                "capacity": int(match.group(1).replace(",", "")),
                "population": int(match.group(2).replace(",", "")),
                "pct_of_total": float(match.group(3)),
            }

    # Total custody population
    custody_match = re.search(r'Custody Population[^)]*\)\s+([\d,]+)\s+([\d.]+)%', text)
    if custody_match:
        data["system_totals"]["custody_population"] = int(custody_match.group(1).replace(",", ""))

    # Total inmate population
    inmate_match = re.search(r'INMATE TOTAL\s+([\d,]+)', text)
    if inmate_match:
        data["system_totals"]["inmate_total"] = int(inmate_match.group(1).replace(",", ""))

    # Total capacity and population
    total_match = re.search(r'TOTAL STATE, COUNTY JAILS, CWC FACILITIES\s+([\d,]+)\s+([\d,]+)', text)
    if total_match:
        data["system_totals"]["state_capacity"] = int(total_match.group(1).replace(",", ""))
        data["system_totals"]["state_population"] = int(total_match.group(2).replace(",", ""))

    # Regional totals
    regional_match = re.search(r'TOTAL REGIONAL FACILITIES\s+([\d,]+)\s+([\d,]+)', text)
    if regional_match:
        data["system_totals"]["regional_capacity"] = int(regional_match.group(1).replace(",", ""))
        data["system_totals"]["regional_population"] = int(regional_match.group(2).replace(",", ""))

    # Private prison totals
    private_match = re.search(r'TOTAL PRIVATE PRISONS\s+([\d,]+)\s+([\d,]+)', text)
    if private_match:
        data["system_totals"]["private_capacity"] = int(private_match.group(1).replace(",", ""))
        data["system_totals"]["private_population"] = int(private_match.group(2).replace(",", ""))

    # Demographics — "Black 12,382 719" (male, female)
    black_match = re.search(r'Black\s+([\d,]+)\s+([\d,]+)', text)
    white_match = re.search(r'White\s+([\d,]+)\s+([\d,]+)', text)
    hispanic_match = re.search(r'Hispanic\s+([\d,]+)\s+([\d,]+)', text)
    if black_match:
        data["system_totals"]["black_male"] = int(black_match.group(1).replace(",", ""))
        data["system_totals"]["black_female"] = int(black_match.group(2).replace(",", ""))
    if white_match:
        data["system_totals"]["white_male"] = int(white_match.group(1).replace(",", ""))
        data["system_totals"]["white_female"] = int(white_match.group(2).replace(",", ""))
    if hispanic_match:
        data["system_totals"]["hispanic_male"] = int(hispanic_match.group(1).replace(",", ""))
        data["system_totals"]["hispanic_female"] = int(hispanic_match.group(2).replace(",", ""))

    # Date from header: "As of September 2, 2025"
    date_match = re.search(r'As of (\w+ \d+, \d{4})', text)
    if date_match:
        data["as_of_date"] = date_match.group(1)

    return data


def sync_mdoc():
    """Scrape recent MDOC monthly fact sheets."""
    print("\n  MISSISSIPPI DOC — Monthly Fact Sheets")
    print("  " + "-" * 50)

    parchman_id = get_facility_id("Mississippi State Penitentiary")

    # Map MDOC facility keys to our database names (for future expansion)
    facility_map = {
        "MSP": "Mississippi State Penitentiary",
    }

    # Try recent months
    attempts = [
        (2026, "January"),
        (2025, "December"),
        (2025, "November"),
        (2025, "October"),
        (2025, "September"),
        (2025, "May"),
        (2025, "March"),
        (2025, "January"),
        (2024, "September"),
        (2024, "March"),
    ]

    stored_months = 0
    for year, month in attempts:
        print(f"\n  Trying {month} {year}...")
        data = scrape_mdoc_fact_sheet(year, month)

        if not data:
            continue

        fac_count = len(data.get("facilities", {}))
        total_keys = len(data.get("system_totals", {}))
        print(f"    Parsed: {fac_count} facilities, {total_keys} system fields")

        if fac_count == 0:
            print(f"    No facility data found in PDF text")
            continue

        for key, fac_data in data["facilities"].items():
            print(f"    {key}: {fac_data['population']} inmates / {fac_data['capacity']} capacity")

        # Store Parchman data
        if parchman_id and "MSP" in data["facilities"]:
            msp = data["facilities"]["MSP"]
            pct = round(msp["population"] / msp["capacity"] * 100) if msp["capacity"] else 0

            stored = store_stat(parchman_id, year, "population_capacity", {
                "total_inmates": msp["population"],
                "rated_capacity": msp["capacity"],
                "overcrowding_pct": pct,
                "pct_of_system_total": msp.get("pct_of_total"),
                "month": month,
                "as_of_date": data.get("as_of_date", ""),
                "system_inmate_total": data["system_totals"].get("inmate_total"),
                "system_custody_population": data["system_totals"].get("custody_population"),
                "source_note": f"MDOC Monthly Fact Sheet, {data.get('as_of_date', month + ' ' + str(year))}"
            }, f"MDOC Monthly Fact Sheet {month} {year}")

            if stored:
                print(f"    -> Stored Parchman: {msp['population']} / {msp['capacity']} ({pct}%)")
                stored_months += 1
            else:
                print(f"    -> Parchman {month} {year} already in DB, skipping")

        # Store system demographics
        totals = data.get("system_totals", {})
        if parchman_id and "black_male" in totals:
            black_total = totals.get("black_male", 0) + totals.get("black_female", 0)
            white_total = totals.get("white_male", 0) + totals.get("white_female", 0)
            hispanic_total = totals.get("hispanic_male", 0) + totals.get("hispanic_female", 0)
            inmate_total = totals.get("inmate_total") or totals.get("custody_population") or 0

            stored = store_stat(parchman_id, year, "demographics", {
                "black": black_total,
                "white": white_total,
                "hispanic": hispanic_total if hispanic_total > 0 else None,
                "total": inmate_total,
                "note": "System-wide MDOC demographics (facility-level not published)",
                "month": month,
                "source_note": f"MDOC Monthly Fact Sheet, {data.get('as_of_date', month + ' ' + str(year))}"
            }, f"MDOC Monthly Fact Sheet {month} {year}")

            if stored:
                print(f"    -> Stored demographics: Black {black_total}, White {white_total}")

        time.sleep(1)

    print(f"\n  MDOC sync complete: {stored_months} new months stored")


# ════════════════════════════════════════════════════════════
# NYC — Comptroller DOC Dashboard + Board of Correction
# ════════════════════════════════════════════════════════════

def sync_nyc():
    """Fetch NYC jail data from Comptroller and BOC sources."""
    print("\n  NYC — Comptroller Dashboard + Board of Correction")
    print("  " + "-" * 50)

    rikers_id = get_facility_id("Rikers Island")
    if not rikers_id:
        print("    Rikers not found, skipping")
        return

    # Try to fetch the NYC Comptroller dashboard page
    try:
        url = "https://comptroller.nyc.gov/reports/the-state-of-new-york-city-jails/"
        print(f"\n  Fetching NYC Comptroller dashboard...")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        text = resp.text

        # Extract population numbers from the page
        # Look for patterns like "5,708" or "6,182" near "detained" or "population"
        pop_matches = re.findall(r'([\d,]+)\s+(?:people|individuals)\s+(?:were\s+)?(?:detained|incarcerated)', text)
        if pop_matches:
            print(f"    Found population references: {pop_matches[:3]}")

        # Look for staff numbers
        staff_matches = re.findall(r'([\d,]+)\s+(?:uniformed\s+)?staff', text)
        if staff_matches:
            print(f"    Found staff references: {staff_matches[:3]}")

        # Look for death counts
        death_matches = re.findall(r'(\d+)\s+(?:people\s+)?(?:have\s+)?died', text)
        if death_matches:
            print(f"    Found death references: {death_matches[:3]}")

        print("    NYC Comptroller page fetched — data available for manual extraction")

    except Exception as e:
        print(f"    Failed to fetch NYC Comptroller: {e}")

    # Try NYC BOC death reports
    try:
        boc_url = "https://www.nyc.gov/assets/boc/downloads/pdf/First-Report-and-Recommendations-on-2025-Deaths-in-NYC-DOC-Custody-with-DOC-and-CHS-responses.pdf"
        filename = "nyc_boc_2025_deaths_report.pdf"
        filepath = download_pdf(boc_url, filename)
        if filepath:
            text = extract_pdf_text(filepath)
            if text:
                # Look for death count
                death_match = re.search(r'(?:reported the deaths of|deaths of)\s+(\d+)\s+individuals', text)
                if death_match:
                    count = int(death_match.group(1))
                    store_stat(rikers_id, 2025, "deaths_in_custody", {
                        "count": count,
                        "period": "Jan 1 - report date 2025",
                        "source_note": "NYC Board of Correction — 2025 Deaths Report"
                    }, "NYC Board of Correction")
                    print(f"    Stored Rikers 2025 deaths: {count}")

                # Extract any specific names/details
                print(f"    BOC report: {len(text)} characters extracted")
    except Exception as e:
        print(f"    Failed to fetch NYC BOC: {e}")


# ════════════════════════════════════════════════════════════
# MDOC PREA Audit Reports
# ════════════════════════════════════════════════════════════

def sync_prea():
    """Check for PREA audit reports."""
    print("\n  PREA AUDIT REPORTS")
    print("  " + "-" * 50)

    # MDOC PREA page
    try:
        url = "https://www.mdoc.ms.gov/general-public/prea-audit-reports"
        print(f"  Fetching MDOC PREA page...")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        # Find PDF links
        pdf_links = re.findall(r'href="([^"]*prea[^"]*\.pdf)"', resp.text, re.IGNORECASE)
        print(f"    Found {len(pdf_links)} PREA report links")
        for link in pdf_links[:5]:
            print(f"      {link}")

    except Exception as e:
        print(f"    Failed to fetch PREA page: {e}")


# ════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════

def run():
    print("=" * 60)
    print("GROUNDTRUTH — State Corrections Data Scraper")
    print("Pulling LIVE data from government sources")
    print("=" * 60)

    sync_mdoc()
    sync_nyc()
    sync_prea()

    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)

    # Summary
    counts = query("""
        SELECT f.name, COUNT(s.id) as count
        FROM facilities f
        JOIN facility_stats s ON s.facility_id = f.id
        GROUP BY f.name
        ORDER BY count DESC
    """)
    if counts:
        print("\nFacility stats summary:")
        for row in counts:
            print(f"  {row['name']}: {row['count']} data points")

    total = query("SELECT COUNT(*) as count FROM facility_stats")
    print(f"\nTotal data points: {total[0]['count']}")


if __name__ == "__main__":
    run()