# scrape_prea_and_annual.py
# Two scrapers for PREA audit reports and MDOC annual reports.
# Run standalone or import functions into scrape_state_data.py.
#
# Sources:
#   - PREA audit PDFs from mdoc.ms.gov (structured fields: capacity, population, staff, sexual abuse reports)
#   - MDOC Annual Report PDFs (facility pages: cost/day, staffing, grievances)

import os
import re
import json
import time
import requests
import pdfplumber
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
        print(f"    DB error: {e}")
        return False


def download_pdf(url, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        print(f"    Cached: {filename}")
        return filepath
    try:
        print(f"    Downloading: {filename}...")
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
# PREA AUDIT REPORTS
# ════════════════════════════════════════════════════════════

def parse_prea_audit(text):
    """Parse structured fields from a PREA audit report PDF.
    
    These reports have a consistent format with labeled fields like:
        Designed facility capacity: 5962
        Current population of facility: 2454
        Number of staff currently employed...: 383
    """
    data = {}

    # Facility name
    m = re.search(r'Name of Facility:\s*(.+?)(?:\n|Facility Type)', text)
    if m:
        data["facility_name"] = m.group(1).strip()

    # Audit dates
    m = re.search(r'Start [Dd]ate of (?:the )?[Oo]n[- ]?[Ss]ite.*?(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})', text)
    if m:
        data["audit_start_date"] = m.group(1)

    m = re.search(r'End [Dd]ate of (?:the )?[Oo]n[- ]?[Ss]ite.*?(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})', text)
    if m:
        data["audit_end_date"] = m.group(1)

    m = re.search(r'Date Final Report Submitted:\s*(\S+)', text)
    if m:
        data["report_date"] = m.group(1)

    # Capacity & population
    m = re.search(r'[Dd]esigned facility capacity:\s*([\d,]+)', text)
    if m:
        data["designed_capacity"] = int(m.group(1).replace(",", ""))

    m = re.search(r'[Cc]urrent population of facility:\s*([\d,]+)', text)
    if m:
        data["current_population"] = int(m.group(1).replace(",", ""))

    m = re.search(r'[Aa]verage daily population for the past 12\s*months:\s*([\d,]+)', text)
    if m:
        data["avg_daily_population_12mo"] = int(m.group(1).replace(",", ""))

    m = re.search(r'[Hh]as the facility been over capacity.*?\s+(Yes|No)', text)
    if m:
        data["over_capacity_past_12mo"] = m.group(1) == "Yes"

    # Population on audit day
    m = re.search(r'total number of inmates.*?first day.*?onsite.*?(\d+)', text, re.DOTALL)
    if m:
        data["population_audit_day"] = int(m.group(1))

    # Staff
    m = re.search(r'[Nn]umber of staff currently employed.*?(\d{2,})', text)

    if m:
        data["staff_with_inmate_contact"] = int(m.group(1))

    m = re.search(r'total number of STAFF.*?first day.*?(\d+)', text, re.DOTALL)
    if m:
        data["staff_audit_day"] = int(m.group(1))

    # Volunteers and contractors
    m = re.search(r'[Nn]umber of.*?volunteers.*?contact with inmates.*?(\d+)', text, re.DOTALL)
    if m:
        data["volunteers"] = int(m.group(1))

    m = re.search(r'[Nn]umber of.*?contractors.*?contact with inmates.*?(\d+)', text, re.DOTALL)
    if m:
        data["contractors"] = int(m.group(1))

    # Vulnerable populations
    m = re.search(r'physical disability.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_physical_disability"] = int(m.group(1))

    m = re.search(r'cognitive or functional disability.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_cognitive_disability"] = int(m.group(1))

    m = re.search(r'[Bb]lind or have low vision.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_blind_low_vision"] = int(m.group(1))

    m = re.search(r'[Dd]eaf or hard-of-hearing.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_deaf_hoh"] = int(m.group(1))

    m = re.search(r'[Ll]imited English Proficient.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_lep"] = int(m.group(1))

    m = re.search(r'identify as lesbian, gay, or bisexual.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_lgb"] = int(m.group(1))

    m = re.search(r'identify as transgender or intersex.*?(\d+)', text, re.DOTALL)
    if m:
        data["inmates_transgender_intersex"] = int(m.group(1))

    # Sexual abuse reports
    m = re.search(r'reported sexual abuse.*?first day.*?(\d+)', text, re.DOTALL)
    if m:
        data["reported_sexual_abuse"] = int(m.group(1))

    m = re.search(r'disclosed prior sexual victimization.*?(\d+)', text, re.DOTALL)
    if m:
        data["prior_sexual_victimization"] = int(m.group(1))

    m = re.search(r'placed in segregated housing.*?risk of sexual victimization.*?(\d+)', text, re.DOTALL)
    if m:
        data["segregated_for_sexual_safety"] = int(m.group(1))

    # Standards compliance
    m = re.search(r'[Nn]umber of standards exceeded:\s*(\d+)', text)
    if m:
        data["standards_exceeded"] = int(m.group(1))

    m = re.search(r'[Nn]umber of standards met:\s*(\d+)', text)
    if m:
        data["standards_met"] = int(m.group(1))

    m = re.search(r'[Nn]umber of standards not met:\s*(\d+)', text)
    if m:
        data["standards_not_met"] = int(m.group(1))

    # Security levels
    m = re.search(r'[Ss]ecurity levels.*?:\s*(.+?)(?:\n|Does)', text)
    if m:
        data["security_levels"] = m.group(1).strip()

    # Age range
    m = re.search(r'[Aa]ge range.*?:\s*(\d+-\d+)', text)
    if m:
        data["age_range"] = m.group(1)

    # Housing units
    m = re.search(r'housing units:\s*(\d+)', text)
    if m:
        data["housing_units"] = int(m.group(1))

    return data


def sync_prea_audits():
    """Download and parse PREA audit reports for known facilities."""
    print("\n  PREA AUDIT REPORTS")
    print("  " + "-" * 50)

    # Known PREA audit URLs — discovered from scraping the MDOC PREA page
    prea_urls = {
        "Mississippi State Penitentiary": [
            ("https://www.mdoc.ms.gov/sites/default/files/PREA_Audit_Reports/2025/MSP_PREA_Audit%20Final%202025_Oct%202025.pdf", 2025),
        ],
        "Central Mississippi Correctional Facility": [
            ("https://www.mdoc.ms.gov/sites/default/files/PREA_Audit_Reports/2025/CMCF_PREA%20Audit%20Final_March%202025.pdf", 2025),
        ],
    }

    # Also try to discover more from the PREA page
    try:
        print("  Checking MDOC PREA page for additional reports...")
        resp = requests.get("https://www.mdoc.ms.gov/general-public/prea-audit-reports", headers=HEADERS, timeout=30)
        resp.raise_for_status()

        # Find all PDF links
        pdf_links = re.findall(r'href="(https://www\.mdoc\.ms\.gov/sites/default/files/PREA_Audit_Reports/[^"]+\.pdf)"', resp.text)
        print(f"    Found {len(pdf_links)} total PREA PDF links")

        # Filter for major facilities we care about
        for link in pdf_links:
            link_lower = link.lower()
            year_match = re.search(r'/(\d{4})/', link)
            year = int(year_match.group(1)) if year_match else 2025

            if "msp" in link_lower and "Mississippi State Penitentiary" not in str(prea_urls.get("Mississippi State Penitentiary", [])):
                prea_urls.setdefault("Mississippi State Penitentiary", []).append((link, year))
            elif "smci" in link_lower or "south_mississippi" in link_lower:
                prea_urls.setdefault("South Mississippi Correctional Institution", []).append((link, year))

    except Exception as e:
        print(f"    Could not fetch PREA page: {e}")

    # Process each facility's PREA audits
    for facility_name, urls in prea_urls.items():
        fid = get_facility_id(facility_name)
        if not fid:
            print(f"\n  {facility_name}: not in database, skipping")
            continue

        for url, year in urls:
            safe_name = re.sub(r'[^a-zA-Z0-9]', '_', facility_name)[:40]
            filename = f"prea_{safe_name}_{year}.pdf"
            print(f"\n  {facility_name} — PREA {year}")

            filepath = download_pdf(url, filename)
            if not filepath:
                continue

            text = extract_pdf_text(filepath)
            if not text:
                continue

            prea_data = parse_prea_audit(text)
            if not prea_data:
                print(f"    No data parsed")
                continue

            print(f"    Parsed: capacity={prea_data.get('designed_capacity')}, pop={prea_data.get('current_population')}, staff={prea_data.get('staff_with_inmate_contact')}")

            # Store as PREA audit stat
            stored = store_stat(fid, year, "prea_audit", {
                "designed_capacity": prea_data.get("designed_capacity"),
                "current_population": prea_data.get("current_population"),
                "avg_daily_population_12mo": prea_data.get("avg_daily_population_12mo"),
                "population_audit_day": prea_data.get("population_audit_day"),
                "over_capacity_past_12mo": prea_data.get("over_capacity_past_12mo"),
                "staff_with_inmate_contact": prea_data.get("staff_with_inmate_contact"),
                "staff_audit_day": prea_data.get("staff_audit_day"),
                "volunteers": prea_data.get("volunteers"),
                "contractors": prea_data.get("contractors"),
                "housing_units": prea_data.get("housing_units"),
                "security_levels": prea_data.get("security_levels"),
                "age_range": prea_data.get("age_range"),
                "inmates_physical_disability": prea_data.get("inmates_physical_disability"),
                "inmates_cognitive_disability": prea_data.get("inmates_cognitive_disability"),
                "inmates_blind_low_vision": prea_data.get("inmates_blind_low_vision"),
                "inmates_deaf_hoh": prea_data.get("inmates_deaf_hoh"),
                "inmates_lep": prea_data.get("inmates_lep"),
                "inmates_lgb": prea_data.get("inmates_lgb"),
                "inmates_transgender_intersex": prea_data.get("inmates_transgender_intersex"),
                "reported_sexual_abuse": prea_data.get("reported_sexual_abuse"),
                "prior_sexual_victimization": prea_data.get("prior_sexual_victimization"),
                "segregated_for_sexual_safety": prea_data.get("segregated_for_sexual_safety"),
                "standards_exceeded": prea_data.get("standards_exceeded"),
                "standards_met": prea_data.get("standards_met"),
                "standards_not_met": prea_data.get("standards_not_met"),
                "audit_start_date": prea_data.get("audit_start_date"),
                "audit_end_date": prea_data.get("audit_end_date"),
                "report_date": prea_data.get("report_date"),
                "pdf_url": url,
                "source_note": f"PREA Facility Audit Report, {facility_name}, {year}"
            }, f"PREA Audit {year}")

            if stored:
                print(f"    -> Stored PREA audit")

                # Also store population_capacity from PREA (more current than fact sheets)
                if prea_data.get("current_population") and prea_data.get("designed_capacity"):
                    pop = prea_data["current_population"]
                    cap = prea_data["designed_capacity"]
                    pct = round(pop / cap * 100) if cap else 0
                    store_stat(fid, year, "population_capacity", {
                        "total_inmates": pop,
                        "rated_capacity": cap,
                        "overcrowding_pct": pct,
                        "avg_daily_population_12mo": prea_data.get("avg_daily_population_12mo"),
                        "source_note": f"PREA Audit Report, {prea_data.get('audit_start_date', str(year))}"
                    }, f"PREA Audit {year}")
                    print(f"    -> Stored population: {pop}/{cap} ({pct}%)")

                # Store staffing from PREA
                if prea_data.get("staff_with_inmate_contact"):
                    store_stat(fid, year, "staffing", {
                        "total_staff": prea_data["staff_with_inmate_contact"],
                        "volunteers": prea_data.get("volunteers"),
                        "contractors": prea_data.get("contractors"),
                        "staff_inmate_ratio": round(prea_data["staff_with_inmate_contact"] / prea_data["current_population"], 3) if prea_data.get("current_population") else None,
                        "source_note": f"PREA Audit Report, {prea_data.get('audit_start_date', str(year))}"
                    }, f"PREA Audit {year}")
                    print(f"    -> Stored staffing: {prea_data['staff_with_inmate_contact']} staff")

            time.sleep(2)


# ════════════════════════════════════════════════════════════
# STATE DOC ANNUAL REPORTS
# ════════════════════════════════════════════════════════════

def parse_annual_report(text, year):
    """Parse MDOC annual report for facility-level data.
    
    Annual reports contain:
    - Facility pages with population, capacity, programs
    - Security filled positions vs authorized positions
    - Cost per day by program
    - Administrative Remedy Program (grievance) stats
    - Total costs of state-operated facilities
    """
    data = {
        "year": year,
        "facilities": {},
        "system": {},
    }

    # Cost per day — look for patterns like "$51.52" near "cost per day"
    cost_match = re.search(r'[Cc]ost per (?:inmate )?(?:per )?day.*?\$([\d.]+)', text, re.DOTALL)
    if cost_match:
        data["system"]["cost_per_day"] = float(cost_match.group(1))

    # Total costs of state-operated facilities
    total_cost_match = re.search(r'[Tt]otal.*?[Cc]ost.*?state.*?facilities.*?\$([\d,.]+)', text, re.DOTALL)
    if total_cost_match:
        data["system"]["total_facility_costs"] = total_cost_match.group(1).replace(",", "")

    # Security staffing — "filled positions" vs "authorized positions"
    # Look for patterns near "security" and "filled" and "authorized"
    filled_match = re.search(r'[Ff]illed\s+[Pp]ositions.*?(\d[\d,]*)', text)
    authorized_match = re.search(r'[Aa]uthorized\s+[Pp]ositions.*?(\d[\d,]*)', text)
    if filled_match:
        data["system"]["security_positions_filled"] = int(filled_match.group(1).replace(",", ""))
    if authorized_match:
        data["system"]["security_positions_authorized"] = int(authorized_match.group(1).replace(",", ""))

    # Administrative Remedy Program (grievances)
    # Look for grievance/ARP stats
    arp_received = re.search(r'[Aa]dministrative [Rr]emedy.*?[Rr]eceived.*?([\d,]{2,})', text, re.DOTALL)
    arp_accepted = re.search(r'[Aa]ccepted.*?(?:at|for)\s+(?:screening|review).*?([\d,]{2,})', text, re.DOTALL)
    if arp_received and arp_received.group(1).strip():
        try:
            data["system"]["grievances_received"] = int(arp_received.group(1).replace(",", ""))
        except ValueError:
            pass
    if arp_accepted and arp_accepted.group(1).strip():
        try:
            data["system"]["grievances_accepted"] = int(arp_accepted.group(1).replace(",", ""))
        except ValueError:
            pass

    # MSP-specific data — look for Mississippi State Penitentiary section
    msp_section = re.search(
        r'Mississippi State Penitentiary(.*?)(?:Central Mississippi Correctional Facility|CMCF)',
        text, re.DOTALL
    )
    if msp_section:
        msp_text = msp_section.group(1)
        data["facilities"]["MSP"] = {}

        # Look for population/capacity in MSP section
        pop_match = re.search(r'(?:population|housed).*?([\d,]+)', msp_text, re.IGNORECASE)
        if pop_match:
            data["facilities"]["MSP"]["population"] = int(pop_match.group(1).replace(",", ""))

        cap_match = re.search(r'[Cc]apacity.*?([\d,]+)', msp_text)
        if cap_match:
            data["facilities"]["MSP"]["capacity"] = int(cap_match.group(1).replace(",", ""))

        # Programs offered
        programs = []
        for prog in ["seminary", "GED", "vocational", "reentry", "substance abuse", "cognitive", "dog program", "rodeo"]:
            if prog.lower() in msp_text.lower():
                programs.append(prog.title())
        if programs:
            data["facilities"]["MSP"]["programs"] = programs

    return data


def sync_annual_reports():
    """Download and parse MDOC annual reports."""
    print("\n  MDOC ANNUAL REPORTS")
    print("  " + "-" * 50)

    parchman_id = get_facility_id("Mississippi State Penitentiary")

    # Annual report URLs — from the MDOC annual reports page
    reports = [
        (2023, "https://www.mdoc.ms.gov/sites/default/files/Annual%20Reports/2023%20Annual%20Report.pdf"),
        (2022, "https://www.mdoc.ms.gov/sites/default/files/2023-01/2022%20Annual%20Report.pdf"),
        (2021, "https://www.mdoc.ms.gov/sites/default/files/2023-01/2021%20Annual%20Report.pdf"),
        (2020, "https://www.mdoc.ms.gov/sites/default/files/2023-01/2020%20Annual%20Report.pdf"),
    ]

    for year, url in reports:
        filename = f"mdoc_annual_report_{year}.pdf"
        print(f"\n  MDOC Annual Report FY{year}")

        filepath = download_pdf(url, filename)
        if not filepath:
            continue

        text = extract_pdf_text(filepath)
        if not text:
            continue

        print(f"    Extracted {len(text)} characters")

        data = parse_annual_report(text, year)
        sys_data = data.get("system", {})

        print(f"    System data found: {list(sys_data.keys())}")

        if parchman_id and sys_data:
            # Store system-wide operational data
            value = {
                "cost_per_day": sys_data.get("cost_per_day"),
                "total_facility_costs": sys_data.get("total_facility_costs"),
                "security_positions_filled": sys_data.get("security_positions_filled"),
                "security_positions_authorized": sys_data.get("security_positions_authorized"),
                "grievances_received": sys_data.get("grievances_received"),
                "grievances_accepted": sys_data.get("grievances_accepted"),
                "source_note": f"MDOC Annual Report FY{year}"
            }
            # Remove None values
            value = {k: v for k, v in value.items() if v is not None}

            if len(value) > 1:  # more than just source_note
                stored = store_stat(parchman_id, year, "annual_report", value,
                                    f"MDOC Annual Report FY{year}")
                if stored:
                    print(f"    -> Stored annual report data: {list(value.keys())}")
                else:
                    print(f"    -> Already in DB")

            # Store MSP-specific if found
            msp_data = data.get("facilities", {}).get("MSP", {})
            if msp_data.get("programs"):
                store_stat(parchman_id, year, "programs", {
                    "programs_offered": msp_data["programs"],
                    "source_note": f"MDOC Annual Report FY{year}"
                }, f"MDOC Annual Report FY{year}")
                print(f"    -> Stored MSP programs: {msp_data['programs']}")

        time.sleep(2)


# ════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════

def run():
    print("=" * 60)
    print("GROUNDTRUTH — PREA Audits + Annual Reports Scraper")
    print("=" * 60)

    sync_prea_audits()
    sync_annual_reports()

    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)

    # Summary
    counts = query("""
        SELECT f.name, s.stat_type, COUNT(*) as count
        FROM facilities f
        JOIN facility_stats s ON s.facility_id = f.id
        GROUP BY f.name, s.stat_type
        ORDER BY f.name, s.stat_type
    """)
    if counts:
        print("\nData by facility and type:")
        current_fac = ""
        for row in counts:
            if row["name"] != current_fac:
                current_fac = row["name"]
                print(f"\n  {current_fac}:")
            print(f"    {row['stat_type']}: {row['count']}")

    total = query("SELECT COUNT(*) as count FROM facility_stats")
    print(f"\nTotal data points: {total[0]['count']}")


if __name__ == "__main__":
    run()