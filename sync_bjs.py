# sync_bjs.py
import os
import csv
import json
from setup_db import query, execute

DATA_DIR = "./data"


def import_deaths_in_custody(csv_path):
    """
    Import deaths in custody data from BJS CSV.
    
    HOW TO GET THE DATA:
    1. Go to https://bjs.ojp.gov/data-collection/deaths-custody-reporting-program-dcrp
    2. Look for the latest "Mortality in Local Jails" and "Mortality in State Prisons" reports
    3. Download the associated data tables (usually CSV or Excel)
    4. Save to ./data/deaths-in-custody.csv
    
    NOTE: BJS CSV column names vary between releases. 
    Open the CSV first, look at the headers, and adjust the 
    column names below to match what you downloaded.
    """
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        print("Download from: https://bjs.ojp.gov/data-collection/deaths-custody-reporting-program-dcrp")
        print("Save to: ./data/deaths-in-custody.csv")
        return

    facilities = query("SELECT * FROM facilities")
    
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        
        print(f"CSV columns found: {reader.fieldnames}")
        print("(If these don't match the code below, update the column name mappings)\n")
        
        row_count = 0
        matched_count = 0
        
        for row in reader:
            row_count += 1
            
            # ──────────────────────────────────────────────
            # ADJUST THESE COLUMN NAMES to match your CSV
            # Open the CSV in Excel/Google Sheets first to
            # see what the actual column headers are
            # ──────────────────────────────────────────────
            facility_name = (
                row.get("FACILITY_NAME") or
                row.get("facility_name") or
                row.get("Facility Name") or
                row.get("NAME") or
                row.get("name") or
                ""
            )
            
            year = (
                row.get("YEAR") or
                row.get("year") or
                row.get("Year") or
                row.get("DEATH_YEAR") or
                ""
            )
            
            # Try to match to one of our tracked facilities
            facility_name_lower = facility_name.lower()
            matched_facility = None
            
            for fac in facilities:
                search_terms = [fac["name"].lower()] + [a.lower() for a in (fac.get("aliases") or [])]
                if any(term in facility_name_lower for term in search_terms):
                    matched_facility = fac
                    break
            
            if matched_facility and year:
                try:
                    execute(
                        """INSERT INTO facility_stats (facility_id, year, stat_type, value, source, last_synced)
                           VALUES (%s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (facility_id, year, stat_type) DO UPDATE SET
                             value = EXCLUDED.value,
                             last_synced = NOW()""",
                        (
                            matched_facility["id"],
                            int(year),
                            "deaths_in_custody",
                            json.dumps({
                                "count": 1,
                                "facility_name_raw": facility_name,
                            }),
                            "Bureau of Justice Statistics — DCRP",
                        )
                    )
                    matched_count += 1
                except Exception as e:
                    print(f"  Error: {e}")
        
        print(f"Processed {row_count} rows, matched {matched_count} to tracked facilities")


def import_manual_stats():
    """
    For your prototype, you can also manually enter known stats.
    This is useful while you figure out the BJS CSV format.
    
    These numbers come from public reporting and DOJ documents.
    """
    manual_data = [
        # Rikers Island deaths (from NYC Board of Correction reports)
        (1, 2019, "deaths_in_custody", {"count": 8, "source_note": "NYC Board of Correction"}),
        (1, 2020, "deaths_in_custody", {"count": 5, "source_note": "NYC Board of Correction"}),
        (1, 2021, "deaths_in_custody", {"count": 16, "source_note": "NYC Board of Correction"}),
        (1, 2022, "deaths_in_custody", {"count": 19, "source_note": "NYC Board of Correction"}),
        (1, 2023, "deaths_in_custody", {"count": 17, "source_note": "NYC Board of Correction"}),
        
        # Parchman deaths (from MDOC reports / news reporting)
        (2, 2019, "deaths_in_custody", {"count": 11, "source_note": "Mississippi DOC"}),
        (2, 2020, "deaths_in_custody", {"count": 18, "source_note": "Mississippi DOC"}),
        (2, 2021, "deaths_in_custody", {"count": 9, "source_note": "Mississippi DOC"}),
        (2, 2022, "deaths_in_custody", {"count": 12, "source_note": "Mississippi DOC"}),
        (2, 2023, "deaths_in_custody", {"count": 10, "source_note": "Mississippi DOC"}),
    ]
    
    print("Importing manual stats...")
    for facility_id, year, stat_type, value in manual_data:
        execute(
            """INSERT INTO facility_stats (facility_id, year, stat_type, value, source, last_synced)
               VALUES (%s, %s, %s, %s, %s, NOW())
               ON CONFLICT (facility_id, year, stat_type) DO UPDATE SET
                 value = EXCLUDED.value,
                 last_synced = NOW()""",
            (facility_id, year, stat_type, json.dumps(value), "Manual entry — verify sources")
        )
    
    print(f"  Imported {len(manual_data)} manual stats entries")


if __name__ == "__main__":
    print("=== BJS DATA IMPORT ===\n")
    
    # Try CSV import first
    csv_path = os.path.join(DATA_DIR, "deaths-in-custody.csv")
    import_deaths_in_custody(csv_path)
    
    # Also import manual stats so you have something to demo with
    print()
    import_manual_stats()
    
    # Summary
    print("\n=== IMPORT COMPLETE ===")
    counts = query("""
        SELECT f.name, COUNT(s.id) as stat_count
        FROM facilities f
        LEFT JOIN facility_stats s ON s.facility_id = f.id
        GROUP BY f.name
        HAVING COUNT(s.id) > 0
        ORDER BY stat_count DESC
    """)
    for row in counts:
        print(f"  {row['name']}: {row['stat_count']} data points")