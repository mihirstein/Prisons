"""
ingest_ccf.py — Import BJS Census of Correctional Facilities (2019) into GROUNDTRUTH
Optimized: loads TSV into memory, then matches only your existing facilities.
"""

import os
import csv
import json
from setup_db import query, execute
TSV_PATH = os.getenv("CCF_TSV_PATH", "38325-0001-Data.tsv")

OPERATOR_MAP = {"1": "Federal", "2": "State", "3": "Local", "4": "Joint State/Local", "5": "Private"}
SECURITY_MAP = {"1": "Supermax", "2": "Maximum/Close/High", "3": "Medium", "4": "Minimum/Low", "5": "Administrative", "6": "Other", "7": "None"}

COURT_ORDER_CONDITIONS = {
    "V048": "Crowding", "V049": "Visiting/Mail/Phone Policy", "V050": "Disabled Accommodation",
    "V051": "Religious Practices", "V052": "Mental Health Treatment", "V053": "Search Policies",
    "V054": "Fire Hazards", "V055": "Medical Facilities", "V056": "Disciplinary Procedures",
    "V057": "Grievance Procedures", "V058": "Staffing", "V059": "Administrative Policies",
    "V060": "Library Services", "V061": "Recreation", "V062": "Inmate Classification",
    "V063": "Food/Cleanliness", "V064": "Counseling", "V065": "Education", "V066": "Other",
}


def safe_int(val):
    try:
        v = int(val.strip())
        return v if v >= 0 else None
    except (ValueError, AttributeError):
        return None


def parse_row(row):
    if row.get("V001", "").strip() != "1":
        return None
    name = row.get("V005", "").strip()
    if not name:
        return None

    capacity = safe_int(row.get("V038"))
    design_capacity = safe_int(row.get("V041"))
    total_inmates = safe_int(row.get("V074"))
    total_staff = safe_int(row.get("V191"))
    security_staff = safe_int(row.get("V197"))

    overcrowding_pct = None
    if capacity and total_inmates and capacity > 0:
        overcrowding_pct = round((total_inmates / capacity) * 100, 1)

    staff_inmate_ratio = None
    if security_staff and total_inmates and total_inmates > 0:
        staff_inmate_ratio = round(security_staff / total_inmates, 3)

    under_court_order = row.get("V043", "").strip() == "1"
    court_order_conditions = []
    if row.get("V047", "").strip() == "1":
        for col, label in COURT_ORDER_CONDITIONS.items():
            if row.get(col, "").strip() == "1":
                court_order_conditions.append(label)

    return {
        "name": name,
        "city": row.get("V007", "").strip() or None,
        "state": row.get("V008", "").strip() or None,
        "facility_type": "Confinement" if row.get("TYPE", "").strip() == "1" else "Community-based",
        "operator": OPERATOR_MAP.get(row.get("V032", "").strip()),
        "security_level": SECURITY_MAP.get(row.get("V034", "").strip()),
        "rated_capacity": capacity,
        "design_capacity": design_capacity,
        "total_inmates": total_inmates,
        "overcrowding_pct": overcrowding_pct,
        "total_staff": total_staff,
        "security_staff": security_staff,
        "staff_inmate_ratio": staff_inmate_ratio,
        "under_court_order": under_court_order,
        "court_order_conditions": court_order_conditions,
        "assaults_on_staff": safe_int(row.get("V225")),
        "assaults_on_inmates_serious": safe_int(row.get("V227")),
        "assaults_on_inmates_other": safe_int(row.get("V229")),
        "assaults_on_inmates_total": safe_int(row.get("V231")),
        "disciplinary_reports": safe_int(row.get("V223")),
        "disturbances": safe_int(row.get("V233")),
        "escapes": safe_int(row.get("V236")),
        "white_inmates": safe_int(row.get("V082")),
        "black_inmates": safe_int(row.get("V084")),
        "hispanic_inmates": safe_int(row.get("V086")),
    }


def upsert_stat(facility_id, stat_type, value_dict, source="BJS Census of Correctional Facilities, 2019"):
    existing = query(
        "SELECT id FROM facility_stats WHERE facility_id = %s AND stat_type = %s AND year = 2019",
        (facility_id, stat_type),
    )
    if existing:
        execute(
            "UPDATE facility_stats SET value = %s, source = %s, last_synced = NOW() WHERE id = %s",
            (json.dumps(value_dict), source, existing[0]["id"]),
        )
    else:
        execute(
            """INSERT INTO facility_stats (facility_id, year, stat_type, value, source, last_synced)
               VALUES (%s, 2019, %s, %s, %s, NOW())""",
            (facility_id, stat_type, json.dumps(value_dict), source),
        )


# ── Manual name mapping for facilities whose BJS names differ ────────
MANUAL_MAP = {
    "Rikers Island": None,           # Jail complex, not in prison census
    "Angola": "LOUISIANA STATE PENITENTIARY",
    "ADX Florence": "USP FLORENCE ADMAX",
    "Cook County Jail": None,        # Jail, not in prison census
}


def find_in_tsv(db_name, db_state, ccf_records):
    """Search the in-memory CCF records for a matching facility."""

    if db_name in MANUAL_MAP:
        mapped = MANUAL_MAP[db_name]
        if mapped is None:
            return None
        for rec in ccf_records:
            if rec["name"] == mapped:
                return rec

    db_upper = db_name.upper()
    db_state_upper = (db_state or "").upper()

    # Exact name match + state
    for rec in ccf_records:
        if rec["name"].upper() == db_upper and (rec["state"] or "").upper() == db_state_upper:
            return rec

    # Substring match + state
    for rec in ccf_records:
        rec_upper = rec["name"].upper()
        rec_state = (rec["state"] or "").upper()
        if rec_state != db_state_upper:
            continue
        if db_upper in rec_upper or rec_upper in db_upper:
            return rec

    # Key words match
    db_words = set(db_upper.replace("CORRECTIONAL", "").replace("FACILITY", "").replace("STATE", "").replace("PRISON", "").split())
    db_words.discard("")
    if len(db_words) >= 1:
        for rec in ccf_records:
            rec_state = (rec["state"] or "").upper()
            if rec_state != db_state_upper:
                continue
            rec_upper = rec["name"].upper()
            if all(w in rec_upper for w in db_words):
                return rec

    return None


def ingest():
    print(f"Loading {TSV_PATH} into memory...")
    ccf_records = []
    with open(TSV_PATH, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rec = parse_row(row)
            if rec:
                ccf_records.append(rec)
    print(f"  {len(ccf_records)} facility records loaded.\n")

    db_facilities = query("SELECT id, name, state, aliases FROM facilities")
    print(f"Matching {len(db_facilities)} GROUNDTRUTH facilities against CCF data...\n")

    matched = 0
    stats_inserted = 0

    for fac in db_facilities:
        db_name = fac["name"]
        db_state = fac["state"]
        db_id = fac["id"]

        match = find_in_tsv(db_name, db_state, ccf_records)

        if not match:
            aliases = fac.get("aliases") or []
            for alias in aliases:
                match = find_in_tsv(alias, db_state, ccf_records)
                if match:
                    break

        if not match:
            print(f"  MISS: {db_name} ({db_state})")
            continue

        print(f"  HIT:  {db_name} ({db_state}) -> {match['name']}")
        matched += 1

        src = "BJS Census of Correctional Facilities"

        if match["total_inmates"] is not None or match["rated_capacity"] is not None:
            upsert_stat(db_id, "population_capacity", {
                "total_inmates": match["total_inmates"],
                "rated_capacity": match["rated_capacity"],
                "design_capacity": match["design_capacity"],
                "overcrowding_pct": match["overcrowding_pct"],
                "source_note": src,
            })
            stats_inserted += 1

        if match["total_staff"] is not None or match["security_staff"] is not None:
            upsert_stat(db_id, "staffing", {
                "total_staff": match["total_staff"],
                "security_staff": match["security_staff"],
                "staff_inmate_ratio": match["staff_inmate_ratio"],
                "source_note": src,
            })
            stats_inserted += 1

        if match["under_court_order"]:
            upsert_stat(db_id, "court_order", {
                "under_court_order": True,
                "conditions": match["court_order_conditions"],
                "source_note": src,
            })
            stats_inserted += 1

        if match["assaults_on_inmates_total"] is not None or match["assaults_on_staff"] is not None:
            upsert_stat(db_id, "assaults_incidents", {
                "assaults_on_staff": match["assaults_on_staff"],
                "assaults_on_inmates_serious": match["assaults_on_inmates_serious"],
                "assaults_on_inmates_other": match["assaults_on_inmates_other"],
                "assaults_on_inmates_total": match["assaults_on_inmates_total"],
                "disciplinary_reports": match["disciplinary_reports"],
                "disturbances": match["disturbances"],
                "escapes": match["escapes"],
                "source_note": src,
            })
            stats_inserted += 1

        if match["white_inmates"] is not None or match["black_inmates"] is not None:
            upsert_stat(db_id, "demographics", {
                "white": match["white_inmates"],
                "black": match["black_inmates"],
                "hispanic": match["hispanic_inmates"],
                "total": match["total_inmates"],
                "source_note": src,
            })
            stats_inserted += 1

    print(f"\nDone!")
    print(f"  Matched:        {matched}/{len(db_facilities)}")
    print(f"  Stats inserted: {stats_inserted}")


if __name__ == "__main__":
    ingest()