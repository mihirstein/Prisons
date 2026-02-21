# scrape_doj_real.py
# Downloads actual DOJ findings letters (PDFs), extracts text,
# and stores real findings in your database.
#
# All sources are public domain government documents.

import os
import time
import requests
import pdfplumber
from setup_db import execute, query

PDF_DIR = "./data/doj_pdfs"
os.makedirs(PDF_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "GroundTruth Legal Research Tool (educational project)"
}

# ── Known DOJ findings letters and actions with real PDF URLs ──────────
# These are the actual documents from justice.gov
DOJ_DOCUMENTS = [
    {
        "facility": "Rikers Island",
        "title": "CRIPA Investigation of NYC Department of Correction Jails on Rikers Island",
        "date": "2014-08-04",
        "action_type": "findings_letter",
        "pdf_url": "https://www.justice.gov/sites/default/files/usao-sdny/legacy/2015/03/25/SDNY%20Rikers%20Report.pdf",
        "press_release": "https://www.justice.gov/usao-sdny/pr/manhattan-us-attorney-finds-pattern-and-practice-excessive-force-and-violence-nyc-jails",
        # From the actual press release and findings letter:
        "known_findings": [
            "Pattern and practice of conduct that violates the constitutional rights of adolescent inmates",
            "Deep-seated culture of violence pervasive throughout the adolescent facilities",
            "Staff routinely use force not as a last resort but to control and punish disorderly behavior",
            "In FY 2013, 565 reported staff use of force incidents in average daily adolescent population of 682, resulting in 1,057 injuries",
            "In FY 2013, 845 reported inmate-on-inmate fights involving adolescents",
            "Approximately 51% of adolescent inmates diagnosed with some form of mental illness",
            "Inadequate protection of adolescents from violence by other inmates",
            "Excessive use of punitive segregation for adolescents",
            "Use of force reports critically inadequate — unnecessary or excessive force goes undiscovered and unchallenged",
            "Staff feel empowered to use force inappropriately because they know they are unlikely to face consequences",
        ],
        "recommended_remedies": [
            "House adolescent inmates separately in a jail not physically located on Rikers Island",
            "Increase the number of cameras in adolescent areas",
            "Revise use of force policy to clarify prohibited conduct",
            "Institute zero-tolerance policy for failing to report use of force",
            "Ensure use of force incidents are investigated thoroughly and promptly",
            "Improve officer training on use of force, conflict resolution, and adolescent handling",
            "Ensure staff are held accountable and disciplined for excessive and unnecessary force",
        ],
    },
    {
        "facility": "Mississippi State Penitentiary",
        "title": "DOJ Findings Letter — Mississippi Department of Corrections (Parchman)",
        "date": "2022-03-17",
        "action_type": "findings_letter",
        "pdf_url": "https://www.justice.gov/d9/press-releases/attachments/2022/04/20/mdoc_parchman_findings_report_0.pdf",
        "press_release": "https://www.justice.gov/opa/pr/justice-department-finds-mississippi-department-corrections-fails-adequately-protect",
        "known_findings": [
            "Reasonable cause to believe conditions at Parchman violate the Eighth Amendment",
            "Pervasive prisoner-on-prisoner violence due to severe understaffing",
            "Prolonged solitary confinement causing serious psychological harm",
            "Grossly inadequate mental health care",
            "Dangerous physical plant conditions including broken locks and lighting",
            "Failure to implement basic safety and supervision protocols",
            "Prisoners subjected to violence from other prisoners with alarming regularity",
            "Staff vacancy rates critically high — insufficient officers to maintain basic safety",
            "Mental health treatment limited to medication management with no meaningful therapy",
            "Solitary confinement used excessively, including for prisoners with serious mental illness",
        ],
        "recommended_remedies": [
            "Hire sufficient correctional staff to maintain safe staffing levels",
            "Implement violence reduction strategies and classification system reforms",
            "End prolonged solitary confinement for prisoners with serious mental illness",
            "Provide adequate mental health care including therapy and crisis intervention",
            "Repair physical plant deficiencies including locks, lighting, and plumbing",
            "Establish effective grievance and incident reporting systems",
        ],
    },
    {
        "facility": "Mississippi State Penitentiary",
        "title": "DOJ Opens Investigation of Mississippi State Penitentiary at Parchman",
        "date": "2020-02-05",
        "action_type": "investigation_opening",
        "pdf_url": "",
        "press_release": "https://www.justice.gov/opa/pr/department-justice-announces-investigation-mississippi-state-penitentiary-parchman",
        "known_findings": [
            "Investigation opened following series of inmate deaths in January 2020",
            "Investigation to examine whether state protects prisoners from harm",
            "Focus on adequacy of mental health care",
            "Investigation conducted under CRIPA authority",
        ],
        "recommended_remedies": [],
    },
    {
        "facility": "Cook County Jail",
        "title": "DOJ CRIPA Findings Letter — Cook County Jail",
        "date": "2008-07-11",
        "action_type": "findings_letter",
        "pdf_url": "https://www.justice.gov/crt/about/spl/documents/cook_county_jail_findlet_7-11-08.pdf",
        "press_release": "",
        "known_findings": [
            "Unconstitutional conditions including excessive use of force by staff",
            "Inadequate medical and mental health care for detainees",
            "Dangerous overcrowding affecting safety and sanitation",
            "Failure to protect inmates from harm by other inmates",
            "Environmental health and safety deficiencies",
            "Inadequate fire safety measures",
        ],
        "recommended_remedies": [
            "Reform use of force policies and training",
            "Improve medical and mental health staffing and services",
            "Reduce overcrowding to safe levels",
            "Implement effective classification and housing systems",
            "Address environmental health and fire safety deficiencies",
        ],
    },
    {
        "facility": "Cook County Jail",
        "title": "DOJ Settlement Agreement — Cook County Jail",
        "date": "2010-05-13",
        "action_type": "consent_decree",
        "pdf_url": "",
        "press_release": "",
        "known_findings": [
            "Settlement resolving CRIPA investigation findings",
            "Cook County agreed to comprehensive reforms",
        ],
        "recommended_remedies": [
            "Comprehensive use of force reform",
            "Improved medical and mental health care delivery",
            "Classification system overhaul",
            "Environmental health remediation",
        ],
    },
    {
        "facility": "Angola",
        "title": "Ball v. LeBlanc — DOJ Statement of Interest (Angola Heat Conditions)",
        "date": "2013-08-01",
        "action_type": "statement_of_interest",
        "pdf_url": "",
        "press_release": "https://www.justice.gov/crt/special-litigation-section-cases-and-matters",
        "known_findings": [
            "DOJ supported claims that extreme heat conditions at Angola violate the Eighth Amendment",
            "Elderly and medically vulnerable prisoners at particular risk from heat exposure",
            "Facility lacked adequate cooling measures",
        ],
        "recommended_remedies": [],
    },
    {
        "facility": "Angola",
        "title": "DOJ Findings Report — Louisiana Department of Corrections (Overdetention)",
        "date": "2023-01-25",
        "action_type": "findings_letter",
        "pdf_url": "",
        "press_release": "https://www.justice.gov/crt/special-litigation-section-cases-and-matters",
        "known_findings": [
            "Overdetention routinely occurring — thousands held past legal release dates each year",
            "State failed to take reasonable steps to address systemic deficiencies despite knowing about the problem for over a decade",
            "Pattern and practice of detaining people after fully completing sentences",
            "Violations of 14th Amendment Due Process Clause",
        ],
        "recommended_remedies": [
            "Implement systems to accurately track release dates",
            "Establish oversight mechanisms to prevent overdetention",
            "Provide compensation or remedies for persons wrongfully detained beyond sentences",
        ],
    },
    {
        "facility": "Angola",
        "title": "United States v. Louisiana — DOJ Complaint (Overdetention)",
        "date": "2024-12-20",
        "action_type": "complaint",
        "pdf_url": "",
        "press_release": "https://www.justice.gov/archives/opa/pr/justice-department-sues-state-louisiana-incarcerating-people-beyond-their-release-dates",
        "known_findings": [
            "Federal lawsuit filed alleging pattern and practice of confining people beyond completed sentences",
            "Violations of 14th Amendment Due Process Clause",
            "State and LDOC deliberately indifferent to overdetention despite longstanding knowledge",
            "Seeks injunctive relief — does not seek monetary damages",
        ],
        "recommended_remedies": [],
    },
    {
        "facility": "Attica Correctional Facility",
        "title": "Disability Rights New York v. DOCCS — DOJ Statement of Interest",
        "date": "2020-06-01",
        "action_type": "statement_of_interest",
        "pdf_url": "",
        "press_release": "https://www.justice.gov/crt/special-litigation-section-cases-and-matters",
        "known_findings": [
            "DOJ supported claims regarding rights of persons with disabilities in New York DOCCS facilities",
            "Statement addressed ADA compliance in state correctional system",
        ],
        "recommended_remedies": [],
    },
]


def download_pdf(url, filename):
    """Download a PDF from DOJ website."""
    filepath = os.path.join(PDF_DIR, filename)
    if os.path.exists(filepath):
        print(f"    Already downloaded: {filename}")
        return filepath

    try:
        print(f"    Downloading: {url[:80]}...")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"    Saved: {filename} ({len(response.content) // 1024}KB)")
        return filepath
    except Exception as e:
        print(f"    Download failed: {e}")
        return None


def extract_pdf_text(filepath):
    """Extract text from a PDF using pdfplumber."""
    try:
        text = ""
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        print(f"    Extracted {len(text)} characters from {len(pdf.pages)} pages")
        return text
    except Exception as e:
        print(f"    PDF extraction failed: {e}")
        return None


def extract_key_sections(text):
    """Try to extract key sections from DOJ findings letter text."""
    sections = {
        "findings": "",
        "background": "",
        "remedies": "",
    }

    if not text:
        return sections

    text_lower = text.lower()

    # Try to find the findings section
    findings_markers = ["our investigation found", "we find that", "findings", "our investigation revealed",
                        "we concluded", "reasonable cause to believe", "pattern or practice"]
    for marker in findings_markers:
        idx = text_lower.find(marker)
        if idx != -1:
            # Grab ~2000 chars after the marker
            sections["findings"] = text[idx:idx + 2000].strip()
            break

    # Try to find remedies section
    remedy_markers = ["minimum remedial measures", "recommended remedial", "recommendations",
                      "we recommend", "remedial measures"]
    for marker in remedy_markers:
        idx = text_lower.find(marker)
        if idx != -1:
            sections["remedies"] = text[idx:idx + 2000].strip()
            break

    return sections


def match_facility(name):
    """Find facility ID by name."""
    facilities = query("SELECT * FROM facilities")
    name_lower = name.lower()
    for f in facilities:
        if f["name"].lower() == name_lower:
            return f["id"]
        for alias in (f.get("aliases") or []):
            if alias.lower() in name_lower or name_lower in alias.lower():
                return f["id"]
    return None


def seed_document(doc):
    """Process and store one DOJ document."""
    facility_id = match_facility(doc["facility"])
    if not facility_id:
        print(f"  SKIP: Cannot match facility '{doc['facility']}'")
        return False

    print(f"\n  Processing: {doc['title'][:70]}...")

    # Try to download and extract PDF if URL exists
    full_text = None
    extracted_sections = {"findings": "", "background": "", "remedies": ""}

    if doc["pdf_url"]:
        safe_name = doc["title"][:50].replace(" ", "_").replace("/", "_") + ".pdf"
        filepath = download_pdf(doc["pdf_url"], safe_name)
        if filepath:
            full_text = extract_pdf_text(filepath)
            if full_text:
                extracted_sections = extract_key_sections(full_text)
            time.sleep(1)  # be polite to justice.gov

    # Build the summary from known findings
    summary_parts = []
    if doc.get("known_findings"):
        summary_parts.append("KEY FINDINGS:")
        for f in doc["known_findings"]:
            summary_parts.append(f"• {f}")

    if doc.get("recommended_remedies"):
        summary_parts.append("\nRECOMMENDED REMEDIES:")
        for r in doc["recommended_remedies"]:
            summary_parts.append(f"• {r}")

    # If we extracted real text, add a note
    if extracted_sections["findings"]:
        summary_parts.append(f"\n[Full findings text extracted from PDF — {len(full_text)} characters]")

    summary = "\n".join(summary_parts)

    # Store
    try:
        execute(
            """INSERT INTO doj_actions 
               (facility_id, title, action_date, action_type, agency, summary, 
                key_findings, pdf_url, full_text, last_synced)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (facility_id, title) DO UPDATE SET
                 summary = EXCLUDED.summary,
                 key_findings = EXCLUDED.key_findings,
                 full_text = EXCLUDED.full_text,
                 pdf_url = EXCLUDED.pdf_url,
                 last_synced = NOW()""",
            (
                facility_id,
                doc["title"],
                doc["date"],
                doc["action_type"],
                "DOJ Civil Rights Division — Special Litigation Section",
                summary,
                doc.get("known_findings", []),
                doc["pdf_url"] or doc.get("press_release", ""),
                full_text[:50000] if full_text else None,  # cap at 50k chars
            )
        )
        print(f"  ✓ Stored: {doc['title'][:60]}")
        return True
    except Exception as e:
        print(f"  ERROR storing: {e}")
        return False


def run():
    print("=" * 60)
    print("GROUNDTRUTH — DOJ Document Scraper")
    print("=" * 60)
    print(f"\nProcessing {len(DOJ_DOCUMENTS)} known DOJ documents...\n")

    success = 0
    failed = 0

    for doc in DOJ_DOCUMENTS:
        if seed_document(doc):
            success += 1
        else:
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"COMPLETE: {success} stored, {failed} failed")
    print(f"{'=' * 60}")

    # Summary
    counts = query("""
        SELECT f.name, COUNT(d.id) as count
        FROM facilities f
        JOIN doj_actions d ON d.facility_id = f.id
        GROUP BY f.name
        ORDER BY count DESC
    """)
    print("\nDOJ actions by facility:")
    for row in counts:
        print(f"  {row['name']}: {row['count']} actions")

    # Check for PDFs downloaded
    pdfs = os.listdir(PDF_DIR)
    print(f"\nPDFs downloaded: {len(pdfs)}")
    for p in pdfs:
        size = os.path.getsize(os.path.join(PDF_DIR, p)) // 1024
        print(f"  {p} ({size}KB)")


if __name__ == "__main__":
    run()