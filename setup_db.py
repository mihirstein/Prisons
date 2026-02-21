# setup_db.py
from db import execute, query

def create_tables():
    execute("""
        CREATE TABLE IF NOT EXISTS facilities (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            aliases TEXT[] DEFAULT '{}',
            state TEXT,
            city TEXT,
            facility_type TEXT,
            operator TEXT,
            capacity INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS cases (
            id SERIAL PRIMARY KEY,
            facility_id INTEGER REFERENCES facilities(id),
            courtlistener_id TEXT,
            docket_number TEXT,
            case_name TEXT NOT NULL,
            court TEXT,
            date_filed DATE,
            status TEXT,
            case_type TEXT,
            summary TEXT,
            url TEXT,
            last_synced TIMESTAMP DEFAULT NOW(),
            UNIQUE(facility_id, case_name)
        );

        CREATE TABLE IF NOT EXISTS doj_actions (
            id SERIAL PRIMARY KEY,
            facility_id INTEGER REFERENCES facilities(id),
            title TEXT NOT NULL,
            action_date TEXT,
            action_type TEXT,
            agency TEXT,
            summary TEXT,
            key_findings TEXT[] DEFAULT '{}',
            pdf_url TEXT,
            full_text TEXT,
            last_synced TIMESTAMP DEFAULT NOW(),
            UNIQUE(facility_id, title)
        );

        CREATE TABLE IF NOT EXISTS facility_stats (
            id SERIAL PRIMARY KEY,
            facility_id INTEGER REFERENCES facilities(id),
            year INTEGER,
            stat_type TEXT,
            value JSONB,
            source TEXT,
            last_synced TIMESTAMP DEFAULT NOW(),
            UNIQUE(facility_id, year, stat_type)
        );
    """)
    print("Tables created.")

def seed_facilities():
    facilities = [
        ("Rikers Island", ["rikers", "rikers island", "nyc jail"], "NY", "New York", "jail", "NYC Department of Correction", 10000),
        ("Mississippi State Penitentiary", ["parchman", "parchman farm"], "MS", "Parchman", "state_prison", "Mississippi DOC", 3560),
        ("Angola", ["angola", "louisiana state penitentiary", "lsp"], "LA", "Angola", "state_prison", "Louisiana DOC", 6300),
        ("San Quentin State Prison", ["san quentin"], "CA", "San Quentin", "state_prison", "California CDCR", 3082),
        ("Attica Correctional Facility", ["attica"], "NY", "Attica", "state_prison", "New York DOCCS", 2200),
        ("Cook County Jail", ["cook county", "ccdoc"], "IL", "Chicago", "jail", "Cook County Sheriff", 10000),
        ("Pelican Bay State Prison", ["pelican bay", "pbsp"], "CA", "Crescent City", "state_prison", "California CDCR", 2280),
        ("ADX Florence", ["adx", "florence", "supermax"], "CO", "Florence", "federal_prison", "Federal BOP", 490),
        ("Sing Sing Correctional Facility", ["sing sing"], "NY", "Ossining", "state_prison", "New York DOCCS", 1700),
        ("Folsom State Prison", ["folsom"], "CA", "Folsom", "state_prison", "California CDCR", 2469),
    ]

    for name, aliases, state, city, ftype, operator, capacity in facilities:
        execute(
            """INSERT INTO facilities (name, aliases, state, city, facility_type, operator, capacity)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (name, aliases, state, city, ftype, operator, capacity)
        )

    print("Facilities seeded.")

if __name__ == "__main__":
    create_tables()
    seed_facilities()
    
    # Verify
    results = query("SELECT id, name, state FROM facilities ORDER BY id")
    print(f"\n{len(results)} facilities in database:")
    for r in results:
        print(f"  [{r['id']}] {r['name']} ({r['state']})")