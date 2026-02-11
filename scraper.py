#!/usr/bin/env python3
import os
import datetime
import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_values

# ===== CONFIG =====
CITY = "Islamabad"
SOURCE = "PAA"
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/" + CITY
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===== DB CONFIG =====
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

# ===== UTILS =====
def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def fetch_flights(date_str, tag):
    url = PAA_TEMPLATE.format(date=date_str, type=tag)
    try:
        r = requests.get(url, verify=False, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                log(f"{len(data)} flights retrieved for {tag} {date_str}")
                return data
        log(f"Fetch failed {url} code {r.status_code}")
    except Exception as e:
        log(f"Error fetching {url}: {e}")
    return []

def flatten_flight(f, tag, date_str, fetched_at):
    flight_number = f.get("FlightNumber")
    if not flight_number:
        log(f"[WARNING] Skipping flight with missing FlightNumber for {tag} {date_str}")
        return None
    return {
        "flight_number": flight_number,
        "scheduled_date": date_str,
        "type": tag,
        "city": f.get("EnglishFromCity") if tag == "Arrival" else f.get("EnglishToCity"),
        "airline_logo": f.get("Logo"),
        "status": f.get("EnglishRemarks"),
        "ST": f.get("ST"),
        "ET": f.get("ET"),
        "last_checked": fetched_at,
        "last_updated": fetched_at
    }

# ===== MAIN =====
def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            sslmode="require"
        )
        log("✅ DB connection successful!")
    except Exception as e:
        log(f"❌ DB connection failed: {e}")
        raise

    cursor = conn.cursor()

    now = datetime.datetime.now()
    dates = [
        (now + datetime.timedelta(days=-1)).strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d"),
        (now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    tags = ["Arrival", "Departure"]

    for date_str in dates:
        for tag in tags:
            fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            flights = fetch_flights(date_str, tag)
            if not flights:
                continue

            changed_count = 0
            snapshot_rows = []

            for f in flights:
                flat = flatten_flight(f, tag, date_str, fetched_at)
                if not flat:
                    continue

                # Upsert flights table
                cursor.execute("""
                    INSERT INTO flights (
                        flight_number, scheduled_date, type, city, airline_logo, status, ST, ET, last_checked, last_updated
                    ) VALUES (
                        %(flight_number)s, %(scheduled_date)s, %(type)s, %(city)s, %(airline_logo)s, %(status)s, %(ST)s, %(ET)s, %(last_checked)s, %(last_updated)s
                    )
                    ON CONFLICT (flight_number, scheduled_date, type)
                    DO UPDATE SET
                        city = EXCLUDED.city,
                        airline_logo = EXCLUDED.airline_logo,
                        status = EXCLUDED.status,
                        ST = EXCLUDED.ST,
                        ET = EXCLUDED.ET,
                        last_checked = EXCLUDED.last_checked,
                        last_updated = CASE
                            WHEN flights.status IS DISTINCT FROM EXCLUDED.status
                                 OR flights.ST IS DISTINCT FROM EXCLUDED.ST
                                 OR flights.ET IS DISTINCT FROM EXCLUDED.ET
                            THEN EXCLUDED.last_updated
                            ELSE flights.last_updated
                        END
                    RETURNING *;
                """, flat)

                updated = cursor.fetchone()
                if updated:
                    changed_count += 1
                    # Collect for batch snapshot insert
                    snapshot_rows.append({
                        **flat,
                        "scraped_at": fetched_at
                    })

            # Batch insert snapshots
            if snapshot_rows:
                execute_values(cursor, """
                    INSERT INTO flight_snapshots (
                        flight_number, scheduled_date, scraped_at, is_changed, status, ST, ET, city, type, airline_logo
                    ) VALUES %s
                """, [
                    (
                        r["flight_number"],
                        r["scheduled_date"],
                        r["scraped_at"],
                        True,
                        r["status"],
                        r["ST"],
                        r["ET"],
                        r["city"],
                        r["type"],
                        r["airline_logo"]
                    ) for r in snapshot_rows
                ])

            log(f"{changed_count} flights changed for {tag} {date_str}")
            conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
