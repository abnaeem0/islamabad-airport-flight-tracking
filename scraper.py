#!/usr/bin/env python3
import os
import requests
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib3

# ===== CONFIG =====
CITY = "Islamabad"
TAGS = ["Arrival", "Departure"]
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/" + CITY
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# DB from secrets
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def fetch_flights(date_str, flight_type):
    url = PAA_TEMPLATE.format(date=date_str, type=flight_type)
    log(f"Fetching {flight_type} flights for {date_str}")
    try:
        r = requests.get(url, timeout=20, verify=False)
        r.raise_for_status()
        data = r.json()
        log(f"{len(data)} flights retrieved")
        return data
    except Exception as e:
        log(f"Error fetching flights: {e}")
        return []

def flatten_flight(f, flight_type, date_str, fetched_at):
    flight_number = f.get("FlightNumber")
    if not flight_number:
        log(f"[WARNING] Missing FlightNumber, skipping: {f}")
        return None

    city = f.get("EnglishFromCity") if flight_type.lower() == "arrival" else f.get("EnglishToCity")

    return {
        "flight_number": flight_number,
        "scheduled_date": date_str,
        "type": flight_type,
        "city": city,
        "airline_logo": f.get("Logo"),
        "status": f.get("EnglishRemarks"),
        "ST": f.get("ST"),
        "ET": f.get("ET"),
        "last_checked": fetched_at,
        "last_updated": fetched_at
    }

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
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        log("✅ DB connection successful!")
    except Exception as e:
        log(f"❌ DB connection failed: {e}")
        return

    # PKT timezone adjustment
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=5)
    dates = [
        (now + datetime.timedelta(days=-1)).strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d"),
        (now + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for date_str in dates:
        for tag in TAGS:
            flights = fetch_flights(date_str, tag)
            changed_count = 0

            for f in flights:
                flat = flatten_flight(f, tag, date_str, fetched_at)
                if not flat:
                    continue

                # Upsert flights
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
                    # Append snapshot
                    cursor.execute("""
                        INSERT INTO flight_snapshots (
                            flight_number, scheduled_date, scraped_at, is_changed, status, ST, ET, city, type, airline_logo
                        ) VALUES (
                            %(flight_number)s, %(scheduled_date)s, %(scraped_at)s, TRUE, %(status)s, %(ST)s, %(ET)s, %(city)s, %(type)s, %(airline_logo)s
                        )
                    """, {**flat, "scraped_at": fetched_at})

            log(f"{changed_count} flights changed for {tag} {date_str}")

    conn.commit()
    cursor.close()
    conn.close()
    log("✅ Scraper run completed!")

if __name__ == "__main__":
    main()
