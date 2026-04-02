#!/usr/bin/env python3
import os
import datetime
import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

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
        "last_updated": f.get("DateUpdated")
    }

def has_flight_changed(existing, flat):
    """Return True when API data differs from the current flights table row."""
    if existing is None:
        return True
    return any([
        existing["city"] != flat["city"],
        existing["airline_logo"] != flat["airline_logo"],
        existing["status"] != flat["status"],
        existing["st"] != flat["ST"],
        existing["et"] != flat["ET"],
    ])

def mark_dropped_flights(cursor, conn, date_str, tag, seen_flight_numbers, fetched_at):
    """
    Find flights that were in the DB for this date+type but are missing
    from the current API response, and mark their status as 'Dropped'.

    Only flights NOT already in a terminal state are eligible:
    - Excludes: Dropped, Cancelled, Landed, Departed
    - This avoids re-flagging flights that already have a final status.
    """
    if not seen_flight_numbers:
        # If API returned nothing at all, skip — likely a failed fetch,
        # not a real drop. We don't want to mass-mark everything as dropped.
        log(f"[DROP] Skipping drop check for {tag} {date_str} — API returned 0 flights")
        return

    TERMINAL_STATUSES = ("Dropped", "Cancelled", "Landed", "Departed")

    # Fetch all active flights for this date+type that weren't in the API response
    cursor.execute("""
        SELECT flight_number, status
        FROM flights
        WHERE scheduled_date = %s
          AND type = %s
          AND flight_number != ALL(%s)
          AND (status IS NULL OR status NOT ILIKE ANY(%s))
    """, (
        date_str,
        tag,
        list(seen_flight_numbers),
        list(TERMINAL_STATUSES)
    ))

    dropped = cursor.fetchall()
    if not dropped:
        return

    dropped_numbers = [row["flight_number"] for row in dropped]
    log(f"[DROP] Marking {len(dropped_numbers)} flights as Dropped for {tag} {date_str}: {dropped_numbers}")

    # Update their status in the flights table
    cursor.execute("""
        UPDATE flights
        SET status = 'Dropped',
            last_checked = %s
        WHERE scheduled_date = %s
          AND type = %s
          AND flight_number = ANY(%s)
    """, (fetched_at, date_str, tag, dropped_numbers))

    # Also insert a snapshot row for each dropped flight so the event is recorded
    snapshot_rows = [
        (fn, date_str, fetched_at, True, "Dropped", None, None, None, tag, None)
        for fn in dropped_numbers
    ]
    try:
        execute_values(cursor, """
            INSERT INTO flight_snapshots (
                flight_number, scheduled_date, scraped_at, is_changed,
                status, ST, ET, city, type, airline_logo
            ) VALUES %s
        """, snapshot_rows)
    except Exception as e:
        log(f"[DROP] Snapshot insert failed for dropped flights: {e}")
        raise


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

    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
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

                # Track which flight numbers the API returned this run
                seen_flight_numbers = set()

                if not flights:
                    # Don't run drop detection — fetch may have simply failed
                    continue

                changed_count = 0
                snapshot_rows = []

                for f in flights:
                    flat = flatten_flight(f, tag, date_str, fetched_at)
                    if not flat:
                        continue

                    seen_flight_numbers.add(flat["flight_number"])

                    cursor.execute("""
                        SELECT city, airline_logo, status, ST, ET
                        FROM flights
                        WHERE flight_number = %(flight_number)s
                          AND scheduled_date = %(scheduled_date)s
                          AND type = %(type)s
                    """, flat)
                    existing = cursor.fetchone()

                    is_changed = has_flight_changed(existing, flat)

                    cursor.execute("""
                        INSERT INTO flights (
                            flight_number, scheduled_date, type, city, airline_logo,
                            status, ST, ET, last_checked, last_updated
                        ) VALUES (
                            %(flight_number)s, %(scheduled_date)s, %(type)s, %(city)s, %(airline_logo)s,
                            %(status)s, %(ST)s, %(ET)s, %(last_checked)s, %(last_updated)s
                        )
                        ON CONFLICT (flight_number, scheduled_date, type)
                        DO UPDATE SET
                            city = EXCLUDED.city,
                            airline_logo = EXCLUDED.airline_logo,
                            status = EXCLUDED.status,
                            ST = EXCLUDED.ST,
                            ET = EXCLUDED.ET,
                            last_updated = CASE
                                WHEN flights.status IS DISTINCT FROM EXCLUDED.status
                                     OR flights.ST IS DISTINCT FROM EXCLUDED.ST
                                     OR flights.ET IS DISTINCT FROM EXCLUDED.ET
                                     OR flights.city IS DISTINCT FROM EXCLUDED.city
                                     OR flights.airline_logo IS DISTINCT FROM EXCLUDED.airline_logo
                                THEN EXCLUDED.last_updated
                                ELSE flights.last_updated
                            END;
                    """, flat)

                    if is_changed:
                        changed_count += 1

                    snapshot_rows.append({
                        **flat,
                        "scraped_at": fetched_at,
                        "is_changed": is_changed,
                        "last_updated": flat["last_updated"]
                    })

                # Batch insert snapshots for seen flights
                if snapshot_rows:
                    try:
                        execute_values(cursor, """
                            INSERT INTO flight_snapshots (
                                flight_number, scheduled_date, scraped_at, is_changed,
                                status, ST, ET, city, type, airline_logo,last_updated
                            ) VALUES %s
                        """, [
                            (
                                r["flight_number"],
                                r["scheduled_date"],
                                r["scraped_at"],
                                r["is_changed"],
                                r["status"],
                                r["ST"],
                                r["ET"],
                                r["city"],
                                r["type"],
                                r["airline_logo"]
                            ) for r in snapshot_rows
                        ])
                    except Exception as e:
                        log(f"SNAPSHOT INSERT FAILED: {e}")
                        conn.rollback()
                        raise

                # Mark any flights missing from this fetch as Dropped
                mark_dropped_flights(cursor, conn, date_str, tag, seen_flight_numbers, fetched_at)

                log(f"{changed_count} flights changed for {tag} {date_str}")
                conn.commit()

    finally:
        cursor.close()
        conn.close()
        log("DB connection closed.")

if __name__ == "__main__":
    main()
