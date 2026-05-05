#!/usr/bin/env python3
import os
import datetime
import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==============================================================================
#   CONFIG
# ==============================================================================

CITY         = "Islamabad"
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/" + CITY

# Statuses that mean a flight is finished — we don't snapshot these again
TERMINAL_STATUSES = ("Dropped", "Cancelled", "Landed", "Departed")

# DB credentials come from environment variables (never hardcode these)
DB_HOST     = os.environ.get("DB_HOST")
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))


# ==============================================================================
#   LOGGING
# ==============================================================================

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# ==============================================================================
#   PAA API
# ==============================================================================

def fetch_flights(date_str, tag):
    """
    Fetch flights from the PAA API for a given date and type (Arrival/Departure).
    Returns a list of raw flight dicts, or an empty list on failure.
    """
    url = PAA_TEMPLATE.format(date=date_str, type=tag)
    try:
        r = requests.get(url, verify=False, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                log(f"  {len(data)} flights fetched for {tag} on {date_str}")
                return data
        log(f"  [WARN] Fetch failed for {url} — HTTP {r.status_code}")
    except Exception as e:
        log(f"  [ERROR] Exception fetching {url}: {e}")
    return []


def flatten_flight(raw, tag, date_str, fetched_at):
    """
    Convert a raw PAA API flight dict into our DB-friendly flat dict.
    Returns None if the flight has no flight number (unusable record).
    """
    flight_number = raw.get("FlightNumber")
    if not flight_number:
        log(f"  [WARN] Skipping record with no FlightNumber for {tag} {date_str}")
        return None

    return {
        "flight_number": flight_number.replace(" ", ""),
        "scheduled_date": date_str,
        "type":           tag,
        "city":           raw.get("EnglishFromCity") if tag == "Arrival" else raw.get("EnglishToCity"),
        "airline_logo":   raw.get("Logo"),
        "status":         raw.get("EnglishRemarks"),
        "ST":             raw.get("ST"),
        "ET":             raw.get("ET"),
        "last_checked":   fetched_at,
        "last_updated":   raw.get("DateUpdated"),
        "nature": raw.get("Nature"),
    }


# ==============================================================================
#   CHANGE DETECTION
# ==============================================================================

def detect_change(existing, flat):
    """
    Compare the current DB row against the freshly fetched flight data.

    Returns a tuple: (is_changed: bool, change_type: str | None)

    change_type values:
      - "new"           — flight not seen before
      - "status_change" — status text changed
      - "time_change"   — ST or ET changed
      - "city_change"   — origin/destination city changed
      - None            — nothing changed
    """

    # Brand new flight — never seen before
    if existing is None:
        return True, "new"

    # Status changed (e.g. On Time → Delayed, or blank → Cancelled)
    if existing["status"] != flat["status"]:
        return True, "status_change"

    # Scheduled or estimated time changed
    if existing["st"] != flat["ST"] or existing["et"] != flat["ET"]:
        return True, "time_change"

    # City changed (rare but possible with routing changes)
    if existing["city"] != flat["city"]:
        return True, "city_change"

    # Nothing changed
    return False, None


# ==============================================================================
#   DROP DETECTION
# ==============================================================================

def mark_dropped_flights(cursor, date_str, tag, seen_flight_numbers, fetched_at):
    """
    After processing all API flights, check whether any flights that were
    previously in our DB are now missing from the API response.

    If a flight disappears without reaching a terminal status, we mark it
    as 'Dropped' — the airport silently removed it without cancelling it.

    Safety check: if the API returned zero flights, we skip this entirely
    to avoid mass-marking everything as dropped due to a failed fetch.
    """
    if not seen_flight_numbers:
        log(f"  [DROP] Skipping drop check — API returned 0 flights for {tag} {date_str}")
        return

    cursor.execute("""
        SELECT flight_number
        FROM flights
        WHERE scheduled_date = %s
          AND type = %s
          AND flight_number != ALL(%s)
          AND (status IS NULL OR status NOT ILIKE ANY(%s))
    """, (
        date_str,
        tag,
        list(seen_flight_numbers),
        list(TERMINAL_STATUSES),
    ))

    dropped = [row["flight_number"] for row in cursor.fetchall()]
    if not dropped:
        return

    log(f"  [DROP] Marking {len(dropped)} flights as Dropped for {tag} {date_str}: {dropped}")

    # Update status in the flights table
    cursor.execute("""
        UPDATE flights
        SET status = 'Dropped', last_checked = %s
        WHERE scheduled_date = %s
          AND type = %s
          AND flight_number = ANY(%s)
    """, (fetched_at, date_str, tag, dropped))

    # Insert one snapshot per dropped flight to record the event
    execute_values(cursor, """
        INSERT INTO flight_snapshots (
            flight_number, scheduled_date, scraped_at, is_changed,
            change_type, status, ST, ET, city, type, airline_logo, nature
        ) VALUES %s
    """, [
        (fn, date_str, fetched_at, True, "dropped", "Dropped", None, None, None, tag, None)
        for fn in dropped
    ])


# ==============================================================================
#   SCRAPER STATUS (freshness timestamp for the frontend)
# ==============================================================================

def update_scraper_status(cursor):
    """
    Update the single-row scraper_status table with the current UTC timestamp.
    The frontend reads this to show users "Last checked X minutes ago."
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO scraper_status (id, last_run)
        VALUES (1, %s)
        ON CONFLICT (id) DO UPDATE SET last_run = EXCLUDED.last_run
    """, (now_utc,))


# ==============================================================================
#   CLEANUP (keeps DB lean — deletes data older than 2 months)
# ==============================================================================

def cleanup_old_data(cursor):
    """
    Delete flights and snapshots older than 2 months.
    Runs at the end of every scrape to keep the DB size manageable.
    """
    cursor.execute("""
        DELETE FROM flight_snapshots
        WHERE scraped_at < NOW() - INTERVAL '2 months'
    """)
    cursor.execute("""
        DELETE FROM flights
        WHERE scheduled_date < (CURRENT_DATE - INTERVAL '2 months')
    """)
    log("  [CLEANUP] Old data deleted (>2 months)")


# ==============================================================================
#   MAIN
# ==============================================================================

def main():

    # --- Connect to DB ---
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER,
            password=DB_PASSWORD, port=DB_PORT, sslmode="require"
        )
        log("✅ DB connected")
    except Exception as e:
        log(f"❌ DB connection failed: {e}")
        raise

    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        now = datetime.datetime.now()

        # Scrape yesterday, today, and tomorrow
        dates = [
            (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
            (now + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        ]

        for date_str in dates:
            for tag in ["Arrival", "Departure"]:

                log(f"\n--- {tag} | {date_str} ---")
                fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

                # --- Fetch from PAA API ---
                raw_flights = fetch_flights(date_str, tag)
                if not raw_flights:
                    continue  # Skip drop detection too — fetch may have failed

                seen_flight_numbers = set()
                changed_count       = 0
                snapshot_rows       = []

                # --- Process each flight ---
                for raw in raw_flights:
                    flat = flatten_flight(raw, tag, date_str, fetched_at)
                    if not flat:
                        continue

                    seen_flight_numbers.add(flat["flight_number"])

                    # Look up existing DB row for this flight
                    cursor.execute("""
                        SELECT city, status, st, et
                        FROM flights
                        WHERE flight_number = %(flight_number)s
                          AND scheduled_date = %(scheduled_date)s
                          AND type = %(type)s
                    """, flat)
                    existing = cursor.fetchone()

                    # Detect what (if anything) changed
                    is_changed, change_type = detect_change(existing, flat)

                    # --- Upsert into flights table ---
                    # Always update the flights table with the latest data.
                    # last_updated is only changed when meaningful fields change.
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
                            city          = EXCLUDED.city,
                            airline_logo  = EXCLUDED.airline_logo,
                            status        = EXCLUDED.status,
                            ST            = EXCLUDED.ST,
                            ET            = EXCLUDED.ET,
                            last_checked  = EXCLUDED.last_checked,
                            nature        = EXCLUDED.nature,
                            last_updated  = CASE
                                WHEN flights.status   IS DISTINCT FROM EXCLUDED.status
                                  OR flights.ST       IS DISTINCT FROM EXCLUDED.ST
                                  OR flights.ET       IS DISTINCT FROM EXCLUDED.ET
                                  OR flights.city     IS DISTINCT FROM EXCLUDED.city
                                THEN EXCLUDED.last_updated
                                ELSE flights.last_updated
                            END
                    """, flat)

                    # --- Only snapshot when something actually changed ---
                    if is_changed:
                        changed_count += 1
                        snapshot_rows.append((
                            flat["flight_number"],
                            flat["scheduled_date"],
                            fetched_at,
                            True,
                            change_type,
                            flat["status"],
                            flat["ST"],
                            flat["ET"],
                            flat["city"],
                            flat["type"],
                            flat["airline_logo"],
                        ))

                # --- Batch insert changed snapshots ---
                if snapshot_rows:
                    try:
                        execute_values(cursor, """
                            INSERT INTO flight_snapshots (
                                flight_number, scheduled_date, scraped_at, is_changed,
                                change_type, status, ST, ET, city, type, airline_logo
                            ) VALUES %s
                        """, snapshot_rows)
                    except Exception as e:
                        log(f"  [ERROR] Snapshot insert failed: {e}")
                        conn.rollback()
                        raise

                # --- Check for silently dropped flights ---
                mark_dropped_flights(cursor, date_str, tag, seen_flight_numbers, fetched_at)

                log(f"  {changed_count} changes recorded")
                conn.commit()

        # --- Update freshness timestamp ---
        update_scraper_status(cursor)

        # --- Clean up old data ---
        cleanup_old_data(cursor)

        conn.commit()
        log("\n✅ Scrape complete")

    finally:
        cursor.close()
        conn.close()
        log("DB connection closed.")


if __name__ == "__main__":
    main()
