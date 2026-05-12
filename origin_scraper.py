#!/usr/bin/env python3
"""
origin_scraper.py — Multi-airport PAA flight tracker
=====================================================
Scrapes flight data from all configured domestic Pakistani airports via the
PAA API and writes it to the origin_flights / origin_snapshots tables.

Key design decisions:
- Completely independent of scraper.py — never touches the original tables
- WATCH_AIRPORTS controls which airports are scraped (trivial to extend)
- data_source = "paa" for all records from this scraper
- Future scrapers (ADS-B, airline websites) write to the same tables
  with a different data_source — no schema changes needed

Run cadence: every 15 minutes via GitHub Actions (see origin_scraper.yml)
"""

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

# All domestic airports to scrape.
# To add a new airport: append its city name exactly as it appears in PAA URLs.
# To restrict to ISB pairs only: set REQUIRE_ISB_LEG = True below.
WATCH_AIRPORTS = [
    "Islamabad",
    "Karachi",
    "Lahore",
    "Faisalabad",
    "Multan",
    "Peshawar",
]

# Set to True to only store flights that have a leg to/from Islamabad.
# Useful if you want to keep the DB lean and ISB-focused.
# Set to False to store all flights at all watched airports.
REQUIRE_ISB_LEG = False

# Data source tag written to every row from this scraper.
# Other scrapers (ADS-B, airline websites) use their own tag.
DATA_SOURCE = "paa"

# PAA API URL template — same pattern as the original scraper, city is variable
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/{city}"

# Statuses that mean a flight is finished — skip re-snapshotting these
TERMINAL_STATUSES = ("Dropped", "Cancelled", "Landed", "Departed")

# How many days on either side of today to scrape
# (yesterday catches late-updated overnight flights; tomorrow gives advance data)
DAY_OFFSETS = [-1, 0, 1]

# DB credentials from environment variables — never hardcoded
DB_HOST     = os.environ.get("DB_HOST")
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))


# ==============================================================================
#   LOGGING
# ==============================================================================

def log(msg: str) -> None:
    """Timestamped stdout log — picked up by GitHub Actions logs."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# ==============================================================================
#   PAA API
# ==============================================================================

def fetch_flights(date_str: str, flight_type: str, city: str) -> list[dict]:
    """
    Fetch flights from the PAA API for a given date, type, and airport city.

    Args:
        date_str:    "YYYY-MM-DD"
        flight_type: "Arrival" or "Departure"
        city:        Airport city name as used in PAA URLs (e.g. "Karachi")

    Returns:
        List of raw flight dicts from the API, or [] on failure.
    """
    url = PAA_TEMPLATE.format(date=date_str, type=flight_type, city=city)
    try:
        r = requests.get(url, verify=False, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                log(f"    {len(data)} flights — {flight_type} | {city} | {date_str}")
                return data
        log(f"  [WARN] HTTP {r.status_code} for {url}")
    except Exception as e:
        log(f"  [ERROR] Exception fetching {url}: {e}")
    return []


def flatten_flight(
    raw: dict,
    flight_type: str,
    date_str: str,
    source_airport: str,
    fetched_at: str,
) -> dict | None:
    """
    Convert a raw PAA API dict into a flat dict ready for DB insertion.

    The PAA API returns the "other end" of the route differently depending
    on flight type:
      - Arrival   → EnglishFromCity (where it came from)
      - Departure → EnglishToCity   (where it's going)

    Returns None if the record has no flight number (unusable).
    """
    flight_number = raw.get("FlightNumber")
    if not flight_number:
        log(f"  [WARN] Skipping record with no FlightNumber ({flight_type} | {source_airport} | {date_str})")
        return None

    return {
        "flight_number":  flight_number.replace(" ", ""),   # normalise: "TK 571" → "TK571"
        "scheduled_date": date_str,
        "type":           flight_type,
        "source_airport": source_airport,
        "data_source":    DATA_SOURCE,
        "city":           raw.get("EnglishFromCity") if flight_type == "Arrival" else raw.get("EnglishToCity"),
        "airline_logo":   raw.get("Logo"),
        "status":         raw.get("EnglishRemarks"),
        "ST":             raw.get("ST"),
        "ET":             raw.get("ET"),
        "nature":         raw.get("Nature"),
        "last_checked":   fetched_at,
        "last_updated":   raw.get("DateUpdated"),
    }


def is_isb_relevant(flat: dict) -> bool:
    """
    Returns True if this flight has a leg to or from Islamabad.
    Used when REQUIRE_ISB_LEG = True to keep the DB lean.
    """
    return (
        flat["source_airport"] == "Islamabad"
        or flat["city"] == "Islamabad"
    )


# ==============================================================================
#   CHANGE DETECTION
# ==============================================================================

def detect_change(existing: dict | None, flat: dict) -> tuple[bool, str | None]:
    """
    Compare the current DB row against freshly fetched data.

    Returns:
        (is_changed, change_type)

    change_type values:
        "new"           — flight not seen before
        "status_change" — status text changed
        "time_change"   — ST or ET changed
        "city_change"   — origin/destination city changed
        None            — nothing changed
    """
    if existing is None:
        return True, "new"

    if existing["status"] != flat["status"]:
        return True, "status_change"

    if existing["st"] != flat["ST"] or existing["et"] != flat["ET"]:
        return True, "time_change"

    if existing["city"] != flat["city"]:
        return True, "city_change"

    return False, None


# ==============================================================================
#   DROP DETECTION
# ==============================================================================

def mark_dropped_flights(
    cursor,
    date_str: str,
    flight_type: str,
    source_airport: str,
    seen_flight_numbers: set,
    fetched_at: str,
) -> None:
    """
    After processing all API flights for one (date, type, airport) batch,
    check whether any flights previously in our DB are now missing.

    A flight that disappears without reaching a terminal status is marked
    "Dropped" — the airport silently removed it without cancelling it.

    Safety: if the API returned zero flights, we skip this entirely to
    avoid mass-marking everything as dropped due to a failed fetch.
    """
    if not seen_flight_numbers:
        log(f"  [DROP] Skipping — API returned 0 flights for {flight_type} | {source_airport} | {date_str}")
        return

    # Find DB rows for this batch that are NOT in the current API response
    # and haven't already reached a terminal status
    cursor.execute("""
        SELECT flight_number
        FROM origin_flights
        WHERE scheduled_date  = %s
          AND type            = %s
          AND source_airport  = %s
          AND data_source     = %s
          AND flight_number  != ALL(%s)
          AND (status IS NULL OR status != ALL(%s))
    """, (
        date_str,
        flight_type,
        source_airport,
        DATA_SOURCE,
        list(seen_flight_numbers),
        list(TERMINAL_STATUSES),
    ))

    dropped = [row["flight_number"] for row in cursor.fetchall()]
    if not dropped:
        return

    log(f"  [DROP] Marking {len(dropped)} as Dropped — {flight_type} | {source_airport} | {date_str}: {dropped}")

    # Update status in origin_flights
    cursor.execute("""
        UPDATE origin_flights
        SET status = 'Dropped', last_checked = %s
        WHERE scheduled_date = %s
          AND type           = %s
          AND source_airport = %s
          AND data_source    = %s
          AND flight_number  = ANY(%s)
    """, (fetched_at, date_str, flight_type, source_airport, DATA_SOURCE, dropped))

    # Insert one snapshot per dropped flight to record the event
    execute_values(cursor, """
        INSERT INTO origin_snapshots (
            flight_number, scheduled_date, source_airport, data_source, type,
            scraped_at, is_changed, change_type,
            status, ST, ET, city, airline_logo, nature
        ) VALUES %s
    """, [
        (fn, date_str, source_airport, DATA_SOURCE, flight_type,
         fetched_at, True, "dropped",
         "Dropped", None, None, None, None, None)
        for fn in dropped
    ])


# ==============================================================================
#   SCRAPER STATUS
# ==============================================================================

def update_scraper_status(cursor) -> None:
    """
    Upsert the last_run timestamp for this scraper into origin_scraper_status.
    The frontend reads this to show "Last checked X minutes ago."
    scraper_id = "paa_origin" identifies this specific scraper.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO origin_scraper_status (scraper_id, last_run)
        VALUES ('paa_origin', %s)
        ON CONFLICT (scraper_id) DO UPDATE SET last_run = EXCLUDED.last_run
    """, (now_utc,))


# ==============================================================================
#   CLEANUP
# ==============================================================================

def cleanup_old_data(cursor) -> None:
    """
    Delete records older than 2 months to keep the DB lean.
    Runs once at the end of every scrape cycle.
    """
    cursor.execute("""
        DELETE FROM origin_snapshots
        WHERE scraped_at < NOW() - INTERVAL '2 months'
    """)
    cursor.execute("""
        DELETE FROM origin_flights
        WHERE scheduled_date < (CURRENT_DATE - INTERVAL '2 months')
    """)
    log("  [CLEANUP] Deleted records older than 2 months")


# ==============================================================================
#   CORE: PROCESS ONE (date, type, airport) BATCH
# ==============================================================================

def process_batch(
    cursor,
    date_str: str,
    flight_type: str,
    airport: str,
) -> int:
    """
    Fetch and process all flights for one (date, type, airport) combination.

    Steps:
        1. Fetch raw flights from PAA API
        2. For each flight: flatten, filter (if REQUIRE_ISB_LEG), detect changes
        3. Upsert into origin_flights
        4. Batch-insert snapshots for changed flights only
        5. Mark any silently dropped flights

    Returns:
        Number of changed flights recorded.
    """
    fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # --- 1. Fetch ---
    raw_flights = fetch_flights(date_str, flight_type, airport)
    if not raw_flights:
        return 0   # Skip drop detection too — fetch may have failed

    seen_flight_numbers = set()
    snapshot_rows       = []
    changed_count       = 0

    # --- 2 & 3. Process each flight ---
    for raw in raw_flights:
        flat = flatten_flight(raw, flight_type, date_str, airport, fetched_at)
        if flat is None:
            continue

        # Optionally filter to ISB-relevant flights only
        if REQUIRE_ISB_LEG and not is_isb_relevant(flat):
            continue

        seen_flight_numbers.add(flat["flight_number"])

        # Look up existing DB row for this exact (flight, date, type, airport, source)
        cursor.execute("""
            SELECT city, status, st, et
            FROM origin_flights
            WHERE flight_number  = %(flight_number)s
              AND scheduled_date = %(scheduled_date)s
              AND type           = %(type)s
              AND source_airport = %(source_airport)s
              AND data_source    = %(data_source)s
        """, flat)
        existing = cursor.fetchone()

        is_changed, change_type = detect_change(existing, flat)

        # --- Upsert into origin_flights ---
        # Always refresh last_checked and current state.
        # last_updated only advances when a meaningful field actually changed.
        cursor.execute("""
            INSERT INTO origin_flights (
                flight_number, scheduled_date, type, source_airport, data_source,
                city, airline_logo, status, ST, ET, nature, last_checked, last_updated
            ) VALUES (
                %(flight_number)s, %(scheduled_date)s, %(type)s, %(source_airport)s, %(data_source)s,
                %(city)s, %(airline_logo)s, %(status)s, %(ST)s, %(ET)s, %(nature)s,
                %(last_checked)s, %(last_updated)s
            )
            ON CONFLICT (flight_number, scheduled_date, type, source_airport, data_source)
            DO UPDATE SET
                city          = EXCLUDED.city,
                airline_logo  = EXCLUDED.airline_logo,
                status        = EXCLUDED.status,
                ST            = EXCLUDED.ST,
                ET            = EXCLUDED.ET,
                nature        = EXCLUDED.nature,
                last_checked  = EXCLUDED.last_checked,
                last_updated  = CASE
                    WHEN origin_flights.status IS DISTINCT FROM EXCLUDED.status
                      OR origin_flights.ST     IS DISTINCT FROM EXCLUDED.ST
                      OR origin_flights.ET     IS DISTINCT FROM EXCLUDED.ET
                      OR origin_flights.city   IS DISTINCT FROM EXCLUDED.city
                    THEN EXCLUDED.last_updated
                    ELSE origin_flights.last_updated
                END
        """, flat)

        # --- Queue snapshot if something changed ---
        if is_changed:
            changed_count += 1
            snapshot_rows.append((
                flat["flight_number"],
                flat["scheduled_date"],
                flat["source_airport"],
                flat["data_source"],
                flat["type"],
                fetched_at,
                True,
                change_type,
                flat["status"],
                flat["ST"],
                flat["ET"],
                flat["city"],
                flat["airline_logo"],
                flat["nature"],
            ))

    # --- 4. Batch insert snapshots ---
    if snapshot_rows:
        execute_values(cursor, """
            INSERT INTO origin_snapshots (
                flight_number, scheduled_date, source_airport, data_source, type,
                scraped_at, is_changed, change_type,
                status, ST, ET, city, airline_logo, nature
            ) VALUES %s
        """, snapshot_rows)

    # --- 5. Drop detection ---
    mark_dropped_flights(cursor, date_str, flight_type, airport, seen_flight_numbers, fetched_at)

    return changed_count


# ==============================================================================
#   MAIN
# ==============================================================================

def main() -> None:

    # --- Connect ---
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

        # Build date list from DAY_OFFSETS (default: yesterday, today, tomorrow)
        dates = [
            (now + datetime.timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in DAY_OFFSETS
        ]

        # Outer loops: date → airport → type
        # Commit after each (airport, date) pair so a single failure doesn't
        # roll back an entire day's worth of work
        for date_str in dates:
            for airport in WATCH_AIRPORTS:
                for flight_type in ["Arrival", "Departure"]:

                    log(f"\n--- {flight_type} | {airport} | {date_str} ---")

                    try:
                        changed = process_batch(cursor, date_str, flight_type, airport)
                        conn.commit()
                        log(f"  {changed} changes recorded — committed")
                    except Exception as e:
                        log(f"  [ERROR] Batch failed: {e} — rolling back")
                        conn.rollback()
                        # Continue with the next batch rather than aborting everything
                        continue

        # --- Update scraper freshness timestamp ---
        update_scraper_status(cursor)

        # --- Prune old data ---
        cleanup_old_data(cursor)

        conn.commit()
        log("\n✅ Scrape complete")

    finally:
        cursor.close()
        conn.close()
        log("DB connection closed.")


if __name__ == "__main__":
    main()
