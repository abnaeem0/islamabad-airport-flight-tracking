#!/usr/bin/env python3
"""
origin_scraper.py — Multi-airport PAA flight tracker
=====================================================
Scrapes flight data from all configured domestic Pakistani airports via the
PAA API and writes it to the origin_flights / origin_snapshots tables.

Performance design:
  - FETCH phase  : all API calls run concurrently (ThreadPoolExecutor)
  - WRITE phase  : all DB writes run sequentially in the main thread
  This keeps psycopg2 single-threaded (safe) while cutting runtime from
  ~18 minutes down to ~4 minutes.

Key design decisions:
  - Completely independent of scraper.py — never touches the original tables
  - WATCH_AIRPORTS controls which airports are scraped (trivial to extend)
  - data_source = "paa" for all records from this scraper
  - Future scrapers (ADS-B, airline websites) write to the same tables
    with a different data_source — no schema changes needed

Run: manually from GitHub Actions until confirmed stable, then add cron.
"""

import os
import datetime
import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==============================================================================
#   CONFIG
# ==============================================================================

# All domestic airports to scrape.
# To add a new airport: append its city name exactly as it appears in PAA URLs.
WATCH_AIRPORTS = [
    "Islamabad",
    "Karachi",
    "Lahore",
    "Faisalabad",
    "Multan",
    "Peshawar",
]

# Set to True to only store flights that have a leg to/from Islamabad.
# Keeps the DB lean if you only care about ISB-connected flights.
REQUIRE_ISB_LEG = False

# Data source tag written to every row from this scraper.
DATA_SOURCE = "paa"

# PAA API URL template
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/{city}"

# Statuses that mean a flight is finished — skip re-snapshotting these
TERMINAL_STATUSES = ("Dropped", "Cancelled", "Landed", "Departed")

# Days relative to today to scrape (-1 = yesterday, 0 = today, 1 = tomorrow)
DAY_OFFSETS = [-1, 0, 1]

# Max concurrent API calls.
# One worker per airport — enough to parallelise without hammering PAA.
FETCH_WORKERS = 6

# Request timeouts: (connect_timeout, read_timeout) in seconds.
# Tuple form fails fast on a stalled connection instead of hanging silently.
REQUEST_TIMEOUT = (5, 15)

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
    """Timestamped stdout log — visible in GitHub Actions live log."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)   # flush=True ensures lines appear immediately in Actions


# ==============================================================================
#   PHASE 1 — FETCH (runs concurrently)
# ==============================================================================

def fetch_flights(date_str: str, flight_type: str, city: str) -> tuple[str, str, str, list[dict]]:
    """
    Fetch flights from the PAA API for one (date, type, airport) combination.
    Designed to run in a thread — does NO database work.

    Returns:
        (date_str, flight_type, city, raw_flights)
        raw_flights is [] on failure so the caller can safely skip it.
    """
    url = PAA_TEMPLATE.format(date=date_str, type=flight_type, city=city)
    try:
        r = requests.get(url, verify=False, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                log(f"  [FETCH] {len(data):3d} flights — {flight_type:<11} | {city:<12} | {date_str}")
                return date_str, flight_type, city, data
        log(f"  [WARN]  HTTP {r.status_code} — {flight_type:<11} | {city:<12} | {date_str}")
    except requests.exceptions.ConnectTimeout:
        log(f"  [WARN]  Connect timeout — {flight_type:<11} | {city:<12} | {date_str}")
    except requests.exceptions.ReadTimeout:
        log(f"  [WARN]  Read timeout — {flight_type:<11} | {city:<12} | {date_str}")
    except Exception as e:
        log(f"  [ERROR] {e} — {flight_type:<11} | {city:<12} | {date_str}")
    return date_str, flight_type, city, []


def fetch_all(dates: list[str]) -> list[tuple]:
    """
    Fire all (date, type, airport) fetch jobs concurrently and collect results.

    Returns:
        List of (date_str, flight_type, city, raw_flights) tuples,
        in completion order (not submission order — doesn't matter for writes).
    """
    # Build every combination upfront
    jobs = [
        (date_str, flight_type, airport)
        for date_str    in dates
        for airport     in WATCH_AIRPORTS
        for flight_type in ["Arrival", "Departure"]
    ]

    total = len(jobs)
    log(f"\n[FETCH] Starting {total} API calls across {FETCH_WORKERS} workers...")

    results = []
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = {
            executor.submit(fetch_flights, date_str, flight_type, airport): (date_str, flight_type, airport)
            for date_str, flight_type, airport in jobs
        }
        for future in as_completed(futures):
            results.append(future.result())

    log(f"[FETCH] All {total} calls complete.\n")
    return results


# ==============================================================================
#   PHASE 2 — PROCESS & WRITE (runs sequentially in main thread)
# ==============================================================================

def flatten_flight(
    raw: dict,
    flight_type: str,
    date_str: str,
    source_airport: str,
    fetched_at: str,
) -> dict | None:
    """
    Convert a raw PAA API dict into a flat dict ready for DB insertion.

    PAA returns the "other end" of the route differently per type:
      Arrival   → EnglishFromCity (where it came from)
      Departure → EnglishToCity   (where it is going)

    Returns None if the record has no flight number (unusable).
    """
    flight_number = raw.get("FlightNumber")
    if not flight_number:
        return None

    return {
        "flight_number":  flight_number.replace(" ", ""),   # "TK 571" → "TK571"
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
    """Returns True if this flight has a leg to or from Islamabad."""
    return (
        flat["source_airport"] == "Islamabad"
        or flat["city"] == "Islamabad"
    )


def detect_change(existing: dict | None, flat: dict) -> tuple[bool, str | None]:
    """
    Compare the current DB row against freshly fetched data.

    Returns (is_changed, change_type).

    change_type: "new" | "status_change" | "time_change" | "city_change" | None
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


def mark_dropped_flights(
    cursor,
    date_str: str,
    flight_type: str,
    source_airport: str,
    seen_flight_numbers: set,
    fetched_at: str,
) -> None:
    """
    Mark flights that were in the DB but are no longer in the API response
    as 'Dropped', unless they already reached a terminal status.

    Skipped entirely if the API returned zero flights (likely a failed fetch).
    """
    if not seen_flight_numbers:
        log(f"  [DROP]  Skipping — 0 flights returned for {flight_type} | {source_airport} | {date_str}")
        return

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
        date_str, flight_type, source_airport, DATA_SOURCE,
        list(seen_flight_numbers), list(TERMINAL_STATUSES),
    ))

    dropped = [row["flight_number"] for row in cursor.fetchall()]
    if not dropped:
        return

    log(f"  [DROP]  {len(dropped)} flights dropped — {flight_type} | {source_airport} | {date_str}: {dropped}")

    cursor.execute("""
        UPDATE origin_flights
        SET status = 'Dropped', last_checked = %s
        WHERE scheduled_date = %s AND type = %s
          AND source_airport = %s AND data_source = %s
          AND flight_number  = ANY(%s)
    """, (fetched_at, date_str, flight_type, source_airport, DATA_SOURCE, dropped))

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


def process_batch(
    cursor,
    date_str: str,
    flight_type: str,
    airport: str,
    raw_flights: list[dict],
) -> int:
    """
    Process and write one (date, type, airport) batch to the DB.
    Runs in the main thread — no concurrent DB access.

    Steps:
      1. Flatten and optionally filter each raw flight
      2. Detect changes against existing DB rows
      3. Upsert into origin_flights
      4. Batch-insert snapshots for changed flights only
      5. Mark silently dropped flights

    Returns:
        Number of changes recorded.
    """
    fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if not raw_flights:
        return 0

    seen_flight_numbers = set()
    snapshot_rows       = []
    changed_count       = 0

    for raw in raw_flights:
        flat = flatten_flight(raw, flight_type, date_str, airport, fetched_at)
        if flat is None:
            continue

        if REQUIRE_ISB_LEG and not is_isb_relevant(flat):
            continue

        seen_flight_numbers.add(flat["flight_number"])

        # Check existing row
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

        # Upsert current state — last_updated only advances on meaningful changes
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

        if is_changed:
            changed_count += 1
            snapshot_rows.append((
                flat["flight_number"], flat["scheduled_date"],
                flat["source_airport"], flat["data_source"], flat["type"],
                fetched_at, True, change_type,
                flat["status"], flat["ST"], flat["ET"],
                flat["city"], flat["airline_logo"], flat["nature"],
            ))

    # Batch insert all snapshots for this batch at once
    if snapshot_rows:
        execute_values(cursor, """
            INSERT INTO origin_snapshots (
                flight_number, scheduled_date, source_airport, data_source, type,
                scraped_at, is_changed, change_type,
                status, ST, ET, city, airline_logo, nature
            ) VALUES %s
        """, snapshot_rows)

    mark_dropped_flights(cursor, date_str, flight_type, airport, seen_flight_numbers, fetched_at)

    return changed_count


# ==============================================================================
#   HOUSEKEEPING
# ==============================================================================

def update_scraper_status(cursor) -> None:
    """Upsert the last_run timestamp so the frontend can show freshness."""
    now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO origin_scraper_status (scraper_id, last_run)
        VALUES ('paa_origin', %s)
        ON CONFLICT (scraper_id) DO UPDATE SET last_run = EXCLUDED.last_run
    """, (now_utc,))


def cleanup_old_data(cursor) -> None:
    """
    Delete records older than 7 days to keep the DB lean.
    6 airports × 2 types × snapshots grows fast — a week is enough
    for operational use.
    """
    cursor.execute("""
        DELETE FROM origin_snapshots
        WHERE scraped_at < NOW() - INTERVAL '7 days'
    """)
    cursor.execute("""
        DELETE FROM origin_flights
        WHERE scheduled_date < (CURRENT_DATE - INTERVAL '7 days')
    """)
    log("  [CLEANUP] Deleted records older than 7 days")


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
        dates = [
            (now + datetime.timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in DAY_OFFSETS
        ]

        # ---- PHASE 1: Fetch all data concurrently ----
        all_results = fetch_all(dates)

        # ---- PHASE 2: Write all results sequentially ----
        log("[WRITE] Processing and writing results to DB...")

        total_changes = 0

        for date_str, flight_type, airport, raw_flights in all_results:
            log(f"\n--- {flight_type:<11} | {airport:<12} | {date_str} ---")

            if not raw_flights:
                log("  Skipped — no data returned")
                continue

            try:
                changed = process_batch(cursor, date_str, flight_type, airport, raw_flights)
                conn.commit()
                total_changes += changed
                log(f"  {changed} changes recorded — committed")
            except Exception as e:
                log(f"  [ERROR] Batch failed: {e} — rolling back")
                conn.rollback()
                continue  # Don't let one bad batch stop the rest

        log(f"\n[WRITE] Done. {total_changes} total changes across all batches.")

        # ---- Housekeeping ----
        update_scraper_status(cursor)
        cleanup_old_data(cursor)
        conn.commit()

        log("\n✅ Scrape complete")

    finally:
        cursor.close()
        conn.close()
        log("DB connection closed.")


if __name__ == "__main__":
    main()
