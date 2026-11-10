#!/usr/bin/env python3
"""
AeroDataBox collector (BL-040): the origin side of Islamabad arrivals.

Started by .github/workflows/aerodatabox_collector.yml after every Origin Scraper run (no cron of
its own). Most runs do nothing: it calls AeroDataBox only in four daily slots, each covering a
12-hour window of Islamabad arrivals with the origin leg (2 units a call, about 8 units a day,
well inside the free plan's 400 a month):

    slot             due (PKT)  window of Islamabad arrivals       why
    next_morning     22:00      tomorrow 00:00-11:59               tonight's overnight arrivals, scheduled
    afternoon        10:00      today    12:00-23:59               the rest of today, scheduled
    morning_done     13:30      today    00:00-11:59               finished: actual departure times
    afternoon_done   01:00      yesterday 12:00-23:59              finished: actual departure times

A slot runs once, within SLOT_GRACE hours after it is due (a failed call is retried on later runs,
at most MAX_ATTEMPTS times). It stops early if the plan's remaining units fall below UNIT_FLOOR.

Writes only its own tables (CLAUDE.md, D-016: new sources stay out of origin_flights):
    adb_arrivals  one row per Islamabad arrival: origin scheduled/revised/runway times, origin date.
                  Rows older than KEEP_DAYS are deleted (AeroDataBox allows keeping data 7 days).
    adb_runs      one row per call: window, HTTP status, counts, and how its list compares with
                  PAA's list for the same window (our own counts; kept).
Both tables have row-level security on. adb_arrivals has a read-only policy for the site's public
role (the share box and check links use origin_date for FlightStats); adb_runs has none.

AeroDataBox statuses and predicted times are stored only as given (adb_status, isb_times) and are
not to be shown (BL-040: statuses unreliable; no invented arrival estimates).

Env: AERODATABOX_KEY, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT.
Optional: ADB_NOW (ISO time, for tests), ADB_BASE (API base URL, for tests).
"""
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

HOST = "aerodatabox.p.rapidapi.com"
BASE = os.environ.get("ADB_BASE", f"https://{HOST}")
PKT = dt.timezone(dt.timedelta(hours=5))

# (slot, due hh:mm PKT, window day offset from the slot's date, window start hh:mm)
SLOTS = [
    ("next_morning",   "22:00", +1, "00:00"),
    ("afternoon",      "10:00",  0, "12:00"),
    ("morning_done",   "13:30",  0, "00:00"),
    ("afternoon_done", "01:00", -1, "12:00"),
]
SLOT_GRACE = dt.timedelta(hours=4)
MAX_ATTEMPTS = 3
MAX_CALLS_PER_RUN = 2
UNIT_FLOOR = 40
KEEP_DAYS = 7

SCHEMA = """
CREATE TABLE IF NOT EXISTS public.adb_arrivals (
    flight_number       text        NOT NULL,
    isb_sched_utc       timestamptz NOT NULL,
    isb_date            date        NOT NULL,
    isb_sched_local     text,
    airline_iata        text,
    origin_iata         text,
    origin_icao         text,
    origin_name         text,
    origin_sched_local  text,
    origin_sched_utc    timestamptz,
    origin_date         date,
    origin_revised_utc  timestamptz,
    origin_runway_utc   timestamptz,
    origin_times        jsonb,
    isb_times           jsonb,
    adb_status          text,
    first_seen          timestamptz NOT NULL DEFAULT now(),
    last_fetched        timestamptz NOT NULL DEFAULT now(),
    last_slot           text,
    PRIMARY KEY (flight_number, isb_sched_utc)
);
CREATE INDEX IF NOT EXISTS adb_arrivals_isb_date ON public.adb_arrivals (isb_date, flight_number);
ALTER TABLE public.adb_arrivals ENABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS public.adb_runs (
    id               bigserial   PRIMARY KEY,
    slot             text        NOT NULL,
    slot_date        date        NOT NULL,
    ran_at           timestamptz NOT NULL DEFAULT now(),
    window_from      text,
    window_to        text,
    http_status      int,
    flights          int,
    with_origin      int,
    with_actual      int,
    paa_arrivals     int,
    matched          int,
    only_adb         text,
    only_paa         text,
    units_remaining  int,
    error            text
);
ALTER TABLE public.adb_runs ENABLE ROW LEVEL SECURITY;

-- The site reads adb_arrivals for the FlightStats day (BL-040, owner 2026-11-11); adb_runs stays private.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    GRANT SELECT ON public.adb_arrivals TO anon;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = 'adb_arrivals'
                   AND policyname = 'adb_arrivals_public_read') THEN
      CREATE POLICY adb_arrivals_public_read ON public.adb_arrivals FOR SELECT TO anon USING (true);
    END IF;
  END IF;
END $$;
"""


def log(msg):
    print(msg, flush=True)


def now_pkt():
    if os.environ.get("ADB_NOW"):
        return dt.datetime.fromisoformat(os.environ["ADB_NOW"]).astimezone(PKT)
    return dt.datetime.now(PKT)


def norm_flight(n):
    return "".join(ch for ch in str(n or "") if ch.isalnum()).upper()


def parse_time(t):
    """AeroDataBox time object -> aware datetime (UTC preferred, else local with offset)."""
    if not isinstance(t, dict):
        return None
    for key in ("utc", "local"):
        s = t.get(key)
        if not s:
            continue
        s = s.strip().replace("T", " ").replace("Z", "+00:00")
        for fmt in ("%Y-%m-%d %H:%M%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                return dt.datetime.strptime(s, fmt)
            except ValueError:
                pass
    return None


def end_times(end):
    """All '...Time' fields of one end, as {name: {'utc':..,'local':..}}."""
    if not isinstance(end, dict):
        return {}
    return {k: v for k, v in end.items() if k.lower().endswith("time") and isinstance(v, dict)}


def due_slots(now):
    """Slot occurrences (slot, slot_date, window_from, window_to) whose grace window holds now."""
    out = []
    for back in (0, 1):                                  # today's and yesterday's occurrences
        day = (now - dt.timedelta(days=back)).date()
        for slot, due, offset, start in SLOTS:
            hh, mm = map(int, due.split(":"))
            due_at = dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=PKT)
            if due_at <= now < due_at + SLOT_GRACE:
                wday = day + dt.timedelta(days=offset)
                sh, sm = map(int, start.split(":"))
                wfrom = dt.datetime(wday.year, wday.month, wday.day, sh, sm)
                wto = wfrom + dt.timedelta(hours=11, minutes=59)
                out.append((slot, day, wfrom.strftime("%Y-%m-%dT%H:%M"), wto.strftime("%Y-%m-%dT%H:%M")))
    return out


def call_fids(key, wfrom, wto):
    params = {"withLeg": "true", "direction": "Arrival", "withCancelled": "true", "withCodeshared": "false",
              "withCargo": "false", "withPrivate": "false", "withLocation": "false"}
    url = f"{BASE}/flights/airports/iata/ISB/{wfrom}/{wto}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"x-rapidapi-key": key, "x-rapidapi-host": HOST,
                                               "User-Agent": "isb-flight-tracker"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            status, headers, body = r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        status, headers, body = e.code, e.headers, e.read()
    units = None
    for k, v in (headers.items() if headers else []):
        if k.lower() == "x-ratelimit-api-units-remaining":
            try:
                units = int(v)
            except ValueError:
                pass
    try:
        data = json.loads(body.decode("utf-8", "replace")) if body else None
    except ValueError:
        data = None
    return status, data, units


def last_units(cur):
    cur.execute("SELECT units_remaining FROM public.adb_runs WHERE units_remaining IS NOT NULL "
                "ORDER BY ran_at DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def attempts(cur, slot, slot_date):
    cur.execute("SELECT count(*), count(*) FILTER (WHERE http_status = 200) FROM public.adb_runs "
                "WHERE slot = %s AND slot_date = %s", (slot, slot_date))
    return cur.fetchone()


def paa_arrivals(cur, wfrom, wto):
    """PAA's Islamabad arrivals scheduled inside the window (for comparison counts only)."""
    d = wfrom[:10]
    h0, h1 = wfrom[11:16], wto[11:16]
    try:
        cur.execute("SELECT flight_number, st FROM public.isb_flights_view "
                    "WHERE type = 'Arrival' AND scheduled_date = %s", (d,))
    except psycopg2.Error as e:
        cur.connection.rollback()
        log(f"   PAA comparison skipped: {e.pgerror or e}")
        return None
    out = set()
    for fn, st in cur.fetchall():
        st = (st or "")[:5]
        if len(st) == 5 and h0 <= st <= h1:
            out.add(norm_flight(fn))
    return out


def store(cur, slot, flights):
    rows, with_origin, with_actual = 0, 0, 0
    for f in flights:
        dep, arr = f.get("departure") or {}, f.get("arrival") or f.get("movement") or {}
        num = norm_flight(f.get("number"))
        isb_s = parse_time(arr.get("scheduledTime"))
        if not num or not isb_s:
            continue
        o_s = parse_time(dep.get("scheduledTime"))
        o_rev = parse_time(dep.get("revisedTime"))
        o_run = parse_time(dep.get("runwayTime"))
        o_local = (dep.get("scheduledTime") or {}).get("local")
        o_date = None
        if o_local and len(o_local) >= 10:
            try:
                o_date = dt.date.fromisoformat(o_local[:10])     # origin's own calendar day
            except ValueError:
                o_date = None
        ap = dep.get("airport") or {}
        with_origin += 1 if o_s else 0
        with_actual += 1 if (o_rev or o_run) else 0
        cur.execute("""
            INSERT INTO public.adb_arrivals AS a (flight_number, isb_sched_utc, isb_date, isb_sched_local,
                airline_iata, origin_iata, origin_icao, origin_name, origin_sched_local, origin_sched_utc,
                origin_date, origin_revised_utc, origin_runway_utc, origin_times, isb_times, adb_status,
                last_fetched, last_slot)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), %s)
            ON CONFLICT (flight_number, isb_sched_utc) DO UPDATE SET
                isb_sched_local    = EXCLUDED.isb_sched_local,
                airline_iata       = COALESCE(EXCLUDED.airline_iata, a.airline_iata),
                origin_iata        = COALESCE(EXCLUDED.origin_iata, a.origin_iata),
                origin_icao        = COALESCE(EXCLUDED.origin_icao, a.origin_icao),
                origin_name        = COALESCE(EXCLUDED.origin_name, a.origin_name),
                origin_sched_local = COALESCE(EXCLUDED.origin_sched_local, a.origin_sched_local),
                origin_sched_utc   = COALESCE(EXCLUDED.origin_sched_utc, a.origin_sched_utc),
                origin_date        = COALESCE(EXCLUDED.origin_date, a.origin_date),
                origin_revised_utc = COALESCE(EXCLUDED.origin_revised_utc, a.origin_revised_utc),
                origin_runway_utc  = COALESCE(EXCLUDED.origin_runway_utc, a.origin_runway_utc),
                origin_times       = COALESCE(a.origin_times, '{}'::jsonb) || COALESCE(EXCLUDED.origin_times, '{}'::jsonb),
                isb_times          = COALESCE(a.isb_times, '{}'::jsonb) || COALESCE(EXCLUDED.isb_times, '{}'::jsonb),
                adb_status         = EXCLUDED.adb_status,
                last_fetched       = now(),
                last_slot          = EXCLUDED.last_slot
        """, (num, isb_s, isb_s.astimezone(PKT).date(), (arr.get("scheduledTime") or {}).get("local"),
              (f.get("airline") or {}).get("iata") or num[:2], ap.get("iata"), ap.get("icao"),
              ap.get("name") or ap.get("shortName") or ap.get("municipalityName"), o_local, o_s, o_date,
              o_rev, o_run, json.dumps(end_times(dep)), json.dumps(end_times(arr)), f.get("status"), slot))
        rows += 1
    return rows, with_origin, with_actual


def main():
    key = os.environ.get("AERODATABOX_KEY", "")
    if not key:
        log("AERODATABOX_KEY not set: nothing to do.")
        return 0
    now = now_pkt()
    slots = due_slots(now)
    if not slots and os.environ.get("ADB_SETUP") != "1":
        log(f"{now:%Y-%m-%d %H:%M} PKT: no AeroDataBox slot due.")
        return 0

    conn = psycopg2.connect(host=os.environ["DB_HOST"], dbname=os.environ["DB_NAME"],
                            user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
                            port=os.environ.get("DB_PORT", "5432"), connect_timeout=20)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(SCHEMA)
    conn.commit()
    cur.execute("SELECT count(*) FROM pg_policies WHERE schemaname = 'public' AND tablename = 'adb_arrivals'")
    log(f"Tables ready (adb_arrivals read policies: {cur.fetchone()[0]}).")

    calls = 0
    for slot, slot_date, wfrom, wto in slots:
        tried, ok = attempts(cur, slot, slot_date)
        if ok or tried >= MAX_ATTEMPTS:
            continue
        units = last_units(cur)
        if units is not None and units < UNIT_FLOOR:
            log(f"Only {units} units left: skipping {slot} until the plan resets.")
            break
        if calls >= MAX_CALLS_PER_RUN:
            log("Call limit for this run reached; the rest waits for the next run.")
            break
        calls += 1
        log(f"{slot} for {slot_date}: Islamabad arrivals {wfrom} to {wto}")
        status, data, units = call_fids(key, wfrom, wto)
        flights = (data or {}).get("arrivals") if isinstance(data, dict) else None
        rec = {"slot": slot, "slot_date": slot_date, "window_from": wfrom, "window_to": wto,
               "http_status": status, "units_remaining": units, "flights": None, "with_origin": None,
               "with_actual": None, "paa_arrivals": None, "matched": None, "only_adb": None,
               "only_paa": None, "error": None}
        if status == 200 and isinstance(flights, list):
            if flights and calls == 1:
                f0 = flights[0]
                log(f"   fields of one flight: {sorted(f0)}; departure: {sorted((f0.get('departure') or {}))}")
            rows, with_origin, with_actual = store(cur, slot, flights)
            adb = {norm_flight(f.get("number")) for f in flights if f.get("number")}
            paa = paa_arrivals(cur, wfrom, wto)
            rec.update(flights=rows, with_origin=with_origin, with_actual=with_actual)
            if paa is not None:
                rec.update(paa_arrivals=len(paa), matched=len(adb & paa),
                           only_adb=",".join(sorted(adb - paa)) or None,
                           only_paa=",".join(sorted(paa - adb)) or None)
            log(f"   {rows} arrivals stored, {with_origin} with origin schedule, {with_actual} with actual times; "
                f"PAA {rec['paa_arrivals']}, matched {rec['matched']}; units left {units}")
        else:
            msg = (data or {}).get("message") if isinstance(data, dict) else None
            rec["error"] = (msg or f"HTTP {status}")[:300]
            log(f"   failed: {rec['error']}")
        cols = list(rec)
        cur.execute(f"INSERT INTO public.adb_runs ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
                    [rec[c] for c in cols])
        conn.commit()

    # AeroDataBox data is kept 7 days (their terms); our run counts stay
    cur.execute("DELETE FROM public.adb_arrivals WHERE isb_date < %s", (now.date() - dt.timedelta(days=KEEP_DAYS),))
    if cur.rowcount:
        log(f"Removed {cur.rowcount} rows older than {KEEP_DAYS} days.")
    conn.commit()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
