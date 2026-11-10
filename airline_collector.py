#!/usr/bin/env python3
"""
airline_collector.py: PIA and Fly Jinnah status pages for Islamabad flights (BL-051, D-016).

Runs after every Origin Scraper run, whether it succeeded or failed (workflow airline_collector.yml).
Each run:
  1. reads the enabled airlines' Islamabad rows from isb_flights_view, the legs already held
     (airline_legs) and recent requests (airline_fetch_log);
  2. plans the fewest airline pages that cover flights at a checkpoint (see [windows]) where the
     airport data leaves a gap (see [gap]); when the airport data is stale, every flight at a
     checkpoint counts, but each route page is fetched at most hourly ([fallback]);
  3. fetches them one at a time, a few seconds apart; a 403/429/check page pauses that source
     until the next Pakistan day;
  4. writes airline_legs (one current row per leg) and airline_fetch_log (one row per request).

All timing and switches live in airline_sources.toml. Airline data never goes into origin_flights:
the site's views rank sources with priority DESC, so it would replace PAA's rows (CLAUDE.md).
Airblue is not here on purpose: its pages show automated clients a check page (BL-053).

    python airline_collector.py --dry-run     # read the database, print the plan; no requests, no writes
    python airline_collector.py --no-write    # also fetch and parse, print what was found; no writes
    python airline_collector.py               # live: fetch and write

Environment: DB_HOST DB_NAME DB_USER DB_PASSWORD DB_PORT (the same secrets as the other collectors).
The first live run creates its two tables (SCHEMA below); --dry-run and --no-write never write.
"""
import argparse
import dataclasses
import datetime as dt
import json
import os
import random
import re
import sys
import time
import tomllib
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

PKT = ZoneInfo("Asia/Karachi")
UTC = dt.timezone.utc
CONFIG_PATH = Path(__file__).with_name("airline_sources.toml")

# --------------------------------------------------------------------------------------------
# Airports. Keys are city names as Islamabad's board writes them, lower case, letters only (the
# same rule and list as ORIGIN_TIMES in docs/flight_detail.js): IATA code, time zone, typical
# scheduled flight minutes to or from Islamabad. A city missing here is logged, not guessed.
# --------------------------------------------------------------------------------------------
CITIES = {
    "islamabad": ("ISB", "Asia/Karachi", 0),
    "karachi": ("KHI", "Asia/Karachi", 120), "lahore": ("LHE", "Asia/Karachi", 55),
    "peshawar": ("PEW", "Asia/Karachi", 40), "faisalabad": ("LYP", "Asia/Karachi", 45),
    "multan": ("MUX", "Asia/Karachi", 60), "quetta": ("UET", "Asia/Karachi", 95),
    "gilgit": ("GIL", "Asia/Karachi", 60), "skardu": ("KDU", "Asia/Karachi", 60),
    "sukkur": ("SKZ", "Asia/Karachi", 85), "gwadar": ("GWD", "Asia/Karachi", 135),
    "turbat": ("TUK", "Asia/Karachi", 130), "bahawalpur": ("BHV", "Asia/Karachi", 70),
    "rahimyarkhan": ("RYK", "Asia/Karachi", 80), "dgkhan": ("DEA", "Asia/Karachi", 70),
    "deraghazikhan": ("DEA", "Asia/Karachi", 70), "sialkot": ("SKT", "Asia/Karachi", 40),
    "chitral": ("CJL", "Asia/Karachi", 60),
    "dubai": ("DXB", "Asia/Dubai", 180), "sharjah": ("SHJ", "Asia/Dubai", 175),
    "abudhabi": ("AUH", "Asia/Dubai", 180), "rasalkhaimah": ("RKT", "Asia/Dubai", 170),
    "alain": ("AAN", "Asia/Dubai", 175), "fujairah": ("FJR", "Asia/Dubai", 165),
    "muscat": ("MCT", "Asia/Muscat", 165), "doha": ("DOH", "Asia/Qatar", 200),
    "bahrain": ("BAH", "Asia/Bahrain", 210), "kuwait": ("KWI", "Asia/Kuwait", 230),
    "dammam": ("DMM", "Asia/Riyadh", 245), "riyadh": ("RUH", "Asia/Riyadh", 255),
    "jeddah": ("JED", "Asia/Riyadh", 295), "madinah": ("MED", "Asia/Riyadh", 280),
    "almadinah": ("MED", "Asia/Riyadh", 280), "medina": ("MED", "Asia/Riyadh", 280),
    "madina": ("MED", "Asia/Riyadh", 280), "qassim": ("ELQ", "Asia/Riyadh", 260),
    "alqassim": ("ELQ", "Asia/Riyadh", 260), "gassim": ("ELQ", "Asia/Riyadh", 260),
    "taif": ("TIF", "Asia/Riyadh", 290), "abha": ("AHB", "Asia/Riyadh", 300),
    "tabuk": ("TUU", "Asia/Riyadh", 300), "yanbu": ("YNB", "Asia/Riyadh", 290),
    "alula": ("ULH", "Asia/Riyadh", 285),
    "istanbul": ("IST", "Europe/Istanbul", 330), "london": ("LHR", "Europe/London", 465),
    "manchester": ("MAN", "Europe/London", 455), "birmingham": ("BHX", "Europe/London", 460),
    "paris": ("CDG", "Europe/Paris", 420), "toronto": ("YYZ", "America/Toronto", 855),
    "baku": ("GYD", "Asia/Baku", 185), "tashkent": ("TAS", "Asia/Tashkent", 105),
    "kabul": ("KBL", "Asia/Kabul", 65), "najaf": ("NJF", "Asia/Baghdad", 225),
    "baghdad": ("BGW", "Asia/Baghdad", 225), "mashhad": ("MHD", "Asia/Tehran", 140),
    "tehran": ("IKA", "Asia/Tehran", 180), "urumqi": ("URC", "Asia/Shanghai", 160),
    "kashgar": ("KHG", "Asia/Shanghai", 90), "beijing": ("PEK", "Asia/Shanghai", 395),
    "bangkok": ("BKK", "Asia/Bangkok", 300), "kualalumpur": ("KUL", "Asia/Kuala_Lumpur", 360),
}
TZ_BY_IATA = {code: tz for code, tz, _ in CITIES.values()}
MINUTES_BY_IATA = {code: m for code, _, m in CITIES.values()}

CHALLENGE_RE = re.compile(r"Just a moment|cf-chl-|Attention Required|captcha", re.I)


def norm_city(s):
    return re.sub(r"[^a-z]", "", (s or "").lower())


def tz_of(iata):
    name = TZ_BY_IATA.get(iata)
    return ZoneInfo(name) if name else None


# --------------------------------------------------------------------------------------------
# Time helpers. Airline and airport pages give clock times; these pick the right calendar day.
# --------------------------------------------------------------------------------------------
def clock(s):
    """'07:30', '0730', '7:30', '07:30 AM' -> (7, 30); anything else -> None."""
    s = (s or "").strip()
    m = re.fullmatch(r"(\d{1,2}):?(\d{2})(?:\s*([AaPp])[Mm])?", s)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if m.group(3):
        if h < 1 or h > 12:
            return None
        h = h % 12 + (12 if m.group(3).lower() == "p" else 0)
    return (h, mi) if h < 24 and mi < 60 else None


def at_local(day, hm, tz):
    return dt.datetime(day.year, day.month, day.day, hm[0], hm[1], tzinfo=tz)


def _candidates(hm, tz, day):
    return [at_local(day + dt.timedelta(days=k), hm, tz) for k in (-1, 0, 1, 2)]


def first_from(hm, tz, day, not_before):
    """Earliest of that clock time on day-1..day+2 that is not before `not_before`.
    For revised/actual departures: they are never much earlier than the schedule."""
    if hm is None or tz is None:
        return None
    for c in _candidates(hm, tz, day):
        if c >= not_before:
            return c
    return None


def closest_to(hm, tz, day, target):
    """That clock time on day-1..day+2 closest to `target` (for arrivals, given the departure)."""
    if hm is None or tz is None or target is None:
        return None
    return min(_candidates(hm, tz, day), key=lambda c: abs(c - target))


# --------------------------------------------------------------------------------------------
# Islamabad flights from the airport data
# --------------------------------------------------------------------------------------------
@dataclasses.dataclass
class IsbFlight:
    number: str            # PK300
    airline: str           # PK
    kind: str              # Arrival | Departure (at Islamabad)
    date: dt.date          # scheduled date at Islamabad
    city: str              # the other airport, as the board writes it
    status: str
    st: dt.datetime | None
    et: dt.datetime | None
    last_checked: dt.datetime | None
    other: tuple | None    # (iata, tz name, minutes) of the other airport, or None if unknown

    @property
    def isb_time(self):        # Islamabad departure or arrival, best known
        return self.et or self.st

    @property
    def other_iata(self):
        return self.other[0] if self.other else None

    def dep_estimate(self):
        """Best guess of the departure time (Islamabad's for departures, the origin's for arrivals)."""
        if self.kind == "Departure":
            return self.isb_time
        if self.isb_time is None or not self.other:
            return None
        return self.isb_time - dt.timedelta(minutes=self.other[2])

    def origin_dates(self):
        """Scheduled departure date(s) at the origin, local to the origin. Two dates when the
        scheduled departure is within 2 h of midnight there and the guess could be either day."""
        if self.kind == "Departure":
            return [self.date]
        if self.st is None or not self.other:
            return []
        tz = ZoneInfo(self.other[1])
        dep = (self.st - dt.timedelta(minutes=self.other[2])).astimezone(tz)
        days = {dep.date()}
        for shift in (-120, 120):
            days.add((dep + dt.timedelta(minutes=shift)).date())
        return sorted(days)


def isb_flight_from_row(row):
    number = (row["flight_number"] or "").replace(" ", "").replace("-", "").upper()
    day = row["scheduled_date"]
    if isinstance(day, str):
        day = dt.date.fromisoformat(day)
    st_hm, et_hm = clock(row.get("st")), clock(row.get("et"))
    st = at_local(day, st_hm, PKT) if st_hm else None
    et = None
    if et_hm:
        et = first_from(et_hm, PKT, day, (st or at_local(day, (0, 0), PKT)) - dt.timedelta(hours=2))
    city = row.get("city") or ""
    return IsbFlight(number=number, airline=number[:2], kind=row["type"], date=day, city=city,
                     status=(row.get("status") or "").strip(), st=st, et=et,
                     last_checked=row.get("last_checked"), other=CITIES.get(norm_city(city)))


# --------------------------------------------------------------------------------------------
# Planner (pure: no network, no database; tested in tests/test_airline_collector.py)
# --------------------------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class Page:
    source: str            # pia_departures | pia_route | flyjinnah_route
    origin: str
    destination: str       # '' for a departures board
    day: dt.date           # departure date, local to the origin

    @property
    def target(self):
        if self.source == "pia_departures":
            return f"{self.origin} departures {self.day.isoformat()}"
        return f"{self.origin}-{self.destination} {self.day.isoformat()}"


SOURCE_FOR = {("PK", "Departure"): "pia_departures", ("PK", "Arrival"): "pia_route",
              ("9P", "Departure"): "flyjinnah_route", ("9P", "Arrival"): "flyjinnah_route"}


def pages_for(f):
    src = SOURCE_FOR.get((f.airline, f.kind))
    if not src or not f.other:
        return []
    if src == "pia_departures":
        return [Page(src, "ISB", "", f.date)]
    if f.kind == "Departure":
        return [Page(src, "ISB", f.other_iata, f.date)]
    return [Page(src, f.other_iata, "ISB", d) for d in f.origin_dates()]


def checkpoints(f, cfg):
    """Times at which this flight's airline page is worth a look."""
    w = cfg["windows"]
    pts = []
    dep = f.dep_estimate()
    if dep is not None:
        pts += [dep + dt.timedelta(minutes=m) for m in w["before_departure"] + w["after_departure"]]
    if f.kind == "Arrival" and f.isb_time is not None:
        pts += [f.isb_time + dt.timedelta(minutes=m) for m in w["after_arrival"]]
    return sorted(pts)


def due_checkpoint(f, now, cfg):
    """The latest checkpoint already reached, if it is not older than max_lateness."""
    reached = [c for c in checkpoints(f, cfg) if c <= now]
    if not reached:
        return None
    last = reached[-1]
    return last if now - last <= dt.timedelta(minutes=cfg["defaults"]["max_lateness"]) else None


def has_gap(f, held, now, cfg):
    """Does the airport data leave something the airline could fill?"""
    g = cfg["gap"]
    if f.status == "Dropped":
        return True
    if f.st is None:
        return True
    if f.et is None:
        return bool(g.get("missing_et", True))
    if abs(f.et - f.st) >= dt.timedelta(minutes=g["et_off_minutes"]):
        return True
    done = "Landed" if f.kind == "Arrival" else "Departed"
    if f.status != done and now > f.isb_time + dt.timedelta(minutes=g["overdue_minutes"]):
        return True
    if f.kind == "Arrival" and f.other and f.other[1] != "Asia/Karachi" and not held:
        return True        # international arrival: no origin side in the airport data
    return False


def held_for(f, held):
    """The airline leg already held for this flight and day: same number and direction, scheduled
    within 6 h of the airport's schedule (daily flights share numbers, so never match by number alone)."""
    best, best_gap = None, dt.timedelta(hours=6)
    for leg in held.get((f.number, f.kind), []):
        leg_time = leg.get("sched_arr") if f.kind == "Arrival" else leg.get("sched_dep")
        if leg_time is None or f.st is None:
            continue
        gap = abs(leg_time - f.st)
        if gap <= best_gap:
            best, best_gap = leg, gap
    return best


def finished(f, held):
    """Nothing more to learn: the airline already gave the actual time that matters."""
    if not held:
        return False
    return bool(held.get("act_arr")) if f.kind == "Arrival" else bool(held.get("act_dep"))


def airport_data_stale(flights, now, cfg):
    seen = [f.last_checked for f in flights if f.last_checked]
    if not seen:
        return True
    limit = dt.timedelta(minutes=cfg["fallback"]["when_airport_data_older_than_minutes"])
    return now - max(seen) > limit


def plan(flights, held_legs, log_rows, now, cfg):
    """-> (pages to fetch in order [(Page, reason, wanted flight numbers)], notes, stale flag).
    held_legs: {(flight_number, kind): [leg dicts]}; log_rows: recent airline_fetch_log rows."""
    enabled = {name for name, s in cfg["source"].items() if s.get("enabled")}
    notes = []
    stale = airport_data_stale(flights, now, cfg)
    if stale:
        notes.append("airport data is stale: fallback mode")

    last_fetch = {}                       # (source, target) -> latest fetch time
    used_today = defaultdict(int)
    paused = set()
    today = now.astimezone(PKT).date()
    recent_by_source = defaultdict(list)
    for r in sorted(log_rows, key=lambda r: r["at"]):
        if r["outcome"].startswith("skipped"):
            continue
        key = (r["source"], r["target"])
        last_fetch[key] = max(last_fetch.get(key, r["at"]), r["at"])
        if r["at"].astimezone(PKT).date() == today:
            used_today[r["source"]] += 1
            if r["outcome"] == "blocked" and cfg["defaults"]["pause_on_block"]:
                paused.add(r["source"])
        recent_by_source[r["source"]].append(r)
    for src, rows in recent_by_source.items():      # 3 failures in a row within 2 h: rest 2 h
        tail = rows[-3:]
        if len(tail) == 3 and all(x["outcome"] in ("error", "parse_fail") for x in tail) \
                and now - tail[-1]["at"] < dt.timedelta(hours=2):
            paused.add(src)

    wanted = defaultdict(set)             # Page -> flight numbers
    reasons = {}
    min_refetch = dt.timedelta(minutes=cfg["defaults"]["min_refetch_minutes"])
    route_every = dt.timedelta(minutes=cfg["fallback"]["route_every_minutes"])

    def fresh_enough(page, since):
        last = last_fetch.get((page.source, page.target))
        if last is None:
            return False
        if last >= since:
            return True
        gap = route_every if (stale and page.source != "pia_departures") else min_refetch
        return now - last < gap

    for f in flights:
        if f.status == "Cancelled":
            continue
        src = SOURCE_FOR.get((f.airline, f.kind))
        if src not in enabled:
            continue
        if not f.other:
            notes.append(f"{f.number}: no airport code for city {f.city!r}")
            continue
        held = held_for(f, held_legs)
        if finished(f, held):
            continue
        cp = due_checkpoint(f, now, cfg)
        if cp is None:
            continue
        if not stale and not has_gap(f, held, now, cfg):
            continue
        for p in pages_for(f):
            if not fresh_enough(p, cp):
                wanted[p].add(f.number)
                reasons.setdefault(p, "fallback" if stale else "gap")

    # The PIA departures board is one page for all PIA departures: also read it on a timer while
    # PIA departures are near, gap or not.
    if "pia_departures" in enabled:
        every = dt.timedelta(minutes=cfg["source"]["pia_departures"].get("every_minutes", 60))
        near = [f for f in flights if (f.airline, f.kind) == ("PK", "Departure") and f.status != "Cancelled"
                and f.isb_time and now - dt.timedelta(hours=1) <= f.isb_time <= now + dt.timedelta(hours=2)]
        for day in sorted({f.date for f in near}):
            p = Page("pia_departures", "ISB", "", day)
            last = last_fetch.get((p.source, p.target))
            if last is None or now - last >= every:
                wanted[p].update(f.number for f in near if f.date == day)
                reasons.setdefault(p, "timer")

    # Morning sweep: once a day, re-read yesterday's pages for late actual times.
    sweep_at = clock(cfg["windows"]["morning_sweep"])
    sweep_start = at_local(today, sweep_at, PKT)
    swept = any(r["reason"] == "sweep" and r["at"] >= sweep_start for r in log_rows)
    if now >= sweep_start and not swept:
        yday = today - dt.timedelta(days=1)
        for r in log_rows:
            if r["outcome"] not in ("ok", "empty") or not r["target"].endswith(yday.isoformat()):
                continue
            if r["source"] not in enabled:
                continue
            p = page_from_target(r["source"], r["target"])
            if p and p not in wanted:
                wanted[p] = set()
                reasons[p] = "sweep"

    out, skipped = [], []
    caps = {name: s.get("daily_cap", cfg["defaults"]["daily_cap"]) for name, s in cfg["source"].items()}
    order = {"timer": 0, "gap": 1, "fallback": 1, "sweep": 2}
    for p in sorted(wanted, key=lambda p: (order[reasons[p]], p.source, p.target)):
        if p.source in paused:
            skipped.append((p, "skipped_paused"))
        elif used_today[p.source] >= caps.get(p.source, cfg["defaults"]["daily_cap"]):
            skipped.append((p, "skipped_cap"))
        else:
            out.append((p, reasons[p], sorted(wanted[p])))
            used_today[p.source] += 1
    return out, skipped, notes, stale


def page_from_target(source, target):
    m = re.fullmatch(r"([A-Z]{3})(?:-([A-Z]{3})| departures) (\d{4}-\d{2}-\d{2})", target)
    if not m:
        return None
    return Page(source, m.group(1), m.group(2) or "", dt.date.fromisoformat(m.group(3)))


# --------------------------------------------------------------------------------------------
# Adapters: build the request, parse the answer into legs (parsers are pure)
# --------------------------------------------------------------------------------------------
PIA_URL = "https://book-pia.crane.aero/ibe/flightStatus/routeSearch/"
FLYJINNAH_URL = "https://www.flyjinnah.com/api/FlightStatus/Flightstatus"


def pia_params(page):
    day = page.day.strftime("%d %b %Y")                     # 07 Nov 2026
    if page.source == "pia_departures":
        return {"tab": "departure", "depPort": page.origin, "startDate": day}
    return {"tab": "route", "depPort": page.origin, "arrPort": page.destination, "startDate": day}


def parse_pia_cards(html):
    """PIA (Hitit Crane) status page -> raw cards. Each flight is a .desktop-route-block with the
    number in .first-container-area, two .info-block ends (.port 'Islamabad (ISB)' and .time
    'Scheduled: 11:55 PM' / 'Estimated: ...' / 'Actual: ...'), .flight-duration '4h 15m' and the
    status in .third-container p. The same card is repeated as .mobile-route-block (ignored)."""
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    for block in soup.select(".desktop-route-block"):
        fn = block.select_one(".first-container-area")
        ends = []
        for ib in block.select(".info-block"):
            port = ib.select_one(".port")
            code = re.search(r"\(([A-Z]{3})\)", port.get_text()) if port else None
            times = {}
            for span in ib.select(".time"):
                m = re.match(r"(Scheduled|Estimated|Actual):\s*(.+)", span.get_text(" ", strip=True))
                if m:
                    times[m.group(1).lower()] = clock(m.group(2))
            ends.append((code.group(1) if code else None, times))
        dur = block.select_one(".flight-duration")
        dm = re.search(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?", dur.get_text(" ", strip=True)) if dur else None
        minutes = (int(dm.group(1) or 0) * 60 + int(dm.group(2) or 0)) if dm and (dm.group(1) or dm.group(2)) else None
        st = block.select_one(".third-container p")
        if fn and len(ends) == 2 and ends[0][0] and ends[1][0]:
            cards.append({"flight_number": re.sub(r"[^A-Z0-9]", "", fn.get_text().upper()),
                          "origin": ends[0][0], "destination": ends[1][0],
                          "dep": ends[0][1], "arr": ends[1][1], "minutes": minutes,
                          "status": st.get_text(strip=True) if st else None})
    return cards


def pia_legs(html, page):
    """Cards -> legs with real timestamps. PIA times are local to each airport. Only cards that
    match the ports asked for are kept: PIA keeps search state per session and was once seen to
    answer a malformed query with the previous search."""
    legs = []
    for c in parse_pia_cards(html):
        if c["origin"] != page.origin or (page.destination and c["destination"] != page.destination):
            continue
        otz, dtz = tz_of(c["origin"]), tz_of(c["destination"])
        dep, arr = c["dep"], c["arr"]
        sched_dep = at_local(page.day, dep["scheduled"], otz) if otz and dep.get("scheduled") else None
        floor = sched_dep - dt.timedelta(hours=2) if sched_dep else None
        est_dep = first_from(dep.get("estimated"), otz, page.day, floor) if floor else None
        act_dep = first_from(dep.get("actual"), otz, page.day, floor) if floor else None
        minutes = c["minutes"] or MINUTES_BY_IATA.get(c["destination"]) or MINUTES_BY_IATA.get(c["origin"])
        span = dt.timedelta(minutes=minutes or 0)
        sched_arr = closest_to(arr.get("scheduled"), dtz, page.day, sched_dep + span if sched_dep else None)
        actual_base = (act_dep or est_dep or sched_dep)
        est_arr = closest_to(arr.get("estimated"), dtz, page.day, actual_base + span if actual_base else None)
        act_arr = closest_to(arr.get("actual"), dtz, page.day, actual_base + span if actual_base else None)
        legs.append({"flight_number": c["flight_number"], "airline": c["flight_number"][:2],
                     "dep_date": page.day, "origin": c["origin"], "destination": c["destination"],
                     "sched_dep": sched_dep, "est_dep": est_dep, "act_dep": act_dep,
                     "sched_arr": sched_arr, "est_arr": est_arr, "act_arr": act_arr,
                     "wheels_off": None, "wheels_on": None, "status_raw": c["status"],
                     "raw": {"dep": {k: _hm(v) for k, v in dep.items()},
                             "arr": {k: _hm(v) for k, v in arr.items()},
                             "minutes": c["minutes"], "status": c["status"]}})
    return legs


def _hm(hm):
    return f"{hm[0]:02d}:{hm[1]:02d}" if hm else None


def flyjinnah_body(page):
    return {"dataModel": {"deparureDate": page.day.strftime("%d/%m/%Y"),   # sic: 'deparureDate'
                          "origin": page.origin, "destination": page.destination}}


def _fj_utc(s):
    """'2026-11-07T08:34:00' or '...Z' (UTC). Unset values come back as 1970-01-01."""
    if not s or s[:4] < "2000":
        return None
    return dt.datetime.fromisoformat(s.rstrip("Z")).replace(tzinfo=UTC)


def _fj_local(s, iata):
    tz = tz_of(iata)
    if not s or s[:4] < "2000" or tz is None:
        return None
    return dt.datetime.fromisoformat(s.rstrip("Z")).replace(tzinfo=tz)


def flyjinnah_legs(text, page):
    """Fly Jinnah JSON -> legs. std/sta/atd/ata/takeOff/touchDown are UTC; *Local are local."""
    j = json.loads(text)
    dtos = ((j.get("data") or {}).get("flightStatusDTOs")) or []
    legs = []
    for f in dtos:
        o, d = f.get("origin"), f.get("destination")
        if o != page.origin or d != page.destination:
            continue
        local_day = (f.get("stdLocal") or "")[:10]
        legs.append({"flight_number": (f.get("flightNumber") or "").upper(), "airline": "9P",
                     "dep_date": dt.date.fromisoformat(local_day) if local_day[:4] >= "2000" else page.day,
                     "origin": o, "destination": d,
                     "sched_dep": _fj_utc(f.get("std")), "est_dep": _fj_local(f.get("etdLocal"), o),
                     "act_dep": _fj_utc(f.get("atd")),
                     "sched_arr": _fj_utc(f.get("sta")), "est_arr": _fj_local(f.get("etaLocal"), d),
                     "act_arr": _fj_utc(f.get("ata")),
                     "wheels_off": _fj_utc(f.get("takeOff")), "wheels_on": _fj_utc(f.get("touchDown")),
                     "status_raw": f.get("status"), "raw": f})
    return legs


def is_blocked(status_code, text):
    if status_code in (403, 429, 503):
        return True
    # Normal Cloudflare pages carry a challenge-platform beacon; a real check page is short.
    return len(text) < 20000 and bool(CHALLENGE_RE.search(text))


class Fetcher:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sessions = {}
        self.sent = 0

    def _session(self, airline):
        if airline not in self.sessions:
            s = requests.Session()
            s.headers.update({"User-Agent": self.cfg["defaults"]["user_agent"], "Accept-Language": "en"})
            self.sessions[airline] = s
        return self.sessions[airline]

    def get(self, page):
        """-> (http_status or None, text, error note or None)"""
        if self.sent:
            lo, hi = self.cfg["defaults"]["gap_seconds"]
            time.sleep(random.uniform(lo, hi))
        self.sent += 1
        try:
            if page.source.startswith("pia_"):
                r = self._session("PK").get(PIA_URL, params=pia_params(page), timeout=40)
            else:
                r = self._session("9P").post(FLYJINNAH_URL, json=flyjinnah_body(page), timeout=30,
                                             headers={"Referer": "https://www.flyjinnah.com/en/manage/flight-status/check-flight-status"})
            return r.status_code, r.text, None
        except requests.RequestException as e:
            return None, "", f"{type(e).__name__}: {e}"[:300]


def parse_page(page, text):
    return pia_legs(text, page) if page.source.startswith("pia_") else flyjinnah_legs(text, page)


# --------------------------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------------------------
def connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    conn = psycopg2.connect(host=os.environ.get("DB_HOST"), dbname=os.environ.get("DB_NAME"),
                            user=os.environ.get("DB_USER"), password=os.environ.get("DB_PASSWORD"),
                            port=int(os.environ.get("DB_PORT", 5432)), sslmode="require")
    return conn, conn.cursor(cursor_factory=RealDictCursor)


def read_state(conn, cur, airlines, today):
    cur.execute("""
        SELECT flight_number, scheduled_date, type, city, status, st, et, last_checked
        FROM public.isb_flights_view
        WHERE scheduled_date BETWEEN %s AND %s AND left(flight_number, 2) = ANY(%s)
    """, (today - dt.timedelta(days=1), today + dt.timedelta(days=1), list(airlines)))
    flights = [isb_flight_from_row(r) for r in cur.fetchall()]
    tables_ready = True
    legs, log_rows = [], []
    try:
        cur.execute("""
            SELECT flight_number, dep_date, origin, destination, sched_dep, sched_arr, act_dep, act_arr
            FROM public.airline_legs WHERE dep_date >= %s
        """, (today - dt.timedelta(days=2),))
        legs = cur.fetchall()
        cur.execute("""
            SELECT at, source, target, reason, outcome FROM public.airline_fetch_log
            WHERE at >= now() - interval '2 days'
        """)
        log_rows = cur.fetchall()
    except Exception as e:                      # tables not created yet (BL-051 SQL not applied)
        if getattr(e, "pgcode", None) != "42P01":
            raise
        conn.rollback()
        tables_ready = False
    held = defaultdict(list)
    for leg in legs:
        kind = "Arrival" if leg["destination"] == "ISB" else "Departure" if leg["origin"] == "ISB" else None
        if kind:
            held[(leg["flight_number"], kind)].append(leg)
    return flights, held, log_rows, tables_ready


# Created by the first live run, like the AeroDataBox collector's tables (safe to repeat). Private:
# row-level security on with no policies, so the site's browser key gets nothing.
SCHEMA = """
CREATE TABLE IF NOT EXISTS public.airline_legs (
  flight_number text        NOT NULL,          -- PK942, 9P670
  dep_date      date        NOT NULL,          -- scheduled departure date, local to the origin
  origin        text        NOT NULL,          -- IATA code
  destination   text        NOT NULL,
  airline       text        NOT NULL,          -- PK, 9P
  sched_dep     timestamptz,
  est_dep       timestamptz,
  act_dep       timestamptz,
  sched_arr     timestamptz,
  est_arr       timestamptz,
  act_arr       timestamptz,
  wheels_off    timestamptz,                   -- Fly Jinnah only
  wheels_on     timestamptz,
  status_raw    text,                          -- the airline's own word (PIA's is not reliable)
  source        text        NOT NULL,          -- pia_departures, pia_route, flyjinnah_route
  data_source   text        NOT NULL DEFAULT 'airline_web',
  raw           jsonb,
  first_seen    timestamptz NOT NULL DEFAULT now(),
  fetched_at    timestamptz NOT NULL,
  PRIMARY KEY (flight_number, dep_date, origin)
);

CREATE TABLE IF NOT EXISTS public.airline_fetch_log (
  id             bigserial   PRIMARY KEY,
  at             timestamptz NOT NULL DEFAULT now(),
  source         text        NOT NULL,
  target         text        NOT NULL,         -- 'KHI-ISB 2026-11-11', 'ISB departures 2026-11-11'
  reason         text,                         -- timer, gap, fallback, sweep, manual
  http_status    integer,
  outcome        text        NOT NULL,         -- ok, empty, blocked, error, parse_fail, skipped_paused, skipped_cap
  flights_parsed integer,
  missing        text[],                       -- flights wanted but not on the page
  ms             integer,
  note           text
);
CREATE INDEX IF NOT EXISTS airline_fetch_log_at_idx ON public.airline_fetch_log (at);

-- Not for the public site: no policies, so the browser key gets nothing.
ALTER TABLE public.airline_legs      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.airline_fetch_log ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airline_legs, public.airline_fetch_log FROM anon, authenticated;
REVOKE ALL ON SEQUENCE public.airline_fetch_log_id_seq FROM anon, authenticated;
"""


UPSERT = """
INSERT INTO public.airline_legs (flight_number, dep_date, origin, destination, airline,
    sched_dep, est_dep, act_dep, sched_arr, est_arr, act_arr, wheels_off, wheels_on,
    status_raw, source, raw, fetched_at)
VALUES (%(flight_number)s, %(dep_date)s, %(origin)s, %(destination)s, %(airline)s,
    %(sched_dep)s, %(est_dep)s, %(act_dep)s, %(sched_arr)s, %(est_arr)s, %(act_arr)s,
    %(wheels_off)s, %(wheels_on)s, %(status_raw)s, %(source)s, %(raw)s, %(fetched_at)s)
ON CONFLICT (flight_number, dep_date, origin) DO UPDATE SET
    destination = EXCLUDED.destination,
    sched_dep = COALESCE(EXCLUDED.sched_dep, airline_legs.sched_dep),
    est_dep   = COALESCE(EXCLUDED.est_dep,   airline_legs.est_dep),
    act_dep   = COALESCE(EXCLUDED.act_dep,   airline_legs.act_dep),
    sched_arr = COALESCE(EXCLUDED.sched_arr, airline_legs.sched_arr),
    est_arr   = COALESCE(EXCLUDED.est_arr,   airline_legs.est_arr),
    act_arr   = COALESCE(EXCLUDED.act_arr,   airline_legs.act_arr),
    wheels_off = COALESCE(EXCLUDED.wheels_off, airline_legs.wheels_off),
    wheels_on  = COALESCE(EXCLUDED.wheels_on,  airline_legs.wheels_on),
    status_raw = EXCLUDED.status_raw, source = EXCLUDED.source, raw = EXCLUDED.raw,
    fetched_at = EXCLUDED.fetched_at
"""

LOG = """
INSERT INTO public.airline_fetch_log (at, source, target, reason, http_status, outcome,
    flights_parsed, missing, ms, note)
VALUES (%(at)s, %(source)s, %(target)s, %(reason)s, %(http_status)s, %(outcome)s,
    %(flights_parsed)s, %(missing)s, %(ms)s, %(note)s)
"""


# --------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------
def load_config(path=CONFIG_PATH):
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def fmt(t):
    return t.astimezone(PKT).strftime("%d %b %H:%M") if t else "-"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="plan only: no requests, no writes")
    mode.add_argument("--no-write", action="store_true", help="fetch and parse, but write nothing")
    ap.add_argument("--max-requests", type=int, help="override max_requests_per_run")
    ap.add_argument("--page", nargs=2, action="append", metavar=("SOURCE", "TARGET"), default=[],
                    help='with --no-write: also fetch one page by hand, e.g. --page pia_route "KHI-ISB 2026-11-11"')
    args = ap.parse_args(argv)
    if args.page and not args.no_write:
        ap.error("--page only works with --no-write")
    cfg = load_config()
    now = dt.datetime.now(UTC)
    today = now.astimezone(PKT).date()
    label = "DRY RUN" if args.dry_run else "NO WRITE" if args.no_write else "LIVE"
    print(f"Airline collector ({label}) at {now:%Y-%m-%d %H:%M}Z / {fmt(now)} PKT")

    airlines = sorted({s["airline"] for s in cfg["source"].values() if s.get("enabled")})
    conn, cur = connect()
    if not (args.dry_run or args.no_write):
        cur.execute(SCHEMA)
        conn.commit()
    flights, held, log_rows, tables_ready = read_state(conn, cur, airlines, today)
    print(f"Airport data: {len(flights)} {'/'.join(airlines)} rows at Islamabad (yesterday to tomorrow); "
          f"airline legs held: {sum(map(len, held.values()))}; fetch log rows (2 days): {len(log_rows)}")
    if flights:
        newest = max((f.last_checked for f in flights if f.last_checked), default=None)
        no_et = sum(1 for f in flights if f.date == today and f.et is None)
        print(f"  newest airport update {fmt(newest)} PKT; today's rows without ET: {no_et} of "
              f"{sum(1 for f in flights if f.date == today)}")
    if not tables_ready:
        print("Tables airline_legs / airline_fetch_log do not exist yet (the first live run creates them).")
        if not (args.dry_run or args.no_write):
            print("Live mode could not create them.")
            return 2

    pages, skipped, notes, stale = plan(flights, held, log_rows, now, cfg)
    for n in sorted(set(notes)):
        print(f"  note: {n}")
    for p, why in skipped:
        print(f"  skip {p.source} {p.target}: {why}")
    for source, target in args.page:
        p = page_from_target(source, target)
        if p is None or source not in SOURCE_FOR.values():
            print(f"  --page {source} {target!r}: not a known source/target")
            return 2
        pages.append((p, "manual", []))
    limit = args.max_requests or cfg["defaults"]["max_requests_per_run"]
    if len(pages) > limit:
        print(f"  {len(pages) - limit} page(s) left for the next run (max {limit} per run)")
        pages = pages[:limit]
    print(f"Plan: {len(pages)} page(s){' (fallback mode)' if stale else ''}")
    for p, why, nums in pages:
        print(f"  {p.source:16} {p.target:28} {why:8} for {', '.join(nums) or '(all on the page)'}")
    if args.dry_run:
        return 0

    fetcher = Fetcher(cfg)
    newly_blocked = []
    log_out, legs_out = [], []
    for p, why, nums in pages:
        t0 = time.monotonic()
        status, text, err = fetcher.get(p)
        ms = int((time.monotonic() - t0) * 1000)
        entry = {"at": dt.datetime.now(UTC), "source": p.source, "target": p.target, "reason": why,
                 "http_status": status, "flights_parsed": None, "missing": None, "ms": ms, "note": err}
        if err:
            entry["outcome"] = "error"
        elif is_blocked(status, text):
            entry["outcome"] = "blocked"
            entry["note"] = f"HTTP {status}, check page or refusal"
            newly_blocked.append(p.source)
        elif status != 200:
            entry["outcome"] = "error"
            entry["note"] = f"HTTP {status}"
        else:
            try:
                legs = parse_page(p, text)
            except Exception as e:        # a changed page should show up in the log, not crash the run
                entry["outcome"] = "parse_fail"
                entry["note"] = f"{type(e).__name__}: {e}"[:300]
            else:
                got = {leg["flight_number"] for leg in legs}
                entry["flights_parsed"] = len(legs)
                entry["outcome"] = "ok" if legs else "empty"
                missing = sorted(set(nums) - got)
                entry["missing"] = missing or None
                for leg in legs:
                    leg.update(source=p.source, fetched_at=entry["at"])
                legs_out.extend(legs)
        log_out.append(entry)
        print(f"  {p.source:16} {p.target:28} {entry['outcome']:10} HTTP {status} {ms} ms"
              f" parsed {entry['flights_parsed']}" + (f" missing {entry['missing']}" if entry["missing"] else "")
              + (f" ({entry['note']})" if entry["note"] else ""))
        if entry["outcome"] == "blocked":
            print(f"  {p.source} paused until tomorrow (Pakistan time)")
            pages = [x for x in pages if x[0].source != p.source]
    for p, why in skipped:
        if why in ("skipped_paused", "skipped_cap"):
            log_out.append({"at": dt.datetime.now(UTC), "source": p.source, "target": p.target, "reason": None,
                            "http_status": None, "outcome": why, "flights_parsed": None, "missing": None,
                            "ms": None, "note": None})

    for leg in legs_out:
        print(f"    {leg['flight_number']:7} {leg['origin']}-{leg['destination']} {leg['dep_date']}"
              f"  dep {fmt(leg['sched_dep'])} est {fmt(leg['est_dep'])} act {fmt(leg['act_dep'])}"
              f"  arr {fmt(leg['sched_arr'])} est {fmt(leg['est_arr'])} act {fmt(leg['act_arr'])}"
              f"  [{leg['status_raw']}]")

    if args.no_write:
        print(f"No-write mode: {len(legs_out)} leg(s) and {len(log_out)} log row(s) not saved.")
    else:
        from psycopg2.extras import Json
        for leg in legs_out:
            cur.execute(UPSERT, {**leg, "raw": Json(leg["raw"], dumps=lambda o: json.dumps(o, default=str))})
        for entry in log_out:
            cur.execute(LOG, entry)
        cur.execute("DELETE FROM public.airline_legs WHERE dep_date < %s", (today - dt.timedelta(days=7),))
        cur.execute("DELETE FROM public.airline_fetch_log WHERE at < now() - interval '30 days'")
        conn.commit()
        print(f"Saved {len(legs_out)} leg(s), {len(log_out)} log row(s).")
    conn.close()

    already = {r["source"] for r in log_rows if r["outcome"] == "blocked"
               and r["at"].astimezone(PKT).date() == today}
    first_blocks = sorted(set(newly_blocked) - already)
    if first_blocks:
        print(f"::error::Blocked today for the first time: {', '.join(first_blocks)}. Paused until tomorrow.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
