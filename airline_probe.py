#!/usr/bin/env python3
"""
Airline flight-status probe for Islamabad (ISB). Read-only, low rate.

Checks the three airline sources that worked in a browser on 2026-11-07, but
from a plain HTTP client, which is what a collector would be:

  airblue    GET  https://www.airblue.com/m/mobile-page.asp?type=Arrivals|Departures&city=ISB&date=7-Nov-2026
             GET  ...&flight=200&origin=KHI&destination=ISB      (per-flight: Sched / Est / Actual, both ends)
             Server-rendered HTML. Times are local to each station. Board times are the CURRENT
             (revised) times, not the original schedule; the per-flight page has both.
  pia        GET  https://book-pia.crane.aero/ibe/flightStatus/routeSearch/?tab=departure&depPort=ISB&startDate=07 Nov 2026
             GET  ...?tab=route&depPort=KHI&arrPort=ISB&startDate=07 Nov 2026
             Hitit Crane booking engine, server-rendered. Redirects to /ibe/flightStatus?_cid=... (keep cookies).
             Departures-by-airport exists; arrivals need one route search per origin.
             Pages carry Cloudflare's bot script; worked in a browser, plain client is what this checks.
  flyjinnah  POST https://www.flyjinnah.com/api/FlightStatus/Flightstatus
             {"dataModel":{"deparureDate":"07/11/2026","origin":"KHI","destination":"ISB"}}
             {"dataModel":{"deparureDate":"07/11/2026","flightNumber":"9P671","carrierCode":""}}
             JSON with std/sta (UTC), *Local, takeOff, touchDown, atd, ata. Worked without cookies.
             Needs a route pair or a flight number; origin-only is rejected.

Usage:
  pip install requests beautifulsoup4
  python3 airline_probe.py                 # all three, ~20 requests, ~1 minute
  python3 airline_probe.py airblue --details
  python3 airline_probe.py pia flyjinnah

Output: one JSON line per flight on stdout, a summary on stderr, raw responses in probe_out/<airline>/.
A Cloudflare challenge, captcha or HTTP 403 is reported and that airline is skipped: do not work around it.
"""
import argparse
import datetime as dt
import json
import pathlib
import re
import sys
import time
from urllib.parse import parse_qs, urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) isb-flight-tracker-probe/0.2"
OUT = pathlib.Path("probe_out")
DELAY_S = 2.0
PKT = ZoneInfo("Asia/Karachi")
CHALLENGE_RE = re.compile(r"Just a moment|cf-chl-|Attention Required|captcha", re.I)


def save(airline: str, tag: str, body: str) -> None:
    d = OUT / airline
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{re.sub(r'[^A-Za-z0-9_.-]+', '_', tag)}").write_text(body, encoding="utf-8", errors="replace")


def blocked(r: requests.Response) -> str | None:
    if r.status_code in (403, 429, 503):
        return f"HTTP {r.status_code}"
    # Cloudflare's normal pages include a challenge-platform beacon script; a real
    # interstitial is a short page with one of these markers and no content.
    if len(r.text) < 20000 and CHALLENGE_RE.search(r.text):
        return "challenge page"
    return None


# ----------------------------------------------------------------- Airblue
AIRBLUE = "https://www.airblue.com/m/mobile-page.asp"


def airblue_date(d: dt.date) -> str:
    return f"{d.day}-{d.strftime('%b')}-{d.year}"  # 7-Nov-2026


def hhmm(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    m = re.fullmatch(r"(\d{1,2}):?(\d{2})", s)
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else None


def parse_airblue_board(html: str) -> list[dict]:
    """<li><h3><a href="...flight=275&origin=RUH&destination=ISB">Flt 275</a></h3>
           <p class="no-ellipses">RUH-ISB<br/>[Cancelled]</p>
           <p class="ui-li-aside">Departure<br/><strong>0130</strong><br/>Arrival<br/><strong>0745</strong></p></li>"""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for li in soup.find_all("li"):
        a = li.find("a", href=re.compile(r"flight="))
        if not a:
            continue
        q = {k.lower(): v[0] for k, v in parse_qs(urlparse(a["href"]).query).items()}
        info = li.find("p", class_="no-ellipses")
        status = re.search(r"\[(.*?)\]", info.get_text(" ", strip=True)) if info else None
        aside = li.find("p", class_="ui-li-aside")
        times = [s.get_text(strip=True) for s in aside.find_all("strong")] if aside else []
        rows.append({
            "airline": "PA", "flight": f"PA{q.get('flight')}", "origin": q.get("origin"),
            "destination": q.get("destination"), "date": q.get("date"),
            "status": status.group(1) if status else None,
            "dep_current_local": hhmm(times[0]) if len(times) > 0 else None,
            "arr_current_local": hhmm(times[1]) if len(times) > 1 else None,
            "_detail_query": {k: q[k] for k in ("type", "city", "date", "flight", "origin", "destination") if k in q},
        })
    return rows


def parse_airblue_detail(html: str) -> dict:
    """<ul><li data-role="list-divider">Departure - Karachi</li>
           <li>Sched<p class="ui-li-aside"><strong>07:30</strong></p></li>
           <li>Actual<p ...><strong>14:11</strong></p></li>
           <li>Status<p ...><strong>Departed</strong></p></li></ul>  (same for Arrival; Est appears instead of Actual)"""
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for ul in soup.find_all("ul"):
        divider = ul.find("li", attrs={"data-role": "list-divider"})
        title = divider.get_text(" ", strip=True) if divider else ""
        m = re.match(r"(Departure|Arrival)\s*-\s*(.+)", title)
        if not m:
            continue
        end = {"station": m.group(2).strip()}
        for li in ul.find_all("li"):
            if li.get("data-role") == "list-divider":
                continue
            strong = li.find("strong")
            if not strong:
                continue
            label = li.get_text(" ", strip=True).replace(strong.get_text(strip=True), "").strip().lower()
            value = strong.get_text(strip=True)
            key = {"sched": "sched", "est": "est", "actual": "actual", "status": "status"}.get(label, label)
            end[key] = value if key == "status" else hhmm(value)
        out[m.group(1).lower()] = end
    return out


def run_airblue(session: requests.Session, details: bool) -> list[dict]:
    today = dt.datetime.now(PKT).date()
    found = []
    for typ in ("Arrivals", "Departures"):
        for d in (today - dt.timedelta(days=1), today, today + dt.timedelta(days=1)):
            params = {"type": typ, "city": "ISB", "date": airblue_date(d)}
            r = session.get(AIRBLUE, params=params, timeout=30)
            save("airblue", f"{typ}_{params['date']}.html", r.text)
            if (b := blocked(r)):
                print(f"# airblue {typ} {params['date']}: BLOCKED ({b})", file=sys.stderr)
                return found
            rows = parse_airblue_board(r.text)
            print(f"# airblue {typ} {params['date']}: HTTP {r.status_code}, {len(rows)} flights", file=sys.stderr)
            for row in rows:
                row["board"] = typ
            found += rows
            time.sleep(DELAY_S)
    if details:
        for row in found:
            if row["date"] != airblue_date(today):
                continue
            r = session.get(AIRBLUE, params=row["_detail_query"], timeout=30)
            save("airblue", f"detail_{row['flight']}_{row['origin']}{row['destination']}_{row['date']}.html", r.text)
            row["detail"] = parse_airblue_detail(r.text)
            time.sleep(DELAY_S)
    return found


# ----------------------------------------------------------------- PIA
PIA = "https://book-pia.crane.aero/ibe/flightStatus/routeSearch/"
PIA_ARRIVAL_ORIGINS = ["KHI", "JED", "DXB"]  # probe only; a collector would cover every origin PIA flies to ISB


def to24(s: str | None) -> str | None:
    if not s:
        return None
    try:
        return dt.datetime.strptime(s.strip(), "%I:%M %p").strftime("%H:%M")
    except ValueError:
        return None


def parse_pia(html: str) -> list[dict]:
    """.desktop-route-block > .first-container-area 'PK-287'
       .info-block (x2) > .port 'Islamabad (ISB)', .time 'Scheduled: 11:55 PM' / 'Estimated: ...' / 'Actual: ...'
       .third-container p  'Earlier' | 'Delayed' | ...   (each card is also repeated as .mobile-route-block)"""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for block in soup.select(".desktop-route-block"):
        fn = block.select_one(".first-container-area")
        ends = []
        for ib in block.select(".info-block"):
            port = ib.select_one(".port")
            code = re.search(r"\(([A-Z]{3})\)", port.get_text()) if port else None
            t = {}
            for span in ib.select(".time"):
                m = re.match(r"(Scheduled|Estimated|Actual):\s*(.+)", span.get_text(" ", strip=True))
                if m:
                    t[m.group(1).lower()] = to24(m.group(2))
            ends.append({"port": code.group(1) if code else None, **t})
        st = block.select_one(".third-container p")
        if len(ends) == 2 and fn:
            rows.append({
                "airline": "PK", "flight": fn.get_text(strip=True).replace("-", ""),
                "origin": ends[0]["port"], "destination": ends[1]["port"],
                "dep": {k: v for k, v in ends[0].items() if k != "port"},
                "arr": {k: v for k, v in ends[1].items() if k != "port"},
                "status": st.get_text(strip=True) if st else None,
            })
    return rows


def run_pia(session: requests.Session) -> list[dict]:
    today = dt.datetime.now(PKT).date()
    date_s = today.strftime("%d %b %Y")  # 07 Nov 2026
    queries = [("departures ISB", {"tab": "departure", "depPort": "ISB", "startDate": date_s}, ("ISB", None))]
    for o in PIA_ARRIVAL_ORIGINS:
        queries.append((f"route {o}-ISB", {"tab": "route", "depPort": o, "arrPort": "ISB", "startDate": date_s}, (o, "ISB")))
    found = []
    for label, params, (want_o, want_d) in queries:
        r = session.get(f"{PIA}?{urlencode(params)}", timeout=40, allow_redirects=True)
        save("pia", f"{label}_{date_s}.html", r.text)
        if (b := blocked(r)):
            print(f"# pia {label}: BLOCKED ({b}) final URL {r.url[:90]}", file=sys.stderr)
            return found
        rows = parse_pia(r.text)
        # The engine keeps search state per session (_cid). A malformed query was seen to return the
        # PREVIOUS search's results, so keep only rows that match what was asked.
        keep = [x for x in rows if x["origin"] == want_o and (want_d is None or x["destination"] == want_d)]
        print(f"# pia {label}: HTTP {r.status_code}, {len(rows)} cards, {len(keep)} match the query", file=sys.stderr)
        for x in keep:
            x["date"] = date_s
        found += keep
        time.sleep(DELAY_S)
    return found


# ----------------------------------------------------------------- Fly Jinnah
FLYJINNAH = "https://www.flyjinnah.com/api/FlightStatus/Flightstatus"


def fj_time(s: str | None) -> str | None:
    """'2026-11-07T09:02:00' / '...Z'. Unset values come back as 1970-01-01T00:00:00.
    Seen 2026-11-07: std/sta/atd/ata/takeOff/touchDown are UTC (atd has a Z, the others do not);
    *Local fields are station-local. atd/ata look like off-block/on-block, takeOff/touchDown like wheels."""
    if not s or s[:4] < "2000":
        return None
    return s


def run_flyjinnah(session: requests.Session) -> list[dict]:
    today = dt.datetime.now(PKT).date().strftime("%d/%m/%Y")  # 07/11/2026
    bodies = [
        ("route KHI-ISB", {"deparureDate": today, "origin": "KHI", "destination": "ISB"}),  # sic: 'deparureDate'
        ("route ISB-KHI", {"deparureDate": today, "origin": "ISB", "destination": "KHI"}),
        ("number 9P671", {"deparureDate": today, "flightNumber": "9P671", "carrierCode": ""}),
    ]
    found = []
    for label, dm in bodies:
        r = session.post(FLYJINNAH, json={"dataModel": dm}, timeout=30,
                         headers={"Referer": "https://www.flyjinnah.com/en/manage/flight-status/check-flight-status"})
        save("flyjinnah", f"{label}.json", r.text)
        if (b := blocked(r)):
            print(f"# flyjinnah {label}: BLOCKED ({b})", file=sys.stderr)
            return found
        try:
            j = r.json()
        except ValueError:
            print(f"# flyjinnah {label}: HTTP {r.status_code}, not JSON: {r.text[:150]!r}", file=sys.stderr)
            continue
        dtos = (j.get("data") or {}).get("flightStatusDTOs") or []
        msg = (j.get("message") or {}).get("title")
        print(f"# flyjinnah {label}: HTTP {r.status_code}, {msg}, {len(dtos)} flights", file=sys.stderr)
        for f in dtos:
            found.append({
                "airline": "9P", "flight": f.get("flightNumber"), "origin": f.get("origin"),
                "destination": f.get("destination"), "status": f.get("status"),
                **{k: fj_time(f.get(k)) for k in ("std", "sta", "stdLocal", "staLocal", "etdLocal", "etaLocal",
                                                   "atd", "atdLocal", "ata", "ataLocal", "takeOff", "touchDown")},
            })
        time.sleep(DELAY_S)
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    # default=None: Python 3.12+ checks a list default against `choices` and rejects it.
    ap.add_argument("airlines", nargs="*", default=None, choices=["airblue", "pia", "flyjinnah"])
    ap.add_argument("--details", action="store_true", help="Airblue: also fetch today's per-flight pages")
    args = ap.parse_args()
    args.airlines = args.airlines or ["airblue", "pia", "flyjinnah"]
    ok = True
    for name in args.airlines:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept-Language": "en"})
        try:
            rows = {"airblue": lambda: run_airblue(s, args.details), "pia": lambda: run_pia(s),
                    "flyjinnah": lambda: run_flyjinnah(s)}[name]()
        except requests.RequestException as e:
            print(f"# {name}: request failed: {type(e).__name__}: {e}", file=sys.stderr)
            rows = []
        for row in rows:
            row.pop("_detail_query", None)
            print(json.dumps(row, ensure_ascii=False))
        print(f"## {name}: {len(rows)} flights", file=sys.stderr)
        ok = ok and bool(rows)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
