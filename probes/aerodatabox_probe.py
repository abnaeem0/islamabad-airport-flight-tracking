#!/usr/bin/env python3
"""
AeroDataBox probe for the Islamabad Flight Tracker (BL-040). Read-only, writes nothing to our database.

What it asks AeroDataBox (about 8 of the free plan's 400 monthly units):
  1. Islamabad arrivals 00:00-11:59 with the origin leg   (2 units)
  2. Islamabad arrivals 12:00-23:59 with the origin leg   (2 units)
  3. Skardu arrivals and departures 00:00-23:59 (12 h max, so 06:00-17:59)  (2 units)
  4. One international arrival looked up by flight number, to see actual departure times  (2 units)

What it answers:
  - How many Islamabad arrivals AeroDataBox knows, and for how many it gives the origin's
    scheduled departure date and time (the "leg match"), by airline.
  - Whether it gives revised/actual departure times (for "has it left?").
  - Whether it knows anything about Skardu.
  - How many units are left on the plan.

On GitHub: Actions -> "AeroDataBox probe" -> Run workflow (key from the AERODATABOX_KEY secret;
the summary shows on the run page and the raw answers are kept 7 days as a run artifact).

Run locally (Python 3.8+, no extra packages):
    python3 aerodatabox_probe.py                 # today in Pakistan time
    python3 aerodatabox_probe.py --date 2026-11-10
    python3 aerodatabox_probe.py --summary-only  # re-read the saved answers, spends no units

The key: set AERODATABOX_KEY in the environment, or paste it when asked (it is not shown or saved).
Raw answers are saved to ./aerodatabox_probe_output/. They contain no key. Send back summary.txt.
"""
import argparse, datetime as dt, getpass, json, os, sys, urllib.error, urllib.parse, urllib.request
from collections import Counter

HOST = "aerodatabox.p.rapidapi.com"
OUT = os.path.join(os.getcwd(), "aerodatabox_probe_output")
PK_AIRLINES = {"PK": "PIA", "PA": "Airblue", "9P": "Fly Jinnah", "PF": "AirSial", "Z8": "South Air"}

lines = []
def say(s=""):
    print(s)
    lines.append(s)

def today_pkt():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5)).strftime("%Y-%m-%d")

def call(key, path, params, name):
    url = f"https://{HOST}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"x-rapidapi-key": key, "x-rapidapi-host": HOST,
                                               "User-Agent": "isb-flight-tracker-probe"})
    info = {"name": name, "path": path, "params": params}
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            body = r.read().decode("utf-8", "replace")
            info["status"] = r.status
            info["headers"] = {k: v for k, v in r.headers.items() if "ratelimit" in k.lower()}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        info["status"] = e.code
        info["headers"] = {k: v for k, v in e.headers.items() if "ratelimit" in k.lower()}
    except Exception as e:  # network trouble
        body = ""
        info["status"] = None
        info["error"] = repr(e)
    try:
        info["json"] = json.loads(body) if body.strip() else None
    except ValueError:
        info["json"] = None
        info["text"] = body[:500]
    with open(os.path.join(OUT, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(info, f, indent=1, ensure_ascii=False)
    return info

def load(name):
    p = os.path.join(OUT, f"{name}.json")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)

# ---- reading AeroDataBox flight objects defensively (shapes differ with/without withLeg) ----
def g(d, *path):
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

def times(end):
    """All '...Time' fields of one end (departure/arrival), local values."""
    out = {}
    if isinstance(end, dict):
        for k, v in end.items():
            if k.lower().endswith("time") and isinstance(v, dict):
                out[k] = v.get("local") or v.get("utc")
    return out

def flights_of(info, key):
    j = info and info.get("json")
    if isinstance(j, dict):
        return j.get(key) or []
    if isinstance(j, list):
        return j
    return []

def code_of(f):
    n = (f.get("number") or "").replace(" ", "")
    return g(f, "airline", "iata") or n[:2]

def origin_end(f):
    # withLeg: f["departure"] is the origin end; without it there is only f["movement"]
    return f.get("departure") if isinstance(f.get("departure"), dict) else None

def describe(info, label):
    if info is None:
        say(f"{label}: not run")
        return
    st = info.get("status")
    say(f"{label}: HTTP {st}" + (f"  ({info.get('error')})" if info.get("error") else ""))
    if st != 200:
        j = info.get("json")
        msg = (j.get("message") if isinstance(j, dict) else None) or info.get("text") or ""
        say(f"   reply: {str(msg)[:300]}")
    if info.get("headers"):
        say("   limits: " + ", ".join(f"{k}={v}" for k, v in info["headers"].items()))

def summarize(date):
    say(f"AeroDataBox probe, {date} (Pakistan date), run {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC")
    say("=" * 70)
    a1, a2, k1, s1 = load("isb_arrivals_am"), load("isb_arrivals_pm"), load("skardu"), load("one_flight")
    describe(a1, "1. ISB arrivals 00-12")
    describe(a2, "2. ISB arrivals 12-24")
    describe(k1, "3. Skardu 06-18")
    describe(s1, "4. One flight by number")
    say()

    arr = flights_of(a1, "arrivals") + flights_of(a2, "arrivals")
    say(f"ISLAMABAD ARRIVALS known to AeroDataBox: {len(arr)}")
    if not arr:
        say("   none: check the HTTP replies above")
    else:
        by_air, with_sched, with_moved = Counter(), Counter(), Counter()
        overnight = []
        status = Counter(f.get("status") for f in arr)
        for f in arr:
            c = code_of(f)
            by_air[c] += 1
            o = origin_end(f)
            ot = times(o)
            if ot.get("scheduledTime"):
                with_sched[c] += 1
            if any(k != "scheduledTime" for k in ot):
                with_moved[c] += 1
            at = times(f.get("arrival") or f.get("movement"))
            if ot.get("scheduledTime") and at.get("scheduledTime") and ot["scheduledTime"][:10] != at["scheduledTime"][:10]:
                overnight.append(f)
        tot_s, tot_m = sum(with_sched.values()), sum(with_moved.values())
        say(f"   with origin scheduled departure (leg match): {tot_s} of {len(arr)}")
        say(f"   with revised/actual origin times:            {tot_m} of {len(arr)}")
        say(f"   origin date differs from Islamabad date:     {len(overnight)} (the overnight cases the share box asks about)")
        say(f"   statuses: {dict(status)}")
        say()
        say("   by airline (flights / with origin schedule / with revised-actual):")
        for c, n in by_air.most_common():
            tag = f"  <- {PK_AIRLINES[c]}" if c in PK_AIRLINES else ""
            say(f"     {c:<3} {n:>3} / {with_sched[c]:>3} / {with_moved[c]:>3}{tag}")
        missing_pk = [f"{c} {n}" for c, n in PK_AIRLINES.items() if c not in by_air]
        if missing_pk:
            say(f"   Pakistani airlines not seen: {', '.join(missing_pk)}")
        say()
        say("   sample (first 12 by Islamabad time):")
        say(f"     {'flight':<8} {'from':<14} {'origin sched (local)':<24} {'origin other times':<34} {'ISB sched (local)':<22} status")
        def isb_time(f):
            return times(f.get("arrival") or f.get("movement")).get("scheduledTime") or ""
        for f in sorted(arr, key=isb_time)[:12]:
            o = origin_end(f) or {}
            ot = times(o)
            other = ", ".join(f"{k.replace('Time','')} {v[11:16]}" for k, v in ot.items() if k != "scheduledTime")
            frm = g(o, "airport", "iata") or g(f, "movement", "airport", "iata") or "?"
            say(f"     {(f.get('number') or '').replace(' ',''):<8} {str(frm):<14} {str(ot.get('scheduledTime') or '-'):<24} "
                f"{other[:34]:<34} {isb_time(f)[:22]:<22} {f.get('status')}")
        if overnight:
            say()
            say("   overnight examples (left the day before landing):")
            for f in overnight[:6]:
                o = origin_end(f)
                say(f"     {(f.get('number') or '').replace(' ',''):<8} leaves {times(o).get('scheduledTime')}  lands {isb_time(f)}")
    say()

    sk = flights_of(k1, "arrivals") + flights_of(k1, "departures")
    say(f"SKARDU flights known (06:00-17:59): {len(sk)}")
    for f in sk[:8]:
        say(f"     {(f.get('number') or '').replace(' ',''):<8} {f.get('status')}  {times(f.get('departure') or f.get('movement'))}")
    say()

    if s1 and isinstance(s1.get("json"), list):
        say(f"ONE FLIGHT BY NUMBER ({s1['params'].get('_flight', '')}): {len(s1['json'])} result(s)")
        for f in s1["json"][:2]:
            say(f"     departure times: {times(f.get('departure'))}")
            say(f"     arrival times:   {times(f.get('arrival'))}")
    say()
    say("Raw answers: aerodatabox_probe_output/*.json  (no key inside)")
    with open(os.path.join(OUT, "summary.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\nSaved {os.path.join(OUT, 'summary.txt')}: send that file back.")

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default=today_pkt(), help="Pakistan date YYYY-MM-DD (default today)")
    ap.add_argument("--summary-only", action="store_true", help="only re-read saved answers (no units)")
    ap.add_argument("--yes", action="store_true", help="do not ask before calling (for GitHub Actions)")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    if a.summary_only:
        return summarize(a.date)

    key = os.environ.get("AERODATABOX_KEY") or getpass.getpass("AeroDataBox (RapidAPI) key, not shown: ").strip()
    if not key:
        sys.exit("No key given.")
    print(f"About to make 4 calls (about 8 units) for {a.date}." + ("" if a.yes else " Press Enter to go, Ctrl+C to stop."))
    if not a.yes:
        input()
    d = a.date
    leg = {"withLeg": "true", "withCancelled": "true", "withCodeshared": "false",
           "withCargo": "false", "withPrivate": "false", "withLocation": "false"}
    a1 = call(key, f"/flights/airports/iata/ISB/{d}T00:00/{d}T11:59", {**leg, "direction": "Arrival"}, "isb_arrivals_am")
    call(key, f"/flights/airports/iata/ISB/{d}T12:00/{d}T23:59", {**leg, "direction": "Arrival"}, "isb_arrivals_pm")
    call(key, f"/flights/airports/iata/KDU/{d}T06:00/{d}T17:59", {**leg, "direction": "Both"}, "skardu")
    # pick one international arrival from the morning list for the by-number lookup
    pick = None
    for f in flights_of(a1, "arrivals"):
        if code_of(f) not in PK_AIRLINES and f.get("number"):
            pick = f["number"].replace(" ", "")
            break
    if pick:
        info = call(key, f"/flights/number/{pick}/{d}", {"dateLocalRole": "Arrival", "withAircraftImage": "false",
                                                           "withLocation": "false"}, "one_flight")
        info["params"]["_flight"] = pick
        with open(os.path.join(OUT, "one_flight.json"), "w", encoding="utf-8") as fh:
            json.dump(info, fh, indent=1, ensure_ascii=False)
    summarize(d)

if __name__ == "__main__":
    main()
