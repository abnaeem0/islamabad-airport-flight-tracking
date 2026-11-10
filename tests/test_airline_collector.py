"""Tests for airline_collector.py (BL-051). No network, no database.

    python -m pytest tests/test_airline_collector.py      or      python tests/test_airline_collector.py

Page fixtures are trimmed from real answers: PIA (book-pia.crane.aero) on 2026-11-07, Fly Jinnah's
status API on 2026-11-07 and 2026-11-10.
"""
import copy
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import airline_collector as ac  # noqa: E402

PKT, UTC = ac.PKT, ac.UTC
CFG = ac.load_config()
D = dt.date(2026, 11, 11)


def pkt(day, hhmm):
    h, m = map(int, hhmm.split(":"))
    return dt.datetime(day.year, day.month, day.day, h, m, tzinfo=PKT)


# ------------------------------------------------------------------ PIA fixtures
def pia_card(num, o_name, o, d_name, d, dep, arr, dur, status):
    """One card in PIA's real markup (desktop block plus its mobile duplicate)."""
    def end(name, code, times, align=""):
        spans = "".join(f'<div class="time-block-{k.lower()}"> <span class="time">{k}: {v}</span> </div>'
                        for k, v in times)
        return (f'<div class="col-xl-4 col-6 info-block{align}"> <span class="port">{name} ({code})</span> '
                f'{spans} </div>')
    middle = f'<div class="col-xl-4 col-12 middle-block"> <span class="flight-duration">{dur}</span> </div>'
    desk = (f'<div class="desktop-route-block col-12"> <div class="first-container"> '
            f'<div class="first-container-area col-12">{num}</div> </div> <div class="second-container"> '
            f'<div class="info-row">{end(o_name, o, dep)}{middle}{end(d_name, d, arr, " text-right")}</div> '
            f'</div> <div class="third-container"> <div class="third-container-area col-12"> '
            f'<p class="flight-status-on-time pt-4">{status}</p> </div> </div> </div>')
    mobile = (f'<div class="mobile-route-block col-12 "> <div class="info-row col-12">{end(o_name, o, dep)}'
              f'{end(d_name, d, arr)}</div> </div>')
    return f'<div class="selection-item"> <div class="row w-100 no-gutters"> {desk} {mobile} </div> </div>'


PIA_ISB_DEPARTURES = "<html><body>Outbound Flights" + "".join([
    pia_card("PK-287", "Islamabad", "ISB", "Doha", "DOH",
             [("Scheduled", "11:55 PM"), ("Estimated", "11:55 PM")],
             [("Scheduled", "02:10 AM"), ("Estimated", "01:59 AM")], "4h 15m", "Earlier"),
    pia_card("PK-739", "Islamabad", "ISB", "Jeddah", "JED",
             [("Scheduled", "07:20 AM"), ("Estimated", "11:00 PM")],
             [("Scheduled", "10:40 AM"), ("Estimated", "02:50 AM")], "5h 20m", "Delayed"),
    pia_card("PK-301", "Islamabad", "ISB", "Karachi", "KHI",
             [("Scheduled", "10:00 AM"), ("Estimated", "10:00 AM"), ("Actual", "10:00 AM")],
             [("Scheduled", "11:55 AM"), ("Estimated", "12:01 PM"), ("Actual", "11:50 AM")], "1h 55m", "Delayed"),
]) + "</body></html>"

PIA_JED_ISB = "<html><body>" + "".join([
    pia_card("PK-942", "Jeddah", "JED", "Islamabad", "ISB",
             [("Scheduled", "12:05 AM"), ("Estimated", "04:05 AM"), ("Actual", "03:57 AM")],
             [("Scheduled", "07:30 AM"), ("Estimated", "11:23 AM"), ("Actual", "11:12 AM")], "5h 25m", "Delayed"),
    # a card from a previous search left in the session: must be dropped
    pia_card("PK-301", "Islamabad", "ISB", "Karachi", "KHI",
             [("Scheduled", "10:00 AM")], [("Scheduled", "11:55 AM")], "1h 55m", "On Time"),
]) + "</body></html>"


# ------------------------------------------------------------------ Fly Jinnah fixtures
FJ_KHI_ISB = json.dumps({"message": {"code": 200, "title": "OK", "description": ""}, "data": {"success": True,
    "flightStatusDTOs": [
        {"flightNumber": "9P670", "durationInMinutes": "115", "origin": "KHI", "destination": "ISB",
         "etdLocal": "2026-11-07T07:10:00", "etaLocal": "2026-11-07T09:05:00", "status": "ARRIVED",
         "std": "2026-11-07T02:10:00", "sta": "2026-11-07T04:05:00", "stdLocal": "2026-11-07T07:10:00",
         "staLocal": "2026-11-07T09:05:00", "rstd": "2026-11-07T02:10:00", "rsta": "2026-11-07T04:05:00",
         "takeOff": "2026-11-07T02:21:00", "touchDown": "2026-11-07T03:52:00", "ata": "2026-11-07T04:02:00",
         "ataLocal": "2026-11-07T09:02:00", "atd": "2026-11-07T02:10:00Z", "atdLocal": "2026-11-07T07:10:00"},
        {"flightNumber": "9P674", "durationInMinutes": "115", "origin": "KHI", "destination": "ISB",
         "etdLocal": "2026-11-07T18:15:00", "etaLocal": "2026-11-07T20:10:00", "status": "SHEDULE",
         "std": "2026-11-07T13:15:00", "sta": "2026-11-07T15:10:00", "stdLocal": "2026-11-07T18:15:00",
         "staLocal": "2026-11-07T20:10:00", "rstd": "2026-11-07T13:15:00", "rsta": "2026-11-07T15:10:00",
         "takeOff": "1970-01-01T00:00:00", "touchDown": "1970-01-01T00:00:00", "ata": "1970-01-01T00:00:00",
         "ataLocal": "1970-01-01T00:00:00", "atd": "1970-01-01T00:00:00Z", "atdLocal": "1970-01-01T00:00:00"},
    ]}})


# ------------------------------------------------------------------ helpers
def flight(num, kind, city, st, et=None, status="", day=D, checked=None):
    row = {"flight_number": num, "scheduled_date": day, "type": kind, "city": city, "status": status,
           "st": st, "et": et, "last_checked": checked}      # None: fresh, set by run_plan
    return ac.isb_flight_from_row(row)


def log(at, source, target, outcome="ok", reason="gap"):
    return {"at": at.astimezone(UTC), "source": source, "target": target, "reason": reason, "outcome": outcome}


def run_plan(flights, now, held=None, logs=(), cfg=CFG):
    for f in flights:
        if f.last_checked is None:                       # airport data updated 5 min ago
            f.last_checked = (now - dt.timedelta(minutes=5)).astimezone(UTC)
    pages, skipped, notes, stale = ac.plan(flights, held or {}, list(logs), now, cfg)
    return [(p.source, p.target, why, nums) for p, why, nums in pages], \
           [(p.source, p.target, why) for p, why in skipped], notes, stale


# ------------------------------------------------------------------ tests: helpers
def test_clock():
    assert ac.clock("07:30") == (7, 30) and ac.clock("0730") == (7, 30) and ac.clock("7:05") == (7, 5)
    assert ac.clock("11:55 PM") == (23, 55) and ac.clock("12:05 AM") == (0, 5) and ac.clock("12:10 PM") == (12, 10)
    assert ac.clock("") is None and ac.clock(None) is None and ac.clock("25:00") is None and ac.clock("Delayed") is None


def test_day_picking():
    sched = pkt(D, "23:55")
    assert ac.first_from((0, 9), PKT, D, sched - dt.timedelta(hours=2)) == pkt(D + dt.timedelta(days=1), "00:09")
    sched = pkt(D, "07:20")       # 15 h 40 late on the same day, not 8 h early the day before
    assert ac.first_from((23, 0), PKT, D, sched - dt.timedelta(hours=2)) == pkt(D, "23:00")
    target = pkt(D, "23:55") + dt.timedelta(minutes=255)
    assert ac.closest_to((2, 10), ac.tz_of("DOH"), D, target).date() == D + dt.timedelta(days=1)


def test_isb_row_next_day_et():
    f = flight("PK287", "Departure", "Doha", "23:55", "00:30")
    assert f.st == pkt(D, "23:55") and f.et == pkt(D + dt.timedelta(days=1), "00:30")
    assert f.other_iata == "DOH" and ac.SOURCE_FOR[(f.airline, f.kind)] == "pia_departures"
    g = flight("PK 942", "Arrival", "JEDDAH", "07:30")
    assert g.number == "PK942" and g.other_iata == "JED" and g.et is None


def test_origin_dates_near_midnight():
    f = flight("PK942", "Arrival", "Jeddah", "07:30")   # 07:30 PKT - 295 min = 00:35 in Jeddah
    assert f.origin_dates() == [D - dt.timedelta(days=1), D]
    g = flight("PK300", "Arrival", "Karachi", "08:55")
    assert g.origin_dates() == [D]


def test_page_target_roundtrip():
    for p in (ac.Page("pia_departures", "ISB", "", D), ac.Page("pia_route", "JED", "ISB", D)):
        assert ac.page_from_target(p.source, p.target) == p
    assert ac.page_from_target("pia_route", "rubbish") is None


def test_blocked():
    assert ac.is_blocked(403, "x") and ac.is_blocked(429, "") and ac.is_blocked(200, "<title>Just a moment...</title>")
    assert not ac.is_blocked(200, "<html>" + "x" * 30000 + "challenge-platform</html>")


# ------------------------------------------------------------------ tests: parsers
def test_pia_departures_board():
    legs = {x["flight_number"]: x for x in ac.pia_legs(PIA_ISB_DEPARTURES, ac.Page("pia_departures", "ISB", "", D))}
    assert set(legs) == {"PK287", "PK739", "PK301"}
    pk287 = legs["PK287"]
    assert pk287["sched_dep"] == pkt(D, "23:55")
    assert pk287["sched_arr"] == dt.datetime(2026, 11, 12, 2, 10, tzinfo=ac.tz_of("DOH"))
    assert pk287["est_arr"] == dt.datetime(2026, 11, 12, 1, 59, tzinfo=ac.tz_of("DOH"))
    assert pk287["act_dep"] is None and pk287["status_raw"] == "Earlier"
    pk739 = legs["PK739"]           # 07:20 -> 23:00 the same day; arrives Jeddah 02:50 the next day
    assert pk739["est_dep"] == pkt(D, "23:00")
    assert pk739["est_arr"] == dt.datetime(2026, 11, 12, 2, 50, tzinfo=ac.tz_of("JED"))
    pk301 = legs["PK301"]
    assert pk301["act_dep"] == pkt(D, "10:00") and pk301["act_arr"] == pkt(D, "11:50")
    assert pk301["raw"]["arr"]["actual"] == "11:50" and pk301["raw"]["minutes"] == 115


def test_pia_route_keeps_only_the_route_asked_for():
    page = ac.Page("pia_route", "JED", "ISB", D)
    legs = ac.pia_legs(PIA_JED_ISB, page)
    assert [x["flight_number"] for x in legs] == ["PK942"]
    pk942 = legs[0]
    jed = ac.tz_of("JED")
    assert pk942["sched_dep"] == dt.datetime(2026, 11, 11, 0, 5, tzinfo=jed)
    assert pk942["act_dep"] == dt.datetime(2026, 11, 11, 3, 57, tzinfo=jed)
    assert pk942["sched_arr"] == pkt(D, "07:30") and pk942["act_arr"] == pkt(D, "11:12")


def test_flyjinnah():
    page = ac.Page("flyjinnah_route", "KHI", "ISB", dt.date(2026, 11, 7))
    legs = {x["flight_number"]: x for x in ac.flyjinnah_legs(FJ_KHI_ISB, page)}
    a, b = legs["9P670"], legs["9P674"]
    assert a["dep_date"] == dt.date(2026, 11, 7)
    assert a["sched_dep"] == dt.datetime(2026, 11, 7, 2, 10, tzinfo=UTC)
    assert a["act_arr"] == dt.datetime(2026, 11, 7, 4, 2, tzinfo=UTC)
    assert a["wheels_on"] == dt.datetime(2026, 11, 7, 3, 52, tzinfo=UTC)
    assert a["est_arr"] == dt.datetime(2026, 11, 7, 9, 5, tzinfo=PKT)
    assert b["act_dep"] is None and b["act_arr"] is None and b["wheels_off"] is None   # 1970 = unset
    assert ac.flyjinnah_legs(FJ_KHI_ISB, ac.Page("flyjinnah_route", "LHE", "ISB", D)) == []
    body = ac.flyjinnah_body(ac.Page("flyjinnah_route", "ISB", "KHI", D))
    assert body == {"dataModel": {"deparureDate": "11/11/2026", "origin": "ISB", "destination": "KHI"}}


def test_pia_params():
    assert ac.pia_params(ac.Page("pia_departures", "ISB", "", D)) == \
        {"tab": "departure", "depPort": "ISB", "startDate": "11 Nov 2026"}
    assert ac.pia_params(ac.Page("pia_route", "KHI", "ISB", D)) == \
        {"tab": "route", "depPort": "KHI", "arrPort": "ISB", "startDate": "11 Nov 2026"}


# ------------------------------------------------------------------ tests: planner
def test_nothing_to_do():
    assert run_plan([], pkt(D, "09:00"))[0] == []
    on_time = flight("PK300", "Arrival", "Karachi", "08:55", "08:55", "Landed")
    assert run_plan([on_time], pkt(D, "09:10"))[0] == []          # at a checkpoint, but no gap


def test_delayed_arrivals_share_one_route_page():
    a = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Delayed")
    b = flight("PK308", "Arrival", "Karachi", "09:30", "10:00", "Delayed")
    pages, skipped, notes, stale = run_plan([a, b], pkt(D, "08:15"))   # Karachi departure (10:00 - 120 min) + 10 = 08:10
    assert pages == [("pia_route", "KHI-ISB 2026-11-11", "gap", ["PK300", "PK308"])]
    assert not stale and not skipped


def test_refetch_rules():
    a = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Delayed")
    now = pkt(D, "08:15")
    recent = log(pkt(D, "08:12"), "pia_route", "KHI-ISB 2026-11-11")
    assert run_plan([a], now, logs=[recent])[0] == []               # already read after the checkpoint
    old = log(pkt(D, "07:30"), "pia_route", "KHI-ISB 2026-11-11")
    assert len(run_plan([a], now, logs=[old])[0]) == 1              # read before this checkpoint


def test_finished_flight_and_daily_flight_numbers():
    a = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Delayed")
    now = pkt(D, "10:15")
    today_leg = {"flight_number": "PK300", "dep_date": D, "origin": "KHI", "destination": "ISB",
                 "sched_dep": pkt(D, "07:00"), "sched_arr": pkt(D, "08:55"),
                 "act_dep": pkt(D, "08:15"), "act_arr": pkt(D, "10:02")}
    assert run_plan([a], now, held={("PK300", "Arrival"): [today_leg]})[0] == []
    yesterday = copy.deepcopy(today_leg)
    for k in ("sched_dep", "sched_arr", "act_dep", "act_arr"):
        yesterday[k] -= dt.timedelta(days=1)
    yesterday["dep_date"] = D - dt.timedelta(days=1)
    assert len(run_plan([a], now, held={("PK300", "Arrival"): [yesterday]})[0]) == 1


def test_departures_board_on_timer():
    d = flight("PK301", "Departure", "Karachi", "10:00", "10:00")
    pages = run_plan([d], pkt(D, "09:00"))[0]
    assert pages == [("pia_departures", "ISB departures 2026-11-11", "timer", ["PK301"])]
    assert run_plan([d], pkt(D, "09:30"), logs=[log(pkt(D, "09:00"), "pia_departures",
                                                    "ISB departures 2026-11-11", reason="timer")])[0] == []
    assert len(run_plan([d], pkt(D, "10:05"), logs=[log(pkt(D, "09:00"), "pia_departures",
                                                         "ISB departures 2026-11-11", reason="timer")])[0]) == 1


def test_flyjinnah_departure_with_gap():
    d = flight("9P671", "Departure", "Karachi", "09:45", "10:30", "Delayed")
    pages = run_plan([d], pkt(D, "10:45"))[0]
    assert pages == [("flyjinnah_route", "ISB-KHI 2026-11-11", "gap", ["9P671"])]


def test_international_arrival_needs_origin_side_once():
    f = flight("PK942", "Arrival", "Jeddah", "07:30", "07:30", "Expected")
    pages = run_plan([f], pkt(D, "00:50"))[0]      # 00:35 Jeddah estimate +10
    assert [p[1] for p in pages] == ["JED-ISB 2026-11-10", "JED-ISB 2026-11-11"]
    held = {("PK942", "Arrival"): [{"flight_number": "PK942", "dep_date": D, "origin": "JED", "destination": "ISB",
                                    "sched_dep": pkt(D, "02:05"), "sched_arr": pkt(D, "07:30"),
                                    "act_dep": None, "act_arr": None}]}
    assert run_plan([f], pkt(D, "00:50"), held=held)[0] == []


def test_stale_airport_data_fallback():
    on_time = flight("9P670", "Arrival", "Karachi", "09:05", "09:05", "Expected",
                     checked=pkt(D, "06:00").astimezone(UTC))
    now = pkt(D, "07:15")
    pages, _, notes, stale = run_plan([on_time], now)
    assert stale and pages == [("flyjinnah_route", "KHI-ISB 2026-11-11", "fallback", ["9P670"])]
    later = pkt(D, "07:45")       # next checkpoint passed, but the route was read 30 min ago
    assert run_plan([on_time], later, logs=[log(now, "flyjinnah_route", "KHI-ISB 2026-11-11")])[0] == []


def test_paused_and_capped():
    a = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Delayed")
    now = pkt(D, "08:15")
    blocked = log(pkt(D, "06:00"), "pia_route", "LHE-ISB 2026-11-11", outcome="blocked")
    pages, skipped, _, _ = run_plan([a], now, logs=[blocked])
    assert pages == [] and skipped == [("pia_route", "KHI-ISB 2026-11-11", "skipped_paused")]
    used = [log(pkt(D, "01:00") + dt.timedelta(minutes=i), "pia_route", f"UET-ISB {D}") for i in range(60)]
    pages, skipped, _, _ = run_plan([a], now, logs=used)
    assert pages == [] and skipped[0][2] == "skipped_cap"
    yesterday_block = log(pkt(D, "06:00") - dt.timedelta(days=1), "pia_route", "LHE-ISB 2026-11-10", outcome="blocked")
    assert len(run_plan([a], now, logs=[yesterday_block])[0]) == 1


def test_three_errors_rest_two_hours():
    a = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Delayed")
    errs = [log(pkt(D, "07:00") + dt.timedelta(minutes=10 * i), "pia_route", "MUX-ISB 2026-11-11", "error")
            for i in range(3)]
    assert run_plan([a], pkt(D, "08:15"), logs=errs)[1] == [("pia_route", "KHI-ISB 2026-11-11", "skipped_paused")]
    assert len(run_plan([a], pkt(D, "10:25"), logs=errs)[0]) == 1   # 2 h after the last error


def test_morning_sweep_once():
    y = D - dt.timedelta(days=1)
    yday = [log(pkt(y, "20:00"), "flyjinnah_route", f"KHI-ISB {y}"),
            log(pkt(y, "21:00"), "pia_departures", f"ISB departures {y}", reason="timer"),
            log(pkt(y, "21:30"), "pia_route", f"JED-ISB {y}", outcome="blocked")]
    assert run_plan([], pkt(D, "04:20"), logs=yday)[0] == []
    pages = run_plan([], pkt(D, "04:35"), logs=yday)[0]
    assert {(p[0], p[1], p[2]) for p in pages} == {("flyjinnah_route", f"KHI-ISB {y}", "sweep"),
                                                   ("pia_departures", f"ISB departures {y}", "sweep")}
    done = yday + [log(pkt(D, "04:35"), "flyjinnah_route", f"KHI-ISB {y}", reason="sweep")]
    assert run_plan([], pkt(D, "05:00"), logs=done)[0] == []


def test_unknown_city_and_cancelled():
    odd = flight("PK999", "Arrival", "Atlantis", "08:55", "10:00", "Delayed")
    gone = flight("PK300", "Arrival", "Karachi", "08:55", "10:00", "Cancelled")
    pages, _, notes, _ = run_plan([odd, gone], pkt(D, "08:15"))
    assert pages == [] and any("Atlantis" in n for n in notes)


def test_disabled_source_and_missing_et_switch():
    a = flight("9P670", "Arrival", "Karachi", "09:05", None, "Expected")
    assert len(run_plan([a], pkt(D, "07:15"))[0]) == 1
    cfg = copy.deepcopy(CFG)
    cfg["gap"]["missing_et"] = False
    assert run_plan([a], pkt(D, "07:15"), cfg=cfg)[0] == []
    cfg["gap"]["missing_et"] = True
    cfg["source"]["flyjinnah_route"]["enabled"] = False
    assert run_plan([a], pkt(D, "07:15"), cfg=cfg)[0] == []


# ------------------------------------------------------------------ tests: a whole run, faked
class FakeCur:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((" ".join(sql.split())[:40], params))


class FakeConn:
    def commit(self):
        self.committed = True

    def close(self):
        pass

    def rollback(self):
        pass


def _fake_run(monkey_args, flights, held, logs, answers, now_pkt):
    cur, conn = FakeCur(), FakeConn()
    for f in flights:
        f.last_checked = f.last_checked or (now_pkt - dt.timedelta(minutes=5)).astimezone(UTC)
    saved = (ac.connect, ac.read_state, ac.Fetcher.get, ac.dt)
    sent = []

    class FrozenDT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return now_pkt.astimezone(tz or UTC)

    class DtModule:
        datetime, date, timedelta, timezone = FrozenDT, dt.date, dt.timedelta, dt.timezone

    def fake_get(self, page):
        sent.append(page.target)
        return answers[page.target]
    try:
        ac.connect = lambda: (conn, cur)
        ac.read_state = lambda *a: (flights, held, logs, True)
        ac.Fetcher.get = fake_get
        ac.dt = DtModule
        rc = ac.main(monkey_args)
    finally:
        ac.connect, ac.read_state, ac.Fetcher.get, ac.dt = saved
    return rc, sent, cur, conn


def test_full_run_no_write_and_live():
    flights = [flight("PK942", "Arrival", "Jeddah", "07:30", "11:23", "Delayed"),
               flight("PK301", "Departure", "Karachi", "10:00", "10:00")]
    now = pkt(D, "09:30")
    answers = {"JED-ISB 2026-11-11": (200, PIA_JED_ISB, None),
               "JED-ISB 2026-11-10": (200, "<html>no flights</html>", None),
               "ISB departures 2026-11-11": (200, PIA_ISB_DEPARTURES, None)}
    rc, sent, cur, conn = _fake_run(["--no-write"], flights, {}, [], answers, now)
    assert rc == 0 and set(sent) <= set(answers) and "ISB departures 2026-11-11" in sent
    assert cur.sql == [] and not hasattr(conn, "committed")

    rc, sent, cur, conn = _fake_run([], flights, {}, [], answers, now)
    upserts = [p for s, p in cur.sql if s.startswith("INSERT INTO public.airline_legs")]
    logs = [p for s, p in cur.sql if s.startswith("INSERT INTO public.airline_fetch_log")]
    assert rc == 0 and conn.committed and len(logs) == len(sent)
    assert {u["flight_number"] for u in upserts} >= {"PK301", "PK287", "PK739"}
    assert all(u["source"] and u["fetched_at"] for u in upserts)

    blocked = {k: (403, "Just a moment...", None) for k in answers}
    rc, sent, cur, _ = _fake_run([], flights, {}, [], blocked, now)
    outcomes = [p["outcome"] for s, p in cur.sql if s.startswith("INSERT INTO public.airline_fetch_log")]
    assert rc == 1 and "blocked" in outcomes
    assert len(sent) == len({ac.page_from_target("x", t).origin for t in sent})   # stops per source after a block


def test_manual_page_only_without_writes():
    answers = {"KHI-ISB 2026-11-07": (200, FJ_KHI_ISB, None)}
    rc, sent, cur, _ = _fake_run(["--no-write", "--page", "flyjinnah_route", "KHI-ISB 2026-11-07"],
                                 [], {}, [], answers, pkt(D, "09:30"))
    assert rc == 0 and sent == ["KHI-ISB 2026-11-07"] and cur.sql == []
    try:
        ac.main(["--page", "flyjinnah_route", "KHI-ISB 2026-11-07"])
    except SystemExit as e:
        assert e.code == 2
    else:
        raise AssertionError("--page without --no-write must be refused")


def test_dry_run_sends_nothing():
    flights = [flight("PK301", "Departure", "Karachi", "10:00", "10:00")]
    rc, sent, cur, _ = _fake_run(["--dry-run"], flights, {}, [], {}, pkt(D, "09:30"))
    assert rc == 0 and sent == [] and cur.sql == []


if __name__ == "__main__":
    import inspect
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and inspect.isfunction(f)]
    for name, fn in tests:
        fn()
        print("ok", name)
    print(f"{len(tests)} tests passed")
