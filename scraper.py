#!/usr/bin/env python3
"""
scraper.py
Fetches PAA arrivals/departures for yesterday, today, tomorrow,
upserts to flights table, inserts snapshots with is_changed flag.
"""

import os
import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values
import pytz

# ===== CONFIG =====
CITY = "Islamabad"
PKT = pytz.timezone("Asia/Karachi")
PAA_TEMPLATE = "https://paaconnectapi.paa.gov.pk/api/flights/{date}/{type}/" + CITY

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", 5432)
# ==================

def get_dates():
    today = datetime.datetime.now(PKT).date()
    return [
        today - datetime.timedelta(days=1),
        today,
        today + datetime.timedelta(days=1)
    ]

def fetch_flights(date, flight_type):
    url = PAA_TEMPLATE.format(date=date, type=flight_type)
    print(f"Fetching {flight_type} flights for {date}")
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        flights = []
        for f in data if isinstance(data, list) else []:
            flights.append({
                "flight_number": f.get("flight_number"),
                "scheduled_date": date,
                "type": flight_type,
                "city": f.get("city"),
                "status": f.get("status"),
                "ST": f.get("scheduled_time"),
                "ET": f.get("estimated_time"),
                "remarks": f.get("remarks"),
                "airline_logo": f.get("airline_logo")
            })
        print(f"{len(flights)} flights retrieved")
        return flights
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def upsert_flights(conn, flights):
    changed_flights = []
    now = datetime.datetime.now(PKT)
    with conn.cursor() as cur:
        for f in flights:
            try:
                cur.execute("""
                    SELECT status, ST, ET, remarks, city, airline_logo
                    FROM flights
                    WHERE flight_number=%s AND scheduled_date=%s AND type=%s
                """, (f["flight_number"], f["scheduled_date"], f["type"]))
                existing = cur.fetchone()

                is_changed = False
                if existing:
                    existing_fields = dict(zip(["status", "ST", "ET", "remarks", "city", "airline_logo"], existing))
                    for key in ["status", "ST", "ET", "remarks", "city", "airline_logo"]:
                        if f.get(key) != existing_fields.get(key):
                            is_changed = True
                            break
                else:
                    is_changed = True

                f["is_changed"] = is_changed
                changed_flights.append(f)

                cur.execute("""
                    INSERT INTO flights (flight_number, scheduled_date, type, city, status, ST, ET, remarks, airline_logo, last_checked, last_updated)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (flight_number, scheduled_date, type)
                    DO UPDATE SET
                        city = EXCLUDED.city,
                        status = EXCLUDED.status,
                        ST = EXCLUDED.ST,
                        ET = EXCLUDED.ET,
                        remarks = EXCLUDED.remarks,
                        airline_logo = EXCLUDED.airline_logo,
                        last_checked = EXCLUDED.last_checked,
                        last_updated = CASE WHEN flights.status <> EXCLUDED.status 
                                                OR flights.ST <> EXCLUDED.ST
                                                OR flights.ET <> EXCLUDED.ET
                                                OR flights.remarks <> EXCLUDED.remarks
                                                OR flights.city <> EXCLUDED.city
                                                OR flights.airline_logo <> EXCLUDED.airline_logo
                                            THEN EXCLUDED.last_checked
                                            ELSE flights.last_updated
                                        END
                """, (
                    f["flight_number"], f["scheduled_date"], f["type"], f["city"], f["status"],
                    f["ST"], f["ET"], f["remarks"], f["airline_logo"],
                    now, now
                ))
            except Exception as e:
                print(f"Error upserting flight {f.get('flight_number')}: {e}")
        conn.commit()
    print(f"{sum(f['is_changed'] for f in changed_flights)} flights changed")
    return changed_flights

def insert_snapshots(conn, flights):
    now = datetime.datetime.now(PKT)
    with conn.cursor() as cur:
        records = []
        for f in flights:
            records.append((
                f["flight_number"], f["scheduled_date"], now, f["is_changed"],
                f["status"], f["ST"], f["ET"], f["remarks"], f["city"], f["airline_logo"]
            ))
        try:
            execute_values(cur, """
                INSERT INTO flight_snapshots
                (flight_number, scheduled_date, scraped_at, is_changed, status, ST, ET, remarks, city, airline_logo)
                VALUES %s
            """, records)
            conn.commit()
        except Exception as e:
            print(f"Error inserting snapshots: {e}")

def main():
    dates = get_dates()
    conn = connect_db()
    all_flights = []
    for date in dates:
        for flight_type in ["Arrival", "Departure"]:
            flights = fetch_flights(date, flight_type)
            all_flights.extend(flights)
    
    if not all_flights:
        print("No flights fetched.")
        return

    changed_flights = upsert_flights(conn, all_flights)
    insert_snapshots(conn, changed_flights)
    conn.close()
    print(f"Processed {len(all_flights)} flights total.")

if __name__ == "__main__":
    main()
