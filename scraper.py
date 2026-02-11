import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import pytz
import requests

# --- CONFIG ---
PKT = pytz.timezone("Asia/Karachi")  # Pakistan Standard Time

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", 5432)

API_URL = "https://api.example.com/flights"  # replace with real PAA API
# ----------------

def get_dates():
    today = datetime.now(PKT).date()
    return [
        today - timedelta(days=1),  # yesterday
        today,                      # today
        today + timedelta(days=1)   # tomorrow
    ]

def fetch_flights(date, flight_type):
    """
    Abstract fetcher. Replace parsing logic with real API fields.
    """
    # Example API call: ?date=YYYY-MM-DD&type=Arrival/Departure
    resp = requests.get(f"{API_URL}?date={date}&type={flight_type}")
    data = resp.json()
    
    flights = []
    for f in data.get("flights", []):
        flights.append({
            "flight_number": f.get("flight_number"),
            "scheduled_date": date,
            "type": flight_type,
            "city": f.get("city"),
            "status": f.get("status"),
            "ST": f.get("scheduled_time"),  # scheduled time
            "ET": f.get("estimated_time"),  # estimated time
            "remarks": f.get("remarks"),
            "airline_logo": f.get("airline_logo"),
        })
    return flights

def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def upsert_flights(conn, flights):
    """
    Upsert latest flight data into `flights` table.
    Returns list of flights with 'changed' flag for snapshot insertion.
    """
    changed_flights = []
    with conn.cursor() as cur:
        for f in flights:
            cur.execute("""
                SELECT status, ST, ET, remarks, city, airline_logo
                FROM flights
                WHERE flight_number=%s AND scheduled_date=%s AND type=%s
            """, (f["flight_number"], f["scheduled_date"], f["type"]))
            existing = cur.fetchone()
            
            # Check if any field changed
            is_changed = False
            if existing:
                existing_fields = dict(zip(["status", "ST", "ET", "remarks", "city", "airline_logo"], existing))
                for key in ["status", "ST", "ET", "remarks", "city", "airline_logo"]:
                    if f.get(key) != existing_fields.get(key):
                        is_changed = True
                        break
            else:
                is_changed = True  # new flight
            
            f["is_changed"] = is_changed
            changed_flights.append(f)
            
            # Upsert flights table
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
                datetime.now(PKT), datetime.now(PKT)
            ))
        conn.commit()
    return changed_flights

def insert_snapshots(conn, flights):
    """
    Insert snapshot for each flight.
    """
    with conn.cursor() as cur:
        records = []
        now = datetime.now(PKT)
        for f in flights:
            records.append((
                f["flight_number"], f["scheduled_date"], now, f["is_changed"],
                f["status"], f["ST"], f["ET"], f["remarks"], f["city"], f["airline_logo"]
            ))
        execute_values(cur, """
            INSERT INTO flight_snapshots
            (flight_number, scheduled_date, scraped_at, is_changed, status, ST, ET, remarks, city, airline_logo)
            VALUES %s
        """, records)
        conn.commit()

def main():
    dates = get_dates()
    conn = connect_db()
    all_flights = []
    for date in dates:
        for flight_type in ["Arrival", "Departure"]:
            flights = fetch_flights(date, flight_type)
            all_flights.extend(flights)
    
    changed_flights = upsert_flights(conn, all_flights)
    insert_snapshots(conn, changed_flights)
    conn.close()
    print(f"Processed {len(all_flights)} flights.")

if __name__ == "__main__":
    main()
