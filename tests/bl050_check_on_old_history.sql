-- BL-050 check on real history: runs the exact rule of sql/isb_flight_moves_view.sql against
-- the old Islamabad table (about two months), read-only. Paste into the Supabase SQL editor and
-- press Run. It creates and changes nothing.
-- Expected on 2026-11-10 (Claude's in-browser run): these 10 rows, while their dates are still
-- inside the old table's two months
--   PK287 Departure 2026-09-15 -> 09-16 00:30 new_row
--   PA230 Departure 2026-09-19 -> 09-20 01:40 new_row
--   FZ987 Departure 2026-09-19 -> 09-20 00:00 new_row
--   PK842 Arrival   2026-09-24 -> 09-25 00:10 new_row
--   PK181 Departure 2026-09-28 -> 09-29 03:10 new_row
--   PA210 Departure 2026-10-06 -> 10-07 04:15 took_over
--   PK842 Arrival   2026-10-08 -> 10-09 00:50 took_over
--   PK714 Arrival   2026-10-10 -> 10-11 00:05 took_over
--   PK181 Departure 2026-10-12 -> 10-13 03:10 new_row
--   PK841 Departure 2026-11-01 -> 11-02 03:10 new_row
-- Any other row: look at it before applying the view.
WITH s AS (                       -- Islamabad board changes, times turned into minutes
  SELECT id, flight_number, scheduled_date, type, scraped_at, change_type, st, et,
         CASE WHEN st ~ '^\d{1,2}:\d{2}'
              THEN substring(st from '^(\d{1,2}):')::int * 60 + substring(st from '^\d{1,2}:(\d{2})')::int END AS st_min,
         CASE WHEN et ~ '^\d{1,2}:\d{2}'
              THEN substring(et from '^(\d{1,2}):')::int * 60 + substring(et from '^\d{1,2}:(\d{2})')::int END AS et_min
  FROM (SELECT id, flight_number, scheduled_date, type, change_type, st, et,
               scraped_at AT TIME ZONE 'UTC' AS scraped_at   -- stored as UTC without a zone
        FROM public.flight_snapshots) old_history
),
first_seen AS (                   -- each row's first recorded scheduled time
  SELECT DISTINCT ON (flight_number, scheduled_date, type)
         flight_number, scheduled_date, type, st, st_min
  FROM s WHERE st_min IS NOT NULL
  ORDER BY flight_number, scheduled_date, type, scraped_at, id
),
dropped AS (                      -- step 1: rows whose latest change is a drop
  SELECT flight_number, scheduled_date, type, scraped_at AS dropped_at
  FROM (SELECT DISTINCT ON (flight_number, scheduled_date, type)
               flight_number, scheduled_date, type, change_type, scraped_at
        FROM s ORDER BY flight_number, scheduled_date, type, scraped_at DESC, id DESC) latest
  WHERE change_type = 'dropped'
),
next_day_times AS (               -- step 2: next-day times A showed before its drop
  SELECT DISTINCT d.flight_number, d.scheduled_date, d.type, d.dropped_at,
         f.st AS from_st, t.m AS moved_min
  FROM dropped d
  JOIN first_seen f USING (flight_number, scheduled_date, type)
  JOIN s a ON a.flight_number = d.flight_number AND a.scheduled_date = d.scheduled_date
          AND a.type = d.type AND a.scraped_at <= d.dropped_at
  CROSS JOIN LATERAL (VALUES (a.et_min), (a.st_min)) AS t(m)
  WHERE t.m IS NOT NULL AND t.m < f.st_min - 720
),
matches AS (                      -- step 3: the next day's row showed that exact time
  SELECT n.flight_number, n.type, n.scheduled_date AS from_date, n.from_st, n.moved_min,
         n.dropped_at, min(b.scraped_at) AS matched_at
  FROM next_day_times n
  JOIN s b ON b.flight_number = n.flight_number AND b.type = n.type
          AND b.scheduled_date = n.scheduled_date + 1
          AND (b.st_min = n.moved_min OR b.et_min = n.moved_min)
          AND b.scraped_at BETWEEN n.dropped_at - interval '24 hours' AND n.dropped_at + interval '12 hours'
  GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT DISTINCT ON (m.flight_number, m.type, m.from_date)
       m.flight_number,
       m.type,
       m.from_date,
       m.from_st,
       m.from_date + 1                                                    AS to_date,
       lpad((m.moved_min / 60)::text, 2, '0') || ':' || lpad((m.moved_min % 60)::text, 2, '0') AS moved_time,
       CASE WHEN EXISTS (SELECT 1 FROM next_day_times n
                         WHERE n.flight_number = m.flight_number AND n.type = m.type
                           AND n.scheduled_date = m.from_date AND n.moved_min = b.st_min)
            THEN 'new_row' ELSE 'took_over' END                           AS kind,
       b.st                                                               AS to_first_st,
       m.dropped_at,
       m.matched_at
FROM matches m
JOIN first_seen b ON b.flight_number = m.flight_number AND b.type = m.type
                 AND b.scheduled_date = m.from_date + 1
ORDER BY m.flight_number, m.type, m.from_date, m.matched_at;;
