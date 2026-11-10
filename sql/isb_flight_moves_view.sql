-- BL-050: Islamabad flights that the airport board moved to the next day's list after a delay
-- past midnight. Read-only view; changes no data. Run in the Supabase SQL editor.
-- Rollback: DROP VIEW IF EXISTS public.isb_flight_moves_view;
--
-- Why: PAA usually keeps a late flight on its own day's list until it lands or leaves. After a
-- long delay it sometimes drops the flight from that day and lists it on the next day at the
-- delayed time instead. Our collector then records "Dropped" on the first day and an unrelated
-- row on the second. This view links the two, using only what the board itself showed.
--
-- The rule. A is a flight's row on day D (flight number + direction), B is the row with the same
-- flight number and direction on D+1. A link exists only if all three hold:
--   1. A ended Dropped: its latest change is a drop and it never came back.
--   2. Before the drop, A showed a next-day time: an expected (ET) or new scheduled (ST) time
--      more than 12 hours earlier on the clock than A's original scheduled time
--      (21:55 -> 03:10 is next day; 21:55 -> 23:40 is not).
--   3. B showed that exact time as its ST or ET, recorded from 24 hours before A's drop to
--      12 hours after it.
-- kind:
--   'new_row'   B first appeared at one of A's delayed times: the board listed the late flight on
--               the next day under its new time.
--   'took_over' B first appeared at another time (the next day's own flight of the same number)
--               and then switched to A's delayed time. The board no longer lists B's own flight
--               separately; whether it was cancelled is unknown.
-- Anything that does not meet all three steps gets no row, and the site shows what PAA said.
-- Tested against real history: tests/bl050_moves_fixtures.sql and PLAN.md BL-050.

CREATE OR REPLACE VIEW public.isb_flight_moves_view
WITH (security_invoker = true) AS
WITH s AS (                       -- Islamabad board changes, times turned into minutes
  SELECT id, flight_number, scheduled_date, type, scraped_at, change_type, st, et,
         CASE WHEN st ~ '^\d{1,2}:\d{2}'
              THEN substring(st from '^(\d{1,2}):')::int * 60 + substring(st from '^\d{1,2}:(\d{2})')::int END AS st_min,
         CASE WHEN et ~ '^\d{1,2}:\d{2}'
              THEN substring(et from '^(\d{1,2}):')::int * 60 + substring(et from '^\d{1,2}:(\d{2})')::int END AS et_min
  FROM public.origin_snapshots
  WHERE source_airport = 'Islamabad' AND data_source = 'paa'
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
ORDER BY m.flight_number, m.type, m.from_date, m.matched_at;

GRANT SELECT ON public.isb_flight_moves_view TO anon, authenticated;
