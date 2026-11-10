-- BL-050 test cases for sql/isb_flight_moves_view.sql. Run on a throwaway Postgres only, never
-- on Supabase: it creates its own origin_snapshots table.
--   psql -f tests/bl050_moves_fixtures.sql   (prints PASS or FAIL lines)
-- Rows are copied from the real Islamabad board history (old flight_snapshots table,
-- Sep-Nov 2026), times in Pakistan time. Expected results are at the bottom.

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
DO $$ BEGIN CREATE ROLE anon; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE ROLE authenticated; EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE public.origin_snapshots (
  id bigserial PRIMARY KEY, flight_number text, scheduled_date date, source_airport text,
  data_source text, type text, scraped_at timestamptz, is_changed boolean, change_type text,
  status text, st text, et text, origin_city text, destination_city text, airline_logo text,
  nature text);

-- snap(flight, list date, direction, 'MM-DD HH:MI' PKT, change, status, ST, ET)
CREATE FUNCTION snap(f text, d date, t text, seen text, c text, stat text, st_ text, et_ text)
RETURNS void LANGUAGE sql AS $$
  INSERT INTO public.origin_snapshots (flight_number, scheduled_date, source_airport, data_source,
         type, scraped_at, is_changed, change_type, status, st, et)
  VALUES (f, d, 'Islamabad', 'paa', t, ('2026-' || seen || '+05')::timestamptz, true, c, stat, st_, et_);
$$;

-- 1. PK841: delayed 21:55 -> 03:10, board makes a new 2 Nov row at 03:10 (expect new_row)
SELECT snap('PK841','2026-11-01','Departure','10-31 05:03','new','Expected','21:55','');
SELECT snap('PK841','2026-11-01','Departure','10-31 15:03','status_change','Delayed','21:55','01:25');
SELECT snap('PK841','2026-11-02','Departure','11-01 16:13','new','Expected','03:10','');
SELECT snap('PK841','2026-11-01','Departure','11-01 16:41','time_change','Delayed','21:55','03:10');
SELECT snap('PK841','2026-11-01','Departure','11-01 16:51','dropped','Dropped',NULL,NULL);
SELECT snap('PK841','2026-11-02','Departure','11-01 23:22','status_change','Check-In','03:10','');
SELECT snap('PK841','2026-11-02','Departure','11-02 03:41','status_change','Departed','03:10','03:30');

-- 2. PA210: 6 Oct 23:55 delayed to 04:15 takes over the 7 Oct 23:55 row (expect took_over)
SELECT snap('PA210','2026-10-06','Departure','10-05 05:03','new','Expected','23:55','');
SELECT snap('PA210','2026-10-06','Departure','10-05 18:32','status_change','On Time','23:55','00:05');
SELECT snap('PA210','2026-10-07','Departure','10-06 05:03','new','Expected','23:55','');
SELECT snap('PA210','2026-10-07','Departure','10-06 18:03','status_change','Delayed','23:55','04:15');
SELECT snap('PA210','2026-10-06','Departure','10-06 20:32','status_change','Delayed','23:55','04:15');
SELECT snap('PA210','2026-10-06','Departure','10-06 21:12','dropped','Dropped',NULL,NULL);
SELECT snap('PA210','2026-10-07','Departure','10-07 00:41','status_change','Check-In','04:15','');
SELECT snap('PA210','2026-10-07','Departure','10-07 06:32','status_change','Departed','04:15','04:27');
SELECT snap('PA210','2026-10-08','Departure','10-07 05:03','new','Expected','23:55','');

-- 3. PK287: 23:55 -> 00:30, new 16 Sep row at 00:30 (expect new_row)
SELECT snap('PK287','2026-09-15','Departure','09-14 05:03','new','Expected','23:55','');
SELECT snap('PK287','2026-09-15','Departure','09-14 15:53','status_change','Delayed','23:55','00:30');
SELECT snap('PK287','2026-09-16','Departure','09-15 15:13','new','Expected','00:30','');
SELECT snap('PK287','2026-09-15','Departure','09-15 16:12','dropped','Dropped',NULL,NULL);
SELECT snap('PK287','2026-09-16','Departure','09-16 01:02','status_change','Departed','00:30','00:48');

-- 4. PK842 arrival: 23:05 -> 00:10/00:15, 25 Sep row created at 00:10 (expect new_row)
SELECT snap('PK842','2026-09-24','Arrival','09-23 05:02','new','Expected','23:05','');
SELECT snap('PK842','2026-09-24','Arrival','09-23 21:22','status_change','Delayed','23:05','00:10');
SELECT snap('PK842','2026-09-25','Arrival','09-24 05:02','new','Expected','00:10','');
SELECT snap('PK842','2026-09-24','Arrival','09-24 16:51','time_change','Delayed','23:05','00:15');
SELECT snap('PK842','2026-09-25','Arrival','09-24 17:03','time_change','Expected','00:15','');
SELECT snap('PK842','2026-09-24','Arrival','09-24 19:41','time_change','Delayed','23:05','23:48');
SELECT snap('PK842','2026-09-24','Arrival','09-24 21:51','dropped','Dropped',NULL,NULL);
SELECT snap('PK842','2026-09-25','Arrival','09-25 01:01','status_change','Arrived','00:15','23:51');

-- 5. PK842 arrival: 8 Oct 23:05 -> 00:50 shows on the 9 Oct 00:05 row (expect took_over)
SELECT snap('PK842','2026-10-08','Arrival','10-07 05:02','new','Expected','23:05','');
SELECT snap('PK842','2026-10-08','Arrival','10-07 17:52','status_change','Delayed','23:05','00:50');
SELECT snap('PK842','2026-10-09','Arrival','10-08 05:02','new','Expected','00:05','');
SELECT snap('PK842','2026-10-09','Arrival','10-08 17:12','status_change','Delayed','00:05','00:50');
SELECT snap('PK842','2026-10-08','Arrival','10-08 21:11','dropped','Dropped',NULL,NULL);
SELECT snap('PK842','2026-10-09','Arrival','10-09 01:41','status_change','Arrived','00:05','00:32');

-- 6. PK181: 28 Sep 21:50 -> 03:10, 29 Sep row at 03:10; drop comes late (expect new_row)
SELECT snap('PK181','2026-09-28','Departure','09-27 05:03','new','Expected','21:50','');
SELECT snap('PK181','2026-09-28','Departure','09-27 16:23','status_change','Delayed','21:50','03:10');
SELECT snap('PK181','2026-09-29','Departure','09-28 13:03','new','Expected','03:10','');
SELECT snap('PK181','2026-09-29','Departure','09-28 23:23','status_change','Check-In','03:10','');
SELECT snap('PK181','2026-09-28','Departure','09-29 02:10','dropped','Dropped',NULL,NULL);
SELECT snap('PK181','2026-09-29','Departure','09-29 03:31','status_change','Departed','03:10','03:10');

-- 7. PK181: 12 Oct, the scheduled time itself changes to 03:10; dropped once, back, dropped (new_row)
SELECT snap('PK181','2026-10-12','Departure','10-11 05:03','new','Expected','21:50','');
SELECT snap('PK181','2026-10-12','Departure','10-11 16:02','dropped','Dropped',NULL,NULL);
SELECT snap('PK181','2026-10-13','Departure','10-12 05:03','new','Expected','03:10','');
SELECT snap('PK181','2026-10-12','Departure','10-12 15:02','status_change','Expected','21:50','');
SELECT snap('PK181','2026-10-12','Departure','10-12 15:51','status_change','Delayed','03:10','03:10');
SELECT snap('PK181','2026-10-12','Departure','10-12 16:32','dropped','Dropped',NULL,NULL);

-- 8. PK261: delayed to 08:00 next morning, then gone; next day never shows 08:00 (expect none)
SELECT snap('PK261','2026-11-04','Departure','11-03 01:32','new','Expected','21:05','');
SELECT snap('PK261','2026-11-04','Departure','11-03 16:52','time_change','Expected','20:50','');
SELECT snap('PK261','2026-11-05','Departure','11-04 01:32','new','Expected','21:05','');
SELECT snap('PK261','2026-11-05','Departure','11-04 14:53','time_change','Expected','20:50','');
SELECT snap('PK261','2026-11-04','Departure','11-04 19:41','status_change','Delayed','20:50','08:00');
SELECT snap('PK261','2026-11-04','Departure','11-04 19:51','dropped','Dropped',NULL,NULL);
SELECT snap('PK261','2026-11-05','Departure','11-05 21:11','status_change','Departed','20:50','21:05');

-- 9. PK181 16 Sep: next-day 03:10 row appears 14 h after the drop, then is dropped (expect none)
SELECT snap('PK181','2026-09-16','Departure','09-15 05:03','new','Expected','22:00','');
SELECT snap('PK181','2026-09-16','Departure','09-15 14:43','status_change','Delayed','22:00','03:10');
SELECT snap('PK181','2026-09-16','Departure','09-15 15:23','dropped','Dropped',NULL,NULL);
SELECT snap('PK181','2026-09-17','Departure','09-16 05:03','new','Expected','03:10','');
SELECT snap('PK181','2026-09-17','Departure','09-16 13:33','dropped','Dropped',NULL,NULL);

-- 10. PK632: ordinary delay past midnight, stays on its own day (expect none)
SELECT snap('PK632','2026-11-05','Arrival','11-04 02:10','new','Expected','23:00','');
SELECT snap('PK632','2026-11-06','Arrival','11-05 02:10','new','Expected','20:55','');
SELECT snap('PK632','2026-11-05','Arrival','11-06 00:11','status_change','Delayed','23:00','00:45');
SELECT snap('PK632','2026-11-05','Arrival','11-06 01:00','status_change','Arrived','23:00','00:48');

-- 11. PK245: 2 Nov delayed to 07:00 stays on its day; 3 Nov row copies the delay, then dropped,
--     and there is no 4 Nov row (expect none)
SELECT snap('PK245','2026-11-02','Departure','11-01 05:04','new','Expected','22:05','');
SELECT snap('PK245','2026-11-02','Departure','11-01 16:42','dropped','Dropped',NULL,NULL);
SELECT snap('PK245','2026-11-03','Departure','11-02 05:03','new','Delayed','22:05','07:00');
SELECT snap('PK245','2026-11-02','Departure','11-02 15:51','status_change','Delayed','22:05','07:00');
SELECT snap('PK245','2026-11-03','Departure','11-02 21:33','dropped','Dropped',NULL,NULL);
SELECT snap('PK245','2026-11-02','Departure','11-03 07:20','status_change','Departed','22:05','07:05');

-- 12. FZ965: same-day delay 01:15 -> 18:30, copied to next day's row, which is dropped (expect none)
SELECT snap('FZ965','2026-11-06','Departure','11-05 02:12','new','Expected','01:15','');
SELECT snap('FZ965','2026-11-06','Departure','11-05 13:51','status_change','Delayed','01:15','18:30');
SELECT snap('FZ965','2026-11-07','Departure','11-06 02:11','new','Expected','01:15','');
SELECT snap('FZ965','2026-11-06','Departure','11-06 19:21','status_change','Departed','18:30','17:50');
SELECT snap('FZ965','2026-11-07','Departure','11-06 17:33','status_change','Delayed','01:15','18:30');
SELECT snap('FZ965','2026-11-07','Departure','11-07 13:50','dropped','Dropped',NULL,NULL);

-- 13. Daily flight cancelled one night: dropped, never showed a next-day time (expect none)
SELECT snap('XX100','2026-11-01','Departure','10-31 05:00','new','Expected','23:30','');
SELECT snap('XX100','2026-11-02','Departure','11-01 05:00','new','Expected','23:30','');
SELECT snap('XX100','2026-11-01','Departure','11-01 20:00','dropped','Dropped',NULL,NULL);

-- 14. Same rule on another airport's board must be ignored (expect none)
INSERT INTO public.origin_snapshots (flight_number, scheduled_date, source_airport, data_source, type, scraped_at, change_type, status, st, et) VALUES
 ('PK300','2026-11-01','Karachi','paa','Departure','2026-11-01 10:00+05','new','Delayed','22:00','03:00'),
 ('PK300','2026-11-01','Karachi','paa','Departure','2026-11-01 11:00+05','dropped','Dropped',NULL,NULL),
 ('PK300','2026-11-02','Karachi','paa','Departure','2026-11-01 10:30+05','new','Expected','03:00','');

-- 15. Hostile text in a time field is ignored, not cast (expect none)
SELECT snap('XX200','2026-11-01','Arrival','10-31 05:00','new','Expected','<b>22:00</b>','');
SELECT snap('XX200','2026-11-01','Arrival','11-01 10:00','status_change','Delayed','<b>22:00</b>','02:00');
SELECT snap('XX200','2026-11-01','Arrival','11-01 11:00','dropped','Dropped',NULL,NULL);
SELECT snap('XX200','2026-11-02','Arrival','11-01 10:30','new','Expected','02:00','');

-- 16. PA230: retimed 23:25 -> 01:40, next-day row seen 12 h later at 01:40, departed (expect new_row)
SELECT snap('PA230','2026-09-19','Departure','09-18 05:03','new','Expected','23:25','');
SELECT snap('PA230','2026-09-19','Departure','09-18 16:53','time_change','Expected','01:40','');
SELECT snap('PA230','2026-09-19','Departure','09-18 17:13','dropped','Dropped',NULL,NULL);
SELECT snap('PA230','2026-09-20','Departure','09-19 05:03','new','Expected','01:40','');
SELECT snap('PA230','2026-09-20','Departure','09-20 03:22','status_change','Departed','01:40','03:10');

-- 17. PK714: afternoon arrival 14:25 delayed to 00:05; the 11 Oct 00:10 row switches to 00:05
--     right after the drop and lands 23:46 (expect took_over)
SELECT snap('PK714','2026-10-10','Arrival','10-09 05:02','new','Expected','14:25','');
SELECT snap('PK714','2026-10-10','Arrival','10-09 17:42','status_change','Delayed','14:25','00:05');
SELECT snap('PK714','2026-10-11','Arrival','10-10 05:02','new','Expected','00:10','');
SELECT snap('PK714','2026-10-10','Arrival','10-10 13:41','dropped','Dropped',NULL,NULL);
SELECT snap('PK714','2026-10-11','Arrival','10-10 13:42','time_change','Expected','00:05','');
SELECT snap('PK714','2026-10-11','Arrival','10-11 00:21','status_change','Arrived','00:05','23:46');

-- 18. PK288 as the old collector saw it: the last change before the drop (ET 05:35) was missed,
--     so A only showed 05:04 and the next-day row never shows 05:04 (expect none: a safe miss)
SELECT snap('PK288','2026-11-04','Arrival','11-03 02:12','new','Expected','18:55','');
SELECT snap('PK288','2026-11-04','Arrival','11-03 16:52','time_change','Expected','18:45','');
SELECT snap('PK288','2026-11-05','Arrival','11-04 14:52','new','Expected','05:35','');
SELECT snap('PK288','2026-11-04','Arrival','11-04 18:11','status_change','Delayed','18:45','05:04');
SELECT snap('PK288','2026-11-04','Arrival','11-04 19:51','dropped','Dropped',NULL,NULL);
SELECT snap('PK288','2026-11-05','Arrival','11-05 06:41','status_change','Arrived','05:35','05:49');

\i sql/isb_flight_moves_view.sql

\echo '--- view output ---'
SELECT flight_number, type, from_date, from_st, to_date, moved_time, kind, to_first_st
FROM public.isb_flight_moves_view ORDER BY from_date, flight_number;

\echo '--- checks ---'
WITH expected(flight_number, from_date, to_date, moved_time, kind, to_first_st) AS (VALUES
  ('PK287','2026-09-15'::date,'2026-09-16'::date,'00:30','new_row','00:30'),
  ('PA230','2026-09-19','2026-09-20','01:40','new_row','01:40'),
  ('PK714','2026-10-10','2026-10-11','00:05','took_over','00:10'),
  ('PK842','2026-09-24','2026-09-25','00:10','new_row','00:10'),
  ('PK181','2026-09-28','2026-09-29','03:10','new_row','03:10'),
  ('PA210','2026-10-06','2026-10-07','04:15','took_over','23:55'),
  ('PK842','2026-10-08','2026-10-09','00:50','took_over','00:05'),
  ('PK181','2026-10-12','2026-10-13','03:10','new_row','03:10'),
  ('PK841','2026-11-01','2026-11-02','03:10','new_row','03:10')),
got AS (SELECT flight_number, from_date, to_date, moved_time, kind, to_first_st FROM public.isb_flight_moves_view)
SELECT CASE WHEN NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM got)
             AND NOT EXISTS (SELECT * FROM got EXCEPT SELECT * FROM expected)
            THEN 'PASS: exactly the 9 expected links, nothing else'
            ELSE 'FAIL: view output differs from expected' END AS result;
SELECT CASE WHEN count(*) = count(DISTINCT (flight_number, type, from_date))
             AND count(*) = count(DISTINCT (flight_number, type, to_date))
            THEN 'PASS: one link per flight-day on each side' ELSE 'FAIL: duplicate links' END AS result
FROM public.isb_flight_moves_view;
