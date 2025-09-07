-- ===== DIM STATION =====
WITH all_stations AS (
  SELECT start_station_id AS station_id,
         start_station_name AS station_name,
         start_station_latitude AS lat,
         start_station_longitude AS lon
  FROM public.stg_citibike
  UNION ALL
  SELECT end_station_id,
         end_station_name,
         end_station_latitude,
         end_station_longitude
  FROM public.stg_citibike
),
one_per_station AS (
  SELECT
    station_id,
    MAX(station_name) FILTER (WHERE station_name IS NOT NULL) AS station_name,
    MAX(lat)          FILTER (WHERE lat IS NOT NULL)          AS lat,
    MAX(lon)          FILTER (WHERE lon IS NOT NULL)          AS lon
  FROM all_stations
  WHERE station_id IS NOT NULL
  GROUP BY station_id
)
INSERT INTO public.dim_station (station_id, station_name, lat, lon)
SELECT station_id, station_name, lat, lon
FROM one_per_station
ON CONFLICT (station_id) DO UPDATE
SET station_name = COALESCE(EXCLUDED.station_name, public.dim_station.station_name),
    lat          = COALESCE(EXCLUDED.lat,          public.dim_station.lat),
    lon          = COALESCE(EXCLUDED.lon,          public.dim_station.lon);

-- ===== FACT DAILY STATION TRIPS =====
TRUNCATE public.fact_daily_station_trips;

WITH
started AS (
  SELECT
    date_trunc('day', starttime)::date AS trip_date,
    start_station_id                  AS station_id,
    COUNT(*)                          AS trips_started,
    AVG(tripduration)::BIGINT         AS avg_trip_duration_sec
  FROM public.stg_citibike
  WHERE starttime IS NOT NULL AND start_station_id IS NOT NULL
  GROUP BY 1, 2
),
ended AS (
  SELECT
    date_trunc('day', starttime)::date AS trip_date,
    end_station_id                     AS station_id,
    COUNT(*)                           AS trips_ended
  FROM public.stg_citibike
  WHERE starttime IS NOT NULL AND end_station_id IS NOT NULL
  GROUP BY 1, 2
)
INSERT INTO public.fact_daily_station_trips (
  trip_date, station_id, trips_started, trips_ended, avg_trip_duration_sec
)
SELECT
  s.trip_date,
  s.station_id,
  s.trips_started,
  COALESCE(e.trips_ended, 0) AS trips_ended,
  s.avg_trip_duration_sec
FROM started s
LEFT JOIN ended e
  ON e.trip_date = s.trip_date
 AND e.station_id = s.station_id;
