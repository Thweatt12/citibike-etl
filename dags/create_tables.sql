-- Keep everything in public and be explicit
SET search_path = public, pg_catalog;

-- Staging table (typed; you load here directly)
CREATE TABLE IF NOT EXISTS public.stg_citibike (
    tripduration BIGINT CHECK (tripduration IS NULL OR tripduration >= 0),
    starttime TIMESTAMP,          -- or TIMESTAMPTZ if you want TZ awareness
    stoptime  TIMESTAMP,          -- (switch both if you change one)
    start_station_id INT CHECK (start_station_id IS NULL OR start_station_id > 0),
    start_station_name TEXT,
    start_station_latitude  DOUBLE PRECISION CHECK (start_station_latitude  IS NULL OR start_station_latitude  BETWEEN -90 AND 90),
    start_station_longitude DOUBLE PRECISION CHECK (start_station_longitude IS NULL OR start_station_longitude BETWEEN -180 AND 180),
    end_station_id INT CHECK (end_station_id IS NULL OR end_station_id > 0),
    end_station_name TEXT,
    end_station_latitude  DOUBLE PRECISION CHECK (end_station_latitude  IS NULL OR end_station_latitude  BETWEEN -90 AND 90),
    end_station_longitude DOUBLE PRECISION CHECK (end_station_longitude IS NULL OR end_station_longitude BETWEEN -180 AND 180),
    bikeid BIGINT,
    usertype TEXT,
    birth_year INT CHECK (birth_year IS NULL OR birth_year BETWEEN 1900 AND EXTRACT(YEAR FROM now())::INT),
    gender INT CHECK (gender IS NULL OR gender IN (0,1,2)) ,  -- matches CitiBike legacy
    ride_id TEXT,
    rideable_type TEXT

);

-- Dimension: stations
CREATE TABLE IF NOT EXISTS public.dim_station (
  station_id INT PRIMARY KEY,
  station_name TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION
);

-- Fact: daily metrics per station
CREATE TABLE IF NOT EXISTS public.fact_daily_station_trips (
  trip_date DATE,
  station_id INT,
  trips_started BIGINT,
  trips_ended BIGINT,
  avg_trip_duration_sec BIGINT,
  PRIMARY KEY (trip_date, station_id),
  FOREIGN KEY (station_id) REFERENCES public.dim_station(station_id)
);

-- Helpful indexes (idempotent)
CREATE INDEX IF NOT EXISTS idx_stg_starttime        ON public.stg_citibike (starttime);
CREATE INDEX IF NOT EXISTS idx_stg_stoptime         ON public.stg_citibike (stoptime);
CREATE INDEX IF NOT EXISTS idx_stg_start_station_id ON public.stg_citibike (start_station_id);
CREATE INDEX IF NOT EXISTS idx_stg_end_station_id   ON public.stg_citibike (end_station_id);

CREATE INDEX IF NOT EXISTS idx_fact_trip_date       ON public.fact_daily_station_trips (trip_date);
CREATE INDEX IF NOT EXISTS idx_fact_station         ON public.fact_daily_station_trips (station_id);
 