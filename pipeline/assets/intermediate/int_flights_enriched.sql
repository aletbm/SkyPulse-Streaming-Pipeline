/* @bruin

name: intermediate.int_flights_enriched
type: pg.sql
connection: supabase

depends:
  - staging.stg_flights
  - staging.stg_airlines
  - intermediate.int_airport_grid

materialization:
  type: table

@bruin */

-- CREATE INDEX IF NOT EXISTS idx_airport_grid_lookup ON intermediate.int_airport_grid (grid_lat, grid_lon);
-- CREATE INDEX IF NOT EXISTS idx_stg_flights_grid ON staging.stg_flights (latitude, longitude);
-- CREATE INDEX IF NOT EXISTS idx_stg_flights_grid_computed ON staging.stg_flights ((FLOOR(latitude * 2) / 2), (FLOOR(longitude * 2) / 2));
-- CREATE INDEX IF NOT EXISTS idx_stg_airlines_country ON staging.stg_airlines (country) WHERE is_active = TRUE;

WITH flight_base AS (
    SELECT
        icao24,
        NULLIF(TRIM(callsign), '')          AS callsign,
        NULLIF(TRIM(origin_country), '')    AS origin_country,
        time_position,
        last_contact,
        longitude,
        latitude,
        geog,

        FLOOR(latitude * 2) / 2  AS grid_lat,
        FLOOR(longitude * 2) / 2 AS grid_lon,

        CASE
            WHEN baro_altitude IS NULL OR baro_altitude < 0 THEN NULL
            ELSE ROUND(CAST(baro_altitude AS numeric), 1)
        END AS baro_altitude_m,

        on_ground,

        CASE
            WHEN velocity IS NULL OR velocity < 0 THEN NULL
            ELSE ROUND(CAST(velocity AS numeric), 2)
        END AS velocity_ms,

        CASE
            WHEN velocity IS NULL OR velocity < 0 THEN NULL
            ELSE ROUND(CAST(velocity * 1.94384 AS numeric), 1)
        END AS velocity_knots,

        CASE
            WHEN on_ground = TRUE                      THEN 'on_ground'
            WHEN baro_altitude IS NULL                 THEN 'unknown'
            WHEN baro_altitude < 300                   THEN 'takeoff_landing'
            WHEN baro_altitude < 3000                  THEN 'climbing_descending'
            ELSE                                            'cruising'
        END                                 AS flight_phase,

        CASE
            WHEN latitude IS NULL OR longitude IS NULL THEN 'unknown'
            WHEN latitude  BETWEEN  23.5 AND  66.5
             AND longitude BETWEEN -130  AND  -60     THEN 'North America'
            WHEN latitude  BETWEEN  -56  AND   13
             AND longitude BETWEEN  -82  AND  -34     THEN 'South America'
            WHEN latitude  BETWEEN   36  AND   71
             AND longitude BETWEEN   -9  AND   40     THEN 'Europe'
            WHEN latitude  BETWEEN  -35  AND   37
             AND longitude BETWEEN   -17 AND   51     THEN 'Africa'
            WHEN latitude  BETWEEN   -10 AND   55
             AND longitude BETWEEN    26 AND  145     THEN 'Asia'
            WHEN latitude  BETWEEN  -47  AND  -10
             AND longitude BETWEEN   112 AND  154     THEN 'Oceania'
            ELSE 'Other'
        END                                 AS current_continent,

        true_track,
        category

    FROM staging.stg_flights
    WHERE
        icao24 IS NOT NULL
        AND latitude  BETWEEN -90  AND 90
        AND longitude BETWEEN -180 AND 180
),

country_airline AS (
    SELECT DISTINCT ON (country)
        country,
        airline_name,
        iata_code AS airline_iata,
        icao_code AS airline_icao
    FROM staging.stg_airlines
    WHERE is_active = TRUE
    ORDER BY country, airline_id
)

SELECT
    f.*,

    ag.airport_name        AS nearest_airport_name,
    ag.iata_code           AS nearest_airport_iata,
    ag.icao_code           AS nearest_airport_icao,
    ag.city                AS nearest_airport_city,
    ag.country             AS nearest_airport_country,
    ag.continent           AS nearest_airport_continent,
    ag.tz_database         AS nearest_airport_timezone,
    ag.altitude_m          AS nearest_airport_altitude_m,

    CASE
        WHEN ag.airport_name IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS near_airport,

    ca.airline_name,
    ca.airline_iata,
    ca.airline_icao,

    NOW() AS _loaded_at

FROM flight_base f
LEFT JOIN intermediate.int_airport_grid ag
    ON f.grid_lat = ag.grid_lat
   AND f.grid_lon = ag.grid_lon

LEFT JOIN country_airline ca
    ON f.origin_country = ca.country;
