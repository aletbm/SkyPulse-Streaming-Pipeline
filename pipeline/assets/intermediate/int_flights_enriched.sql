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

        CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
        CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon,

        baro_altitude_m,
        on_ground,

        CASE
            WHEN velocity IS NULL OR velocity < 0 THEN NULL
            ELSE ROUND(CAST(velocity AS numeric), 2)
        END AS velocity_ms,

        velocity_knots,

        CASE
            WHEN on_ground = TRUE                      THEN 'on_ground'
            WHEN baro_altitude IS NULL                 THEN 'unknown'
            WHEN baro_altitude < 300                   THEN 'takeoff_landing'
            WHEN baro_altitude < 3000                  THEN 'climbing_descending'
            ELSE                                            'cruising'
        END                                 AS flight_phase,

        continent                           AS current_continent,
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
        airline_iata,
        airline_icao
    FROM (
        SELECT
            f.origin_country            AS country,
            a.airline_name,
            a.iata_code                 AS airline_iata,
            a.icao_code                 AS airline_icao,
            COUNT(*)                    AS flight_count
        FROM flight_base f
        JOIN staging.stg_airlines a
            ON f.origin_country = a.country
            AND a.is_active = TRUE
        GROUP BY
            f.origin_country,
            a.airline_name,
            a.iata_code,
            a.icao_code
    ) ranked
    ORDER BY
        country,
        flight_count DESC,
        airline_name ASC  -- desempate determinístico
)

SELECT
    f.icao24,
    f.callsign,
    f.origin_country,
    f.time_position,
    f.last_contact,
    f.longitude,
    f.latitude,
    f.geog,
    f.baro_altitude_m,
    f.on_ground,
    f.velocity_ms,
    f.velocity_knots,
    f.flight_phase,
    f.current_continent,
    f.grid_lat,
    f.grid_lon,
    f.true_track,
    f.category,

    ag.airport_name        AS nearest_airport_name,
    ag.iata_code           AS nearest_airport_iata,
    ag.icao_code           AS nearest_airport_icao,
    ag.city                AS nearest_airport_city,
    ag.country             AS nearest_airport_country,
    ag.continent           AS nearest_airport_continent,
    ag.tz_database         AS nearest_airport_timezone,
    ag.altitude_m          AS nearest_airport_altitude_m,

    (ag.airport_name IS NOT NULL) AS near_airport,

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
