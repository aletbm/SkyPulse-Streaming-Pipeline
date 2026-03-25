/* @bruin

name: mart.mart_flight_context
type: pg.sql
connection: supabase
depends:
  - intermediate.int_flights_enriched
  - staging.stg_seismics
  - staging.stg_weather
  - staging.stg_airports
  - staging.stg_flight_context

materialization:
  type: table

@bruin */

-- Cross-stream enriched grid mart — flights + seismic + weather + airport context
-- Each row = one 10-degree grid cell in the latest Flink window.
-- Enriched with nearest airports and flight phase breakdown from intermediate.int_flights_enriched.

WITH latest_context AS (
    SELECT *
    FROM staging.stg_flight_context
    WHERE window_end = (SELECT MAX(window_end) FROM staging.stg_flight_context)
),

-- Live flight detail per grid cell from intermediate.int_flights_enriched
live_flights_grid AS (
    SELECT
        grid_lat,
        grid_lon,
        COUNT(*)                                                        AS live_flight_count,
        COUNT(*) FILTER (WHERE flight_phase = 'cruising')               AS cruising_count,
        COUNT(*) FILTER (WHERE flight_phase = 'climbing_descending')    AS climb_descend_count,
        COUNT(*) FILTER (WHERE flight_phase = 'takeoff_landing')        AS takeoff_landing_count,
        COUNT(*) FILTER (WHERE near_airport = TRUE)                     AS near_airport_count,
        ROUND(AVG(velocity_knots)::numeric, 1)                          AS avg_speed_knots,
        ROUND(MAX(baro_altitude_m)::numeric, 1)                         AS max_altitude_m,

        -- Most active airport in this grid cell
        MODE() WITHIN GROUP (ORDER BY nearest_airport_name)             AS primary_airport_name,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_iata)             AS primary_airport_iata,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_city)             AS primary_airport_city,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_country)          AS primary_airport_country,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_timezone)         AS primary_airport_timezone,
        MODE() WITHIN GROUP (ORDER BY current_continent)                AS continent,

        -- Airline diversity in the cell
        COUNT(DISTINCT airline_iata)                            AS distinct_airlines,
        MODE() WITHIN GROUP (ORDER BY airline_name)             AS dominant_airline
    FROM intermediate.int_flights_enriched
    GROUP BY grid_lat, grid_lon
),

live_weather_grid AS (
    SELECT
        grid_lat,
        grid_lon,
        ROUND(AVG(temperature_c)::numeric,    2) AS avg_temp_c,
        ROUND(AVG(windgusts_ms)::numeric,     2) AS avg_windgusts_ms,
        ROUND(AVG(visibility_m)::numeric,     0) AS avg_visibility_m,
        ROUND(AVG(precipitation_mm)::numeric, 2) AS avg_precip_mm,
        MODE() WITHIN GROUP (ORDER BY wind_severity) AS wind_severity,
        MODE() WITHIN GROUP (ORDER BY weather_label) AS weather_label
    FROM staging.stg_weather
    GROUP BY grid_lat, grid_lon
),

recent_seismic_grid AS (
    SELECT
        grid_lat,
        grid_lon,
        COUNT(*)                                                AS eq_count_24h,
        ROUND(MAX(mag)::numeric, 2)                             AS max_mag_24h,
        SUM(CASE WHEN is_tsunami_related THEN 1 ELSE 0 END)     AS tsunami_count_24h,
        ROUND(AVG(mag)::numeric, 2)                             AS avg_mag_24h
    FROM staging.stg_seismics
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY grid_lat, grid_lon
)

SELECT
    fc.window_start,
    fc.window_end,
    fc.grid_lat,
    fc.grid_lon,

    -- Continent & airport context (from live intermediate.int_flights_enriched)
    lg.continent,
    lg.primary_airport_name,
    lg.primary_airport_iata,
    lg.primary_airport_city,
    lg.primary_airport_country,
    lg.primary_airport_timezone,
    lg.distinct_airlines,
    lg.dominant_airline,

    -- Flink window aggregates
    fc.flight_count,
    fc.airborne_count,
    fc.flight_count - fc.airborne_count         AS ground_count,
    ROUND(fc.avg_altitude_m::numeric, 1)        AS avg_altitude_m,

    -- Live flight phase breakdown
    COALESCE(lg.cruising_count,       0)        AS cruising_count,
    COALESCE(lg.climb_descend_count,  0)        AS climb_descend_count,
    COALESCE(lg.takeoff_landing_count,0)        AS takeoff_landing_count,
    COALESCE(lg.near_airport_count,   0)        AS near_airport_count,
    COALESCE(lg.avg_speed_knots,      0)        AS avg_speed_knots,
    COALESCE(lg.max_altitude_m,       0)        AS max_altitude_m,

    -- Flink seismic (within window)
    fc.nearby_eq_count,
    fc.max_eq_magnitude,
    fc.tsunami_count,

    -- 24h seismic context
    COALESCE(rs.eq_count_24h,      0)           AS eq_count_24h,
    COALESCE(rs.max_mag_24h,       0)           AS max_mag_24h,
    COALESCE(rs.tsunami_count_24h, 0)           AS tsunami_count_24h,

    -- Flink weather (within window)
    ROUND(fc.avg_temperature_c::numeric, 2)     AS avg_temperature_c,
    ROUND(fc.avg_windgusts_ms::numeric,  2)     AS avg_windgusts_ms,
    ROUND(fc.avg_visibility_m::numeric,  0)     AS avg_visibility_m,

    -- Live weather overlay
    lw.wind_severity,
    lw.weather_label,
    lw.avg_precip_mm,

    -- Composite risk score (0–100)
    LEAST(100, ROUND((
        LEAST(25, fc.airborne_count::float / NULLIF(fc.flight_count, 0) * 25)
        +
        CASE
            WHEN COALESCE(rs.max_mag_24h, 0) >= 7 THEN 35
            WHEN COALESCE(rs.max_mag_24h, 0) >= 5 THEN 20
            WHEN COALESCE(rs.max_mag_24h, 0) >= 3 THEN 10
            ELSE 0
        END
        +
        CASE lw.wind_severity
            WHEN 'storm'    THEN 25
            WHEN 'gale'     THEN 18
            WHEN 'strong'   THEN 10
            WHEN 'moderate' THEN 5
            ELSE 0
        END
        +
        CASE
            WHEN COALESCE(lw.avg_visibility_m, 99999) < 500  THEN 15
            WHEN COALESCE(lw.avg_visibility_m, 99999) < 1000 THEN 8
            WHEN COALESCE(lw.avg_visibility_m, 99999) < 3000 THEN 3
            ELSE 0
        END
    )::numeric, 1))                             AS risk_score,

    NOW()                                       AS refreshed_at

FROM latest_context fc
LEFT JOIN live_flights_grid  lg ON fc.grid_lat = lg.grid_lat AND fc.grid_lon = lg.grid_lon
LEFT JOIN live_weather_grid  lw ON fc.grid_lat = lw.grid_lat AND fc.grid_lon = lw.grid_lon
LEFT JOIN recent_seismic_grid rs ON fc.grid_lat = rs.grid_lat AND fc.grid_lon = rs.grid_lon
WHERE fc.flight_count > 0
ORDER BY fc.airborne_count DESC
