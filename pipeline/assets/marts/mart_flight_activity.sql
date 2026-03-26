/* @bruin

name: mart.mart_flight_activity
type: pg.sql
connection: supabase
depends:
  - staging.stg_airlines
  - staging.stg_flights_tumbling
  - intermediate.int_flights_enriched

materialization:
  type: table

@bruin */

-- Global flight activity by country and continent
-- Combines latest snapshot (stg_flights) with 5-min tumbling aggregates.
-- Enriched with OpenFlights airline metadata per country.

WITH latest_window AS (
    SELECT
        origin_country,
        window_start,
        window_end,
        flight_count,
        airborne_count,
        ROUND(avg_altitude_m::numeric,  1) AS avg_altitude_m,
        ROUND(avg_velocity_ms::numeric, 2) AS avg_velocity_ms
    FROM staging.stg_flights_tumbling
    WHERE window_end = (SELECT MAX(window_end) FROM staging.stg_flights_tumbling)
),

country_totals AS (
    SELECT
        origin_country,
        current_continent                                           AS continent,

        -- Airline reference from OpenFlights (same value per country — take first)
        MAX(airline_name)                                   AS airline_name,
        MAX(airline_iata)                                   AS airline_iata,
        MAX(airline_icao)                                   AS airline_icao,

        -- Live flight counts
        COUNT(*)                                                    AS live_flight_count,
        COUNT(*) FILTER (WHERE on_ground = FALSE)                   AS live_airborne_count,
        COUNT(*) FILTER (WHERE on_ground = TRUE)                    AS live_on_ground_count,

        -- Flight phase breakdown
        COUNT(*) FILTER (WHERE flight_phase = 'cruising')           AS cruising_count,
        COUNT(*) FILTER (WHERE flight_phase = 'climbing_descending') AS climb_descend_count,
        COUNT(*) FILTER (WHERE flight_phase = 'takeoff_landing')    AS takeoff_landing_count,

        -- Performance
        ROUND(AVG(baro_altitude_m)::numeric,  1)                   AS avg_altitude_m,
        ROUND(AVG(velocity_knots)::numeric,   1)                   AS avg_speed_knots,
        ROUND(MAX(velocity_knots)::numeric,   1)                   AS max_speed_knots,
        ROUND(MAX(baro_altitude_m)::numeric,  1)                   AS max_altitude_m,

        -- Airport proximity
        COUNT(*) FILTER (WHERE near_airport = TRUE)                 AS flights_near_airport,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_name)         AS most_common_airport,
        MODE() WITHIN GROUP (ORDER BY nearest_airport_city)         AS most_common_city,

        COUNT(DISTINCT grid_lat || '_' || grid_lon)                 AS active_grid_cells
    FROM intermediate.int_flights_enriched
    WHERE origin_country IS NOT NULL
    GROUP BY origin_country, current_continent
),

history_agg AS (
    SELECT
        origin_country,
        MAX(flight_count)                      AS peak_flights_2h,
        MIN(flight_count)                      AS min_flights_2h,
        ROUND(AVG(flight_count)::numeric, 1)   AS avg_flights_2h
    FROM staging.stg_flights_tumbling
    WHERE window_end >= NOW() - INTERVAL '2 hours'
    GROUP BY origin_country
)

SELECT
    ct.origin_country,
    ct.continent,

    -- Airline enrichment
    ct.airline_name,
    ct.airline_iata,
    ct.airline_icao,

    -- Live counts
    ct.live_flight_count,
    ct.live_airborne_count,
    ct.live_on_ground_count,
    ct.cruising_count,
    ct.climb_descend_count,
    ct.takeoff_landing_count,

    -- Performance
    ct.avg_altitude_m,
    ct.avg_speed_knots,
    ct.max_speed_knots,
    ct.max_altitude_m,

    -- Airport context
    ct.flights_near_airport,
    ROUND(
        ct.flights_near_airport::numeric / NULLIF(ct.live_flight_count, 0) * 100,
        1
    )                                                               AS pct_near_airport,
    ct.most_common_airport,
    ct.most_common_city,
    ct.active_grid_cells,

    -- Latest tumbling window
    lw.window_start                                                 AS last_window_start,
    lw.window_end                                                   AS last_window_end,
    lw.flight_count                                                 AS window_flight_count,
    lw.avg_velocity_ms                                              AS window_avg_velocity_ms,

    -- 2h trend
    ha.peak_flights_2h,
    ha.min_flights_2h,
    ha.avg_flights_2h,

    NOW()                                                           AS refreshed_at

FROM country_totals ct
LEFT JOIN latest_window lw USING (origin_country)
LEFT JOIN history_agg   ha USING (origin_country)
GROUP BY
    ct.origin_country, ct.continent,
    ct.airline_name, ct.airline_iata, ct.airline_icao,
    ct.live_flight_count, ct.live_airborne_count, ct.live_on_ground_count,
    ct.cruising_count, ct.climb_descend_count, ct.takeoff_landing_count,
    ct.avg_altitude_m, ct.avg_speed_knots, ct.max_speed_knots, ct.max_altitude_m,
    ct.flights_near_airport, ct.most_common_airport, ct.most_common_city,
    ct.active_grid_cells,
    lw.window_start, lw.window_end, lw.flight_count, lw.avg_velocity_ms,
    ha.peak_flights_2h, ha.min_flights_2h, ha.avg_flights_2h
ORDER BY ct.live_flight_count DESC
