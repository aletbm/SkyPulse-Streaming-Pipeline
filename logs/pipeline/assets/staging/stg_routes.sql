/* @bruin

name: staging.stg_routes
type: pg.sql
connection: supabase
depends:
  - public.raw_airports
  - public.raw_airlines
  - public.raw_routes
  - staging.stg_airports
  - staging.stg_airlines

materialization:
  type: table
  drop_cascade: true

columns:

  - name: airline
    type: string
    description: "2-letter IATA or 3-letter ICAO airline code"

  - name: airline_id
    type: integer
    description: "Internal OpenFlights airline ID, -1 if unknown"

  - name: src_airport
    type: string
    description: "3-letter IATA or 4-letter ICAO source airport code"

  - name: src_airport_id
    type: integer
    description: "Internal OpenFlights source airport ID, -1 if unknown"

  - name: dst_airport
    type: string
    description: "3-letter IATA or 4-letter ICAO destination airport code"

  - name: dst_airport_id
    type: integer
    description: "Internal OpenFlights destination airport ID, -1 if unknown"

  - name: codeshare
    type: string
    description: "Y if this is a codeshare flight, empty otherwise"

  - name: stops
    type: integer
    description: "Number of stops — 0 means direct flight"

  - name: equipment
    type: string
    description: "Space-separated IATA aircraft type codes"

@bruin */

-- Cleaned route reference data from OpenFlights
-- Enriched with airport and airline names via JOIN.
-- Direct routes only by default (stops = 0); change the filter if needed.

-- Cleaned route reference data from OpenFlights.
-- Enriched with airport and airline names.
--
-- Performance fix: removed OR conditions from JOINs.
-- Each airport code field (IATA or ICAO) gets its own
-- LEFT JOIN — PostgreSQL can use indexes on each.
-- COALESCE picks whichever lookup matched.

-- Before to run this asset you need to create the following indexes:
-- CREATE INDEX IF NOT EXISTS idx_stg_airports_iata ON staging.stg_airports (iata_code);
-- CREATE INDEX IF NOT EXISTS idx_stg_airports_icao ON staging.stg_airports (icao_code);
-- CREATE INDEX IF NOT EXISTS idx_stg_airlines_iata ON staging.stg_airlines (iata_code);
-- CREATE INDEX IF NOT EXISTS idx_stg_airlines_icao ON staging.stg_airlines (icao_code);

WITH airline_lookup AS (
    -- One row per airline, indexed by both IATA and ICAO
    SELECT
        iata_code   AS airline_match_key,
        airline_name,
        country     AS airline_country,
        is_active   AS airline_is_active
    FROM staging.stg_airlines
    WHERE iata_code IS NOT NULL

    UNION ALL

    SELECT
        icao_code,
        airline_name,
        country,
        is_active
    FROM staging.stg_airlines
    WHERE icao_code IS NOT NULL
      AND iata_code IS NULL  -- avoid duplicates when both codes exist
),

airport_lookup AS (
    -- One row per airport, indexed by both IATA and ICAO
    SELECT
        iata_code   AS airport_match_key,
        airport_name,
        city,
        country,
        continent,
        latitude,
        longitude,
        tz_database
    FROM staging.stg_airports
    WHERE iata_code IS NOT NULL

    UNION ALL

    SELECT
        icao_code,
        airport_name,
        city,
        country,
        continent,
        latitude,
        longitude,
        tz_database
    FROM staging.stg_airports
    WHERE icao_code IS NOT NULL
      AND iata_code IS NULL
)

SELECT
    r.airline                                   AS airline_code,
    al.airline_name,
    al.airline_country,
    al.airline_is_active,

    r.src_airport                               AS origin_code,
    src.airport_name                            AS origin_airport_name,
    src.city                                    AS origin_city,
    src.country                                 AS origin_country,
    src.continent                               AS origin_continent,
    src.latitude                                AS origin_lat,
    src.longitude                               AS origin_lon,
    src.tz_database                             AS origin_timezone,

    r.dst_airport                               AS destination_code,
    dst.airport_name                            AS destination_airport_name,
    dst.city                                    AS destination_city,
    dst.country                                 AS destination_country,
    dst.continent                               AS destination_continent,
    dst.latitude                                AS destination_lat,
    dst.longitude                               AS destination_lon,
    dst.tz_database                             AS destination_timezone,

    r.codeshare = 'Y'                           AS is_codeshare,
    r.stops,
    r.equipment                                 AS aircraft_codes,

    -- Great-circle distance in km (Haversine)
    CASE
        WHEN src.latitude IS NOT NULL AND dst.latitude IS NOT NULL
        THEN ROUND((
            6371 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS((dst.latitude  - src.latitude)  / 2)), 2) +
                COS(RADIANS(src.latitude)) * COS(RADIANS(dst.latitude)) *
                POWER(SIN(RADIANS((dst.longitude - src.longitude) / 2)), 2)
            ))
        )::numeric, 1)
        ELSE NULL
    END                                         AS distance_km,

    CASE
        WHEN src.continent = dst.continent THEN 'domestic/regional'
        ELSE                                    'international'
    END                                         AS route_type,

    NOW()                                       AS _loaded_at

FROM public.raw_routes r
LEFT JOIN airline_lookup al ON UPPER(r.airline)   = al.airline_match_key
LEFT JOIN airport_lookup src ON r.src_airport     = src.airport_match_key
LEFT JOIN airport_lookup dst ON r.dst_airport     = dst.airport_match_key
WHERE
    r.src_airport IS NOT NULL
    AND r.dst_airport IS NOT NULL
