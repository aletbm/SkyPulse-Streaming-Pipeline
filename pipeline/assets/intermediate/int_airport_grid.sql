/* @bruin

name: intermediate.int_airport_grid
type: pg.sql
connection: supabase

depends:
  - staging.stg_airports

materialization:
  type: table

@bruin */

-- 🔥 Grid espacial para lookup O(1)

WITH base AS (
    SELECT
        airport_name,
        iata_code,
        icao_code,
        city,
        country,
        continent,
        tz_database,
        altitude_m,
        geog,
        latitude,
        longitude,
        CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
        CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon
    FROM staging.stg_airports
),

expanded AS (
    -- 🔥 expandimos a celdas vecinas (3x3)
    SELECT
        airport_name,
        iata_code,
        icao_code,
        city,
        country,
        continent,
        tz_database,
        altitude_m,
        geog,
        grid_lat + dlat AS grid_lat,
        grid_lon + dlon AS grid_lon
    FROM base
    CROSS JOIN (
        VALUES (-1), (0), (1)
    ) AS lat(dlat)
    CROSS JOIN (
        VALUES (-1), (0), (1)
    ) AS lon(dlon)
)

SELECT DISTINCT ON (grid_lat, grid_lon)
    grid_lat,
    grid_lon,
    airport_name,
    iata_code,
    icao_code,
    city,
    country,
    continent,
    tz_database,
    altitude_m,
    geog
FROM base
ORDER BY
    grid_lat,
    grid_lon,

    ST_Distance(
        geog,
        ST_SetSRID(
            ST_MakePoint(
                grid_lon + 5.0,
                grid_lat + 5.0
            ),
            4326
        )::geography
    ) ASC
