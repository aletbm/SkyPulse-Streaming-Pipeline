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

        FLOOR(latitude * 2) / 2  AS grid_lat,
        FLOOR(longitude * 2) / 2 AS grid_lon

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
FROM expanded
ORDER BY
    grid_lat,
    grid_lon,
    airport_name;
