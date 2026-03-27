/* @bruin

name: intermediate.int_airport_grid
type: pg.sql
connection: supabase

depends:
  - staging.stg_airports

materialization:
  type: table

@bruin */

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
                CAST(FLOOR(longitude / 10) * 10 AS FLOAT) + 5.0,
                CAST(FLOOR(latitude  / 10) * 10 AS FLOAT) + 5.0
            ),
            4326
        )::geography
    ) ASC
