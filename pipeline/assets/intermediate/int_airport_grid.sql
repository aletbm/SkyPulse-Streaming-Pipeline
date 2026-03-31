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
        -- geog,
        latitude,
        longitude,
        CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
        CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon
    FROM staging.stg_airports
),

grid_centers AS (
    SELECT DISTINCT
        grid_lat,
        grid_lon,
        ST_Point(
            CAST(grid_lon AS float) + 5.0,
            CAST(grid_lat AS float) + 5.0,
            4326
        )::geography AS center_geog
    FROM base
)

SELECT DISTINCT ON (b.grid_lat, b.grid_lon)
    b.grid_lat,
    b.grid_lon,
    b.airport_name,
    b.iata_code,
    b.icao_code,
    b.city,
    b.country,
    b.continent,
    b.tz_database,
    b.altitude_m
FROM base b
JOIN grid_centers gc
    ON b.grid_lat = gc.grid_lat
   AND b.grid_lon = gc.grid_lon
ORDER BY
    b.grid_lat,
    b.grid_lon,
    b.geog <-> gc.center_geog
