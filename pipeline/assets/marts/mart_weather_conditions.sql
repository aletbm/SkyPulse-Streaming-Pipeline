/* @bruin

name: mart.mart_weather_conditions
type: pg.sql
connection: supabase
depends:
  - staging.stg_weather
  - staging.stg_weather_tumbling

materialization:
  type: table

@bruin */

-- Global weather conditions aggregated by region
-- Powers weather tile: temperature distribution + wind/precip trends

WITH regional_weather AS (
    SELECT
        region_name,
        grid_lat,
        grid_lon,
        temperature_c,
        apparent_temperature_c,
        humidity_pct,
        windspeed_ms,
        windgusts_ms,
        precipitation_mm,
        rain_mm,
        snowfall_cm,
        cloudcover_pct,
        visibility_m,
        pressure_hpa,
        weather_label,
        wind_severity,
        weathercode,
        snapshot_ts
    FROM staging.stg_weather
    WHERE region_name IS NOT NULL
),

region_agg AS (
    SELECT
        region_name,
        COUNT(*)                                        AS station_count,
        ROUND(AVG(temperature_c)::numeric, 2)           AS avg_temp_c,
        ROUND(MIN(temperature_c)::numeric, 2)           AS min_temp_c,
        ROUND(MAX(temperature_c)::numeric, 2)           AS max_temp_c,
        ROUND(AVG(humidity_pct)::numeric,  1)           AS avg_humidity_pct,
        ROUND(AVG(windspeed_ms)::numeric,  2)           AS avg_windspeed_ms,
        ROUND(MAX(windgusts_ms)::numeric,  2)           AS max_windgusts_ms,
        ROUND(SUM(precipitation_mm)::numeric, 2)        AS total_precip_mm,
        ROUND(SUM(snowfall_cm)::numeric, 2)             AS total_snowfall_cm,
        ROUND(AVG(visibility_m)::numeric,  0)           AS avg_visibility_m,
        ROUND(AVG(pressure_hpa)::numeric,  1)           AS avg_pressure_hpa,
        ROUND(AVG(cloudcover_pct)::numeric,1)           AS avg_cloudcover_pct,

        -- Most common weather condition
        MODE() WITHIN GROUP (ORDER BY weather_label)    AS dominant_weather,
        MODE() WITHIN GROUP (ORDER BY wind_severity)    AS dominant_wind_severity,

        -- Precipitation presence
        SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS stations_with_precip,
        SUM(CASE WHEN snowfall_cm      > 0 THEN 1 ELSE 0 END) AS stations_with_snow,

        MAX(snapshot_ts)                                AS latest_snapshot
    FROM regional_weather
    GROUP BY region_name
),

tumbling_last AS (
    SELECT DISTINCT ON (region_name)
        region_name,
        window_start,
        window_end,
        avg_temperature_c,
        avg_windspeed_ms,
        avg_windgusts_ms,
        avg_visibility_m,
        avg_humidity_pct,
        total_precip_mm,
        snapshot_count
    FROM staging.stg_weather_tumbling
    ORDER BY region_name, window_end DESC
),

hourly_temp_trend AS (
    -- Temperature trend from tumbling windows — last 6h
    SELECT
        region_name,
        DATE_TRUNC('hour', window_start)                AS hour_bucket,
        ROUND(AVG(avg_temperature_c)::numeric, 2)       AS avg_temp_c,
        ROUND(AVG(avg_windspeed_ms)::numeric,  2)       AS avg_wind_ms,
        ROUND(SUM(total_precip_mm)::numeric,   2)       AS total_precip_mm
    FROM staging.stg_weather_tumbling
    WHERE window_end >= NOW() - INTERVAL '6 hours'
    GROUP BY region_name, DATE_TRUNC('hour', window_start)
),

region_hourly_weather AS (
    SELECT
        region_name,
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'hour',          hour_bucket,
                'avg_temp_c',    avg_temp_c,
                'avg_wind_ms',   avg_wind_ms,
                'total_precip_mm', total_precip_mm
            ) ORDER BY hour_bucket
        ) AS hourly_trend
    FROM hourly_temp_trend
    GROUP BY region_name
)

SELECT
    ra.region_name,
    ra.station_count,
    ra.avg_temp_c,
    ra.min_temp_c,
    ra.max_temp_c,
    ra.avg_humidity_pct,
    ra.avg_windspeed_ms,
    ra.max_windgusts_ms,
    ra.total_precip_mm,
    ra.total_snowfall_cm,
    ra.avg_visibility_m,
    ra.avg_pressure_hpa,
    ra.avg_cloudcover_pct,
    ra.dominant_weather,
    ra.dominant_wind_severity,
    ra.stations_with_precip,
    ra.stations_with_snow,
    ra.latest_snapshot,

    -- From tumbling
    tl.window_start                                     AS last_window_start,
    tl.snapshot_count                                   AS last_window_snapshot_count,

    rhw.hourly_trend,

    -- Composite condition label for the tile
    CASE
        WHEN ra.total_precip_mm > 10               THEN 'Heavy precipitation'
        WHEN ra.total_snowfall_cm > 5              THEN 'Significant snowfall'
        WHEN ra.max_windgusts_ms >= 30             THEN 'High wind alert'
        WHEN ra.avg_visibility_m < 1000            THEN 'Low visibility'
        WHEN ra.avg_temp_c < -10                   THEN 'Extreme cold'
        WHEN ra.avg_temp_c > 40                    THEN 'Extreme heat'
        ELSE ra.dominant_weather
    END                                                 AS condition_summary,

    NOW()                                               AS refreshed_at

FROM region_agg ra
LEFT JOIN tumbling_last tl USING (region_name)
LEFT JOIN region_hourly_weather rhw USING (region_name)
ORDER BY ra.region_name
