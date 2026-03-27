/* @bruin

name: mart.mart_seismic_activity
type: pg.sql
connection: supabase
depends:
  - staging.stg_seismics
  - staging.stg_seismics_tumbling

materialization:
  type: table

@bruin */

-- Seismic activity by region — categorical + temporal distribution
-- Powers the seismic tile in the dashboard

WITH recent_events AS (
    SELECT
        region,
        magnitude_class,
        mag,
        depth_km,
        is_tsunami_related,
        event_time,
        grid_lat,
        grid_lon
    FROM staging.stg_seismics
    WHERE event_time >= NOW() - INTERVAL '24 hours'
),

region_stats AS (
    SELECT
        region,
        COUNT(*)                                                AS event_count_24h,
        ROUND(AVG(mag)::numeric, 2)                             AS avg_magnitude,
        ROUND(MAX(mag)::numeric, 2)                             AS max_magnitude,
        ROUND(MIN(mag)::numeric, 2)                             AS min_magnitude,
        ROUND(AVG(depth_km)::numeric, 2)                        AS avg_depth_km,
        SUM(CASE WHEN is_tsunami_related THEN 1 ELSE 0 END)     AS tsunami_events,
        -- Distribution across magnitude classes
        SUM(CASE WHEN magnitude_class = 'micro'    THEN 1 ELSE 0 END) AS micro_count,
        SUM(CASE WHEN magnitude_class = 'minor'    THEN 1 ELSE 0 END) AS minor_count,
        SUM(CASE WHEN magnitude_class = 'light'    THEN 1 ELSE 0 END) AS light_count,
        SUM(CASE WHEN magnitude_class = 'moderate' THEN 1 ELSE 0 END) AS moderate_count,
        SUM(CASE WHEN magnitude_class = 'strong'   THEN 1 ELSE 0 END) AS strong_count,
        SUM(CASE WHEN magnitude_class IN ('major', 'great') THEN 1 ELSE 0 END) AS major_great_count
    FROM recent_events
    WHERE region IS NOT NULL AND region != ''
    GROUP BY region
),

hourly_trend AS (
    -- Events per hour in the last 24h for temporal tile
    SELECT
        DATE_TRUNC('hour', event_time)                          AS hour_bucket,
        magnitude_class,
        COUNT(*)                                                AS events,
        ROUND(MAX(mag)::numeric, 2)                             AS peak_mag,
        SUM(CASE WHEN is_tsunami_related THEN 1 ELSE 0 END)     AS tsunami_count
    FROM recent_events
    GROUP BY DATE_TRUNC('hour', event_time), magnitude_class
),

region_hourly AS (
    SELECT
        re.region,
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'hour',          ht.hour_bucket,
                'magnitude_class', ht.magnitude_class,
                'events',        ht.events,
                'peak_mag',      ht.peak_mag,
                'tsunami_count', ht.tsunami_count
            ) ORDER BY ht.hour_bucket
        ) AS hourly_trend
    FROM recent_events re
    JOIN hourly_trend ht
        ON DATE_TRUNC('hour', re.event_time) = ht.hour_bucket
        AND re.magnitude_class = ht.magnitude_class
    GROUP BY re.region
),

tumbling_recent AS (
    SELECT
        window_start,
        window_end,
        region,
        avg_magnitude,
        max_magnitude,
        event_count,
        tsunami_count,
        avg_depth
    FROM staging.stg_seismics_tumbling
    WHERE window_end >= NOW() - INTERVAL '3 hours'
)

-- Main mart: one row per region with full context
SELECT
    rs.region,
    rs.event_count_24h,
    rs.avg_magnitude,
    rs.max_magnitude,
    rs.min_magnitude,
    rs.avg_depth_km,
    rs.tsunami_events,
    rs.micro_count,
    rs.minor_count,
    rs.light_count,
    rs.moderate_count,
    rs.strong_count,
    rs.major_great_count,

    -- Risk flag
    CASE
        WHEN rs.max_magnitude >= 7.0 OR rs.tsunami_events > 0 THEN 'high'
        WHEN rs.max_magnitude >= 5.0 OR rs.event_count_24h   > 10 THEN 'medium'
        ELSE 'low'
    END                                                         AS risk_level,

    -- Most recent tumbling window stats for this region
    tr.window_start                                             AS last_window_start,
    tr.event_count                                              AS last_window_event_count,
    tr.max_magnitude                                            AS last_window_max_mag,
    rh.hourly_trend,

    NOW()                                                       AS refreshed_at

FROM region_stats rs
LEFT JOIN LATERAL (
    SELECT * FROM tumbling_recent
    WHERE region = rs.region
    ORDER BY window_end DESC
    LIMIT 1
) tr ON TRUE
LEFT JOIN region_hourly rh ON rh.region = rs.region
ORDER BY rs.max_magnitude DESC, rs.event_count_24h DESC
