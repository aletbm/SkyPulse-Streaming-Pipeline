/* @bruin

name: staging.stg_weather
type: pg.sql
connection: supabase

materialization:
  type: table

columns:

  - name: latitude
    type: float
    description: WGS-84 latitude in decimal degrees.
    nullable: false
    primary_key: true
    check:
      - name: not_null

  - name: longitude
    type: float
    description: WGS-84 longitude in decimal degrees.
    nullable: false
    primary_key: true
    check:
      - name: not_null

  - name: region_name
    type: string
    description: Grid point name.
    nullable: True

  - name: elevation_m
    type: float
    description: Point altitude above sea level in meters.
    nullable: True
    check:
      - name: non_negative

  - name: weathercode
    type: integer
    description: WMO code.
    nullable: True

  - name: interval_s
    type: integer
    description: Model update interval in seconds.
    nullable: True
    check:
      - name: non_negative

  - name: windspeed_ms
    type: float
    description: Wind speed at 10m height in m/s.
    nullable: True
    check:
      - name: non_negative

  - name: winddirection_deg
    type: float
    description: Wind direction in degrees.
    nullable: True
    check:
      - name: non_negative

  - name: windgusts_ms
    type: float
    description: Maximum gust speed in m/s.
    nullable: True
    check:
      - name: non_negative

  - name: precipitation_mm
    type: float
    description: Total accumulated precipitation in mm.
    nullable: True
    check:
      - name: non_negative

  - name: rain_mm
    type: float
    description: Accumulated rainfall in mm.
    nullable: True
    check:
      - name: non_negative

  - name: snowfall_cm
    type: float
    description: Snowfall in cm.
    nullable: True
    check:
      - name: non_negative

  - name: showers_mm
    type: float
    description: Shower precipitation in mm.
    nullable: True
    check:
      - name: non_negative

  - name: snow_depth_m
    type: float
    description: Snow accumulated on the ground in meters.
    nullable: True
    check:
      - name: non_negative

  - name: cloudcover_pct
    type: float
    description: Total cloud cover in %.
    nullable: True
    check:
      - name: non_negative

  - name: cloudcover_low_pct
    type: float
    description: Low cloud cover in %.
    nullable: True
    check:
      - name: non_negative

  - name: temperature_c
    type: float
    description: Air temperature at 2m height in °C.
    nullable: True

  - name: apparent_temperature_c
    type: float
    description: Feels-like temperature in °C.
    nullable: True

  - name: humidity_pct
    type: float
    description: Relative humidity in %.
    nullable: True
    check:
      - name: non_negative

  - name: visibility_m
    type: float
    description: Horizontal visibility in meters.
    nullable: True
    check:
      - name: non_negative

  - name: pressure_hpa
    type: float
    description: Atmospheric pressure in hPa (hectopascals).
    nullable: True
    check:
      - name: non_negative

  - name: snapshot_ts
    type: timestamp
    description: Unix timestamp (seconds) of the moment the data was captured.
    nullable: True
    check:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: SELECT COUNT(*) > 0 FROM public.weather
    value: 1

@bruin */

SELECT
    latitude,
    longitude,
    TRIM(region_name)                   AS region_name,
    ROUND(elevation_m::numeric, 1)      AS elevation_m,
    weathercode,
    interval_s,
    ROUND(windspeed_ms::numeric,    2)  AS windspeed_ms,
    ROUND(winddirection_deg::numeric,1) AS winddirection_deg,
    ROUND(windgusts_ms::numeric,    2)  AS windgusts_ms,
    ROUND(precipitation_mm::numeric,2)  AS precipitation_mm,
    ROUND(rain_mm::numeric,         2)  AS rain_mm,
    ROUND(snowfall_cm::numeric,     2)  AS snowfall_cm,
    ROUND(showers_mm::numeric,      2)  AS showers_mm,
    ROUND(snow_depth_m::numeric,    2)  AS snow_depth_m,
    ROUND(cloudcover_pct::numeric,  1)  AS cloudcover_pct,
    ROUND(cloudcover_low_pct::numeric,1)AS cloudcover_low_pct,
    ROUND(temperature_c::numeric,   2)  AS temperature_c,
    ROUND(apparent_temperature_c::numeric,2) AS apparent_temperature_c,
    ROUND(humidity_pct::numeric,    1)  AS humidity_pct,
    ROUND(visibility_m::numeric,    0)  AS visibility_m,
    ROUND(pressure_hpa::numeric,    2)  AS pressure_hpa,
    snapshot_ts,

    -- WMO weathercode human label
    CASE weathercode
      WHEN 0  THEN 'Clear sky'
      WHEN 1  THEN 'Mainly clear'
      WHEN 2  THEN 'Partly cloudy'
      WHEN 3  THEN 'Overcast'
      WHEN 45 THEN 'Fog'
      WHEN 48 THEN 'Freezing fog'
      WHEN 51 THEN 'Light drizzle'
      WHEN 53 THEN 'Moderate drizzle'
      WHEN 55 THEN 'Dense drizzle'
      WHEN 56 THEN 'Light freezing drizzle'
      WHEN 57 THEN 'Heavy freezing drizzle'
      WHEN 61 THEN 'Slight rain'
      WHEN 63 THEN 'Moderate rain'
      WHEN 65 THEN 'Heavy rain'
      WHEN 66 THEN 'Light freezing rain'
      WHEN 67 THEN 'Heavy freezing rain'
      WHEN 71 THEN 'Slight snow'
      WHEN 73 THEN 'Moderate snow'
      WHEN 75 THEN 'Heavy snow'
      WHEN 77 THEN 'Snow grains'
      WHEN 80 THEN 'Slight showers'
      WHEN 81 THEN 'Moderate showers'
      WHEN 82 THEN 'Violent showers'
      WHEN 85 THEN 'Slight snow showers'
      WHEN 86 THEN 'Heavy snow showers'
      WHEN 95 THEN 'Thunderstorm'
      WHEN 96 THEN 'Thunderstorm w/ slight hail'
      WHEN 99 THEN 'Thunderstorm w/ heavy hail'
      ELSE         'Unknown'
    END                                 AS weather_label,

    -- Wind severity
    CASE
        WHEN windgusts_ms < 10  THEN 'calm'
        WHEN windgusts_ms < 20  THEN 'moderate'
        WHEN windgusts_ms < 30  THEN 'strong'
        WHEN windgusts_ms < 40  THEN 'gale'
        ELSE                         'storm'
    END                                 AS wind_severity,

    CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
    CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon,

    NOW()                               AS _loaded_at

FROM public.weather
WHERE
    latitude  IS NOT NULL
    AND longitude IS NOT NULL
    AND temperature_c BETWEEN -90 AND 60
