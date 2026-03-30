/* @bruin

name: staging.stg_flights
type: pg.sql
connection: supabase

materialization:
  type: table
  drop_cascade: true

columns:

  - name: icao24
    type: string
    description: Unique ICAO 24-bit address of the transponder in hex string representation.
    nullable: false
    primary_key: true
    checks:
      - name: not_null

  - name: callsign
    type: string
    description: Callsign of the vehicle.
    nullable: true

  - name: origin_country
    type: string
    description: Country name inferred from the ICAO 24-bit address.
    nullable: true

  - name: time_position
    type: timestamp
    description: Unix timestamp (seconds) for the last position update.
    nullable: true

  - name: last_contact
    type: timestamp
    description: Unix timestamp (seconds) for the last update in general.
    nullable: true

  - name: longitude
    type: float
    description: WGS-84 longitude in decimal degrees.
    nullable: true

  - name: latitude
    type: float
    description: WGS-84 latitude in decimal degrees.
    nullable: true

  - name: baro_altitude
    type: float
    description: Barometric altitude in meters.
    nullable: true

  - name: on_ground
    type: boolean
    description: Boolean value which indicates if the position was retrieved from a surface position report.
    nullable: true

  - name: velocity
    type: float
    description: Velocity over ground in m/s.
    nullable: True
    checks:
      - name: non_negative

  - name: true_track
    type: float
    description: True track in decimal degrees clockwise from north (north=0°).
    nullable: True
    checks:
      - name: non_negative

  - name: category
    type: integer
    description: Aircraft category.
    nullable: true
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: SELECT COUNT(*) > 0 FROM public.flights
    value: 1

@bruin */

SELECT
    icao24,
    NULLIF(TRIM(callsign), '')          AS callsign,
    NULLIF(TRIM(origin_country), '')    AS origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,

    CAST(
        ST_SetSRID(
            ST_MakePoint(longitude, latitude),
            4326
        ) AS geography
    ) AS geog,

    baro_altitude,
    on_ground,
    velocity,
    true_track,
    category,

    CASE
        WHEN baro_altitude IS NULL OR baro_altitude < 0 THEN NULL
        ELSE ROUND(CAST(baro_altitude AS numeric), 1)
    END                                 AS baro_altitude_m,

    CASE
        WHEN velocity IS NULL OR velocity < 0 THEN NULL
        ELSE ROUND(CAST(velocity * 1.94384 AS numeric), 1)
    END                                 AS velocity_knots,

    CASE
        WHEN latitude  IS NULL OR longitude IS NULL THEN 'unknown'
        WHEN latitude  BETWEEN  23.5 AND  66.5
         AND longitude BETWEEN -130  AND  -60   THEN 'North America'
        WHEN latitude  BETWEEN  -56  AND   13
         AND longitude BETWEEN  -82  AND  -34   THEN 'South America'
        WHEN latitude  BETWEEN   36  AND   71
         AND longitude BETWEEN   -9  AND   40   THEN 'Europe'
        WHEN latitude  BETWEEN  -35  AND   37
         AND longitude BETWEEN   -17 AND   51   THEN 'Africa'
        WHEN latitude  BETWEEN   -10 AND   55
         AND longitude BETWEEN    26 AND  145   THEN 'Asia'
        WHEN latitude  BETWEEN  -47  AND  -10
         AND longitude BETWEEN   112 AND  154   THEN 'Oceania'
        ELSE 'Other'
    END                                 AS continent,

    CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
    CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon,

    NOW()                               AS _loaded_at

FROM public.flights
WHERE
    icao24 IS NOT NULL
    AND latitude  BETWEEN -90  AND 90
    AND longitude BETWEEN -180 AND 180;
