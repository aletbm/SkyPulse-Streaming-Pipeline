/* @bruin

name: staging.stg_seismics
type: pg.sql
connection: supabase

materialization:
  type: table
  drop_cascade: true

columns:

  - name: id
    type: integer
    description: Unique identifier.
    nullable: false
    primary_key: true
    check:
      - name: not_null

  - name: mag
    type: float
    description: Earthquake magnitude.
    nullable: true

  - name: mag_type
    type: string
    description: Magnitude calculation method.
    nullable: true

  - name: place
    type: string
    description: Human-readable location description.
    nullable: true

  - name: tsunami
    type: boolean
    description: Tsunami alert.
    nullable: true

  - name: event_time
    type: timestamp
    description: Unix timestamp in milliseconds.
    nullable: true

  - name: event_type
    type: string
    description: Event type.
    nullable: true

  - name: title
    type: string
    description: Human-readable summary.
    nullable: true

  - name: sig
    type: integer
    description: Significance score 0–1000.
    nullable: true
    checks:
      - name: non_negative

  - name: lat
    type: float
    description: Latitude of the epicenter in decimal degrees.
    nullable: true

  - name: lon
    type: float
    description: Longitude of the epicenter in decimal degrees.
    nullable: true

  - name: depth
    type: float
    description: Depth of the hypocenter in kilometers below the surface.
    nullable: true

custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: SELECT COUNT(*) > 0 FROM public.seismics
    value: 1

@bruin */

SELECT
    id,
    ROUND(mag::numeric, 2)              AS mag,
    UPPER(TRIM(mag_type))               AS mag_type,
    TRIM(place)                         AS place,
    tsunami,
    event_time,
    event_type,
    title,
    sig,
    lat,
    lon,
    ROUND(depth::numeric, 2)            AS depth_km,

    -- Region extraction (after the comma)
    CASE
        WHEN place LIKE '%, %'
        THEN TRIM(SPLIT_PART(place, ',', -1))
        ELSE TRIM(place)
    END                                 AS region,

    -- Magnitude classification
    CASE
        WHEN mag < 2.0  THEN 'micro'
        WHEN mag < 4.0  THEN 'minor'
        WHEN mag < 5.0  THEN 'light'
        WHEN mag < 6.0  THEN 'moderate'
        WHEN mag < 7.0  THEN 'strong'
        WHEN mag < 8.0  THEN 'major'
        ELSE                 'great'
    END                                 AS magnitude_class,

    CAST(tsunami AS BOOLEAN)            AS is_tsunami_related,

    CAST(FLOOR(lat / 10) * 10 AS INTEGER) AS grid_lat,
    CAST(FLOOR(lon / 10) * 10 AS INTEGER) AS grid_lon,

    NOW()                               AS _loaded_at

FROM public.seismics
WHERE
    mag   IS NOT NULL
    AND lat BETWEEN -90  AND  90
    AND lon BETWEEN -180 AND 180
    AND event_time IS NOT NULL
