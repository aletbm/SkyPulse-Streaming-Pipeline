/* @bruin

name: staging.stg_airports
type: pg.sql
connection: supabase
depends:
  - public.raw_airports

materialization:
  type: table

@bruin */

-- CREATE INDEX IF NOT EXISTS idx_stg_airports_geog ON staging.stg_airports USING gist (geog);

SELECT
    airport_id,
    TRIM(name)                              AS airport_name,
    TRIM(city)                              AS city,
    TRIM(country)                           AS country,
    NULLIF(TRIM(iata), '')                  AS iata_code,
    NULLIF(TRIM(icao), '')                  AS icao_code,

    ROUND(CAST(latitude AS numeric),  6)    AS latitude,
    ROUND(CAST(longitude AS numeric), 6)    AS longitude,

    altitude_ft,
    ROUND(CAST(altitude_ft * 0.3048 AS numeric), 1) AS altitude_m,

    timezone_offset,
    NULLIF(TRIM(tz_database), '')           AS tz_database,
    dst,

    CAST(
        ST_SetSRID(
            ST_MakePoint(longitude, latitude),
            4326
        ) AS geography
    ) AS geog,

    CASE
        WHEN country IN (
            'United States','Canada','Mexico','Cuba','Jamaica',
            'Dominican Republic','Haiti','Guatemala','Honduras',
            'El Salvador','Nicaragua','Costa Rica','Panama',
            'Bahamas','Barbados','Trinidad and Tobago'
        ) THEN 'North America'
        WHEN country IN (
            'Brazil','Argentina','Colombia','Chile','Peru','Venezuela',
            'Ecuador','Bolivia','Paraguay','Uruguay','Guyana',
            'Suriname','French Guiana'
        ) THEN 'South America'
        WHEN country IN (
            'United Kingdom','France','Germany','Spain','Italy',
            'Netherlands','Belgium','Switzerland','Austria','Sweden',
            'Norway','Denmark','Finland','Portugal','Greece','Poland',
            'Czech Republic','Hungary','Romania','Ukraine','Russia',
            'Turkey','Ireland','Croatia','Serbia','Bulgaria','Slovakia',
            'Iceland','Luxembourg','Slovenia','Estonia','Latvia',
            'Lithuania','Albania','Bosnia and Herzegovina','Moldova',
            'North Macedonia','Montenegro','Kosovo','Belarus'
        ) THEN 'Europe'
        WHEN country IN (
            'China','Japan','South Korea','India','Indonesia',
            'Thailand','Vietnam','Malaysia','Philippines','Pakistan',
            'Bangladesh','Myanmar','Saudi Arabia','Iran','Iraq',
            'United Arab Emirates','Israel','Jordan','Kuwait',
            'Qatar','Bahrain','Oman','Yemen','Syria','Lebanon',
            'Afghanistan','Nepal','Sri Lanka','Cambodia','Laos',
            'Mongolia','Kazakhstan','Uzbekistan','Kyrgyzstan',
            'Tajikistan','Turkmenistan','Azerbaijan','Georgia',
            'Armenia','Singapore','Taiwan','Hong Kong'
        ) THEN 'Asia'
        WHEN country IN (
            'Nigeria','South Africa','Kenya','Ethiopia','Egypt',
            'Tanzania','Uganda','Ghana','Mozambique','Angola',
            'Cameroon','Ivory Coast','Niger','Burkina Faso','Mali',
            'Senegal','Guinea','Zambia','Zimbabwe','Somalia',
            'South Sudan','Sudan','Libya','Algeria','Morocco',
            'Tunisia','Madagascar','Rwanda','Burundi','Benin',
            'Togo','Sierra Leone','Eritrea','Djibouti','Comoros',
            'Mauritius','Seychelles','Cape Verde','Gambia'
        ) THEN 'Africa'
        WHEN country IN (
            'Australia','New Zealand','Papua New Guinea','Fiji',
            'Solomon Islands','Vanuatu','Samoa','Tonga','Kiribati',
            'Micronesia','Palau','Marshall Islands','Nauru','Tuvalu'
        ) THEN 'Oceania'
        ELSE 'Other'
    END                                     AS continent,

    airport_type,
    source,
    NOW()                                   AS _loaded_at

FROM public.raw_airports
WHERE
    latitude  IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude  BETWEEN -90  AND  90
    AND longitude BETWEEN -180 AND 180
    AND airport_type IN ('airport', 'unknown');
