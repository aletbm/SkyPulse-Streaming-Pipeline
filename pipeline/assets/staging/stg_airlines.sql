/* @bruin

name: staging.stg_airlines
type: pg.sql
connection: supabase
depends:
  - public.raw_airlines

materialization:
  type: table

columns:
  - name: airline_id
    type: integer
    description: "Internal OpenFlights airline ID"
    primary_key: true

  - name: name
    type: string
    description: "Full airline name"

  - name: alias
    type: string
    description: "Alias or also-known-as name, null if unknown"

  - name: iata
    type: string
    description: "2-letter IATA code, null if not assigned"

  - name: icao
    type: string
    description: "3-letter ICAO code, null if not assigned"

  - name: callsign
    type: string
    description: "Radio telephony callsign"

  - name: country
    type: string
    description: "Country or territory where airline is incorporated"

  - name: active
    type: string
    description: "Y = currently active, N = defunct"

@bruin */

-- Cleaned airline reference data from OpenFlights
-- Only active airlines with at least one valid identifier (IATA or ICAO).

SELECT
    airline_id,
    TRIM(name)                      AS airline_name,
    NULLIF(TRIM(alias), '')         AS alias,
    NULLIF(UPPER(TRIM(iata)), '')   AS iata_code,
    NULLIF(UPPER(TRIM(icao)), '')   AS icao_code,
    NULLIF(TRIM(callsign), '')      AS callsign,
    TRIM(country)                   AS country,
    CASE active WHEN 'Y' THEN TRUE ELSE FALSE END AS is_active,
    NOW()                           AS _loaded_at

FROM public.raw_airlines
WHERE
    name IS NOT NULL
    AND (
        NULLIF(TRIM(iata), '') IS NOT NULL
        OR NULLIF(TRIM(icao), '') IS NOT NULL
    )
