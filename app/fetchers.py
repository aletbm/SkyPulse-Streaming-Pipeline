import pandas as pd
import streamlit as st
from database import query

TIME_REFRESH = 120


@st.cache_data(ttl=TIME_REFRESH // 2)
def fetch_flights() -> pd.DataFrame:
    df = query("""
        SELECT
            f.icao24,
            COALESCE(f.callsign, f.icao24) AS callsign,
            f.origin_country,
            f.longitude  AS lon,
            f.latitude   AS lat,
            f.baro_altitude,
            f.velocity,
            f.true_track,
            f.on_ground,
            f.time_position,
            COALESCE(e.flight_phase, 'unknown') AS flight_phase,
            COALESCE(e.nearest_airport_name, '')  AS nearest_airport,
            COALESCE(e.airline_name, '')           AS airline
        FROM public.flights f
        LEFT JOIN intermediate.int_flights_enriched e USING (icao24)
        WHERE f.latitude  IS NOT NULL
          AND f.longitude IS NOT NULL
          AND f.latitude  BETWEEN -90  AND 90
          AND f.longitude BETWEEN -180 AND 180
        LIMIT 15000
    """)
    if df.empty:
        return df
    df["baro_altitude"] = pd.to_numeric(df["baro_altitude"], errors="coerce").fillna(0)
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0)
    df["true_track"] = pd.to_numeric(df["true_track"], errors="coerce").fillna(0)
    df["angle"] = df["true_track"]
    df["color"] = df["on_ground"].map(
        lambda g: [80, 200, 120, 200] if g else [0, 229, 255, 220]
    )
    df["tooltip"] = df.apply(
        lambda r: (
            f"{r['callsign']} | {r['origin_country']} | "
            f"{int(r['baro_altitude'])}m | {int(r['velocity'])}m/s | "
            f"{r['flight_phase']}"
            + (
                f""" | {
                    pd.to_datetime(r["time_position"]).strftime("%Y-%m-%d %H:%M UTC")
                }"""
                if pd.notna(r["time_position"])
                else ""
            )
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=TIME_REFRESH)
def fetch_seismics() -> pd.DataFrame:
    df = query("""
        SELECT
            id, mag, place, lat, lon, depth,
            event_time, tsunami,
            CASE
                WHEN mag >= 7   THEN 'Major'
                WHEN mag >= 5   THEN 'Strong'
                WHEN mag >= 3   THEN 'Moderate'
                ELSE 'Minor'
            END AS mag_class
        FROM public.seismics
        WHERE event_time >= NOW() - INTERVAL '24 hours'
          AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY event_time DESC
        LIMIT 500
    """)
    if df.empty:
        return df
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce").fillna(0)
    df["radius"] = df["mag"].apply(
        lambda m: max(25_000, int((10 ** (-2.44 + 0.59 * m)) * 1_000 * 8))
    )
    df["color"] = df["mag"].apply(
        lambda m: (
            [255, 0, 85, 130]
            if m >= 7
            else [255, 107, 53, 90]
            if m >= 5
            else [255, 200, 50, 50]
            if m >= 3
            else [255, 220, 100, 10]
        )
    )
    df["tooltip"] = df.apply(
        lambda r: (
            f"M{r['mag']:.1f} — {r['place']} | depth {r['depth']:.0f}km"
            + (
                f" | {pd.to_datetime(r['event_time']).strftime('%Y-%m-%d %H:%M UTC')}"
                if pd.notna(r["event_time"])
                else ""
            )
            + (" ⚠ TSUNAMI" if r["tsunami"] else "")
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=TIME_REFRESH)
def fetch_weather() -> pd.DataFrame:
    df = query("""
        SELECT
            latitude AS lat, longitude AS lon,
            windspeed_ms, temperature_c,
            visibility_m, precipitation_mm
        FROM public.weather
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 2000
    """)
    if df.empty:
        return df
    df["windspeed_ms"] = pd.to_numeric(df["windspeed_ms"], errors="coerce").fillna(0)
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce").fillna(0)
    df["visibility_m"] = pd.to_numeric(df["visibility_m"], errors="coerce").fillna(
        10000
    )
    df["precipitation_mm"] = pd.to_numeric(
        df["precipitation_mm"], errors="coerce"
    ).fillna(0)
    df["weight"] = df["windspeed_ms"].clip(0, 40) / 40
    df["vis_weight"] = 1 - df["visibility_m"].clip(0, 10000) / 10000
    return df


@st.cache_data(ttl=TIME_REFRESH)
def fetch_kpis() -> dict:
    r = query("""
        SELECT
            (SELECT COUNT(*)
              FROM public.flights
              WHERE latitude IS NOT NULL)
              AS flights,
            (SELECT COUNT(*)
              FROM public.seismics
              WHERE event_time >= NOW() - INTERVAL '24 hours')
              AS seismics_24h,
            (SELECT MAX(mag)
              FROM public.seismics
              WHERE event_time >= NOW() - INTERVAL '24 hours')
              AS max_mag,
            (SELECT MAX(windgusts_ms)
              FROM public.weather)
              AS max_wind,
            (SELECT MAX(risk_score)
              FROM mart.mart_flight_context
              WHERE window_end >= NOW() - INTERVAL '10 minutes')
              AS risk_score
    """)
    row = r.iloc[0] if not r.empty else {}
    return {
        "flights": int(row.get("flights", 0) or 0),
        "seismics_24h": int(row.get("seismics_24h", 0) or 0),
        "max_mag": float(row.get("max_mag", 0) or 0),
        "max_wind": float(row.get("max_wind", 0) or 0),
        "risk_score": float(row.get("risk_score", 0) or 0),
    }


@st.cache_data(ttl=TIME_REFRESH)
def fetch_flight_trend() -> pd.DataFrame:
    return query("""
        SELECT
            window_start,
            SUM(flight_count)   AS flight_count,
            SUM(airborne_count) AS airborne_count
        FROM public.flights_tumbling
        WHERE window_start >= NOW() - INTERVAL '2 hours'
        GROUP BY window_start
        ORDER BY window_start
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_seismic_trend() -> pd.DataFrame:
    return query("""
        SELECT
            DATE_TRUNC('hour', event_time) AS hour,
            COUNT(*) AS events,
            MAX(mag)  AS max_mag
        FROM public.seismics
        WHERE event_time >= NOW() - INTERVAL '24 hours'
        GROUP BY 1
        ORDER BY 1
    """)


@st.cache_data(ttl=3600)
def fetch_active_routes() -> pd.DataFrame:
    df = query("""
        SELECT DISTINCT ON (r.origin_code, r.destination_code)
            r.origin_code,
            r.origin_airport_name,
            r.origin_city,
            r.origin_lat        AS src_lat,
            r.origin_lon        AS src_lon,
            r.destination_code,
            r.destination_airport_name,
            r.destination_city,
            r.destination_lat   AS tgt_lat,
            r.destination_lon   AS tgt_lon,
            r.distance_km,
            r.route_type,
            r.airline_name,
            COUNT(f.icao24) OVER (
                PARTITION BY r.origin_code, r.destination_code
            ) AS active_flights
        FROM staging.stg_routes r
        JOIN public.flights f
            ON UPPER(f.callsign) LIKE '%' || r.airline_code || '%'
           AND f.on_ground = FALSE
        WHERE
            r.origin_lat      IS NOT NULL
            AND r.origin_lon  IS NOT NULL
            AND r.destination_lat  IS NOT NULL
            AND r.destination_lon  IS NOT NULL
        ORDER BY r.origin_code, r.destination_code, active_flights DESC
        LIMIT 300
    """)
    if df.empty:
        return df
    for col in ["src_lat", "src_lon", "tgt_lat", "tgt_lon"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["src_lat", "src_lon", "tgt_lat", "tgt_lon"])
    df["tooltip"] = df.apply(
        lambda r: (
            f"{r['origin_code']} → {r['destination_code']} | "
            f"{r['airline_name']} | {r['distance_km']} km | {r['route_type']}"
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=3600)
def fetch_airports() -> pd.DataFrame:
    df = query("""
        SELECT
            airport_name,
            iata_code,
            city,
            country,
            latitude  AS lat,
            longitude AS lon
        FROM staging.stg_airports
        WHERE latitude  IS NOT NULL
          AND longitude IS NOT NULL
    """)
    if df.empty:
        return df
    df["tooltip"] = df.apply(
        lambda r: (
            f"""{r["airport_name"]}
            ({r["iata_code"] or "—"}) · {r["city"]},
            {r["country"]}"""
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=TIME_REFRESH)
def fetch_risk_grid() -> pd.DataFrame:
    return query("""
        SELECT
            grid_lat, grid_lon,
            risk_score,
            flight_count,
            primary_airport_city    AS city,
            continent,
            wind_severity,
            max_mag_24h
        FROM mart.mart_flight_context
        WHERE window_end = (SELECT MAX(window_end) FROM mart.mart_flight_context)
          AND risk_score > 0
        ORDER BY risk_score DESC
        LIMIT 500
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_altitude_scatter() -> pd.DataFrame:
    return query("""
        SELECT
            baro_altitude_m         AS altitude_m,
            velocity_ms             AS speed_ms,
            flight_phase,
            origin_country
        FROM intermediate.int_flights_enriched
        WHERE baro_altitude_m IS NOT NULL
          AND velocity_ms     IS NOT NULL
          AND baro_altitude_m > 0
          AND velocity_ms     > 0
          AND flight_phase    != 'unknown'
        ORDER BY RANDOM()
        LIMIT 2000
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_continent_breakdown() -> pd.DataFrame:
    return query("""
        SELECT
            COALESCE(current_continent, 'Other') AS continent,
            COUNT(*)                              AS flights,
            COUNT(*) FILTER (WHERE NOT on_ground) AS airborne,
            ROUND(AVG(baro_altitude_m)::numeric, 0) AS avg_alt_m
        FROM intermediate.int_flights_enriched
        WHERE current_continent IS NOT NULL
        GROUP BY 1
        ORDER BY flights DESC
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_top_countries_full() -> pd.DataFrame:
    return query("""
        SELECT
            origin_country,
            COUNT(*)                                        AS total_flights,
            SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) AS airborne,
            ROUND(AVG(baro_altitude)::numeric, 0)           AS avg_altitude_m,
            ROUND(AVG(velocity)::numeric, 1)                AS avg_speed_ms
        FROM public.flights
        WHERE origin_country IS NOT NULL
        GROUP BY origin_country
        ORDER BY total_flights DESC
        LIMIT 25
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_top_airlines() -> pd.DataFrame:
    return query("""
        SELECT
            COALESCE(e.airline_name, 'Unknown')             AS airline,
            COUNT(*)                                        AS flights,
            SUM(CASE WHEN NOT f.on_ground THEN 1 ELSE 0 END) AS airborne,
            ROUND(AVG(f.baro_altitude)::numeric, 0)         AS avg_altitude_m
        FROM public.flights f
        LEFT JOIN intermediate.int_flights_enriched e USING (icao24)
        WHERE f.latitude IS NOT NULL
        GROUP BY COALESCE(e.airline_name, 'Unknown')
        ORDER BY flights DESC
        LIMIT 20
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_top_airports() -> pd.DataFrame:
    return query("""
        SELECT
            COALESCE(e.nearest_airport_name, 'Unknown') AS airport,
            COALESCE(e.nearest_airport_iata, '—')       AS iata,
            COALESCE(e.nearest_airport_city, '')        AS city,
            COUNT(*)                                    AS nearby_flights
        FROM intermediate.int_flights_enriched e
        JOIN public.flights f USING (icao24)
        WHERE e.nearest_airport_name IS NOT NULL
          AND f.latitude IS NOT NULL
        GROUP BY e.nearest_airport_name, e.nearest_airport_iata, e.nearest_airport_city
        ORDER BY nearby_flights DESC
        LIMIT 20
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_seismic_by_region() -> pd.DataFrame:
    return query("""
        SELECT
            COALESCE(
                REGEXP_REPLACE(place, '^.*,\\s*', ''),
                place
            )                           AS region,
            COUNT(*)                    AS events,
            ROUND(MAX(mag)::numeric, 1) AS max_mag,
            ROUND(AVG(mag)::numeric, 2) AS avg_mag,
            SUM(tsunami)                AS tsunami_events
        FROM public.seismics
        WHERE event_time >= NOW() - INTERVAL '24 hours'
        GROUP BY 1
        ORDER BY max_mag DESC, events DESC
        LIMIT 25
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_flight_phase_breakdown() -> pd.DataFrame:
    return query("""
        SELECT
            COALESCE(e.flight_phase, 'unknown') AS phase,
            COUNT(*) AS flights
        FROM public.flights f
        LEFT JOIN intermediate.int_flights_enriched e USING (icao24)
        WHERE f.latitude IS NOT NULL
        GROUP BY 1
        ORDER BY flights DESC
    """)


@st.cache_data(ttl=TIME_REFRESH)
def fetch_country_flight_counts() -> pd.DataFrame:
    return query("""
        SELECT origin_country, COUNT(*) AS flights
        FROM public.flights
        WHERE origin_country IS NOT NULL
        GROUP BY origin_country
        ORDER BY flights DESC
        LIMIT 100
    """)
