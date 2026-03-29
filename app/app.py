"""
SkyPulse Dashboard — Real-time flight, seismic & weather intelligence.
Run: streamlit run dashboard/app.py
"""

import os
import time

import pandas as pd
import psycopg2
import psycopg2.extras
import pydeck as pdk
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SkyPulse",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    background-color: #080d14;
    color: #c8d8e8;
    font-family: 'Barlow', sans-serif;
}

/* Header */
.sp-header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 18px 0 10px 0;
    border-bottom: 1px solid #1a2a3a;
    margin-bottom: 20px;
}
.sp-logo {
    font-family: 'Share Tech Mono', monospace;
    font-size: 28px;
    color: #00e5ff;
    letter-spacing: 3px;
    text-shadow: 0 0 20px #00e5ff88;
}
.sp-tagline {
    font-size: 12px;
    color: #4a6a8a;
    letter-spacing: 2px;
    text-transform: uppercase;
    margin-top: 2px;
}
.sp-pulse {
    width: 10px; height: 10px;
    background: #00ff88;
    border-radius: 50%;
    box-shadow: 0 0 0 0 #00ff8866;
    animation: pulse 2s infinite;
    margin-left: auto;
    margin-right: 8px;
}
.sp-live {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #00ff88;
    letter-spacing: 2px;
}
@keyframes pulse {
    0%   { box-shadow: 0 0 0 0 #00ff8866; }
    70%  { box-shadow: 0 0 0 10px #00ff8800; }
    100% { box-shadow: 0 0 0 0 #00ff8800; }
}

/* KPI cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 20px;
}
.kpi-card {
    background: #0d1825;
    border: 1px solid #1a2e42;
    border-radius: 8px;
    padding: 16px 20px;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}
.kpi-card.flights::before  { background: linear-gradient(90deg, #00e5ff, #0077ff); }
.kpi-card.seismic::before  { background: linear-gradient(90deg, #ff6b35, #ff0055); }
.kpi-card.weather::before  { background: linear-gradient(90deg, #7b61ff, #00e5ff); }
.kpi-card.risk::before     { background: linear-gradient(90deg, #ff0055, #ff6b35); }

.kpi-label {
    font-size: 10px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4a6a8a;
    margin-bottom: 8px;
}
.kpi-value {
    font-family: 'Share Tech Mono', monospace;
    font-size: 32px;
    font-weight: 700;
    line-height: 1;
}
.kpi-value.flights  { color: #00e5ff; text-shadow: 0 0 20px #00e5ff55; }
.kpi-value.seismic  { color: #ff6b35; text-shadow: 0 0 20px #ff6b3555; }
.kpi-value.weather  { color: #7b61ff; text-shadow: 0 0 20px #7b61ff55; }
.kpi-value.risk     { color: #ff0055; text-shadow: 0 0 20px #ff005555; }
.kpi-sub {
    font-size: 11px;
    color: #4a6a8a;
    margin-top: 4px;
}

/* Section labels */
.section-label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 10px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #4a6a8a;
    margin-bottom: 8px;
    border-left: 2px solid #1a2e42;
    padding-left: 10px;
}

/* Toggle buttons */
div[data-testid="stCheckbox"] label {
    font-size: 12px !important;
    letter-spacing: 1px;
    color: #8aa0b8 !important;
}

/* Metric tweaks */
div[data-testid="stMetric"] {
    background: #0d1825;
    border: 1px solid #1a2e42;
    border-radius: 8px;
    padding: 12px 16px;
}

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #080d14; }
::-webkit-scrollbar-thumb { background: #1a2e42; border-radius: 4px; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #0d1825;
    border-right: 1px solid #1a2e42;
}

/* Selectbox / slider */
div[data-testid="stSlider"] label { font-size: 11px; color: #4a6a8a; }

/* Timestamp */
.sp-ts {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #2a4a6a;
    text-align: right;
    padding-top: 4px;
}
</style>
""",
    unsafe_allow_html=True,
)


# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT", 5432)),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        dbname=os.getenv("SUPABASE_DATABASE", "postgres"),
        sslmode="require",
        connect_timeout=10,
    )


def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception:
        conn.rollback()
        raise


# ── Data fetchers ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=15)
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
    # pydeck IconLayer needs angle in degrees, clockwise from north
    df["angle"] = df["true_track"]
    df["color"] = df["on_ground"].map(
        lambda g: [80, 200, 120, 200] if g else [0, 229, 255, 220]
    )
    df["tooltip"] = df.apply(
        lambda r: (
            f"{r['callsign']} | {r['origin_country']} | "
            f"{int(r['baro_altitude'])}m | {int(r['velocity'])}m/s | "
            f"{r['flight_phase']}"
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=30)
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
    # radius scales with magnitude (exponential feel)
    df["radius"] = df["mag"].apply(lambda m: max(30_000, int(20_000 * (2**m))))
    df["color"] = df["mag"].apply(
        lambda m: (
            [255, 0, 85, 180]
            if m >= 7
            else [255, 107, 53, 160]
            if m >= 5
            else [255, 200, 50, 120]
            if m >= 3
            else [255, 220, 100, 80]
        )
    )
    df["tooltip"] = df.apply(
        lambda r: (
            f"M{r['mag']:.1f} — {r['place']} | depth {r['depth']:.0f}km"
            + (" ⚠ TSUNAMI" if r["tsunami"] else "")
        ),
        axis=1,
    )
    return df


@st.cache_data(ttl=60)
def fetch_weather() -> pd.DataFrame:
    df = query("""
        SELECT
            latitude AS lat, longitude AS lon,
            region_name, temperature_c, windspeed_ms,
            windgusts_ms, visibility_m, weathercode,
            precipitation_mm
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
    df["weight"] = df["windspeed_ms"].clip(0, 40) / 40
    return df


@st.cache_data(ttl=30)
def fetch_kpis() -> dict:
    kpis = {}
    r = query("SELECT COUNT(*) AS n FROM public.flights WHERE latitude IS NOT NULL")
    kpis["flights"] = int(r.iloc[0]["n"]) if not r.empty else 0

    r = query("""
        SELECT COUNT(*) AS n FROM public.seismics
        WHERE event_time >= NOW() - INTERVAL '24 hours'
    """)
    kpis["seismics_24h"] = int(r.iloc[0]["n"]) if not r.empty else 0

    r = query(
        """SELECT MAX(mag) AS m FROM public.seismics
        WHERE event_time >= NOW() - INTERVAL '24 hours'"""
    )
    kpis["max_mag"] = float(r.iloc[0]["m"]) if not r.empty and r.iloc[0]["m"] else 0.0

    r = query("SELECT MAX(windgusts_ms) AS w FROM public.weather")
    kpis["max_wind"] = float(r.iloc[0]["w"]) if not r.empty and r.iloc[0]["w"] else 0.0

    r = query(
        """SELECT MAX(risk_score) AS rs FROM mart.mart_flight_context
        WHERE window_end >= NOW() - INTERVAL '10 minutes'"""
    )
    kpis["risk_score"] = (
        float(r.iloc[0]["rs"]) if not r.empty and r.iloc[0]["rs"] else 0.0
    )

    return kpis


@st.cache_data(ttl=30)
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


@st.cache_data(ttl=30)
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


@st.cache_data(ttl=60)
def fetch_top_countries() -> pd.DataFrame:
    return query("""
        SELECT origin_country, COUNT(*) AS flights
        FROM public.flights
        WHERE origin_country IS NOT NULL
        GROUP BY origin_country
        ORDER BY flights DESC
        LIMIT 12
    """)


# ── Airplane icon (SVG data URI) ──────────────────────────────────────────────
PLANE_ICON_URL = "./assets/images/aircraft.png"

QUAKE_ICON_URL = (
    "data:image/svg+xml;charset=utf-8,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='%23ff6b35'%3E"
    "%3Cpath d='M4 10.8L7 9l2 3 3-7 2.5 5H20v2h-6.5l-1-2L10 17l-2-3-1.5.8z'/%3E"
    "%3C/svg%3E"
)


# ── Build pydeck layers ───────────────────────────────────────────────────────
def build_map(
    flights: pd.DataFrame,
    seismics: pd.DataFrame,
    weather: pd.DataFrame,
    show_flights: bool,
    show_seismics: bool,
    show_weather: bool,
) -> pdk.Deck:

    layers = []

    if show_weather and not weather.empty:
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                data=weather,
                get_position=["lon", "lat"],
                get_weight="weight",
                radius_pixels=60,
                intensity=1.2,
                threshold=0.05,
                color_range=[
                    [0, 50, 150, 0],
                    [0, 100, 200, 80],
                    [100, 0, 200, 130],
                    [180, 0, 255, 180],
                ],
                pickable=False,
            )
        )

    if show_seismics and not seismics.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=seismics,
                get_position=["lon", "lat"],
                get_radius="radius",
                get_fill_color="color",
                get_line_color=[255, 100, 50, 200],
                line_width_min_pixels=1,
                stroked=True,
                filled=True,
                pickable=True,
            )
        )

    if show_flights and not flights.empty:
        # Ground aircraft — small dots
        on_ground = flights[flights["on_ground"]]
        if not on_ground.empty:
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    data=on_ground,
                    get_position=["lon", "lat"],
                    get_radius=8000,
                    get_fill_color=[80, 200, 120, 160],
                    pickable=True,
                )
            )

        # Airborne — icon layer with rotation
        airborne = flights[not flights["on_ground"]]
        if not airborne.empty:
            airborne = airborne.copy()
            airborne["icon_data"] = airborne.apply(
                lambda _: {
                    "url": PLANE_ICON_URL,
                    "width": 128,
                    "height": 128,
                    "anchorY": 64,
                },
                axis=1,
            )
            layers.append(
                pdk.Layer(
                    "IconLayer",
                    data=airborne,
                    get_icon="icon_data",
                    get_position=["lon", "lat"],
                    get_angle="angle",
                    get_size=18,
                    size_scale=1,
                    pickable=True,
                    get_color=[0, 229, 255, 220],
                )
            )

    view = pdk.ViewState(
        latitude=20,
        longitude=0,
        zoom=1.8,
        pitch=30,
        bearing=0,
    )

    tooltip = {
        "html": """<div style='font-family:monospace;font-size:12px;background:#0d1825;
        border:1px solid #1a2e42;padding:8px 12px;border-radius:6px;color:#c8d8e8'>
        {tooltip}</div>""",
        "style": {"background": "transparent", "border": "none"},
    }

    return pdk.Deck(
        layers=layers,
        initial_view_state=view,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        parameters={"depthTest": False},
    )


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙ Controls")
    show_flights = st.checkbox("✈ Flights", value=True)
    show_seismics = st.checkbox("🌍 Seismics", value=True)
    show_weather = st.checkbox("🌩 Weather", value=True)
    st.divider()
    refresh_rate = st.select_slider(
        "Auto-refresh (seconds)",
        options=[10, 15, 30, 60, 120],
        value=15,
    )
    st.divider()
    st.markdown(
        "<div style='font-size:11px;color:#2a4a6a;font-family:monospace'>"
        "SkyPulse v1.0<br>DE Zoomcamp 2026</div>",
        unsafe_allow_html=True,
    )

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    """
<div class="sp-header">
    <div>
        <div class="sp-logo">✈ SKYPULSE</div>
        <div class="sp-tagline">Real-time intelligence for a restless planet</div>
    </div>
    <div class="sp-pulse"></div>
    <div class="sp-live">LIVE</div>
</div>
""",
    unsafe_allow_html=True,
)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner(""):
    try:
        kpis = fetch_kpis()
        flights = fetch_flights()
        seismics = fetch_seismics()
        weather = fetch_weather()
        trend_df = fetch_flight_trend()
        seis_trend = fetch_seismic_trend()
        top_countries = fetch_top_countries()
        load_ok = True
    except Exception as e:
        st.error(f"Database connection error: {e}")
        load_ok = False

if not load_ok:
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
airborne_count = len(flights[not flights["on_ground"]]) if not flights.empty else 0
risk_color = "risk" if kpis["risk_score"] > 60 else "weather"

st.markdown(
    f"""
<div class="kpi-grid">
    <div class="kpi-card flights">
        <div class="kpi-label">Active Flights</div>
        <div class="kpi-value flights">{kpis["flights"]:,}</div>
        <div class="kpi-sub">{airborne_count:,} airborne now</div>
    </div>
    <div class="kpi-card seismic">
        <div class="kpi-label">Seismic Events (24h)</div>
        <div class="kpi-value seismic">{kpis["seismics_24h"]}</div>
        <div class="kpi-sub">Max M{kpis["max_mag"]:.1f}</div>
    </div>
    <div class="kpi-card weather">
        <div class="kpi-label">Max Wind Gusts</div>
        <div class="kpi-value weather">{kpis["max_wind"]:.0f}
        <span style="font-size:16px"> m/s</span></div>
        <div class="kpi-sub">
        {"⚠ STORM ALERT" if kpis["max_wind"] > 25 else "Normal conditions"}
        </div>
    </div>
    <div class="kpi-card {risk_color}">
        <div class="kpi-label">Peak Risk Score</div>
        <div class="kpi-value {risk_color}">
        {kpis["risk_score"]:.0f}<span style="font-size:16px">/100</span>
        </div>
        <div class="kpi-sub">
        {
        "🔴 HIGH RISK"
        if kpis["risk_score"] > 60
        else "🟡 ELEVATED"
        if kpis["risk_score"] > 30
        else "🟢 Normal"
    }
         </div>
    </div>
</div>
""",
    unsafe_allow_html=True,
)

# ── Map ───────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="section-label">GLOBAL SITUATION MAP</div>', unsafe_allow_html=True
)

deck = build_map(flights, seismics, weather, show_flights, show_seismics, show_weather)
st.pydeck_chart(deck, use_container_width=True, height=520)

# Legend
leg_col1, leg_col2, leg_col3, leg_col4 = st.columns(4)
with leg_col1:
    st.markdown(
        """<span style='color:#00e5ff;font-size:12px;font-family:monospace'>
        ✈ Airborne flight</span>""",
        unsafe_allow_html=True,
    )
with leg_col2:
    st.markdown(
        """<span style='color:#50c878;font-size:12px;font-family:monospace'>
        ● On ground</span>""",
        unsafe_allow_html=True,
    )
with leg_col3:
    st.markdown(
        """<span style='color:#ff6b35;font-size:12px;font-family:monospace'>
        ◉ Seismic event (size = magnitude)</span>""",
        unsafe_allow_html=True,
    )
with leg_col4:
    st.markdown(
        """<span style='color:#7b61ff;font-size:12px;font-family:monospace'>
        ▓ Wind heatmap</span>""",
        unsafe_allow_html=True,
    )

st.divider()

# ── Charts row ────────────────────────────────────────────────────────────────
col_left, col_mid, col_right = st.columns([2, 2, 1.5])

with col_left:
    st.markdown(
        '<div class="section-label">FLIGHT ACTIVITY — LAST 2 HOURS</div>',
        unsafe_allow_html=True,
    )
    if not trend_df.empty and "window_start" in trend_df.columns:
        trend_df["window_start"] = pd.to_datetime(trend_df["window_start"])
        trend_df = trend_df.sort_values("window_start")
        st.area_chart(
            trend_df.set_index("window_start")[["flight_count", "airborne_count"]],
            color=["#00e5ff", "#0077ff"],
            height=220,
        )
    else:
        st.info("Waiting for tumbling window data...")

with col_mid:
    st.markdown(
        '<div class="section-label">SEISMIC EVENTS — LAST 24 HOURS</div>',
        unsafe_allow_html=True,
    )
    if not seis_trend.empty and "hour" in seis_trend.columns:
        seis_trend["hour"] = pd.to_datetime(seis_trend["hour"])
        seis_trend = seis_trend.sort_values("hour")
        st.bar_chart(
            seis_trend.set_index("hour")["events"],
            color="#ff6b35",
            height=220,
        )
    else:
        st.info("No seismic data in the last 24 hours.")

with col_right:
    st.markdown(
        '<div class="section-label">TOP COUNTRIES BY FLIGHTS</div>',
        unsafe_allow_html=True,
    )
    if not top_countries.empty:
        st.dataframe(
            top_countries.rename(columns={"origin_country": "Country", "flights": "✈"}),
            hide_index=True,
            height=220,
            use_container_width=True,
        )

st.divider()

# ── Recent seismics table ─────────────────────────────────────────────────────
col_s, col_f = st.columns(2)

with col_s:
    st.markdown(
        '<div class="section-label">RECENT SEISMIC EVENTS</div>', unsafe_allow_html=True
    )
    if not seismics.empty:
        display_seis = seismics[
            ["mag", "mag_class", "place", "depth", "event_time", "tsunami"]
        ].copy()
        display_seis["tsunami"] = display_seis["tsunami"].map(
            lambda t: "⚠ YES" if t else ""
        )
        display_seis.columns = [
            "Mag",
            "Class",
            "Location",
            "Depth (km)",
            "Time",
            "Tsunami",
        ]
        display_seis["Time"] = pd.to_datetime(display_seis["Time"]).dt.strftime(
            "%H:%M:%S"
        )
        st.dataframe(
            display_seis.head(10), hide_index=True, use_container_width=True, height=300
        )
    else:
        st.info("No seismic events in the last 24 hours.")

with col_f:
    st.markdown(
        '<div class="section-label">FLIGHT SAMPLE — LIVE POSITIONS</div>',
        unsafe_allow_html=True,
    )
    if not flights.empty:
        display_flights = flights[
            [
                "callsign",
                "origin_country",
                "flight_phase",
                "baro_altitude",
                "velocity",
                "nearest_airport",
            ]
        ].copy()
        display_flights["baro_altitude"] = display_flights["baro_altitude"].apply(
            lambda x: f"{x:.0f} m"
        )
        display_flights["velocity"] = display_flights["velocity"].apply(
            lambda x: f"{x:.0f} m/s"
        )
        display_flights.columns = [
            "Callsign",
            "Country",
            "Phase",
            "Altitude",
            "Speed",
            "Near Airport",
        ]
        st.dataframe(
            display_flights[display_flights["Phase"] != "on_ground"].head(10),
            hide_index=True,
            use_container_width=True,
            height=300,
        )
    else:
        st.info("No flight data available.")

# ── Timestamp + auto-refresh ──────────────────────────────────────────────────
ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S UTC")
st.markdown(
    f'<div class="sp-ts">Last updated: {ts} · Refreshing every {refresh_rate}s</div>',
    unsafe_allow_html=True,
)

time.sleep(refresh_rate)
st.rerun()
