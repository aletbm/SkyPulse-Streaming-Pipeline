import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
from config import (
    COUNTRY_ISO3,
    DASHBOARD_CSS,
    ISO3_TO_NUMERIC,
)
from fetchers import (
    fetch_active_routes,
    fetch_airports,
    fetch_altitude_scatter,
    fetch_continent_breakdown,
    fetch_country_flight_counts,
    fetch_flight_phase_breakdown,
    fetch_flight_trend,
    fetch_flights,
    fetch_kpis,
    fetch_risk_grid,
    fetch_seismic_by_region,
    fetch_seismic_trend,
    fetch_seismics,
    fetch_seismics_map,
    fetch_top_airlines,
    fetch_top_airports,
    fetch_top_countries,
    fetch_top_countries_full,
    fetch_weather,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SkyPulse",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙ Controls")
    show_flights = st.checkbox("✈ Flights", value=True)
    show_seismics = st.checkbox("🌍 Seismics", value=True)
    show_weather = st.checkbox("🌩 Weather", value=True)
    show_airports = st.checkbox("🛬 Airports", value=True)
    show_density = st.checkbox("🔥 Flight density", value=True)
    show_routes = st.checkbox("🛣 Active routes", value=True)
    show_tracks = st.checkbox("➤ Direction tracks", value=True)
    st.divider()

    st.markdown("### 🔍 Flight Filters")
    filter_callsign = (
        st.text_input("Callsign", placeholder="e.g. AAL, IBE...", max_chars=10)
        .upper()
        .strip()
    )

    filter_country = st.session_state.get("_filter_country", "All")
    filter_ground = st.session_state.get("_filter_ground", "All")
    filter_airline = st.session_state.get("_filter_airline", "All")

    country_options = ["All"] + st.session_state.get("_country_options", [])
    airline_options = ["All"] + st.session_state.get("_airline_options", [])

    filter_country = st.selectbox(
        "Origin country",
        country_options,
        index=country_options.index(filter_country)
        if filter_country in country_options
        else 0,
    )
    filter_ground = st.selectbox("On ground", ["All", "Airborne", "On ground"])
    filter_airline = st.selectbox(
        "Airline",
        airline_options,
        index=airline_options.index(filter_airline)
        if filter_airline in airline_options
        else 0,
    )

    st.session_state["_filter_country"] = filter_country
    st.session_state["_filter_ground"] = filter_ground
    st.session_state["_filter_airline"] = filter_airline

    st.divider()
    refresh_rate = st.select_slider(
        "Auto-refresh (seconds)",
        options=[10, 15, 30, 60, 120],
        value=15,
    )
    st.divider()
    st.markdown(
        "<div style='font-size:11px;color:#2a4a6a;font-family:monospace'>"
        "SkyPulse v1.1<br>DE Zoomcamp 2026</div>",
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

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_live, tab_analytics = st.tabs(["🌐  LIVE MAP", "📊  ANALYTICS & RANKINGS"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Live Map
# ═══════════════════════════════════════════════════════════════════════════════

with tab_live:

    @st.fragment(run_every=refresh_rate)
    def _live_data_fragment():
        try:
            kpis = fetch_kpis()
            flights = fetch_flights()
            seismics = fetch_seismics()
            seismics_map = fetch_seismics_map()
            weather = fetch_weather()
            airports = fetch_airports()
            routes = fetch_active_routes() if show_routes else pd.DataFrame()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        st.session_state["_seismics"] = seismics
        st.session_state["_seismics_map"] = seismics_map

        if not flights.empty:
            countries = sorted(flights["origin_country"].dropna().unique().tolist())
            airlines = sorted(
                [a for a in flights["airline"].dropna().unique().tolist() if a]
            )
            st.session_state["_country_options"] = countries
            st.session_state["_airline_options"] = airlines

        filtered = flights.copy() if not flights.empty else flights

        if not filtered.empty:
            if filter_callsign:
                filtered = filtered[
                    filtered["callsign"]
                    .str.upper()
                    .str.contains(filter_callsign, na=False)
                ]
            if filter_country != "All":
                filtered = filtered[filtered["origin_country"] == filter_country]
            if filter_ground == "Airborne":
                filtered = filtered[~filtered["on_ground"]]
            elif filter_ground == "On ground":
                filtered = filtered[filtered["on_ground"]]
            if filter_airline != "All":
                filtered = filtered[filtered["airline"] == filter_airline]

        _DEFAULT_VIEW_STATE = {
            "latitude": 20,
            "longitude": 0,
            "zoom": 1.8,
            "pitch": 0,
            "bearing": 0,
        }
        if filter_callsign and not filtered.empty:
            import math as _math

            lats = filtered["lat"].astype(float)
            lons = filtered["lon"].astype(float)
            center_lat = float(lats.mean())
            center_lon = float(lons.mean())
            if len(filtered) == 1:
                zoom = 8.0
            else:
                lat_range = float(lats.max() - lats.min())
                lon_range = float(lons.max() - lons.min())
                span = max(lat_range, lon_range)
                zoom = (
                    max(1.5, min(8.0, _math.log2(360 / span) - 0.5))
                    if span > 0
                    else 6.0
                )
            st.session_state["_view_state"] = {
                "latitude": center_lat,
                "longitude": center_lon,
                "zoom": zoom,
                "pitch": 0,
                "bearing": 0,
            }
        else:
            st.session_state["_view_state"] = _DEFAULT_VIEW_STATE

        airborne_count = (
            len(filtered[~filtered["on_ground"]]) if not filtered.empty else 0
        )
        risk_color = "risk" if kpis["risk_score"] > 60 else "weather"

        st.markdown(
            f"""
        <div class="kpi-grid">
            <div class="kpi-card flights">
                <div class="kpi-label">Active Flights</div>
                <div class="kpi-value flights">{len(filtered):,}</div>
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

        ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        st.markdown(
            f"""<div class="sp-ts">
            Last updated: {ts} · Auto-refreshing every {refresh_rate}s
            </div>""",
            unsafe_allow_html=True,
        )

        from map_layers import build_live_deck as _build

        base_deck = _build(
            flights=filtered,
            weather=weather,
            seismics=pd.DataFrame(),
            airports=airports,
            routes=routes,
            show_flights=show_flights,
            show_weather=show_weather,
            show_seismics=False,
            show_airports=show_airports,
            show_density=show_density,
            show_routes=show_routes,
            show_tracks=show_tracks,
        )
        st.session_state["_base_layers"] = base_deck.layers

        st.markdown(
            '<div class="section-label">GLOBAL SITUATION MAP</div>',
            unsafe_allow_html=True,
        )

        active_filters = []
        if filter_callsign:
            active_filters.append(f"✈ <b>{filter_callsign}</b>")
        if filter_country != "All":
            active_filters.append(f"🌍 {filter_country}")
        if filter_ground != "All":
            active_filters.append(
                f"{'🛬' if filter_ground == 'On ground' else '🛫'} {filter_ground}"
            )
        if filter_airline != "All":
            active_filters.append(f"🏷 {filter_airline}")
        if active_filters:
            st.markdown(
                "<div style='font-family:monospace;font-size:11px;color:#00e5ff;"
                "margin-bottom:6px;letter-spacing:1px'>FILTERS: "
                + " · ".join(active_filters)
                + "</div>",
                unsafe_allow_html=True,
            )

    _live_data_fragment()

    @st.fragment(run_every=30)
    def _seismic_pulse_fragment():
        base_layers = st.session_state.get("_base_layers", [])
        seismics = st.session_state.get("_seismics_map", pd.DataFrame())

        seismic_layers = []

        if show_seismics and not seismics.empty:
            df = seismics[["lon", "lat", "radius", "mag", "tooltip"]].copy()
            df["lon"] = df["lon"].astype(float)
            df["lat"] = df["lat"].astype(float)
            df["radius"] = df["radius"].astype(float)

            ring_configs = [
                (5.0, [255, 220, 50], [255, 200, 30], 10, 30),
                (3.5, [255, 160, 20], [255, 140, 10], 18, 55),
                (2.2, [255, 90, 10], [255, 70, 0], 30, 90),
                (1.4, [255, 30, 0], [255, 20, 0], 55, 130),
                (1.0, [255, 0, 30], [255, 50, 50], 110, 170),
            ]

            for i, (scale, fill_rgb, stroke_rgb, fill_a, stroke_a) in enumerate(
                ring_configs
            ):
                ring = df.copy()
                ring["_rr"] = (ring["radius"] * scale).astype(float)
                seismic_layers.append(
                    pdk.Layer(
                        "ScatterplotLayer",
                        id=f"seis-ring-{i}",
                        data=ring[["lon", "lat", "_rr", "tooltip"]].to_dict("records"),
                        get_position=["lon", "lat"],
                        get_radius="_rr",
                        get_fill_color=fill_rgb + [fill_a],
                        get_line_color=stroke_rgb + [stroke_a],
                        line_width_min_pixels=1,
                        stroked=True,
                        filled=True,
                        pickable=(i == len(ring_configs) - 1),
                    )
                )

        layers = seismic_layers + list(base_layers)

        _vs = st.session_state.get(
            "_view_state",
            {"latitude": 20, "longitude": 0, "zoom": 1.8, "pitch": 0, "bearing": 0},
        )
        full_deck = pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=_vs["latitude"],
                longitude=_vs["longitude"],
                zoom=_vs["zoom"],
                pitch=_vs["pitch"],
                bearing=_vs["bearing"],
            ),
            tooltip={
                "html": """
                    <div style="background-color:#080d14;color:#c8d8e8;
                    padding:10px;border:1px solid #1a2a3a;border-radius:4px;">
                        {tooltip}
                    </div>
                """,
                "style": {
                    "backgroundColor": "transparent",
                    "color": "white",
                    "zIndex": "10001",
                },
            },
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        )

        st.pydeck_chart(full_deck, width="stretch", height=520)

    _seismic_pulse_fragment()

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
            ◉ Seismic event (last 10 · size = magnitude)</span>""",
            unsafe_allow_html=True,
        )
    with leg_col4:
        st.markdown(
            """<span style='color:#7b61ff;font-size:12px;font-family:monospace'>
            ▓ Wind heatmap</span>""",
            unsafe_allow_html=True,
        )

    st.divider()

    @st.fragment(run_every=refresh_rate)
    def _live_charts_fragment():
        try:
            trend_df = fetch_flight_trend()
            seis_trend = fetch_seismic_trend()
            top_countries = fetch_top_countries()
            flights = fetch_flights()
            seismics = fetch_seismics()
        except Exception as e:
            st.error(f"Error: {e}")
            return

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
                    trend_df.set_index("window_start")[
                        ["flight_count", "airborne_count"]
                    ],
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
                    seis_trend.set_index("hour")["events"], color="#ff6b35", height=220
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
                    top_countries.rename(
                        columns={"origin_country": "Country", "flights": "✈"}
                    ),
                    hide_index=True,
                    height=220,
                    width="stretch",
                )

        st.divider()

        col_s, col_f = st.columns(2)

        with col_s:
            st.markdown(
                '<div class="section-label">LAST 10 SEISMIC EVENTS</div>',
                unsafe_allow_html=True,
            )
            if not seismics.empty:
                display_seis = seismics[
                    ["mag", "mag_class", "place", "depth", "event_time", "tsunami"]
                ].copy()
                display_seis["tsunami"] = display_seis["tsunami"].map(
                    lambda t: "⚠ YES" if t else "NO"
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
                st.dataframe(display_seis, hide_index=True, width="stretch", height=320)
            else:
                st.info("No seismic events available.")

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
                display_flights["baro_altitude"] = display_flights[
                    "baro_altitude"
                ].apply(lambda x: f"{x:.0f} m")
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
                    width="stretch",
                    height=320,
                )
            else:
                st.info("No flight data available.")

    _live_charts_fragment()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Analytics & Rankings
# ═══════════════════════════════════════════════════════════════════════════════

with tab_analytics:

    @st.fragment(run_every=60)
    def analytics_tab():
        try:
            top_countries_full = fetch_top_countries_full()
            top_airlines = fetch_top_airlines()
            top_airports = fetch_top_airports()
            seis_regions = fetch_seismic_by_region()
            phase_df = fetch_flight_phase_breakdown()
            country_flights = fetch_country_flight_counts()
        except Exception as e:
            st.error(f"Database error: {e}")
            return

        # ── Row 1: Choropleth ─────────────────────────────────────────────────
        st.markdown(
            '<div class="section-label">FLIGHTS BY COUNTRY — WORLD HEATMAP</div>',
            unsafe_allow_html=True,
        )

        if not country_flights.empty:
            choro_df = country_flights.copy()
            choro_df["iso_a3"] = choro_df["origin_country"].map(COUNTRY_ISO3)
            choro_df = choro_df.dropna(subset=["iso_a3"]).copy()
            choro_df["numeric_id"] = choro_df["iso_a3"].map(ISO3_TO_NUMERIC)
            choro_df = choro_df.dropna(subset=["numeric_id"]).copy()
            choro_df["numeric_id"] = (
                choro_df["numeric_id"].astype(int).astype(str).str.zfill(3)
            )

            countries_topo = alt.topo_feature(
                "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json",
                "countries",
            )
            background = alt.Chart(countries_topo).mark_geoshape(
                fill="#0d1825", stroke="#1a2e42", strokeWidth=0.4
            )
            foreground = (
                alt.Chart(countries_topo)
                .mark_geoshape(stroke="#1a2a3a", strokeWidth=0.5)
                .transform_lookup(
                    lookup="id",
                    from_=alt.LookupData(
                        data=choro_df,
                        key="numeric_id",
                        fields=["flights", "origin_country"],
                    ),
                )
                .encode(
                    color=alt.Color(
                        "flights:Q",
                        scale=alt.Scale(scheme="blues"),
                        legend=alt.Legend(
                            title="Flights",
                            labelColor="#c8d8e8",
                            titleColor="#4a6a8a",
                            orient="bottom-right",
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("origin_country:N", title="Country"),
                        alt.Tooltip("flights:Q", title="Flights", format=","),
                    ],
                )
                .transform_filter("datum.flights != null")
            )
            choropleth = (
                (background + foreground)
                .project("naturalEarth1")
                .properties(height=340)
                .configure_view(strokeWidth=0, fill="#080d14")
                .configure(background="#080d14")
            )
            st.altair_chart(choropleth, width="stretch")
        else:
            st.info("No flight data available for choropleth.")

        st.divider()

        # ── Row 2: Top Countries + Top Airlines ───────────────────────────────
        col_countries, col_airlines = st.columns(2)

        with col_countries:
            st.markdown(
                '<div class="section-label">TOP 25 COUNTRIES BY FLIGHTS</div>',
                unsafe_allow_html=True,
            )
            if not top_countries_full.empty:
                df_c = top_countries_full.copy()
                df_c.columns = [
                    "Country",
                    "Total",
                    "Airborne",
                    "Avg Alt (m)",
                    "Avg Speed (m/s)",
                ]
                st.dataframe(
                    df_c,
                    hide_index=True,
                    width="stretch",
                    height=420,
                    column_config={
                        "Total": st.column_config.NumberColumn(format="%d ✈"),
                        "Airborne": st.column_config.NumberColumn(format="%d ↑"),
                    },
                )
            else:
                st.info("No data.")

            if not top_countries_full.empty:
                bar_df = top_countries_full.head(15).copy()
                bar_df.columns = [
                    "country",
                    "flights",
                    "airborne",
                    "avg_alt",
                    "avg_speed",
                ]
                bar = (
                    alt.Chart(bar_df)
                    .mark_bar(
                        color="#00e5ff",
                        opacity=0.8,
                        cornerRadiusTopRight=3,
                        cornerRadiusBottomRight=3,
                    )
                    .encode(
                        x=alt.X(
                            "flights:Q",
                            title="Flights",
                            axis=alt.Axis(
                                labelColor="#4a6a8a",
                                titleColor="#4a6a8a",
                                gridColor="#1a2e42",
                            ),
                        ),
                        y=alt.Y(
                            "country:N",
                            sort="-x",
                            title=None,
                            axis=alt.Axis(labelColor="#c8d8e8"),
                        ),
                        tooltip=["country:N", "flights:Q", "airborne:Q"],
                    )
                    .properties(height=300)
                    .configure_view(strokeWidth=0, fill="#080d14")
                    .configure(background="#080d14")
                )
                st.altair_chart(bar, width="stretch")

        with col_airlines:
            st.markdown(
                '<div class="section-label">TOP 20 AIRLINES BY ACTIVE FLIGHTS</div>',
                unsafe_allow_html=True,
            )
            if not top_airlines.empty:
                df_a = top_airlines.copy()
                df_a.columns = ["Airline", "Flights", "Airborne", "Avg Alt (m)"]
                st.dataframe(
                    df_a,
                    hide_index=True,
                    width="stretch",
                    height=420,
                    column_config={
                        "Flights": st.column_config.NumberColumn(format="%d ✈"),
                        "Airborne": st.column_config.NumberColumn(format="%d ↑"),
                    },
                )
            else:
                st.info("No airline data.")

            # Flight phase donut
            if not phase_df.empty:
                st.markdown(
                    """<div class="section-label" style="margin-top:12px">
                    FLIGHT PHASE BREAKDOWN
                    </div>""",
                    unsafe_allow_html=True,
                )
                phase_colors = {
                    "cruising": "#00e5ff",
                    "climbing_descending": "#7b61ff",
                    "takeoff_landing": "#ff6b35",
                    "on_ground": "#50c878",
                    "unknown": "#2a4a6a",
                }
                phase_df["color"] = phase_df["phase"].map(
                    lambda p: phase_colors.get(p, "#2a4a6a")
                )
                donut = (
                    alt.Chart(phase_df)
                    .mark_arc(innerRadius=60, outerRadius=100)
                    .encode(
                        theta=alt.Theta("flights:Q"),
                        color=alt.Color(
                            "phase:N",
                            scale=alt.Scale(
                                domain=list(phase_colors.keys()),
                                range=list(phase_colors.values()),
                            ),
                            legend=alt.Legend(
                                title=None, labelColor="#c8d8e8", orient="right"
                            ),
                        ),
                        tooltip=["phase:N", "flights:Q"],
                    )
                    .properties(height=220)
                    .configure_view(strokeWidth=0, fill="#080d14")
                    .configure(background="#080d14")
                )
                st.altair_chart(donut, width="stretch")

        st.divider()

        # ── Row 3: Top Airports + Seismic by Region ───────────────────────────
        col_ap, col_seis = st.columns(2)

        with col_ap:
            st.markdown(
                '<div class="section-label">TOP 20 AIRPORTS BY NEARBY FLIGHTS</div>',
                unsafe_allow_html=True,
            )
            if not top_airports.empty:
                df_ap = top_airports.copy()
                df_ap.columns = ["Airport", "IATA", "City", "Nearby Flights"]
                st.dataframe(
                    df_ap,
                    hide_index=True,
                    width="stretch",
                    height=520,
                    column_config={
                        "Nearby Flights": st.column_config.ProgressColumn(
                            format="%d",
                            min_value=0,
                            max_value=int(df_ap["Nearby Flights"].max())
                            if not df_ap.empty
                            else 100,
                        ),
                    },
                )
            else:
                st.info("No airport data. Run int_flights_enriched first.")

        with col_seis:
            st.markdown(
                '<div class="section-label">SEISMIC HOT ZONES — LAST 24 HOURS</div>',
                unsafe_allow_html=True,
            )
            if not seis_regions.empty:
                df_sr = seis_regions.copy()
                df_sr.columns = ["Region", "Events", "Max Mag", "Avg Mag", "Tsunami"]
                df_sr["Tsunami"] = df_sr["Tsunami"].apply(
                    lambda t: "⚠ YES" if t and int(t) > 0 else "NO"
                )
                st.dataframe(
                    df_sr,
                    hide_index=True,
                    width="stretch",
                    height=380,
                    column_config={
                        "Max Mag": st.column_config.NumberColumn(format="%.1f 🔴"),
                        "Events": st.column_config.ProgressColumn(
                            format="%d",
                            min_value=0,
                            max_value=int(df_sr["Events"].max())
                            if not df_sr.empty
                            else 10,
                        ),
                    },
                )

                bubble = (
                    alt.Chart(seis_regions.head(15))
                    .mark_circle()
                    .encode(
                        x=alt.X(
                            "events:Q",
                            title="Event count",
                            axis=alt.Axis(
                                labelColor="#4a6a8a",
                                titleColor="#4a6a8a",
                                gridColor="#1a2e42",
                            ),
                        ),
                        y=alt.Y(
                            "max_mag:Q",
                            title="Max magnitude",
                            axis=alt.Axis(
                                labelColor="#4a6a8a",
                                titleColor="#4a6a8a",
                                gridColor="#1a2e42",
                            ),
                        ),
                        size=alt.Size("events:Q", legend=None),
                        color=alt.Color(
                            "max_mag:Q",
                            scale=alt.Scale(scheme="orangered", domain=[0, 8]),
                            legend=None,
                        ),
                        tooltip=["region:N", "events:Q", "max_mag:Q", "avg_mag:Q"],
                    )
                    .properties(height=130)
                    .configure_view(strokeWidth=0, fill="#080d14")
                    .configure(background="#080d14")
                )
                st.altair_chart(bubble, width="stretch")
            else:
                st.info("No seismic data in the last 24 hours.")

        st.divider()

        # ── Row 4: Risk Grid + Continent Breakdown ────────────────────────────
        col_risk, col_continent = st.columns([3, 2])

        with col_risk:
            st.markdown(
                '<div class="section-label">RISK SCORE BY GRID CELL</div>',
                unsafe_allow_html=True,
            )
            try:
                risk_df = fetch_risk_grid()
            except Exception:
                risk_df = pd.DataFrame()

            if not risk_df.empty:
                risk_df["risk_score"] = pd.to_numeric(
                    risk_df["risk_score"], errors="coerce"
                )
                risk_df["flight_count"] = pd.to_numeric(
                    risk_df["flight_count"], errors="coerce"
                )
                risk_chart = (
                    alt.Chart(risk_df)
                    .mark_circle()
                    .encode(
                        x=alt.X(
                            "grid_lon:Q",
                            title="Longitude",
                            scale=alt.Scale(domain=[-180, 180]),
                            axis=alt.Axis(
                                labelColor="#4a6a8a",
                                titleColor="#4a6a8a",
                                gridColor="#1a2e42",
                            ),
                        ),
                        y=alt.Y(
                            "grid_lat:Q",
                            title="Latitude",
                            scale=alt.Scale(domain=[-90, 90]),
                            axis=alt.Axis(
                                labelColor="#4a6a8a",
                                titleColor="#4a6a8a",
                                gridColor="#1a2e42",
                            ),
                        ),
                        size=alt.Size(
                            "flight_count:Q",
                            scale=alt.Scale(range=[20, 400]),
                            legend=None,
                        ),
                        color=alt.Color(
                            "risk_score:Q",
                            scale=alt.Scale(scheme="orangered", domain=[0, 100]),
                            legend=alt.Legend(
                                title="Risk",
                                labelColor="#c8d8e8",
                                titleColor="#4a6a8a",
                                orient="right",
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("city:N", title="City"),
                            alt.Tooltip("continent:N", title="Continent"),
                            alt.Tooltip(
                                "risk_score:Q", title="Risk Score", format=".0f"
                            ),
                            alt.Tooltip("flight_count:Q", title="Flights", format=","),
                            alt.Tooltip("wind_severity:N", title="Wind"),
                            alt.Tooltip("max_mag_24h:Q", title="Max Mag", format=".1f"),
                        ],
                    )
                    .properties(height=340)
                    .configure_view(strokeWidth=0, fill="#080d14")
                    .configure(background="#080d14")
                )
                st.altair_chart(risk_chart, width="stretch")
            else:
                st.info("No risk data available. mart_flight_context may be empty.")

        with col_continent:
            st.markdown(
                '<div class="section-label">FLIGHTS BY CONTINENT</div>',
                unsafe_allow_html=True,
            )
            try:
                continent_df = fetch_continent_breakdown()
            except Exception:
                continent_df = pd.DataFrame()

            if not continent_df.empty:
                continent_colors = {
                    "North America": "#00e5ff",
                    "Europe": "#7b61ff",
                    "Asia": "#ff6b35",
                    "South America": "#00ff88",
                    "Africa": "#ffdc00",
                    "Oceania": "#ff0055",
                    "Other": "#2a4a6a",
                }
                donut_c = (
                    alt.Chart(continent_df)
                    .mark_arc(innerRadius=55, outerRadius=105)
                    .encode(
                        theta=alt.Theta("flights:Q"),
                        color=alt.Color(
                            "continent:N",
                            scale=alt.Scale(
                                domain=list(continent_colors.keys()),
                                range=list(continent_colors.values()),
                            ),
                            legend=alt.Legend(
                                title=None,
                                labelColor="#c8d8e8",
                                orient="bottom",
                                columns=2,
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("continent:N", title="Continent"),
                            alt.Tooltip("flights:Q", title="Flights", format=","),
                            alt.Tooltip("airborne:Q", title="Airborne", format=","),
                            alt.Tooltip(
                                "avg_alt_m:Q", title="Avg Alt (m)", format=".0f"
                            ),
                        ],
                    )
                    .properties(height=260)
                    .configure_view(strokeWidth=0, fill="#080d14")
                    .configure(background="#080d14")
                )
                st.altair_chart(donut_c, width="stretch")
                ct = continent_df[["continent", "flights", "airborne"]].copy()
                ct.columns = ["Continent", "Total", "Airborne"]
                st.dataframe(ct, hide_index=True, width="stretch", height=200)
            else:
                st.info("No continent data.")

        st.divider()

        # ── Row 5: Altitude vs Speed scatter ──────────────────────────────────
        st.markdown(
            '<div class="section-label">ALTITUDE vs SPEED — BY FLIGHT PHASE</div>',
            unsafe_allow_html=True,
        )
        try:
            scatter_df = fetch_altitude_scatter()
        except Exception:
            scatter_df = pd.DataFrame()

        if not scatter_df.empty:
            scatter_df["altitude_m"] = pd.to_numeric(
                scatter_df["altitude_m"], errors="coerce"
            )
            scatter_df["speed_ms"] = pd.to_numeric(
                scatter_df["speed_ms"], errors="coerce"
            )
            scatter_df = scatter_df.dropna(subset=["altitude_m", "speed_ms"])
            phase_colors_s = {
                "cruising": "#00e5ff",
                "climbing_descending": "#7b61ff",
                "takeoff_landing": "#ff6b35",
                "on_ground": "#50c878",
            }
            scatter = (
                alt.Chart(scatter_df.sample(min(800, len(scatter_df))))
                .mark_circle(size=18, opacity=0.55)
                .encode(
                    x=alt.X(
                        "speed_ms:Q",
                        title="Speed (m/s)",
                        scale=alt.Scale(domain=[0, 350]),
                        axis=alt.Axis(
                            labelColor="#4a6a8a",
                            titleColor="#4a6a8a",
                            gridColor="#1a2e42",
                        ),
                    ),
                    y=alt.Y(
                        "altitude_m:Q",
                        title="Altitude (m)",
                        scale=alt.Scale(domain=[0, 14000]),
                        axis=alt.Axis(
                            labelColor="#4a6a8a",
                            titleColor="#4a6a8a",
                            gridColor="#1a2e42",
                        ),
                    ),
                    color=alt.Color(
                        "flight_phase:N",
                        scale=alt.Scale(
                            domain=list(phase_colors_s.keys()),
                            range=list(phase_colors_s.values()),
                        ),
                        legend=alt.Legend(
                            title="Phase",
                            labelColor="#c8d8e8",
                            titleColor="#4a6a8a",
                            orient="right",
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("flight_phase:N", title="Phase"),
                        alt.Tooltip("altitude_m:Q", title="Altitude (m)", format=".0f"),
                        alt.Tooltip("speed_ms:Q", title="Speed (m/s)", format=".1f"),
                        alt.Tooltip("origin_country:N", title="Country"),
                    ],
                )
                .properties(height=320)
                .configure_view(strokeWidth=0, fill="#080d14")
                .configure(background="#080d14")
            )
            st.altair_chart(scatter, width="stretch")
        else:
            st.info("No flight data for scatter.")

        ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        st.markdown(
            f"""<div class="sp-ts">
            Analytics updated: {ts} · Auto-refreshing every 60s
            </div>""",
            unsafe_allow_html=True,
        )

    analytics_tab()
